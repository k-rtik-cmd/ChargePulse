#!/usr/bin/env node
// Fetch the current AWS Academy sandbox credentials, write them into the
// ChargePulse .env (switching the stack to real S3), and start the stack.
//
//   run-aws.cmd                 fetch creds (starts the lab if needed), update .env, docker compose up
//   run-aws.cmd --no-run        only refresh credentials
//   run-aws.cmd --show          always show the browser window
//   run-aws.cmd --clipboard     skip the browser: read the [default] block you copied from "AWS Details"
//   run-aws.cmd --no-start      never press "Start Lab" (fail instead if the lab is stopped)
//   run-aws.cmd --url <link>    use a different lab link
//   run-aws.cmd --brave         reuse the AWS Academy login from your Brave browser
//                               (close Brave first; its session is copied, Brave itself is untouched)
//
// The browser uses its own Edge profile under %LOCALAPPDATA%\chargepulse-aws,
// so you log in to AWS Academy once and later runs reuse that session.
// With --brave, a copy of your Brave session (cookies, site storage,
// preferences; no history, caches or passwords) is driven by brave.exe instead.

import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';

const HERE = path.dirname(fileURLToPath(import.meta.url));
const REPO = path.resolve(HERE, '..', '..');
const ENV_FILE = path.join(REPO, '.env');
const MINIO_BACKUP = path.join(REPO, '.env.minio.backup');
const DEFAULT_URL = 'https://awsacademy.instructure.com/courses/180742/modules/items/17793774';
const LOCAL = process.env.LOCALAPPDATA || os.homedir();
const EDGE_PROFILE_DIR = path.join(LOCAL, 'chargepulse-aws', 'edge-profile');
const BRAVE_PROFILE_DIR = path.join(LOCAL, 'chargepulse-aws', 'brave-profile');
const BRAVE_USER_DATA = path.join(LOCAL, 'BraveSoftware', 'Brave-Browser', 'User Data');
const BRAVE_EXE_CANDIDATES = [
  path.join(process.env.ProgramFiles || 'C:\\Program Files', 'BraveSoftware', 'Brave-Browser', 'Application', 'brave.exe'),
  path.join(process.env['ProgramFiles(x86)'] || 'C:\\Program Files (x86)', 'BraveSoftware', 'Brave-Browser', 'Application', 'brave.exe'),
  path.join(LOCAL, 'BraveSoftware', 'Brave-Browser', 'Application', 'brave.exe'),
];
// Only what a login needs; caches, history and saved passwords are not copied.
const BRAVE_SESSION_PARTS = [
  'Network', 'Local Storage', 'Session Storage', 'Preferences', 'Secure Preferences', 'Web Data',
];
const FAILURE_SHOT = path.join(HERE, 'last-failure.png');

const CREDS_RE = /aws_access_key_id\s*=\s*([A-Z0-9]{16,})\s+aws_secret_access_key\s*=\s*(\S+)\s+aws_session_token\s*=\s*(\S+)/i;

// ------------------------------------------------------------------ args
const argv = process.argv.slice(2);
const flag = (name) => argv.includes(name);
const opt = (name, dflt) => {
  const i = argv.indexOf(name);
  return i >= 0 && argv[i + 1] ? argv[i + 1] : dflt;
};
if (flag('-h') || flag('--help')) {
  const lines = fs.readFileSync(fileURLToPath(import.meta.url), 'utf8').split(/\r?\n/).slice(1);
  const header = lines.slice(0, lines.findIndex((l) => !l.startsWith('//')));
  console.log(header.map((l) => l.replace(/^\/\/ ?/, '')).join('\n'));
  process.exit(0);
}
const LAB_URL = opt('--url', process.env.CHARGEPULSE_LAB_URL || DEFAULT_URL);
const ALWAYS_SHOW = flag('--show');
const NO_RUN = flag('--no-run');
const NO_START = flag('--no-start');
const FROM_CLIPBOARD = flag('--clipboard');
const USE_BRAVE = flag('--brave') || /^brave$/i.test(process.env.CHARGEPULSE_BROWSER || '');
const PROFILE_DIR = USE_BRAVE ? BRAVE_PROFILE_DIR : EDGE_PROFILE_DIR;

const log = (...a) => console.log('[aws-sandbox]', ...a);
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function parseCreds(text) {
  const m = CREDS_RE.exec(text || '');
  if (!m) return null;
  return { accessKeyId: m[1], secretAccessKey: m[2], sessionToken: m[3] };
}

function parseSessionInfo(text) {
  const remaining = /Remaining session time:\s*([\d:]+)/i.exec(text)?.[1];
  const endsAt = /Session to end\s*at:\s*(\S+)/i.exec(text)?.[1];
  return { remaining, endsAt };
}

// ------------------------------------------------------------- verify
async function verify(creds, region) {
  const { STSClient, GetCallerIdentityCommand } = await import('@aws-sdk/client-sts');
  const sts = new STSClient({ region, credentials: creds, maxAttempts: 1 });
  try {
    const id = await sts.send(new GetCallerIdentityCommand({}));
    return { ok: true, arn: id.Arn };
  } catch (e) {
    return { ok: false, error: `${e.name}: ${e.message}` };
  }
}

// ------------------------------------------------------------- browser
async function allFrameText(page) {
  let out = '';
  for (const f of page.frames()) {
    try {
      out += '\n' + (await f.evaluate(() => (document.body ? document.body.innerText : '')));
    } catch {
      /* frame detached or navigating */
    }
  }
  return out;
}

async function clickText(page, text, { exact = true } = {}) {
  for (const f of page.frames()) {
    try {
      const loc = f.getByText(text, { exact }).filter({ visible: true }).first();
      if ((await loc.count()) > 0) {
        await loc.click({ timeout: 5000 });
        return true;
      }
    } catch {
      /* try next frame */
    }
  }
  return false;
}

async function findLabFrame(page, timeoutMs) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const text = await allFrameText(page);
    if (/AWS Details|Start Lab/i.test(text)) return text;
    await sleep(1500);
  }
  return null;
}

// Look for credentials in the page, opening the "AWS Details" panel if needed.
async function scanForCreds(page, rejectKeyId) {
  let text = await allFrameText(page);
  let c = parseCreds(text);
  if (c && c.accessKeyId !== rejectKeyId) return { creds: c, text };

  if (!/Cloud Access|AWS CLI:/i.test(text)) {
    await clickText(page, 'AWS Details', { exact: false });
    await sleep(2500);
    text = await allFrameText(page);
  }
  c = parseCreds(text);
  if (c && c.accessKeyId !== rejectKeyId) return { creds: c, text };

  // Some lab layouts hide the CLI block behind a "Show" button.
  if (/AWS CLI:/i.test(text) && (await clickText(page, 'Show'))) {
    await sleep(1500);
    text = await allFrameText(page);
    c = parseCreds(text);
    if (c && c.accessKeyId !== rejectKeyId) return { creds: c, text };
  }
  return { creds: null, text };
}

function braveExe() {
  const exe = BRAVE_EXE_CANDIDATES.find((p) => fs.existsSync(p));
  if (!exe) throw new Error(`brave.exe not found (looked in: ${BRAVE_EXE_CANDIDATES.join(', ')})`);
  return exe;
}

// Copy the login-relevant parts of Brave's Default profile into our own
// user-data dir. Brave encrypts cookies with a key bound to brave.exe, so the
// copy is only readable when brave.exe itself is launched on it. Chromium
// refuses automation of the real profile dir, hence the copy.
function seedBraveProfile() {
  const srcDefault = path.join(BRAVE_USER_DATA, 'Default');
  if (!fs.existsSync(srcDefault)) throw new Error(`No Brave profile at ${srcDefault}`);
  const cookies = path.join(srcDefault, 'Network', 'Cookies');
  try {
    fs.closeSync(fs.openSync(cookies, 'r'));
  } catch (e) {
    if (['EBUSY', 'EPERM', 'EACCES'].includes(e.code)) {
      if (fs.existsSync(path.join(BRAVE_PROFILE_DIR, 'Default'))) {
        log('Brave is running, so reusing the Brave session copied on an earlier run.');
        return;
      }
      throw new Error(
        'Brave is still running, so its session cannot be copied. Close every Brave window ' +
          '(and quit it from the system tray if "continue running in background" is on), then rerun with --brave.',
      );
    }
    throw e;
  }
  const dstDefault = path.join(BRAVE_PROFILE_DIR, 'Default');
  fs.mkdirSync(dstDefault, { recursive: true });
  fs.copyFileSync(path.join(BRAVE_USER_DATA, 'Local State'), path.join(BRAVE_PROFILE_DIR, 'Local State'));
  for (const part of BRAVE_SESSION_PARTS) {
    const src = path.join(srcDefault, part);
    if (!fs.existsSync(src)) continue;
    const dst = path.join(dstDefault, part);
    fs.rmSync(dst, { recursive: true, force: true });
    fs.cpSync(src, dst, { recursive: true });
  }
  log('Copied the Brave session (cookies, site storage, preferences) into a private profile.');
}

async function browserFetch({ headed, region }) {
  const { chromium } = await import('playwright-core');
  fs.mkdirSync(PROFILE_DIR, { recursive: true });
  const ctx = await chromium.launchPersistentContext(PROFILE_DIR, {
    ...(USE_BRAVE ? { executablePath: braveExe() } : { channel: 'msedge' }),
    headless: !headed,
    viewport: { width: 1400, height: 900 },
  });
  let page = ctx.pages()[0] ?? (await ctx.newPage());
  try {
    log(`Opening lab page${headed ? '' : ' (in the background)'}...`);
    await page.goto(LAB_URL, { waitUntil: 'domcontentloaded', timeout: 60000 });

    const onLogin = () => /\/login|\/saml|signin|sso/i.test(page.url()) || !page.url().includes('/courses/');
    if (onLogin()) {
      if (!headed) return { needHeaded: 'login required' };
      log('Please log in to AWS Academy in the browser window (only needed once). Waiting up to 5 minutes...');
      // The sign-in may open a popup or replace the tab, so watch every page
      // in the window instead of only the first one.
      const labHost = new URL(LAB_URL).hostname;
      const loginDeadline = Date.now() + 300000;
      for (;;) {
        const open = ctx.pages().filter((p) => !p.isClosed());
        if (open.length === 0) throw new Error('The browser window was closed before the credentials were read.');
        const done = open.find((p) => {
          try {
            const u = new URL(p.url());
            return u.hostname === labHost && !/\/login/i.test(u.pathname);
          } catch {
            return false;
          }
        });
        if (done) {
          page = done;
          break;
        }
        if (Date.now() > loginDeadline) throw new Error('Timed out after 5 minutes waiting for the AWS Academy login.');
        await sleep(1000);
      }
      if (!page.url().startsWith(LAB_URL)) await page.goto(LAB_URL, { waitUntil: 'domcontentloaded' });
      if (onLogin()) throw new Error('Still not logged in after waiting. Rerun with --show and log in.');
    }

    const labText = await findLabFrame(page, 60000);
    if (!labText) {
      if (!headed) return { needHeaded: 'lab panel not found' };
      throw new Error('Could not find the lab panel (Start Lab / AWS Details) on the page.');
    }

    // 1) Credentials already available?
    let { creds, text } = await scanForCreds(page);
    let staleKey = null;
    if (creds) {
      const v = await verify(creds, region);
      if (v.ok) return { creds, info: parseSessionInfo(text), arn: v.arn };
      log(`Credentials on the page are not valid (${v.error}) - the lab session has probably ended.`);
      staleKey = creds.accessKeyId;
      creds = null;
    }

    // 2) Start the lab and wait for fresh credentials.
    if (NO_START) throw new Error('Lab is not running and --no-start was given. Press "Start Lab" and rerun.');
    log('Starting the lab (this usually takes 1-3 minutes)...');
    if (!(await clickText(page, 'Start Lab', { exact: false }))) {
      throw new Error('Could not find the "Start Lab" button.');
    }
    const deadline = Date.now() + 8 * 60 * 1000;
    while (Date.now() < deadline) {
      await sleep(5000);
      ({ creds, text } = await scanForCreds(page, staleKey));
      if (!creds) {
        // The lab (or its AWS Details panel) may have opened in another tab, or
        // you opened it yourself; read every tab in the window, without clicking.
        for (const p of ctx.pages()) {
          if (p === page || p.isClosed()) continue;
          const t = await allFrameText(p);
          const c = parseCreds(t);
          if (c && c.accessKeyId !== staleKey) {
            ({ creds, text } = { creds: c, text: t });
            break;
          }
        }
      }
      if (!creds) {
        process.stdout.write('.');
        continue;
      }
      // New keys can take a few seconds to become active.
      for (let i = 0; i < 12; i++) {
        const v = await verify(creds, region);
        if (v.ok) {
          process.stdout.write('\n');
          return { creds, info: parseSessionInfo(text), arn: v.arn };
        }
        await sleep(5000);
      }
      staleKey = creds.accessKeyId;
    }
    throw new Error('Timed out waiting for the lab to start and show credentials.');
  } catch (e) {
    try {
      await page.screenshot({ path: FAILURE_SHOT, fullPage: true });
      log(`Saved a screenshot of the page to ${FAILURE_SHOT}`);
    } catch {}
    throw e;
  } finally {
    await ctx.close().catch(() => {});
  }
}

// ------------------------------------------------------------- clipboard
function clipboardFetch() {
  const r = spawnSync('powershell', ['-NoProfile', '-Command', 'Get-Clipboard -Raw'], { encoding: 'utf8' });
  const creds = parseCreds(r.stdout);
  if (!creds) {
    throw new Error('Clipboard does not contain an aws_access_key_id / aws_secret_access_key / aws_session_token block. Copy it from "AWS Details" first.');
  }
  return { creds, info: {} };
}

// ------------------------------------------------------------- files
function readEnv() {
  if (!fs.existsSync(ENV_FILE)) {
    const example = path.join(REPO, '.env.example');
    if (!fs.existsSync(example)) throw new Error(`No .env or .env.example in ${REPO}`);
    fs.copyFileSync(example, ENV_FILE);
    log('Created .env from .env.example');
  }
  return fs.readFileSync(ENV_FILE, 'utf8');
}

function envValue(text, key) {
  const m = new RegExp(`^${key}=(.*)$`, 'm').exec(text);
  return m ? m[1].replace(/\s+#.*$/, '').trim() : undefined;
}

function writeEnv(text, updates) {
  const eol = text.includes('\r\n') ? '\r\n' : '\n';
  const lines = text.split(/\r?\n/);
  const done = new Set();
  for (let i = 0; i < lines.length; i++) {
    const m = /^([A-Za-z_][A-Za-z0-9_]*)=/.exec(lines[i]);
    if (m && m[1] in updates) {
      lines[i] = `${m[1]}=${updates[m[1]]}`;
      done.add(m[1]);
    }
  }
  const missing = Object.keys(updates).filter((k) => !done.has(k));
  if (missing.length) {
    if (lines[lines.length - 1] === '') lines.pop();
    lines.push('', '# Added by tools/aws-sandbox', ...missing.map((k) => `${k}=${updates[k]}`), '');
  }
  const tmp = ENV_FILE + '.tmp';
  fs.writeFileSync(tmp, lines.join(eol));
  fs.renameSync(tmp, ENV_FILE);
}

function writeAwsCredentials(creds, region) {
  const dir = path.join(os.homedir(), '.aws');
  fs.mkdirSync(dir, { recursive: true });
  const file = path.join(dir, 'credentials');
  const block = `[default]\naws_access_key_id=${creds.accessKeyId}\naws_secret_access_key=${creds.secretAccessKey}\naws_session_token=${creds.sessionToken}\n`;
  let text = fs.existsSync(file) ? fs.readFileSync(file, 'utf8').replace(/\r\n/g, '\n') : '';
  const re = /^\[default\]\n(?:(?!\[).*\n?)*/m;
  text = re.test(text) ? text.replace(re, block + '\n') : block + (text ? '\n' + text : '');
  fs.writeFileSync(file, text.trimEnd() + '\n');

  const cfg = path.join(dir, 'config');
  if (!fs.existsSync(cfg)) fs.writeFileSync(cfg, `[default]\nregion=${region}\n`);
}

// ------------------------------------------------------------- docker
function sh(cmd, args, opts = {}) {
  return spawnSync(cmd, args, { cwd: REPO, stdio: 'inherit', shell: false, ...opts });
}

function runStack(envOverrides, wasLocalS3) {
  const probe = spawnSync('docker', ['info', '--format', '{{.ServerVersion}}'], { encoding: 'utf8' });
  if (probe.status !== 0) throw new Error('Docker is not running. Start Docker Desktop and rerun (the .env is already updated).');

  // Make sure stale values from the calling shell can't override the new .env.
  const env = { ...process.env, ...envOverrides };

  if (wasLocalS3) {
    log('Stopping the local MinIO container (now using real S3)...');
    sh('docker', ['compose', '--profile', 'local-s3', 'stop', 'minio'], { env, stdio: 'ignore' });
  }
  log('docker compose up -d --build');
  const r = sh('docker', ['compose', 'up', '-d', '--build'], { env });
  if (r.status !== 0) throw new Error(`docker compose up failed (exit ${r.status}).`);
  sh('docker', ['compose', 'ps'], { env });
}

// ------------------------------------------------------------- main
async function main() {
  const envText = readEnv();
  const region = envValue(envText, 'AWS_DEFAULT_REGION') || 'us-east-1';

  let result;
  if (FROM_CLIPBOARD) {
    result = clipboardFetch();
    const v = await verify(result.creds, region);
    if (!v.ok) throw new Error(`Copied credentials are not valid: ${v.error}`);
    result.arn = v.arn;
  } else {
    if (USE_BRAVE) seedBraveProfile();
    result = await browserFetch({ headed: ALWAYS_SHOW, region });
    if (result.needHeaded) {
      log(`Opening a visible browser window (${result.needHeaded})...`);
      result = await browserFetch({ headed: true, region });
    }
  }
  const { creds, info, arn } = result;
  log(`Credentials OK: ${arn}`);
  if (info?.remaining || info?.endsAt) {
    log(`Lab session: ${info.remaining ?? '?'} remaining, ends at ${info.endsAt ?? '?'} - rerun this when it expires.`);
  }

  // Keep a copy of the local-MinIO .env the first time we switch it to AWS.
  const wasLocalS3 = /local-s3/.test(envValue(envText, 'COMPOSE_PROFILES') || '');
  if (wasLocalS3 && !fs.existsSync(MINIO_BACKUP)) {
    fs.copyFileSync(ENV_FILE, MINIO_BACKUP);
    log(`Saved your local-MinIO settings to ${path.basename(MINIO_BACKUP)} (copy it back over .env to return to local mode).`);
  }

  const updates = {
    AWS_ACCESS_KEY_ID: creds.accessKeyId,
    AWS_SECRET_ACCESS_KEY: creds.secretAccessKey,
    AWS_SESSION_TOKEN: creds.sessionToken,
    AWS_DEFAULT_REGION: region,
    S3_ENDPOINT_URL: '',
    COMPOSE_PROFILES: '',
  };
  writeEnv(envText, updates);
  writeAwsCredentials(creds, region);
  log(`Updated ${ENV_FILE} and ~/.aws/credentials [default].`);

  if (NO_RUN) return;
  runStack(updates, wasLocalS3);
  log('ChargePulse is up. Grafana: http://localhost:3000');
}

main().catch((e) => {
  if (/has been closed/i.test(e.message)) {
    e.message = 'The browser window was closed before the credentials were read. Rerun and keep it open until "Credentials OK".';
  }
  console.error(`\n[aws-sandbox] ERROR: ${e.message}`);
  process.exit(1);
});
