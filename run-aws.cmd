@echo off
rem Fetch fresh AWS Academy sandbox credentials into .env and start ChargePulse.
rem   run-aws.cmd [--no-run] [--show] [--clipboard] [--no-start] [--url <lab link>]
setlocal
set "TOOL=%~dp0tools\aws-sandbox"
if not exist "%TOOL%\node_modules\playwright-core" (
  echo [aws-sandbox] Installing helper dependencies ^(one time^)...
  call npm install --prefix "%TOOL%" --no-fund --no-audit --loglevel=error || exit /b 1
)
node "%TOOL%\sandbox.mjs" %*
exit /b %ERRORLEVEL%
