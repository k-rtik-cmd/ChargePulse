ChargePulse ⚡

Lambda Architecture for Real-Time EV Charging Analytics and Cost Reconciliation
Phase 1 --- Proposed System Design

Submitted by: Laksh Sharma and Kartik Singh

Course: Big Data Analytics with Hadoop and Spark (CS1243)

Submitted to: Deepika Prakash Ma'am

Date: 31 October 2026

📌 Project Overview


Problem Statement

Operators of electric-vehicle (EV) charging networks need to understand
station utilisation and charging expenditure as charging sessions occur.
Two practical challenges arise:

1. Operators need timely visibility into energy consumption and
   estimated charging costs so unusual usage or spending can be noticed
   quickly.
2. Final station settlement or billing reports may arrive after the
   charging activity, requiring accurate station-wise and session-wise
   reconciliation of estimated costs against reported amounts.
   
ChargePulse proposes a data engineering pipeline that processes a
continuous stream of simulated charging activity, retains an immutable
raw history, and combines fast operational estimates with authoritative
historical recomputation.

Objectives

- Track charging activity, energy consumption, and estimated costs by
  charging station, connector, and charging session.
- Monitor live station consumption and trigger configurable alerts
  when energy or cost thresholds are crossed.
- Generate accurate hourly and daily charging summaries by removing
  duplicate events and incorporating late-arriving settlement records.
- Reconcile calculated session costs with reported billed energy and
  settlement amounts.
- Display current consumption, estimated expenditure, station
  utilisation, and reconciled historical results on a single
  dashboard.
  
Design Assumptions

- Default input rate: 50 data points per second.
- Settlement reports may arrive a few seconds after the corresponding
  charging events.
- Streaming uses approximately one-second processing windows for
  operational visibility.
- Speed-layer output files are written at one-minute intervals.
- Alerts are expected within a few seconds.
- At 50 events/second, the generator would emit approximately 4.32
  million events in 24 hours (50 × 60 × 60 × 24). This is a
  derived estimate, not a fixed dataset size.
- Event payload size and S3 archive retention duration will be
  measured or configured during implementation.
- Kafka event retention is seven days.
  
🏗️ Architecture Design

Why Lambda Architecture?

ChargePulse uses Lambda architecture because it requires two
different processing behaviours:

- Speed layer: prioritises low-latency estimates for active
  charging activity.
- Batch layer: prioritises accurate and repeatable historical
  results, including late settlement records and corrections.
  
A Kappa design would require replaying the event log to apply historical
corrections. The Lambda design instead maintains an append-only S3
master archive and recomputes affected hourly batch views.

High-Level Data Flow

1. A Python producer generates charging-session events and station
   settlement reports.
2. Records are validated and written to Amazon S3 first.
3. After a successful S3 write, events are published to Apache
   Kafka for the speed-processing path.
4. Spark Structured Streaming consumes Kafka events and produces
   low-latency energy and estimated-cost aggregates.
5. PySpark batch processing reads the S3 archive, removes
   duplicates, joins settlement reports, applies effective-dated tariff
   rules, and recomputes hourly aggregates.
6. Processed speed, batch, and metadata results are stored in separate
   PostgreSQL tables.
7. A merged SQL view prevents overlap between batch and speed results.
8. Grafana reads the merged view from PostgreSQL to display live
   and historical analytics.
   
Processing Layers
  Layer                               Design and Responsibility
  Batch                           PySpark reads raw S3 data, removes
duplicate event IDs, joins
settlement reports by session ID,
applies effective-dated tariff
rules, and recomputes hourly
aggregates. Results are written to
batch/; processing watermarks are
stored in meta/.

  Speed                           Spark Structured Streaming consumes
Kafka, maintains running totals by
station, and computes approximately
one-second windows for energy and
estimated-cost calculations.
Sliding windows help detect unusual
consumption or cost spikes. Speed
outputs are written to speed/
every minute.

  Serving                         PostgreSQL stores batch, speed, and
metadata results in separate
tables. A merged SQL view uses
batch results for completed periods
and speed results only for the
uncovered interval.
Visualisation                   Grafana reads merged_view through
its native PostgreSQL data source
and displays live operational and
historical analytics.
Master Archive and Storage Tiers
PostgreSQL is a derived serving store. Amazon S3 remains the
append-only master archive and source of truth, allowing the serving
database to be rebuilt from archived data if required.
  S3 Prefix                           Contents and Write Behaviour
  raw/                              Append-only Parquet event files
partitioned by year, month, day,
and hour. Minute and second remain
columns.

  batch/                            Authoritative hourly aggregates.
The relevant hour is overwritten
when recomputed.

  speed/                            Approximate streaming aggregates,
with one output file per minute.

  meta/                             Processing watermarks and the
latest completed batch boundary,
stored per station or processing
key.
dead-letter/                      Rejected events, stored separately
and never overwritten.

📥 Data Sources and Event Contract

  Source                  Format / Type           Proposed Columns
  Charging Session      JSON; semi-structured   event_id,
Events                                        session_id,
event_ts,
station_id,
connector_id,
vehicle_id,
energy_delta_kwh,
power_kw,
session_status

  Station Settlement    JSON; semi-structured   report_id,
Reports                                       session_id,
billed_energy_kwh,
billed_amount,
settlement_status,
reported_at
Tariff / Station      CSV; structured and     station_id,
Policy                effective-dated         tariff_id,
price_per_kwh,
service_fee,
alert_threshold_kwh,
effective_from,
effective_to

Key Data Rules

- event_id is the deduplication key.
- session_id links charging events to their later settlement report.
- event_ts records when the charging activity occurred, not when the
  producer or processor received it.
- Energy consumption is represented numerically in kWh.
- Estimated cost is derived from the effective-dated tariff table
  rather than treated as an immutable property of the raw event.
- A tariff change is applied by recomputing the affected batch period,
  not by rewriting raw history.
  
Validation and Idempotency

Every record is checked against an explicit schema in a shared Python
module. Invalid records---such as records with a missing event ID,
negative energy delta, invalid timestamp, unknown station, or malformed
settlement amount---are routed to dead-letter/ and counted for
dashboard visibility.


Idempotency is enforced in two stages:

1. Deduplication on load: PySpark's dropDuplicates(event_id)
   removes duplicate charging events within the processed dataset.
2. Watermarks and partition overwrite: The batch job reads the
   stored maximum event timestamp from batch metadata, processes newly
   arrived or re-queued late data, and overwrites the affected hourly
   partition instead of appending a second copy. The watermark strategy
   permits controlled recomputation when a late settlement report
   arrives.
   
The producer supports configurable duplicate and late/out-of-order event
injection so these behaviours can be demonstrated and tested.

🧰 Technology Stack and Justification

  Technology                          Role and Justification
  Python                          Generates simulated
charging-session events and
settlement reports, validates
records, and coordinates the
S3-first split-write workflow.

  Apache Kafka                    Provides a durable, ordered event
log with seven-day retention and
replay support. It runs in Docker
Compose, avoiding an external
broker dependency. Replay supports
recovery and validation of the
streaming path.

  PySpark                         Reads the partitioned S3 archive,
deduplicates events, joins sessions
to settlement reports, applies
effective-dated tariffs, checks
watermarks, and recomputes hourly
aggregates. Partition overwrite
makes hourly recomputation
idempotent.

  Amazon S3 + Parquet             Stores the immutable raw master
dataset and derived batch, speed,
metadata, and dead-letter data.
Parquet supports analytical scans.
Raw data is partitioned by
year/month/day/hour.

  Spark Structured Streaming      Consumes Kafka and maintains
running station-level energy and
estimated-cost totals.
Approximately one-second event-time
windows provide low-latency
estimates; sliding windows detect
sustained spikes. Checkpointing
supports recovery after restarts.

  PostgreSQL                      Stores compact, pre-aggregated
batch and speed results in separate
indexed tables, along with
batch-boundary metadata. SQL views
provide Grafana with a low-latency
serving interface without
repeatedly scanning raw S3 files.

  Grafana                         Connects to PostgreSQL through its
native data source and displays
auto-refreshing time-series charts,
station comparisons, and alert
panels. Dashboard definitions will
be exported as JSON for
reproducibility.
Docker Compose                  Runs the self-managed components
together in a reproducible
deployment.
Why PostgreSQL for the Serving Layer?
The dashboard repeatedly queries compact, pre-aggregated operational and
historical results. PostgreSQL provides a low-latency SQL interface for
Grafana and avoids scanning raw S3 files on every dashboard refresh.

PostgreSQL stores derived serving tables, not the source-of-truth
event history. Separate batch_view, speed_view, and batch_meta
tables support a merged SQL view that selects authoritative batch
results for completed periods and speed results only after the last
completed batch boundary.

✅ Lambda Validation and Correctness

1. Split-Write Fault Tolerance
The producer writes each event to S3 first and publishes it to Kafka
only after the S3 write succeeds.
- If S3 fails, the producer does not publish to Kafka; it retries and
  records the failure.
- If S3 succeeds but Kafka publication fails, the event remains safe
  in the master archive. The producer retries Kafka publication.
- If Kafka retries are exhausted, the event ID is logged. A
  reconciliation task compares the producer's per-minute event-ID
  ledger with S3 records and republishes missing Kafka events.
This ordering prevents a Kafka-only event from existing without an S3
master record. The two writes are not an atomic transaction, so
retries and reconciliation are part of the design.
2. Batch--Stream Consistency
Both PySpark jobs import the same transformations module and validate
against the same event schema contract. Common energy aggregation and
tariff calculation rules are maintained in one place, so a formula
change is applied consistently to both processing paths.

Streaming outputs are estimates. Batch outputs are authoritative after
deduplication and settlement reconciliation.
3. Serving Strategy and Double-Counting Prevention
- Batch processing writes authoritative hourly aggregates to batch/
  and updates the relevant watermark and completed-hour boundary in
  meta/.
- Streaming writes minute-level aggregates to speed/ and supplies
  data only after the last completed batch boundary.
- Grafana queries merged_view, which selects batch rows for
  completed hours and speed rows only for the uncovered interval.
The cutoff timestamp marks the last hour fully processed by batch. The
batch and speed ranges are disjoint. For example, if the cutoff is
09:00, batch supplies completed periods before 09:00, while speed
supplies 09:00 through the current time.

When the 09:00 batch finishes, its output is written first and the
metadata boundary is advanced second. The next merged-view query then
uses the batch result for that hour and no longer includes the speed
result for the same period.

Batch rewrites are safe because duplicate event IDs are removed,
processing metadata is checked, and only the affected hourly partition
is overwritten. Re-running an hour replaces its previous aggregate
instead of appending a duplicate.
4. Late Data and Reconciliation
A settlement report may arrive after the streaming window has closed.
The speed layer does not retroactively revise already emitted windows,
so its result remains an operational estimate.

The report is retained in raw/ and joined to its charging session by
session_id during the next batch recomputation of the affected hour.
The batch result then reflects reported billed energy and settlement
amount. After recomputation succeeds, batch metadata is updated and the
merged view switches that period from the older speed estimate to the
authoritative batch result.

☁️ Infrastructure and Deployment

Proposed Deployment
- Compute: One Ubuntu EC2 t3.xlarge instance (4 vCPU / 16 GB
  RAM).
- Disk: 50 GB gp3 storage, exceeding the guideline's minimum 30
  GB.
- Orchestration: All self-managed components run in one Docker
  Compose project.
- Cost management: The instance is stopped when not in use to
  manage AWS Academy Learner Lab credits.
Services
  Service                             Role
  Python producer                     Generates charging-session events
and settlement reports; validates
records and performs S3-first split
writes.

  Apache Kafka                        Durable speed-path event broker
with seven-day retention.

  Spark Structured Streaming          Continuously processes Kafka events
and writes speed aggregates.

  PySpark batch scheduler             Runs hourly S3 batch recomputation
and metadata updates.

  Grafana                             Dashboard and alert visualisation.
AWS S3 + PostgreSQL                 S3 provides the archive; PostgreSQL
serves derived aggregates and
metadata.
Ports and Network Access
  Service                                       Port Reachability
  SSH                                           22 Team public IP /32
only.

  Grafana                                     3000 Team public IP /32
only.

  Kafka                                       9092 Internal Docker
network only; open
externally only if an
external producer is
required.

  Spark UI                                    4040 Internal network or
SSH tunnel only; do
not expose publicly.
PostgreSQL / S3                        HTTPS 443 Outbound AWS service
access; credentials
supplied securely.
AWS Resources and Storage Layout
The planned AWS resources include one S3 bucket with raw/, batch/,
speed/, meta/, and dead-letter/ prefixes, plus an EC2 instance
profile or temporary Learner Lab credentials.

PostgreSQL runs as a Docker Compose service on the EC2 instance and
stores derived serving aggregates and metadata.

Security and reproducibility practices:
- Store secrets in an uncommitted .env file or use an instance role;
  never commit credentials to Git.
- Pin container images and libraries instead of using latest.
- Restrict SSH and Grafana inbound rules to the team's public IP as a
  /32.
- Do not expose processing UIs, databases, or broker ports to the
  public internet.
- Document fresh-instance setup and reproducible deployment in the
  repository README.

  
🎯 Expected Outcomes

- A configurable producer capable of generating at least 50 charging
  data points per second by default, including duplicates and
  late/out-of-order events.
- An append-only S3 archive preserving raw charging and settlement
  records for historical recomputation.
- A continuously updated speed view for station-level energy
  consumption and estimated charging costs.
- An idempotent hourly batch view incorporating deduplication, late
  settlement reports, and effective-dated tariff rules.
- A merged PostgreSQL view that prevents double counting across the
  batch/speed boundary.
- A Grafana dashboard with live station metrics, estimated
  expenditure, reconciliation differences, and threshold alerts.

  
📝 Conclusion

ChargePulse applies Lambda architecture to EV charging analytics while
retaining the scale and engineering pattern of the reference AI Bill
Analytics design. Python generates a continuous stream of charging
activity; Kafka supports the speed path; S3 preserves the append-only
master history; PySpark provides streaming and batch processing;
PostgreSQL exposes the serving views; and Grafana presents the results.

The key design feature is the separation between low-latency estimates
and authoritative historical recomputation, with explicit handling for
duplicates, late settlement reports, split-write failures, and
double-counting at the serving boundary.

This document describes the Phase 1 blueprint. Actual event payload
size, S3 storage growth, and measured throughput will be validated
during implementation. The fixed default event rate is 50 events per
second, matching the reference design and project guideline.
