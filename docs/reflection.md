# Reflection: ChargePulse

*Laksh Sharma and Kartik Singh*

## Where we started

When we handed in Phase 1 we were fairly confident about the design. Lambda made sense for our problem: operators want to see charging activity as it happens, but the money side (settlements, tariffs, disputes) only becomes final later. One fast path for estimates and one slow, correct path for the final numbers seemed like an obvious fit.

The early prototype told a different story. We had a producer that ran on a laptop and stopped after a fixed number of events, a streaming job that wrote Parquet files to a local folder, and a separate script that copied those files into Postgres. The batch job read everything from S3 with pandas, deleted the whole batch table, and wrote it again. Each piece worked on its own, which made it feel further along than it was. Nothing was scheduled, nothing was connected end to end, and about 21,000 Spark checkpoint files had been committed to git because nobody had looked at the git status closely.

The worst problem was one we didn't notice for a while. The batch job set its "completed up to" boundary to the end of the *current* hour, using the latest event it had seen. So the moment batch ran, it claimed an hour that was still in progress, and the merged view hid the live speed data for that hour. On a dashboard this just looks like the numbers stopped moving. The fix was a single idea, "an hour is only complete once it has ended plus a grace period", but finding it made us read the merge logic much more carefully than we had in Phase 1.

## Things that changed from the Phase 1 design

**The split write.** In Phase 1 we wrote "S3 first, then Kafka, per event". Once we tried to build it, we saw the problem. At 50 events per second that is either one S3 object per event (thousands of tiny files an hour, and every event waits on an S3 round trip), or holding events back from Kafka until a batch is ready, which defeats the point of a speed layer. We ended up with a local write-ahead log: every event is appended to a file on disk first, then sent to Kafka, and a background thread uploads one Parquet file per minute to S3. That keeps the property we cared about (nothing reaches Kafka without a durable copy) without the tiny files. It also made the failure cases easy to explain. When we stopped Kafka for 45 seconds during testing, the producer kept going at 50 events a second and nothing was lost. Afterwards our reconcile script found the same 829 events in S3 and in Kafka for the hour we checked.

**Window size.** We had promised roughly one-second windows. We moved to one minute because the batch view has to use the same windows as the speed view, otherwise the merged view mixes different granularities and the "speed vs batch" comparison means nothing. In hindsight, the one-second figure in the report came from copying the reference design rather than from anything our use case needed.

**Batch through boto3 instead of s3a.** Reading S3 directly from Spark needs the hadoop-aws jar plus an AWS SDK bundle of around half a gigabyte. For one hour of data (around 180k rows) it was simpler to download that hour's files and let Spark read them locally. That's a pragmatic choice, not a scalable one, and we would revisit it if the volume grew.

## What testing actually caught

We wrote a test that runs the same events through a real streaming query and through the batch code, then checks that the outputs match. We mostly wrote it to back up the "batch and speed stay consistent" claim in the README. It ended up finding a real bug. Spark 4 runs in ANSI mode, so casting a malformed timestamp throws an error instead of returning null. One bad record on Kafka would have crashed the speed layer, and because it restarts from its checkpoint, it would have hit the same record and crashed again, forever. Switching to `try_cast` fixed it, but we would never have seen it with our own clean test data. We only caught it because the producer can deliberately generate broken events.

The same test failed a second time over a difference in the sixth decimal place, from floating-point sums being added in a different order. That was a good reminder that "identical" needs a definition. We now compare counts exactly and floating-point values with a tolerance.

Smaller lessons: kafka-python 3 renamed an exception we imported, so the producer crashed on startup in the container even though it worked on the laptop. Pinning versions only helps if you test with the versions you pinned.

## Seeing Lambda work

The moment the architecture made sense to us was the late-data test. We pushed 300 events backdated about 45 minutes into an hour that batch had already processed. The speed layer ignored all of them, which is correct: they were far beyond its 2-minute watermark. The next batch run noticed that the hour's raw files had changed, recomputed only that hour, and the hourly comparison went from 529 to 829 events. Running the same hour again changed nothing except the run counter. Reading about "batch is authoritative" is one thing; watching a dashboard correct itself is another.

We also felt the cost of Lambda that people usually mention: two code paths. Our answer was to put the schema, validation rules, tariff logic and aggregation into one shared module that both jobs import, and to test that they agree. If we had not done that, the two layers would have drifted apart within days.

## Infrastructure surprises

The guidelines recommend a t3.xlarge. Our AWS Academy sandbox turned out to have a policy that only allows nano, micro and small instances, so the largest machine we could use was a t3.small with 2 GB of RAM. The whole stack used about 1.9 GB on a laptop. We made the memory settings configurable, added swap, and reduced the Kafka and Spark heaps for the small instance. It runs, but there is very little headroom, and the hourly batch job leans on swap. We documented this in the README rather than pretending the recommended setup was used.

The sandbox credentials also expire every few hours. A launcher script that fetches fresh keys and restarts the stack saved us a lot of copy-pasting. We also learned that a browser's login cannot just be copied to another program, because modern browsers deliberately encrypt cookies so that only the browser itself can read them.

## What we would do differently

- **Start from the data contract.** Most of our early bugs came from each component having its own idea of the event fields. Once there was a single `contract.py`, a whole class of problems disappeared.
- **Test on the target environment earlier.** We found the instance-size restriction and the credential expiry at the very end. A one-hour spike on the real sandbox in week one would have changed some decisions.
- **Commit small and often, from both of us.** Our history doesn't show how the work was really shared, and the guidelines ask for it.
- **Be more careful with numbers in the design document.** A few values in Phase 1 (one-second windows, S3-first writes) sounded reasonable but didn't survive contact with an implementation.

## Would we pick Lambda again?

For this domain, yes, but for a narrower reason than we gave in Phase 1. The real value was not "fast plus accurate" in general. It was that billing data arrives late and gets disputed, and keeping an append-only raw archive that we can recompute from at any time made those corrections safe and boring. If the late settlements weren't part of the problem, a Kappa design with a replayable log would have been less code, and we would probably choose it.

## Who did what

*[Fill in honestly: e.g. who built the producer and simulation, the Spark jobs, the serving layer and Grafana, the deployment and testing, and the documents.]*
