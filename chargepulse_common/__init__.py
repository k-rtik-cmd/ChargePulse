"""Code shared by the ChargePulse producer, speed layer and batch layer.

`contract` and `tariffs` are pure Python (no Spark dependency) so the producer
can import them. `transforms` holds the Spark DataFrame logic that both the
speed layer and the batch layer call, which keeps the two paths semantically
identical.
"""

SCHEMA_VERSION = 1
