"""Etapa 2: Kafka -> event-time windows -> finalized atomic CSV batches."""
from rtap.config import configure_storage
configure_storage()

import argparse
import logging
import os
import time
from pathlib import Path

from pyspark.sql import SparkSession, functions as F

from rtap.config import ROOT, data_dir, load_config
from rtap.storage import write_csv, write_json

SCHEMA = """timestamp DOUBLE, source_id STRING, n_person INT, noise_level DOUBLE,
bias_flag INT, voice_intensity DOUBLE, speech_flag INT, object_count INT,
top_object STRING, event_id STRING, source_kind STRING"""


def parse_events(raw):
    parsed = raw.select(F.from_json(F.col("value").cast("string"), SCHEMA).alias("e"),
                        F.col("timestamp").alias("broker_time"), "value", "partition", "offset")
    return parsed.select("e.*", "broker_time", "value", "partition", "offset").withColumn(
        "event_time", F.timestamp_seconds("timestamp"))


def valid_condition():
    valid = (F.col("event_id").isNotNull() & (F.length("event_id") > 0) &
             F.col("source_id").isNotNull() & (F.length(F.trim("source_id")) > 0) &
             F.col("source_kind").isin("webcam", "test_fixture") &
             F.col("n_person").isNotNull() & (F.col("n_person") >= 0) &
             F.col("object_count").isNotNull() & (F.col("object_count") >= F.col("n_person")) &
             F.col("timestamp").isNotNull() & (~F.isnan("timestamp")) &
             (F.col("timestamp") > 0) &
             (F.col("timestamp") <= F.col("broker_time").cast("double") + 60) &
             (F.col("timestamp") >= F.col("broker_time").cast("double") - 86400))
    for name in ("noise_level", "voice_intensity", "speech_flag", "bias_flag"):
        valid = valid & (F.col(name).isNull() | ((~F.isnan(name)) & F.col(name).between(0, 1)))
    for name in ("speech_flag", "bias_flag"):
        valid = valid & (F.col(name).isNull() | F.col(name).isin(0, 1))
    return F.coalesce(valid, F.lit(False))


def aggregate(events, window_seconds=10, watermark_seconds=5):
    events = events.withWatermark("event_time", f"{watermark_seconds} seconds").dropDuplicatesWithinWatermark(["event_id"])
    result = events.groupBy(F.window("event_time", f"{window_seconds} seconds"), "source_id").agg(
        F.avg("n_person").alias("avg_person"), F.max("n_person").alias("max_person"),
        F.avg("noise_level").alias("avg_noise"), F.avg("bias_flag").alias("avg_bias"),
        F.avg("voice_intensity").alias("avg_voice"), F.avg("speech_flag").alias("avg_speech"),
        F.avg("object_count").alias("avg_objects"), F.count("*").alias("event_count"),
        F.count("noise_level").alias("audio_samples"), F.max("timestamp").alias("last_event_timestamp"))
    # Collecting TimestampType on Windows converts to local naive time; format in Spark UTC.
    return result.select(F.date_format("window.start", "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").alias("window_start"),
                         F.date_format("window.end", "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").alias("window_end"),
                         "source_id", *[c for c in result.columns if c not in ("window", "source_id")])


def create_spark():
    jars = sorted((Path(".runtime/tooling/spark-kafka-jars").resolve()).glob("*.jar"))
    if not jars:
        raise RuntimeError("Kafka connector missing: run SETUP_LAB.bat")
    return (SparkSession.builder.master("local[2]").appName("RTAP-SparkTrio-MD")
            .config("spark.jars", ",".join(str(p) for p in jars))
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.sql.shuffle.partitions", "2")
            .config("spark.driver.bindAddress", "127.0.0.1")
            .config("spark.driver.host", "127.0.0.1")
            .config("spark.sql.warehouse.dir", (data_dir()/"warehouse").resolve().as_uri())
            .config("spark.jars.ivy", str((ROOT/".runtime/cache/ivy").resolve()))
            .config("spark.ui.enabled", "false")
            .config("spark.driver.memory", "768m")
            .getOrCreate())


def main():
    config = load_config()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--window", type=int, default=config["window_seconds"])
    parser.add_argument("--topic", default=config["topic"])
    parser.add_argument("--output", type=Path, default=data_dir() / "aggregated_events")
    parser.add_argument("--checkpoint", type=Path, default=data_dir() / "chk_events")
    parser.add_argument("--include-test", action="store_true")
    parser.add_argument("--seconds", type=int, default=0)
    parser.add_argument("--stop-file", type=Path, default=data_dir() / "stop-spark")
    args = parser.parse_args()
    if not 1 <= args.window <= 60:
        parser.error("Window must be 1..60 seconds; baseline = 10")
    args.output.mkdir(parents=True, exist_ok=True)
    args.checkpoint.mkdir(parents=True, exist_ok=True)
    signature = args.checkpoint / "rtap-config.json"
    from rtap.storage import read_json
    expected = dict(topic=args.topic, window=args.window, watermark=config["watermark_seconds"],
                    output=str(args.output.resolve()), include_test=args.include_test, schema_version=1)
    previous = read_json(signature)
    if previous and previous != expected:
        raise RuntimeError("Checkpoint configuration changed. Use a NEW --checkpoint and --output for experiments.")
    write_json(signature, expected)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    logging.getLogger("py4j").setLevel(logging.WARNING)
    status_path = args.output / "spark.json"
    write_json(status_path, dict(status="starting", updated=time.time()))
    spark = None
    queries = []
    try:
        spark = create_spark()
        spark.sparkContext.setLogLevel("WARN")
        raw = (spark.readStream.format("kafka").option("kafka.bootstrap.servers", config["bootstrap_servers"])
               .option("subscribe", args.topic).option("startingOffsets", "earliest")
               .option("failOnDataLoss", "true").option("maxOffsetsPerTrigger", 5000).load())
        parsed = parse_events(raw)
        valid = valid_condition()
        events = parsed.filter(valid)
        if not args.include_test:
            events = events.filter(F.col("source_kind") == "webcam")
        agg = aggregate(events, args.window, config["watermark_seconds"])

        def save_batch(batch, batch_id):
            rows = [r.asDict() for r in batch.orderBy("window_start", "source_id").collect()]
            for row in rows:
                row["completed_at"] = time.time()
            write_csv(args.output / f"batch-{batch_id:012d}.csv", rows)
            if rows:
                write_json(args.output / "last_aggregation.json", rows[-1])
                logging.info("Finalized batch %s: %s", batch_id, rows)

        def reject_batch(batch, batch_id):
            rows = [r.asDict() for r in batch.select("partition", "offset", F.col("value").cast("string")).limit(100).collect()]
            if rows:
                write_json(args.output / "rejected" / f"batch-{batch_id:012d}.json", rows)

        queries.append(agg.writeStream.outputMode("append").foreachBatch(save_batch)
                       .option("checkpointLocation", str(args.checkpoint / "aggregation"))
                       .trigger(processingTime=f"{config['trigger_seconds']} seconds").start())
        queries.append(parsed.filter(~valid).writeStream.foreachBatch(reject_batch)
                       .option("checkpointLocation", str(args.checkpoint / "rejected"))
                       .trigger(processingTime="5 seconds").start())
        start = time.monotonic()
        while not args.stop_file.exists() and (not args.seconds or time.monotonic()-start < args.seconds):
            for query in queries:
                if query.exception():
                    raise RuntimeError(str(query.exception()))
            progress = queries[0].lastProgress
            write_json(status_path, dict(status="running", updated=time.time(), query_id=str(queries[0].id),
                                        run_id=str(queries[0].runId), window_seconds=args.window,
                                        topic=args.topic, progress=progress))
            time.sleep(1)
    except KeyboardInterrupt:
        pass
    except Exception as exc:
        write_json(status_path, dict(status="error", error=str(exc), updated=time.time()))
        raise
    finally:
        for query in queries:
            query.stop()
        if spark is not None:
            spark.stop()
        state = read_json(status_path, {})
        if state.get("status") != "error":
            state.update(status="stopped", updated=time.time())
            write_json(status_path, state)


if __name__ == "__main__":
    main()
