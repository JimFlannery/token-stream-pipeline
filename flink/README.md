# flink/

Custom Flink image with Python + `apache-flink` installed, used by both `flink-jobmanager` and `flink-taskmanager` in `docker-compose.yml`.

The official `flink:1.20.0-scala_2.12-java17` image ships without Python, so we add it here.

## TODO (day 3)

- [ ] `Dockerfile` that extends the upstream Flink image, installs Python 3.11 + `apache-flink==1.20.*`
- [ ] Copy `flink-jobs/` into `/opt/jobs/` in the image (or bind-mount at runtime for dev)
- [ ] Add Flink Kafka connector JAR to `/opt/flink/lib/`
