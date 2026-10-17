# HDoi — Queue Service và hướng dẫn triển khai

**Đọc [IMPLEMENTATION_STATUS.md](IMPLEMENTATION_STATUS.md) trước khi triển khai.**
Xem [báo cáo runtime hiện tại](../docs/HDOI_RUNTIME_CUTOVER.md): mã backend đã chuyển sang HDoi, chưa deploy/cutover production.

This directory packages an Express API on port 3100, a separate BullMQ/ioredis
worker, and Redis. The application entry points are `src/app.js` and
`src/workers/index.js`; image installation uses `package.json` and
`package-lock.json`. The API and worker are implemented. Backend integration is
Backend runtime now uses HDoi for queue/state/locks without Neon fallback. Existing source rows and jobs are retained for offline migration. Docker startup, restart and recovery still require staging verification; do not deploy before completing the cutover checklist.

## Windows VPS prerequisites

These are **Linux containers**. On a Windows Server VPS, provision a Linux VM
with Docker Engine and the Compose v2 plugin, persistent disk, and automatic boot.
The hosting provider must permit hardware/nested virtualization if that VM runs
inside the VPS. If unavailable, use a separate Linux VPS. Docker Desktop is not
supported on Windows Server ([Docker requirements](https://docs.docker.com/desktop/setup/install/windows-install/)).
Run the commands below in the Linux Docker host's shell, with the repository
checked out there. Remote Docker contexts resolve bind mounts on the daemon host.
Reserve RAM/disk headroom for Redis, AOF rewrites, and backups; monitor both.

## Safe image context and configuration

The build context is `HDoi` itself. Docker copies only `package*.json` and `src/`;
the allowlist in `.dockerignore` also limits the context to manifests and source.
No backend sources or `.env` files are copied. Review source for embedded secrets
before release; do not put credentials under `src/`. The worker calls the backend
over HTTP using `BACKEND_URL` and `BACKEND_WORKER_TOKEN`, with no direct backend
imports. Backend callbacks require the existing backend with HDoi configuration and Google/SMTP credentials. Redis is technical state only; Google Sheets remains the business source.

Use a clean staging checkout of reviewed, committed files. Example:

```sh
# Run in the source repository; choose a fresh staging path.
git clone --no-local . /srv/hdoi-release
cd /srv/hdoi-release
cp HDoi/.env.example HDoi/.env
chmod 600 HDoi/.env
# Edit HDoi/.env before proceeding.
docker compose --env-file HDoi/.env -f HDoi/docker-compose.yml config --quiet
docker compose --env-file HDoi/.env -f HDoi/docker-compose.yml build
```

Keep staging outside the source checkout. Commit the completed app and packaging
before cloning; uncommitted files are not included. Do not commit `.env`.
Generate independent random tokens (for example `openssl rand -hex 32`), replace
the example placeholders, and use a stable unique `APP_NAMESPACE` per deployment.
The namespace must actually be applied consistently by the app to queue names
or BullMQ prefixes. Changing it can leave existing jobs unprocessed.
Compose rejects empty/missing variables, but does not reject example placeholders.

`REDIS_URL` is supplied internally as `redis://redis:6379`. `BACKEND_URL` must be
reachable inside Linux containers; use a private reachable address or HTTPS DNS
name. The existing backend must implement and validate `BACKEND_WORKER_TOKEN`.
No backend service is provisioned here. Environment secrets are visible to Docker
administrators; restrict host access and rotate secrets without logging them.

`WORKER_CONCURRENCY`, `SHEETS_READS_PER_MINUTE`, and
`SHEETS_WRITES_PER_MINUTE` are passed to both processes. Defaults are **2, 40,
and 40** respectively. Set positive integers; application maximums are **16**
for concurrency and **50 each** for Sheets reads and writes per minute. Larger
values fail application validation. Tune within these bounds to actual Google
quotas and verify limits across workers. `GOOGLE_SERVICE_ACCOUNT_JSON` is optional compact JSON, supplied
via the runtime environment only; the application must support and validate it.
Use single quotes around populated JSON in `.env` to avoid interpolation.
File-based secret loading is not configured by this packaging.

Each app container is limited to 512 MiB and 1 CPU; Redis is limited to 1 GiB
and 1 CPU, with Redis `maxmemory` set to 512 MiB to leave some overhead for AOF
and process memory. Size the host for both app containers, Redis, the OS, and
rewrite/copy-on-write peaks; these defaults require workload validation.

## Private HTTPS access and release checks

The API publishes only `127.0.0.1:3100` on the **Linux Docker host**. Place an
HTTPS reverse proxy on that host and forward to that address; a proxy on the
Windows host needs a deliberate private tunnel to the Linux host. A proxy in a
container cannot use its own loopback to reach this API. Provision a valid TLS
certificate, firewall ingress, and restrict access with VPN or an IP allowlist
and proxy authentication. Never expose raw port 3100 to the Internet. Forward
authorization headers and redact them from access logs.

The API uses `Authorization: Bearer <QUEUE_API_TOKEN>` for `/v1/jobs`
(GET and POST), `/v1/jobs/:id`, `/v1/jobs/:id/reconcile`, `/v1/metrics`, and
`/health/ready`. `/health/live` is unauthenticated. The API must listen on
`0.0.0.0:3100` inside its container. Preserve the Bearer header through the proxy;
use proxy access controls that do not overwrite it. Require HTTPS for backend traffic outside a trusted private
network. Redis has **no published port**; do not add one. Its default Compose
network is a trusted boundary, not authentication against other attached
containers. Do not attach untrusted workloads to it.

Before starting production, verify API authentication/rejection, backend token
validation, enqueue-to-worker completion, retry/idempotency behavior, namespace
isolation, and SIGTERM handling for both processes. The worker must drain/close
BullMQ and Redis connections within the 60-second grace period.

The `queue-api` healthcheck uses built-in Node fetch against `/health/live` with
a three-second request timeout and requires a successful HTTP status. This
local liveness route is implemented without a token requirement; keep public access to it
restricted at the proxy. The `queue-worker` healthcheck reads an epoch timestamp
in **milliseconds** from `/tmp/hdoi-worker-heartbeat`, requiring an age below
45 seconds. The worker implements a timestamp write every 10 seconds. An initial
startup write is still pending in the application; the 30-second startup grace
allows the periodic writer to run. Verify that startup fix before release.
Heartbeat freshness alone does not prove successful job processing.
Both checks run every 10 seconds with a 30-second startup grace and three
failure retries. Redis health verifies only PING. Liveness does not prove backend
or backend readiness, and Compose does not restart a container merely because it
becomes unhealthy; monitor health and investigate failures.

After completing those checks in an isolated environment, an operator can run:

```sh
cd /srv/hdoi-release/HDoi
docker compose up -d
docker compose ps
docker compose logs --tail=100 queue-api queue-worker
```

Pin approved image digests before production; the supplied Node and Redis tags
can receive updates. Do not run `docker compose down -v`: it deletes queue data.

## Redis durability, backup, and restore

Redis uses AOF with `appendfsync always`, `maxmemory-policy noeviction`, and the
named volume selected by `REDIS_VOLUME_NAME` (default `hdoi-redis-data`) mounted
at `/data`. RDB snapshots are disabled.
Every write requests an AOF fsync; this costs latency and still depends on host
storage guarantees. It does not provide exactly-once backend effects or replace
backups. Monitor memory and disk, including AOF rewrite peaks. With noeviction, exhausted configured
memory rejects writes; host memory exhaustion can kill Redis.

Use a coordinated cold backup for a consistent full Redis 7 multipart AOF,
including its manifest and all files under `/data`. Schedule a maintenance
window, block new requests at the proxy, and let active jobs finish before
stopping services. Run from the staging `HDoi` directory:

```sh
mkdir -p backups
chmod 700 backups
docker compose stop queue-api queue-worker
docker compose stop redis
# Resolve the active volume even after a previous restore.
redis_volume=$(docker inspect "$(docker compose ps -aq redis)" --format '{{range .Mounts}}{{if eq .Destination "/data"}}{{.Name}}{{end}}{{end}}')
test -n "$redis_volume" || exit 1
docker run --rm --network none --user 0:0 \
  -v "$redis_volume:/data:ro" -v "$PWD/backups:/backup" \
  redis:7.4-bookworm tar -czf /backup/redis-data.tgz -C /data .
sha256sum backups/redis-data.tgz > backups/redis-data.tgz.sha256
docker compose up -d
```

Use a unique filename per backup instead of overwriting the example archive.
If any stop/archive/checksum step fails, investigate before treating it as a
backup. Encrypt and copy backups off-host with restricted access and retention;
jobs may contain private data. Preserve matching application version, namespace,
and separately secured configuration. A Docker volume is not an off-host backup.

Restore during maintenance with all clients stopped. Preserve the current volume
and record its name for rollback. Verify the checksum and use a trusted archive.
Extract into a **new, empty volume**, leaving the original untouched:

```sh
docker compose stop queue-api queue-worker redis
sha256sum -c backups/redis-data.tgz.sha256 || exit 1
# Choose a unique name for every restore; refuse to reuse an existing volume.
restore_volume="hdoi-redis-restore-$(date -u +%Y%m%dT%H%M%SZ)"
if docker volume inspect "$restore_volume" >/dev/null 2>&1; then exit 1; fi
docker volume create "$restore_volume" || exit 1
docker run --rm --network none --user 0:0 \
  -v "$restore_volume:/data" -v "$PWD/backups:/backup:ro" \
  redis:7.4-bookworm sh -ec \
  'test -z "$(ls -A /data)"; tar -xzf /backup/redis-data.tgz -C /data; chown -R redis:redis /data' || exit 1
printf 'New restored volume: %s\n' "$restore_volume"
# Set REDIS_VOLUME_NAME in .env to that exact name before continuing.
# Remove any shell override so Compose reads the saved .env selection.
unset REDIS_VOLUME_NAME
docker compose config --quiet || exit 1
docker compose up -d --force-recreate redis
docker compose exec redis redis-cli ping
docker compose exec redis redis-cli INFO persistence
# Review loading/AOF errors in logs before restarting consumers.
docker compose logs --tail=100 redis
docker compose up -d queue-api queue-worker
```

Restore initially with the same Redis version used for the backup. Inspect AOF
status and recovered queue state, then exercise an authorized test job before
reopening ingress. Recovered jobs may execute again: backend operations need
idempotency. Rehearse recovery on an isolated Docker host with separate backend
credentials before relying on this procedure. For rollback, stop all services,
set `REDIS_VOLUME_NAME` back to the preserved original in `.env`, and recreate
Redis before restarting clients. Reconcile backend effects before replaying
jobs. Do not delete either volume until recovery is verified. Independent stacks
need distinct project names, volume names, API ports, and namespaces.
