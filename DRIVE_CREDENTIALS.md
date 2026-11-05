# Durable Drive credentials

Set `DRIVE_CREDENTIAL_ENCRYPTION_KEY` to exactly 64 hexadecimal characters representing 32 cryptographically random bytes. Keep this backend secret stable across restarts and replicas; changing it makes the existing archive unreadable. Generate it with `node -e "console.log(require('node:crypto').randomBytes(32).toString('hex'))"` and transfer it through the deployment secret manager.

For Docker Compose, put the key in `HDoi/.env` before deploying. Compose passes it to both `queue-api` and `queue-worker`, and rejects deployment when it is empty. The archive remains in the existing `image-data` volume at `/data/images/.drive-credentials`.

If PDF upload/download returns `IMAGE_HDOI_503`, check HDoi API logs for `DRIVE_CREDENTIAL_ENCRYPTION_REQUIRED`. Earlier Compose configuration did not forward this key, even when it was present in `.env`. Restore the existing archive key, or generate one only for a new archive, then recreate both application containers:

```sh
docker compose --env-file .env -f docker-compose.yml config --quiet
docker compose --env-file .env -f docker-compose.yml up -d --build queue-api queue-worker
```

Run these commands from `HDoi/`. Retain the existing named volumes. After startup, `/v1/drive-credentials/chamcong/current` should return 200 for an initialized archive or 404 for a new archive; a new archive is initialized by ChamCong on first Drive use. A 503 still needs investigation in HDoi logs.

`DRIVE_CREDENTIAL_STORAGE_DIR` defaults to `.drive-credentials` beneath `IMAGE_STORAGE_DIR` (or `/data/images`). Mount this directory on persistent storage shared by the API and image worker processes. Credentials, active pointers, broker retry IDs and file ownership records have no TTL. Files use AES-256-GCM encryption and atomic, synced replacement. Per-consumer exclusive lock files coordinate acquisition across processes; the 120-second abandoned lock threshold exceeds the broker's 20-second request timeout.

The broker uses `DATA_DONG_BO_URL` and `DATA_DONG_BO_TOKEN`. HTTPS is required except for literal loopback development hosts; redirects are rejected. Only confirmed `userRateLimitExceeded` after backoff permits rotation. Generic 429, project quota, network, authorization and permission failures retain ownership. An ambiguous upload response pins its original preallocated file ID and credential.

Existing complete OAuth environment configuration is saved as `environment` when first used. ChamCong supplies its own environment fallback explicitly; its archive never borrows HDoi's account. Each broker credential ID must have immutable OAuth credentials and folder: mint a new ID when these change, so historical file ownership remains valid.

All `/v1/drive-credentials/:consumer/...` endpoints require the existing HDoi bearer token and reject browser Origin/Fetch Metadata headers. They return secrets only to backend callers; never expose these endpoints or their responses through frontend settings. Settings should project only safe ID, label, masked token, revision and issue time.
