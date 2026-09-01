#!/usr/bin/env bash
set -euo pipefail

: "${GITHUB_ENV:?GITHUB_ENV must name the GitHub Actions environment file}"

# linux/amd64 manifest for RELEASE.2025-04-22T22-12-26Z.
minio_image='minio/minio@sha256:3f97c5651cb6662b880c787a232b6b34fec8d8922e08d6617b25d241a21164bb'
minio_access_key="vortex$(openssl rand -hex 8)"
minio_secret_key="$(openssl rand -hex 24)"
minio_endpoint='http://127.0.0.1:9000'
minio_region='us-east-1'
minio_bucket='vortex-ci'

printf '::add-mask::%s\n' "$minio_access_key"
printf '::add-mask::%s\n' "$minio_secret_key"

docker run --detach --name vortex-minio --publish 127.0.0.1:9000:9000 \
  --env "MINIO_ROOT_USER=$minio_access_key" \
  --env "MINIO_ROOT_PASSWORD=$minio_secret_key" \
  "$minio_image" server /data --address :9000

ready=false
for _ in $(seq 1 60); do
  if curl --connect-timeout 1 --max-time 2 --fail --silent --show-error \
    "$minio_endpoint/minio/health/ready" >/dev/null; then
    ready=true
    break
  fi
  sleep 1
done
if [[ "$ready" != true ]]; then
  docker logs vortex-minio
  exit 1
fi

curl --fail-with-body --silent --show-error \
  --connect-timeout 5 --max-time 30 \
  --aws-sigv4 "aws:amz:${minio_region}:s3" \
  --user "${minio_access_key}:${minio_secret_key}" \
  --request PUT "${minio_endpoint}/${minio_bucket}" >/dev/null

{
  printf 'VORTEX_MINIO_ENDPOINT=%s\n' "$minio_endpoint"
  printf 'VORTEX_MINIO_ACCESS_KEY=%s\n' "$minio_access_key"
  printf 'VORTEX_MINIO_SECRET_KEY=%s\n' "$minio_secret_key"
  printf 'VORTEX_MINIO_REGION=%s\n' "$minio_region"
  printf 'VORTEX_MINIO_BUCKET=%s\n' "$minio_bucket"
} >>"$GITHUB_ENV"
