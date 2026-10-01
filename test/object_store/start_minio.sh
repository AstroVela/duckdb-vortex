#!/usr/bin/env bash
set -euo pipefail

: "${GITHUB_ENV:?GITHUB_ENV must name the GitHub Actions environment file}"

# Upstream registry images are no longer public. Build the Linux amd64 test
# image from the same release's static binary, verified before Docker sees it.
minio_release='RELEASE.2025-09-07T16-13-09Z'
minio_sha256='7c5bd8512c6e966455b1d198209358b2d191c77a83ab377c4073281065fb855f'
minio_image="vortex-minio-ci:${minio_release}"
minio_build_dir="$(mktemp -d)"
trap 'rm -rf "$minio_build_dir"' EXIT
mkdir -p "$minio_build_dir/rootfs/data" "$minio_build_dir/rootfs/tmp"
chmod 1777 "$minio_build_dir/rootfs/tmp"
curl --fail --location --silent --show-error \
  --retry 3 --connect-timeout 10 --max-time 180 \
  "https://github.com/minio/minio/releases/download/${minio_release}/minio.linux-amd64.${minio_release}" \
  --output "$minio_build_dir/rootfs/minio"
printf '%s  %s\n' "$minio_sha256" "$minio_build_dir/rootfs/minio" | sha256sum --check --strict
chmod 0755 "$minio_build_dir/rootfs/minio"
cat >"$minio_build_dir/Dockerfile" <<'DOCKERFILE'
FROM scratch
COPY rootfs /
WORKDIR /data
ENTRYPOINT ["/minio"]
DOCKERFILE
docker build --platform linux/amd64 --tag "$minio_image" "$minio_build_dir"

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
