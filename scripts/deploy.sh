#!/usr/bin/env bash
# Build + deploy the anki-import-worker as an Azure Container App.
# The image is built from THIS repo (the public AGPL repo), not the SIA monorepo.
#
# ONE app serves both environments, which is deliberate rather than missing
# infrastructure: prod and staging enqueue to the same `anki-requests` queue and
# each job carries its own `callback_url`, checked against
# ANKI_ALLOWED_CALLBACK_HOSTS on the worker. So there is nothing to deploy "to
# prod" separately — the environment lives in the message, not the container.
set -euo pipefail

REGISTRY="siaacrcczxjknokvf8ddmu"              # az acr name
RESOURCE_GROUP="sia-rg-dev-tzej8as31k1vbtlw"
CONTAINER_NAME="crtx-staging-ca-anki-worker"   # named staging; serves both
IMAGE_REPO="anki-import-worker"

# Images in this repository are identified by commit, never by the version in
# pyproject.toml — so a dirty tree ships something no SHA describes.
if [[ -n "$(git status --porcelain)" ]]; then
  echo "WARNING: working tree is dirty; the image will be tagged with HEAD's SHA anyway." >&2
fi

TAG="$(git rev-parse --short HEAD)"
IMAGE="${IMAGE_REPO}:${TAG}"

echo "Building ${IMAGE} from $(pwd) ..."
az acr build --registry "${REGISTRY}" --image "${IMAGE}" .

echo "Updating container app ${CONTAINER_NAME} ..."
az containerapp update \
  --name "${CONTAINER_NAME}" \
  --resource-group "${RESOURCE_GROUP}" \
  --image "${REGISTRY}.azurecr.io/${IMAGE}"

echo "Done. Ensure these are set on ${CONTAINER_NAME}:"
echo "  AZURE_STORAGE_CONNECTION_STRING, ANKI_CALLBACK_SECRET,"
echo "  ANKI_CALLBACK_URL (fallback for a job that sends none),"
echo "  ANKI_ALLOWED_CALLBACK_HOSTS (every backend host allowed to receive results),"
echo "  ANKI_QUEUE_NAME (defaults to anki-requests)"
echo "GIT_SHA pinned to ${TAG} — keep the published source at this commit."
