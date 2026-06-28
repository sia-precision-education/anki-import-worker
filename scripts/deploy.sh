#!/usr/bin/env bash
# Build + deploy the anki-import-worker as an Azure Container App.
# Mirrors crtx-backend/exam-worker/scripts/deploy.sh. Fill in the names/registry
# for your environment, or wire this into CI. The worker image is built from
# THIS repo (the public AGPL repo), not the SIA monorepo.
set -euo pipefail

ENVIRONMENT="${1:-staging}"

# --- per-environment config (edit me) ---------------------------------------
REGISTRY="crtxregistry"                       # az acr name
if [[ "$ENVIRONMENT" == "prod" ]]; then
  CONTAINER_NAME="crtx-prod-ca-anki-worker"
  IMAGE_REPO="anki-import-worker-prod"
else
  CONTAINER_NAME="crtx-staging-ca-anki-worker"
  IMAGE_REPO="anki-import-worker-staging"
fi
TAG="$(git rev-parse --short HEAD 2>/dev/null || echo latest)"
IMAGE="${IMAGE_REPO}:${TAG}"
# ----------------------------------------------------------------------------

echo "Building ${IMAGE} from $(pwd) ..."
az acr build --registry "${REGISTRY}" --image "${IMAGE}" .

echo "Updating container app ${CONTAINER_NAME} ..."
az containerapp update \
  --name "${CONTAINER_NAME}" \
  --image "${REGISTRY}.azurecr.io/${IMAGE}"

echo "Done. Ensure these are set on ${CONTAINER_NAME}:"
echo "  AZURE_STORAGE_CONNECTION_STRING, ANKI_CALLBACK_URL, ANKI_CALLBACK_SECRET"
echo "GIT_SHA pinned to ${TAG} — keep the published source at this commit."
