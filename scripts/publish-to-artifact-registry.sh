#!/bin/bash
set -euo pipefail

# Build the wheel and publish it to Relay's private python registry.
# Expects application default credentials from google-github-actions/auth.

: "${AR_PROJECT:?AR_PROJECT must be set}"

AR_LOCATION="${AR_LOCATION:-europe-west1}"
AR_REPOSITORY="${AR_REPOSITORY:-relaycode-python}"
AR_REPOSITORY_URL="https://${AR_LOCATION}-python.pkg.dev/${AR_PROJECT}/${AR_REPOSITORY}/"
PACKAGE="strawberry-sqlalchemy-mapper"

python -m pip install --quiet --upgrade poetry twine keyrings.google-artifactregistry-auth

VERSION=$(poetry version --short)

# Artifact Registry versions are immutable, so re-uploading one is a 400 rather
# than a no-op. Skip so re-runs and the second half of the matrix stay green.
# `versions describe` cannot address a version containing '+', so list and match
if gcloud artifacts versions list \
  --package="${PACKAGE}" \
  --repository="${AR_REPOSITORY}" \
  --location="${AR_LOCATION}" \
  --project="${AR_PROJECT}" \
  --format="value(name.basename())" 2>/dev/null | grep -qxF "${VERSION}"; then
  echo "${PACKAGE} ${VERSION} is already published to ${AR_PROJECT}; nothing to do"
  exit 0
fi

rm -rf dist/
poetry build

echo "Publishing ${PACKAGE} ${VERSION} to ${AR_REPOSITORY_URL}"
# keyrings.google-artifactregistry-auth authenticates twine from the ADC above
twine upload --repository-url "${AR_REPOSITORY_URL}" --non-interactive dist/*

echo "Published:"
ls -1 dist/
