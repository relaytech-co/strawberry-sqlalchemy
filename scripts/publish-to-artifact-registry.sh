#!/bin/bash
set -euo pipefail

# Build the wheel and publish it to Relay's private python registry.
# Expects application default credentials from google-github-actions/auth.

: "${AR_REPOSITORY_URL:?AR_REPOSITORY_URL must be set}"

python -m pip install --quiet --upgrade poetry twine keyrings.google-artifactregistry-auth

rm -rf dist/
poetry build

echo "Publishing to ${AR_REPOSITORY_URL}"
# keyrings.google-artifactregistry-auth authenticates twine from the ADC above
twine upload --repository-url "${AR_REPOSITORY_URL}" --non-interactive dist/*

echo "Published:"
ls -1 dist/
