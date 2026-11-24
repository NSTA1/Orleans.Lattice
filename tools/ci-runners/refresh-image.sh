#!/usr/bin/env bash
# Weekly runner-image refresh, run by the scheduled job-<prefix>-refresh
# Container Apps Job (see deploy.ps1). It rebuilds the runner image from the
# cloned branch with no layer cache, so the apt upgrade, the .NET runtime and
# the npm tree pick up the week's security fixes, then points the runner Job
# at the new image. It runs as id-<prefix>-refresh, which holds only
# Contributor on the registry and on the runner Job - never the GitHub App key.
#
# Required environment: AZURE_CLIENT_ID, RESOURCE_GROUP, REGISTRY, RUNNER_JOB.
# The working directory is the root of a clone of the repository.
set -euo pipefail

: "${AZURE_CLIENT_ID:?}" "${RESOURCE_GROUP:?}" "${REGISTRY:?}" "${RUNNER_JOB:?}"
KEEP_IMAGES="${KEEP_IMAGES:-4}"

log() { printf '[refresh] %s\n' "$*"; }

az login --identity --client-id "$AZURE_CLIENT_ID" -o none

# Stage the same context deploy.ps1 stages: the runner's files plus, under
# restore/, every file `dotnet restore` reads. Keep the pattern in step with
# $restorePattern in deploy.ps1.
context="$(mktemp -d)"
mkdir -p "$context/tools/ci-runners"
cp tools/ci-runners/Dockerfile tools/ci-runners/mint-jit.py tools/ci-runners/runner-entrypoint.sh "$context/tools/ci-runners/"
restore_pattern='(\.(csproj|fsproj|proj|props|targets|slnx|sln)$|(^|/)nuget\.config$|(^|/)global\.json$|(^|/)packages\.lock\.json$)'
count=0
while IFS= read -r f; do
	mkdir -p "$context/restore/$(dirname "$f")"
	cp "$f" "$context/restore/$f"
	count=$((count + 1))
done < <(git ls-files | grep -Ei "$restore_pattern")
[ "$count" -gt 0 ] || { log "no restore inputs found"; exit 1; }

# A dated tag: the content-hash tag deploy.ps1 uses would repeat for an
# unchanged tree, and a node may then serve its cached, unpatched copy.
tag="lattice-ci-runner:$(git rev-parse --short=12 HEAD)-$(date -u +%Y%m%d%H%M)"
login_server="$(az acr show -g "$RESOURCE_GROUP" -n "$REGISTRY" --query loginServer -o tsv)"
image="$login_server/$tag"

log "building $image from $(git rev-parse HEAD) ($count restore inputs)"
az acr build --registry "$REGISTRY" --image "$tag" --file tools/ci-runners/Dockerfile \
	--platform linux/amd64 --no-cache "$context"

# Repoint both the init container and the runner container. A PATCH of
# properties.template leaves the configuration (secrets, scale rule) alone.
job_id="$(az containerapp job show -g "$RESOURCE_GROUP" -n "$RUNNER_JOB" --query id -o tsv)"
body="$(mktemp)"
az containerapp job show -g "$RESOURCE_GROUP" -n "$RUNNER_JOB" -o json |
	jq --arg img "$image" '{properties: {template: (.properties.template
		| .containers |= map(.image = $img)
		| if .initContainers then .initContainers |= map(.image = $img) else . end)}}' > "$body"
az rest --method patch --url "https://management.azure.com${job_id}?api-version=2024-03-01" --body "@$body" -o none

current="$(az containerapp job show -g "$RESOURCE_GROUP" -n "$RUNNER_JOB" \
	--query "join(',', [properties.template.containers[].image, properties.template.initContainers[].image][])" -o tsv)"
case ",$current," in
	*",$image,"*) ;;
	*) log "runner job still references $current"; exit 1 ;;
esac
if printf '%s' "$current" | tr ',' '\n' | grep -vqxF "$image"; then
	log "runner job images are mixed: $current"; exit 1
fi
log "runner job now uses $image"

# Keep the newest few runner images; the Basic registry is capped at 10 GiB.
az acr run --registry "$REGISTRY" \
	--cmd "acr purge --filter 'lattice-ci-runner:.*' --ago 0d --keep $KEEP_IMAGES --untagged" /dev/null
log "done"
