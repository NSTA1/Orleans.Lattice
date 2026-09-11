# syntax=docker/dockerfile:1

# ---------------------------------------------------------------------------
# RepoContext MCP container host - "codebase memory in a box".
#
# A single, restart-durable image that composes the already-shipped Lattice
# seams (core silo + file/Azure WAL + MCP binding + repository-context tools +
# Onyx embedding client + membership/auth) behind an environment-selected
# durability profile. The container's ONLY application listener is the MCP
# endpoint (plus HTTP health probes, and in the azure profile the scaling
# scrape); no gRPC facade and no Explorer UI are bundled.
#
# Build context is the REPOSITORY ROOT (the host project ProjectReferences the
# just-built src/ bits rather than NuGet packages), so build from the repo root:
#
#   docker build -f apps/repocontext/Dockerfile -t repocontext-mcp:local .
#
# Behind a private / corporate-proxy / offline NuGet feed, where public
# nuget.org is unreachable, point the restore at your own NuGet.Config with the
# optional BuildKit secret (the same 'nugetcfg' convention the local-dev
# reference architecture uses). Nothing tracked needs editing:
#
#   docker build -f apps/repocontext/Dockerfile -t repocontext-mcp:local \
#     --secret id=nugetcfg,src=$env:APPDATA\NuGet\NuGet.Config .
#
# The cold-start rig's `rig.ps1 build` passes it for you from
# $env:NUGET_CONFIG_FILE.
#
# The runtime stage is the distroless, shell-less, non-root chiseled ASP.NET
# image. There is no shell in the final image, so health probing is HTTP-only
# and a shell-form HEALTHCHECK cannot work. The host instead answers
# --healthcheck by probing its own /health/silo endpoint and exiting 0/1, the
# same self-probe pattern the ONNX embedder image uses (see the exec-form
# HEALTHCHECK below). That endpoint re-exercises silo membership and the grain
# layer on every call, so the probe goes RED for a silo that died after reaching
# readiness while the process kept listening - the outage recorded by issue
# #2666, which a bare "is Kestrel listening" probe (GET /health/live, always
# green) could not detect. The /health/live and /health/ready HTTP probes remain
# for Kubernetes httpGet or an external checker.
# ---------------------------------------------------------------------------

# --- Build stage -----------------------------------------------------------
FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src

# Copy the whole repo (the .dockerignore trims bin/obj/test/docs/.git so the
# context stays small) and publish the host. The host project pulls in only the
# src/ projects it references - no gRPC facade, no Explorer.
COPY . .

# Restore third-party packages (the Orleans.Lattice.* bits are project
# references from src/**, so they are built, not restored). The optional
# BuildKit secret 'nugetcfg' points the restore at a private, corporate-proxy
# or offline NuGet.Config for a host where public nuget.org is unreachable;
# without it the SDK default (nuget.org) is used and this build is byte-for-byte
# what it was before. The secret is read straight from its tmpfs mount via
# --configfile and is never copied into the build context, so a NuGet.Config
# carrying feed credentials is not written into an image layer or the build
# cache.
RUN --mount=type=secret,id=nugetcfg \
    if [ -f /run/secrets/nugetcfg ]; then set -- --configfile /run/secrets/nugetcfg; fi && \
    dotnet restore apps/repocontext/Orleans.Lattice.Api.Mcp.RepoContext.Host.csproj "$@"

RUN dotnet publish apps/repocontext/Orleans.Lattice.Api.Mcp.RepoContext.Host.csproj \
    -c Release \
    -o /app/publish \
    --no-restore \
    /p:UseAppHost=false

# Stage an empty data root here (this stage has a shell) so the runtime stage can
# copy it in with the non-root UID as owner. A fresh named volume mounted at
# /data inherits the ownership of the image directory at that path, so seeding it
# owned by the runtime UID is what lets the non-root process create the WAL and
# SQLite sub-directories on first start.
RUN mkdir -p /data

# --- Runtime stage ---------------------------------------------------------
# Distroless, shell-less, non-root. InvariantGlobalization is set in the csproj
# (the ingest path uses ordinal string comparison only), so the ICU-less chiseled
# image is safe and smaller.
FROM mcr.microsoft.com/dotnet/aspnet:10.0-noble-chiseled AS runtime
WORKDIR /app

COPY --from=build /app/publish ./

# The single, documented, stable mount point for ALL durable local state: the
# file WAL directory and the SQLite database file live under this root. It MUST
# be backed by a bind mount or named volume so state survives docker stop,
# docker rm, and image upgrades. The host fails fast at startup if this path is
# missing or not writable by the non-root runtime UID.
#
# The directory is seeded owned by the non-root runtime UID (the base image's
# $APP_UID) so that a fresh named volume mounted here is writable by the process:
# Docker initializes an empty named volume with the ownership and permissions of
# the image directory at the mount point. Without this the volume would default
# to root:root and the non-root process could not create the WAL/SQLite
# sub-directories. $APP_UID is resolved from the chiseled base image rather than
# hardcoded so it stays correct across base-image UID changes.
COPY --from=build --chown=$APP_UID:$APP_UID /data /data
ENV LATTICE_DATA_ROOT=/data
VOLUME ["/data"]

# The MCP listener port (the container's only application listener).
ENV LATTICE_MCP_PORT=8080
EXPOSE 8080

# The commit this image was built from, stamped INTO THE IMAGE so that what was
# built is readable from the artefact rather than inferred from a checkout.
#
# This exists because the inference is wrong and was believed anyway. A build
# worktree's HEAD tells you what would be built NOW; only the image tells you
# what WAS built. During bucket-4 gate run 4 a deployment gate read a worktree
# HEAD, concluded the running container carried a two-commit delta, and measured
# for eleven hours against an image built 45 commits earlier (issue #2686).
#
# Supplied by the compose `build.args` stanza from a GIT_COMMIT environment
# variable, so an operator building by hand passes it the same way:
#
#     docker build --build-arg GIT_COMMIT=$(git rev-parse HEAD) ...
#
# An unset arg leaves the label EMPTY rather than absent, which
# Assert-ContainerProvenance.ps1 treats as unresolved and refuses on, falling
# back to a `candidate-<sha>` image tag first. Fail-closed is deliberate: a
# provenance instrument that cannot answer must not report success.
ARG GIT_COMMIT=""
LABEL org.opencontainers.image.revision="$GIT_COMMIT"

# The chiseled ASP.NET image already runs as a non-root user (the base image's
# $APP_UID). No shell, no package manager, no setuid binaries.
#
# Exec-form health probe. The chiseled runtime has NO SHELL, so a shell-form
# HEALTHCHECK cannot work; the host answers --healthcheck by probing its own
# /health/silo endpoint and exiting 0 (healthy) or 1 (starting/unhealthy). The
# start period is sized for real silo startup - cluster join plus WAL replay
# warmup - which is far longer than a stateless service's, so a silo still
# joining reads as "starting" rather than a failure and does not crash-loop the
# container under compose's restart: unless-stopped. The compose file restates
# this healthcheck (see samples/RepoContextContainer/docker-compose.yml) so the
# behaviour holds whether the image is run directly or via compose.
HEALTHCHECK --interval=15s --timeout=10s --start-period=180s --retries=5 \
    CMD ["dotnet", "Orleans.Lattice.Api.Mcp.RepoContext.Host.dll", "--healthcheck"]
ENTRYPOINT ["dotnet", "Orleans.Lattice.Api.Mcp.RepoContext.Host.dll"]
