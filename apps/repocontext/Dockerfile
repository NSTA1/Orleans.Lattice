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
# The runtime stage is the distroless, shell-less, non-root chiseled ASP.NET
# image. There is no shell in the final image, so health probing is HTTP-only
# (GET /health/live and GET /health/ready) - there is deliberately no
# shell-exec HEALTHCHECK. Orchestrate liveness/readiness with an HTTP probe
# (Kubernetes httpGet, an external checker, or a sidecar).
# ---------------------------------------------------------------------------

# --- Build stage -----------------------------------------------------------
FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src

# Copy the whole repo (the .dockerignore trims bin/obj/test/docs/.git so the
# context stays small) and publish the host. The host project pulls in only the
# src/ projects it references - no gRPC facade, no Explorer.
COPY . .

RUN dotnet publish apps/repocontext/Orleans.Lattice.Api.Mcp.RepoContext.Host.csproj \
    -c Release \
    -o /app/publish \
    /p:UseAppHost=false

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
ENV LATTICE_DATA_ROOT=/data
VOLUME ["/data"]

# The MCP listener port (the container's only application listener).
ENV LATTICE_MCP_PORT=8080
EXPOSE 8080

# The chiseled ASP.NET image already runs as a non-root user (UID 64198). No
# shell, no package manager, no setuid binaries.
ENTRYPOINT ["dotnet", "Orleans.Lattice.Api.Mcp.RepoContext.Host.dll"]
