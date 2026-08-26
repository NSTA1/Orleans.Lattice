# syntax=docker/dockerfile:1
# ---------------------------------------------------------------------------
# Local-dev Silo image (project-reference build).
#
# Unlike reference-architecture/hosts/Silo (which restores the Orleans.Lattice.*
# NuGet packages), this local-dev head references the library projects directly
# from src/**, so it always builds against the working tree - no publish/pack step
# is needed to try a change. The build context is therefore the REPOSITORY ROOT
# (see docker-compose.yml `context: ../..`), so both src/ and the shared
# reference-architecture/hosts/Common helper are visible to the build.
#
# Multi-stage, framework-dependent build:
#   * build stage : mcr.microsoft.com/dotnet/sdk:10.0
#   * final stage : mcr.microsoft.com/dotnet/aspnet:10.0-noble-chiseled
#
# The chiseled runtime is distroless and shell-less; health probes MUST be HTTP/TCP
# (the /health endpoint), never shell exec. InvariantGlobalization is enabled (see
# the reference-architecture/hosts README audit); the env var below reinforces it.
# ---------------------------------------------------------------------------

FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /repo

# The head references ~26 projects under src/ plus the shared hosting helper, so
# bring in the whole source tree the project graph can reach. bin/obj are excluded
# by the repo-root .dockerignore.
COPY src/ src/
COPY reference-architecture/hosts/Common/ reference-architecture/hosts/Common/
COPY reference-architecture/local-dev/hosts/Silo/ reference-architecture/local-dev/hosts/Silo/

# Restore third-party packages (Orleans, Grpc, Azure SDK, OpenTelemetry, ...). The
# Orleans.Lattice.* libraries are NOT restored - they are project references built
# from src/** above. An optional BuildKit secret 'nugetcfg' supplies a NuGet.Config
# (e.g. a private feed when public nuget.org is unreachable); without it the tracked
# reference-architecture/local-dev/nuget.config (public nuget.org) is used. The
# secret is used only for restore and never lands in an image layer.
COPY reference-architecture/local-dev/nuget.config nuget.config
RUN --mount=type=secret,id=nugetcfg \
    if [ -f /run/secrets/nugetcfg ]; then cp /run/secrets/nugetcfg nuget.config; fi && \
    dotnet restore reference-architecture/local-dev/hosts/Silo/Orleans.Lattice.ReferenceArchitecture.Silo.csproj
RUN dotnet publish reference-architecture/local-dev/hosts/Silo/Orleans.Lattice.ReferenceArchitecture.Silo.csproj \
    -c Release -o /app --no-restore

# ---------------------------------------------------------------------------
FROM mcr.microsoft.com/dotnet/aspnet:10.0-noble-chiseled AS final
WORKDIR /app

ENV DOTNET_SYSTEM_GLOBALIZATION_INVARIANT=1

# The chiseled base already runs as the non-root 'app' user (UID 1654).
COPY --from=build /app .

# HTTP port (health + scaling signal) and gRPC port (state/auth/replication).
EXPOSE 8080
EXPOSE 8081

ENTRYPOINT ["dotnet", "Orleans.Lattice.ReferenceArchitecture.Silo.dll"]
