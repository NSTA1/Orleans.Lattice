# syntax=docker/dockerfile:1
# ---------------------------------------------------------------------------
# Local-dev Explorer head image (project-reference build).
#
# References the Orleans.Lattice.Explorer.* library projects directly from src/**
# (not NuGet), so it always builds against the working tree. The build context is
# the REPOSITORY ROOT (see docker-compose.yml `context: ../..`) so src/ and the
# shared reference-architecture/hosts/Common helper are visible.
#
# Chiseled final stage: distroless, shell-less, non-root; HTTP/TCP health probes
# only. InvariantGlobalization enabled (reinforced by the env var below).
# ---------------------------------------------------------------------------

FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /repo

COPY src/ src/
COPY reference-architecture/hosts/Common/ reference-architecture/hosts/Common/
COPY reference-architecture/local-dev/hosts/Explorer/ reference-architecture/local-dev/hosts/Explorer/

# Restore third-party packages only (Orleans.Lattice.* are project references from
# src/**). Optional BuildKit secret 'nugetcfg' overrides the tracked default feed
# for private/offline feeds; used only for restore, never baked into a layer.
COPY reference-architecture/local-dev/nuget.config nuget.config
RUN --mount=type=secret,id=nugetcfg \
    if [ -f /run/secrets/nugetcfg ]; then cp /run/secrets/nugetcfg nuget.config; fi && \
    dotnet restore reference-architecture/local-dev/hosts/Explorer/Orleans.Lattice.ReferenceArchitecture.Explorer.csproj
RUN dotnet publish reference-architecture/local-dev/hosts/Explorer/Orleans.Lattice.ReferenceArchitecture.Explorer.csproj \
    -c Release -o /app --no-restore

# ---------------------------------------------------------------------------
FROM mcr.microsoft.com/dotnet/aspnet:10.0-noble-chiseled AS final
WORKDIR /app

ENV DOTNET_SYSTEM_GLOBALIZATION_INVARIANT=1

COPY --from=build /app .

# HTTP port (Blazor console + /health). Bound to 8082 by configuration so it can
# share the silo's network namespace and reach the state API over loopback:8081.
EXPOSE 8082

ENTRYPOINT ["dotnet", "Orleans.Lattice.ReferenceArchitecture.Explorer.dll"]
