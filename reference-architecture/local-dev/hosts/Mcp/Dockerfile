# syntax=docker/dockerfile:1
# ---------------------------------------------------------------------------
# Local-dev MCP head image (project-reference build).
#
# References the Orleans.Lattice.Api.Mcp* library projects directly from src/**
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
COPY reference-architecture/local-dev/hosts/Mcp/ reference-architecture/local-dev/hosts/Mcp/

# Restore third-party packages only (Orleans.Lattice.* are project references from
# src/**). Optional BuildKit secret 'nugetcfg' overrides the tracked default feed
# for private/offline feeds; used only for restore, never baked into a layer.
COPY reference-architecture/local-dev/nuget.config nuget.config
RUN --mount=type=secret,id=nugetcfg \
    if [ -f /run/secrets/nugetcfg ]; then cp /run/secrets/nugetcfg nuget.config; fi && \
    dotnet restore reference-architecture/local-dev/hosts/Mcp/Orleans.Lattice.ReferenceArchitecture.Mcp.csproj
RUN dotnet publish reference-architecture/local-dev/hosts/Mcp/Orleans.Lattice.ReferenceArchitecture.Mcp.csproj \
    -c Release -o /app --no-restore

# ---------------------------------------------------------------------------
FROM mcr.microsoft.com/dotnet/aspnet:10.0-noble-chiseled AS final
WORKDIR /app

ENV DOTNET_SYSTEM_GLOBALIZATION_INVARIANT=1

COPY --from=build /app .

# HTTP port (MCP JSON-RPC over Streamable HTTP + /health).
EXPOSE 8080

ENTRYPOINT ["dotnet", "Orleans.Lattice.ReferenceArchitecture.Mcp.dll"]
