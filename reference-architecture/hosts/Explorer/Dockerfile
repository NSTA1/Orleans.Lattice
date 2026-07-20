# syntax=docker/dockerfile:1
# ---------------------------------------------------------------------------
# Reference-architecture Explorer head image.
#
# Multi-stage, framework-dependent build:
#   * build stage : mcr.microsoft.com/dotnet/sdk:10.0
#   * final stage : mcr.microsoft.com/dotnet/aspnet:10.0-noble-chiseled
#
# The chiseled ASP.NET runtime is distroless, shell-less, and runs as a non-root
# user by default. Health probes MUST be HTTP/TCP (the /health endpoint), not
# shell exec probes - there is no shell in the final image. The explorer persists
# its JSON config to a writable path (LATTICE_EXPLORER_CONFIG below), since the
# chiseled non-root image has no writable per-user app-data directory.
#
# InvariantGlobalization: ENABLED (ICU dropped). See
# reference-architecture/hosts/README.md for the ordinal-only audit. For this
# Blazor Server head the residual culture-sensitivity is display-only formatting;
# tree keys and access decisions are compared ordinally underneath. NativeAOT /
# trimming remain OUT of scope (Orleans / Blazor Server do not support them).
# ---------------------------------------------------------------------------

FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src

COPY hosts/Explorer/Orleans.Lattice.ReferenceArchitecture.Explorer.csproj hosts/Explorer/
RUN dotnet restore hosts/Explorer/Orleans.Lattice.ReferenceArchitecture.Explorer.csproj

COPY hosts/Explorer/ hosts/Explorer/
RUN dotnet publish hosts/Explorer/Orleans.Lattice.ReferenceArchitecture.Explorer.csproj \
    -c Release -o /app --no-restore

# ---------------------------------------------------------------------------
FROM mcr.microsoft.com/dotnet/aspnet:10.0-noble-chiseled AS final
WORKDIR /app

ENV DOTNET_SYSTEM_GLOBALIZATION_INVARIANT=1
# Writable config backing store for the non-root, read-only-friendly image.
ENV LATTICE_EXPLORER_CONFIG=/tmp/lattice-explorer/config.json

# The chiseled base already runs as the non-root 'app' user (UID 1654).
COPY --from=build /app .

# Blazor Server web console + /health.
EXPOSE 8080

ENTRYPOINT ["dotnet", "Orleans.Lattice.ReferenceArchitecture.Explorer.dll"]
