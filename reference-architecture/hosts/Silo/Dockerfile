# syntax=docker/dockerfile:1
# ---------------------------------------------------------------------------
# Reference-architecture Silo image.
#
# Multi-stage, framework-dependent build:
#   * build stage : mcr.microsoft.com/dotnet/sdk:10.0
#   * final stage : mcr.microsoft.com/dotnet/aspnet:10.0-noble-chiseled
#
# The chiseled ASP.NET runtime is distroless and shell-less: there is no shell,
# no package manager, and the image runs as a non-root user by default. Health
# probes MUST therefore be HTTP/TCP (the /health endpoint on the HTTP port), not
# shell exec probes - there is no shell to exec.
#
# InvariantGlobalization: ENABLED (ICU dropped).
#   Audit (recorded in reference-architecture/hosts/README.md): the Lattice core
#   is overwhelmingly StringComparer.Ordinal (487 ordinal / OrdinalIgnoreCase
#   sites). The only culture-sensitive operations across the whole surface these
#   hosts touch are a handful of ToLowerInvariant() calls, all on guaranteed-ASCII
#   inputs (lowercased gRPC header names, enum names, a boolean-ish config token,
#   and hex digest strings), plus number formatting that explicitly pins
#   CultureInfo.InvariantCulture. ASCII invariant casing and invariant number
#   formatting do not depend on ICU, so running with InvariantGlobalization=true (set in the
#   .csproj and reinforced by the env var below) is correctness-safe and shrinks
#   the image. NativeAOT / aggressive trimming remain OUT of scope (Orleans does
#   not support them).
# ---------------------------------------------------------------------------

FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src

# Restore against the pinned project first for layer caching. The shared hosting
# helper library is a project reference, so its csproj must be present to restore.
COPY hosts/Common/Orleans.Lattice.ReferenceArchitecture.Hosting.csproj hosts/Common/
COPY hosts/Silo/Orleans.Lattice.ReferenceArchitecture.Silo.csproj hosts/Silo/
RUN dotnet restore hosts/Silo/Orleans.Lattice.ReferenceArchitecture.Silo.csproj

COPY hosts/Common/ hosts/Common/
COPY hosts/Silo/ hosts/Silo/
RUN dotnet publish hosts/Silo/Orleans.Lattice.ReferenceArchitecture.Silo.csproj \
    -c Release -o /app --no-restore

# ---------------------------------------------------------------------------
FROM mcr.microsoft.com/dotnet/aspnet:10.0-noble-chiseled AS final
WORKDIR /app

# Reinforce the InvariantGlobalization decision at the runtime layer.
ENV DOTNET_SYSTEM_GLOBALIZATION_INVARIANT=1

# The chiseled base already runs as the non-root 'app' user (UID 1654).
COPY --from=build /app .

# HTTP port (health + scaling signal) and gRPC port (state/auth/replication).
EXPOSE 8080
EXPOSE 8081

ENTRYPOINT ["dotnet", "Orleans.Lattice.ReferenceArchitecture.Silo.dll"]
