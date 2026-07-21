# syntax=docker/dockerfile:1
# ---------------------------------------------------------------------------
# Reference-architecture MCP head image.
#
# Multi-stage, framework-dependent build:
#   * build stage : mcr.microsoft.com/dotnet/sdk:10.0
#   * final stage : mcr.microsoft.com/dotnet/aspnet:10.0-noble-chiseled
#
# The chiseled ASP.NET runtime is distroless, shell-less, and runs as a non-root
# user by default. Health probes MUST be HTTP/TCP (the /health endpoint), not
# shell exec probes - there is no shell in the final image.
#
# InvariantGlobalization: ENABLED (ICU dropped). See
# reference-architecture/hosts/README.md for the ordinal-only audit. This head
# is a stateless gRPC client + MCP transport with no culture-sensitive
# comparison on any correctness-relevant path. NativeAOT / trimming remain OUT
# of scope (Orleans does not support them).
# ---------------------------------------------------------------------------

FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src

COPY hosts/Common/Orleans.Lattice.ReferenceArchitecture.Hosting.csproj hosts/Common/
COPY hosts/Mcp/Orleans.Lattice.ReferenceArchitecture.Mcp.csproj hosts/Mcp/
RUN dotnet restore hosts/Mcp/Orleans.Lattice.ReferenceArchitecture.Mcp.csproj

COPY hosts/Common/ hosts/Common/
COPY hosts/Mcp/ hosts/Mcp/
RUN dotnet publish hosts/Mcp/Orleans.Lattice.ReferenceArchitecture.Mcp.csproj \
    -c Release -o /app --no-restore

# ---------------------------------------------------------------------------
FROM mcr.microsoft.com/dotnet/aspnet:10.0-noble-chiseled AS final
WORKDIR /app

ENV DOTNET_SYSTEM_GLOBALIZATION_INVARIANT=1

# The chiseled base already runs as the non-root 'app' user (UID 1654).
COPY --from=build /app .

# MCP streamable-HTTP transport + /health.
EXPOSE 8080

ENTRYPOINT ["dotnet", "Orleans.Lattice.ReferenceArchitecture.Mcp.dll"]
