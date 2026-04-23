# syntax=docker/dockerfile:1.7
#
# MultiSiteManufacturing sample host image.
#
# Build context MUST be the repository root: the Host csproj has a
# ProjectReference to ../../../../src/lattice/Orleans.Lattice.csproj and
# that path only resolves when both trees are present in the build
# context. docker-compose.yml pins context to ../../ (repo root).
#
# The same image is used for all four silos - they differ only by the
# CLUSTER_NAME + SILO_ID environment variables supplied by Compose.

FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src

# Copy only what the Host csproj transitively needs, so the build layer
# cache invalidates on meaningful changes only.
COPY src/lattice/ ./src/lattice/
COPY samples/MultiSiteManufacturing/src/ ./samples/MultiSiteManufacturing/src/

RUN dotnet publish \
    samples/MultiSiteManufacturing/src/MultiSiteManufacturing.Host/MultiSiteManufacturing.Host.csproj \
    -c Release \
    -o /app \
    --nologo

FROM mcr.microsoft.com/dotnet/aspnet:10.0 AS runtime
WORKDIR /app
COPY --from=build /app ./

# Fixed in-container ports. Host-side publishing (5001..5004) is handled
# by docker-compose.yml; inside the container everything is 8080 HTTP,
# 11111 Orleans silo, 30000 Orleans gateway. Because each silo is its
# own container with its own IP, A/B no longer need different ports.
EXPOSE 8080
EXPOSE 11111
EXPOSE 30000

ENV ASPNETCORE_URLS=http://+:8080 \
    DOTNET_EnableDiagnostics=0 \
    DOTNET_PRINT_TELEMETRY_MESSAGE=false

# Drop to the pre-created non-root `app` user baked into the ASP.NET
# Core runtime image (UID exposed via the $APP_UID env var, currently
# 1654). The image exposes port 8080 instead of 80 precisely so this
# user can bind it without CAP_NET_BIND_SERVICE. /app is COPY'd as
# root but is world-readable, which is all the runtime needs.
USER $APP_UID

ENTRYPOINT ["dotnet", "MultiSiteManufacturing.Host.dll"]

