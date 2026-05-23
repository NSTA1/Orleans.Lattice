# syntax=docker/dockerfile:1.7
#
# Build context: lattice repo root.
# Azure throughput benchmark harness - single-silo lattice host image.

FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src

COPY src/lattice/                       src/lattice/
COPY src/lattice.replication/           src/lattice.replication/
COPY src/lattice.replication.grpc/      src/lattice.replication.grpc/
COPY src/lattice.storage.azuretable/    src/lattice.storage.azuretable/

COPY samples/VehicleFleetSimulator/src/VehicleFleetSimulator.Abstractions/ samples/VehicleFleetSimulator/src/VehicleFleetSimulator.Abstractions/

COPY benchmark/azure-throughput/Silo/ benchmark/azure-throughput/Silo/

RUN dotnet restore benchmark/azure-throughput/Silo/VehicleFleetSimulator.AzureThroughput.Silo.csproj
RUN dotnet publish benchmark/azure-throughput/Silo/VehicleFleetSimulator.AzureThroughput.Silo.csproj \
    -c Release -o /app /p:UseAppHost=false

FROM mcr.microsoft.com/dotnet/aspnet:10.0 AS runtime
WORKDIR /app
COPY --from=build /app ./

# TCP ingest port (producer -> silo).
EXPOSE 7000

ENV DOTNET_RUNNING_IN_CONTAINER=true \
    ASPNETCORE_URLS=http://+:8080

ENTRYPOINT ["dotnet", "VehicleFleetSimulator.AzureThroughput.Silo.dll"]
