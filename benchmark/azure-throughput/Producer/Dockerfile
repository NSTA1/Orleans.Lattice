# syntax=docker/dockerfile:1.7
#
# Build context: lattice repo root.
# Azure throughput benchmark harness - workload producer image.

FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src

COPY samples/VehicleFleetSimulator/src/VehicleFleetSimulator.Abstractions/ samples/VehicleFleetSimulator/src/VehicleFleetSimulator.Abstractions/
COPY benchmark/azure-throughput/Producer/ benchmark/azure-throughput/Producer/

RUN dotnet restore benchmark/azure-throughput/Producer/VehicleFleetSimulator.AzureThroughput.Producer.csproj
RUN dotnet publish benchmark/azure-throughput/Producer/VehicleFleetSimulator.AzureThroughput.Producer.csproj \
    -c Release -o /app /p:UseAppHost=false

FROM mcr.microsoft.com/dotnet/runtime:10.0 AS runtime
WORKDIR /app
COPY --from=build /app ./

ENV DOTNET_RUNNING_IN_CONTAINER=true

ENTRYPOINT ["dotnet", "VehicleFleetSimulator.AzureThroughput.Producer.dll"]
