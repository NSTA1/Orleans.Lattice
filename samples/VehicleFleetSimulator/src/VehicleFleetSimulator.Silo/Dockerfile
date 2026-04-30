# syntax=docker/dockerfile:1.7
# Build context: repo root (so the .slnx and all referenced projects are visible).

FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src

COPY VehicleFleetSimulator.slnx ./
COPY src/VehicleFleetSimulator.Abstractions/ src/VehicleFleetSimulator.Abstractions/
COPY src/VehicleFleetSimulator.Grains/ src/VehicleFleetSimulator.Grains/
COPY src/VehicleFleetSimulator.Silo/ src/VehicleFleetSimulator.Silo/

RUN dotnet restore src/VehicleFleetSimulator.Silo/VehicleFleetSimulator.Silo.csproj
RUN dotnet publish src/VehicleFleetSimulator.Silo/VehicleFleetSimulator.Silo.csproj \
    -c Release -o /app /p:UseAppHost=false

FROM mcr.microsoft.com/dotnet/runtime:10.0 AS runtime
WORKDIR /app
COPY --from=build /app ./

# Orleans silo + gateway endpoints (exposed for inter-silo / client connectivity).
EXPOSE 11111 30000

ENV DOTNET_RUNNING_IN_CONTAINER=true \
    DOTNET_SYSTEM_GLOBALIZATION_INVARIANT=false

ENTRYPOINT ["dotnet", "VehicleFleetSimulator.Silo.dll"]
