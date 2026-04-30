# syntax=docker/dockerfile:1.7
# Build context: repo root (so the .slnx and all referenced projects are visible).

FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src

COPY VehicleFleetSimulator.slnx ./
COPY src/VehicleFleetSimulator.Abstractions/ src/VehicleFleetSimulator.Abstractions/
COPY src/VehicleFleetSimulator.Api/ src/VehicleFleetSimulator.Api/

RUN dotnet restore src/VehicleFleetSimulator.Api/VehicleFleetSimulator.Api.csproj
RUN dotnet publish src/VehicleFleetSimulator.Api/VehicleFleetSimulator.Api.csproj \
    -c Release -o /app /p:UseAppHost=false

FROM mcr.microsoft.com/dotnet/aspnet:10.0 AS runtime
WORKDIR /app
COPY --from=build /app ./

EXPOSE 8080 8081

ENV DOTNET_RUNNING_IN_CONTAINER=true

ENTRYPOINT ["dotnet", "VehicleFleetSimulator.Api.dll"]
