# Build context: repository root.
# Example: docker build -f benchmark/azure-throughput/Producer/Dockerfile .
FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src
COPY . .
RUN dotnet publish benchmark/azure-throughput/Producer/VehicleFleetSimulator.AzureThroughput.Producer.csproj -c Release -o /app/publish

FROM mcr.microsoft.com/dotnet/runtime:10.0 AS runtime
WORKDIR /app
COPY --from=build /app/publish .
ENTRYPOINT ["dotnet", "VehicleFleetSimulator.AzureThroughput.Producer.dll"]
