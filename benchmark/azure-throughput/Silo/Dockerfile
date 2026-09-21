# Build context: repository root.
# Example: docker build -f benchmark/azure-throughput/Silo/Dockerfile .
FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src
COPY . .
RUN dotnet publish benchmark/azure-throughput/Silo/VehicleFleetSimulator.AzureThroughput.Silo.csproj -c Release -o /app/publish

FROM mcr.microsoft.com/dotnet/aspnet:10.0 AS runtime
WORKDIR /app
EXPOSE 11111
EXPOSE 30000
COPY --from=build /app/publish .
ENTRYPOINT ["dotnet", "VehicleFleetSimulator.AzureThroughput.Silo.dll"]
