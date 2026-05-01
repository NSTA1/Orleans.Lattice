# syntax=docker/dockerfile:1.7
#
# Build context: lattice repo root (the directory containing src/lattice/, src/lattice.replication/,
# samples/VehicleFleetSimulator/, and benchmark/). benchmark/docker-compose.yml sets context: ..
# so this Dockerfile sees both the lattice projects and the simulator's Grains/Abstractions.

FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src

# Lattice core + replication.
COPY src/lattice/ src/lattice/
COPY src/lattice.replication/ src/lattice.replication/
COPY src/lattice.replication.grpc/ src/lattice.replication.grpc/

# Simulator's reused projects (unmodified).
COPY samples/VehicleFleetSimulator/src/VehicleFleetSimulator.Abstractions/ samples/VehicleFleetSimulator/src/VehicleFleetSimulator.Abstractions/
COPY samples/VehicleFleetSimulator/src/VehicleFleetSimulator.Grains/        samples/VehicleFleetSimulator/src/VehicleFleetSimulator.Grains/

# Benchmark host projects.
COPY benchmark/host/Bench.Sink/ benchmark/host/Bench.Sink/
COPY benchmark/host/Bench.Silo/ benchmark/host/Bench.Silo/

RUN dotnet restore benchmark/host/Bench.Silo/VehicleFleetSimulator.Benchmark.Silo.csproj
RUN dotnet publish benchmark/host/Bench.Silo/VehicleFleetSimulator.Benchmark.Silo.csproj \
    -c Release -o /app /p:UseAppHost=false

FROM mcr.microsoft.com/dotnet/aspnet:10.0 AS runtime
WORKDIR /app
COPY --from=build /app ./

# Orleans silo + gateway endpoints.
EXPOSE 11111 30000
# Prometheus exporter.
EXPOSE 9090
# Replication gRPC receiver (HTTP/2). Only listened on when Replication:GrpcServerEnabled=true.
EXPOSE 5001

ENV DOTNET_RUNNING_IN_CONTAINER=true

ENTRYPOINT ["dotnet", "VehicleFleetSimulator.Benchmark.Silo.dll"]
