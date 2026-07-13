# ClusterScaling silo image. Build from the REPOSITORY ROOT so the
# ProjectReferences to src/ resolve:
#
#   az acr build --registry <myregistry> --image clusterscaling-silo:latest \
#     --file samples/ClusterScaling/src/ClusterScaling.Silo/Dockerfile .
#
# or, with a local Docker daemon:
#
#   docker build -f samples/ClusterScaling/src/ClusterScaling.Silo/Dockerfile \
#     -t clusterscaling-silo:latest .

FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src
COPY . .
RUN dotnet publish samples/ClusterScaling/src/ClusterScaling.Silo/ClusterScaling.Silo.csproj \
    -c Release -o /app

FROM mcr.microsoft.com/dotnet/aspnet:10.0
WORKDIR /app
COPY --from=build /app .

# ACA ingress targetPort. The silo also reads CLUSTERSCALING_TABLE_URI (required)
# and the LATTICE_DATA_USER_<admin> credential hash secret, injected by the
# container app the bicep provisions.
ENV CLUSTERSCALING_HTTP_PORT=8080
EXPOSE 8080

ENTRYPOINT ["dotnet", "Orleans.Lattice.Samples.ClusterScaling.Silo.dll"]
