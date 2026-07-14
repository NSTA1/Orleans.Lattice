# ClusterScaling silo image. Build from the REPOSITORY ROOT so the source of the
# referenced src/ projects is in the build context:
#
#   az acr build --registry <myregistry> --image clusterscaling-silo:latest \
#     --file samples/ClusterScaling/src/ClusterScaling.Silo/Dockerfile .
#
# or, with a local Docker daemon:
#
#   docker build -f samples/ClusterScaling/src/ClusterScaling.Silo/Dockerfile \
#     -t clusterscaling-silo:latest .
#
# deploy.ps1 orchestrates the az acr build for you. The repo-root .dockerignore
# strips bin/obj, tests, docs, and scratch so only source is uploaded/copied.

FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src

# Copy only what the silo build needs: the root build props and the source of
# the src/ projects (the whole src/ tree, so transitive ProjectReferences
# resolve without enumerating the exact chain here) plus the sample sources.
COPY Directory.Build.targets ./
COPY src/ src/
COPY samples/ClusterScaling/src/ samples/ClusterScaling/src/

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
