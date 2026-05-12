# syntax=docker/dockerfile:1.7
# Build context: repo root.
#
# Stage 1 publishes the Blazor WebAssembly app (static files under wwwroot/).
# Stage 2 serves it from nginx:alpine. The published `wwwroot/appsettings.json`
# is overwritten so the in-browser app talks to the API on its host-mapped port
# (the browser sees localhost:8080, not the docker-compose hostname).

FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src

COPY VehicleFleetSimulator.slnx ./
COPY src/VehicleFleetSimulator.Ui/ src/VehicleFleetSimulator.Ui/

RUN dotnet restore src/VehicleFleetSimulator.Ui/VehicleFleetSimulator.Ui.csproj
RUN dotnet publish src/VehicleFleetSimulator.Ui/VehicleFleetSimulator.Ui.csproj \
    -c Release -o /publish /p:UseAppHost=false

# Bake the docker-compose API URL into the published SPA. Because Blazor WASM
# runs in the browser, this URL must be what the *user's browser* can reach -
# i.e. the host-side port mapping, not an in-network compose service name.
RUN printf '{\n  "Api": {\n    "BaseAddress": "http://localhost:8080"\n  }\n}\n' \
    > /publish/wwwroot/appsettings.json

FROM nginx:alpine AS runtime
COPY --from=build /publish/wwwroot/ /usr/share/nginx/html/
COPY src/VehicleFleetSimulator.Ui/nginx.conf /etc/nginx/conf.d/default.conf
EXPOSE 80
