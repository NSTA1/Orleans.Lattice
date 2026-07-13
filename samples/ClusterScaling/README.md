# ClusterScaling - autoscaling on Azure Container Apps

A deployable, end-to-end sample that runs a real multi-silo Orleans.Lattice
cluster on **Azure Container Apps (ACA)** and proves the
`Orleans.Lattice.Scaling` autoscaling signal drives **KEDA replica scale-out on
the compute axis**. A bundled .NET load driver generates compute-axis pressure,
and the deploy tooling wires an ACA KEDA `metrics-api` scale rule to the
`/lattice/scale` signal so ACA adds replicas under load and removes them
afterwards.

This is the "does it work in anger" capstone for the autoscaling-signal work
(issue #1216, epic #1190). It **consumes** the shipped scaling surface
(`MapLatticeScalingSignal`, `AddLatticeScalingSignal`, the health check, and the
reference ACA scale rule from `Orleans.Lattice.Scaling`); it does not redefine
them.

## The two axes (read this first)

Orleans.Lattice's scaling signal reports two axes:

- **Compute axis** - activation and dispatch pressure (CPU, activation counts).
  This is the **only** axis wired to replica count. When compute pressure rises,
  `scaleValue` climbs and the autoscaler adds replicas.
- **Storage axis** - retained WAL bytes. This is **advisory**: it feeds
  observability and the health check, and it never inflates replica count.
  Relieving storage pressure is an operational action (rebalancing WAL
  partitions), not an autoscaling one.

**This sample drives the compute axis.** The load driver issues a high op rate
across many distinct trees and keys with a tiny fixed payload, so it grows
activation + dispatch pressure without growing retained bytes. Bulk-loading
large values would move the storage axis and KEDA would never scale - so the
driver keeps payloads deliberately small.

## Architecture

```mermaid
flowchart LR
    subgraph WS["Your workstation"]
        LD["drive-load.ps1 -&gt; LoadDriver<br/>(compute load)"]
        POLL["az poll"]
    end

    subgraph ACA["Azure Container Apps environment"]
        subgraph APP["Container app (1..N replicas)"]
            API["data API gRPC<br/>(Basic-gated)"]
            SCALE["/lattice/scale<br/>(scrape target)"]
            SILO["Orleans silo<br/>(Azure clustering)"]
        end
        KEDA["KEDA metrics-api"]
    end

    STORE[("Azure Storage - Tables, managed identity<br/>clustering, reminders, grain state, WAL")]

    LD -->|"gRPC + Basic over managed TLS"| API
    POLL -->|"reads replica count"| APP
    KEDA -->|"reads scaleValue"| SCALE
    KEDA -->|"sets replica count"| APP
    SILO --> STORE
```

- **Silo host** (`src/ClusterScaling.Silo`) - one container image, run as many
  ACA replicas. Each replica joins a genuine Orleans cluster over **Azure
  Storage clustering (managed identity)**, persists grain state and the Lattice
  **WAL** to **Azure Table storage (managed identity)**, and co-hosts:
  - the write-capable **data API gRPC** surface, gated by a Basic admin
    credential whose salted PBKDF2 hash arrives as an ACA **secret**; and
  - the `/lattice/scale` HTTP signal endpoint the KEDA scale rule scrapes, plus
    `/healthz` and `/readyz` health endpoints.
- **Load driver** (`src/ClusterScaling.LoadDriver`) - a small .NET console that
  speaks gRPC to the data API over TLS, presents the admin Basic credential, and
  drives sustained compute-axis load, printing offered-load throughput.
- **Deploy tooling** (`deploy/`) - `main.bicep` plus `deploy.ps1`,
  `drive-load.ps1`, and `teardown.ps1`.

## Credential and TLS posture

- The operator supplies a **plaintext** admin password to `deploy.ps1` (as a
  `SecureString`). The script hashes it with the repository's
  `tools/New-LatticeStateCredential.ps1` helper (salted PBKDF2-SHA256) and passes
  only the **hash** to the bicep template.
- The bicep injects the hash as a container-app **secret**, surfaced through the
  `LATTICE_DATA_USER_<admin>` environment variable the data-API authorizer reads.
  The plaintext is never stored, never baked into the image, and never passed on
  a command line.
- The data-API `BasicAdminDataApiAuthorizer` verifies the inbound
  `authorization: Basic base64(user:pass)` header against that hash in constant
  time. An anonymous or wrong-password call is rejected with `PermissionDenied`.
- Basic-over-cleartext would be unsafe on its own. It is legitimate here because
  **ACA terminates TLS at its managed ingress**: the credential rides an
  encrypted HTTP/2 channel from the driver to the ingress, and the container is
  reachable only through that ingress. This is the upgrade over the localhost
  `PasswordProtection` sample, which has no transport encryption.

## Prerequisites

- An Azure subscription and `az login`, on an account that can **create role
  assignments** (the deploy assigns *Storage Table Data Contributor* to the
  app's managed identity).
- Azure CLI with the `containerapp` extension (`deploy.ps1` installs/updates it).
- The .NET SDK (net10.0) to build the silo image and run the load driver.
- A container registry the ACA environment can pull from (e.g. Azure Container
  Registry), and a built-and-pushed silo image.
- PowerShell 7+.

## Build and push the silo image

The deploy consumes a container image; build and push it first. A minimal
Dockerfile alongside the silo project:

```dockerfile
# samples/ClusterScaling/src/ClusterScaling.Silo/Dockerfile
FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src
COPY . .
RUN dotnet publish samples/ClusterScaling/src/ClusterScaling.Silo/ClusterScaling.Silo.csproj -c Release -o /app

FROM mcr.microsoft.com/dotnet/aspnet:10.0
WORKDIR /app
COPY --from=build /app .
ENV CLUSTERSCALING_HTTP_PORT=8080
EXPOSE 8080
ENTRYPOINT ["dotnet", "Orleans.Lattice.Samples.ClusterScaling.Silo.dll"]
```

Build the image from the **repository root** (the publish copies the whole repo
so the `ProjectReference`s to `src/` resolve) and push it to your registry:

```powershell
az acr build --registry <myregistry> --image clusterscaling-silo:latest `
  --file samples/ClusterScaling/src/ClusterScaling.Silo/Dockerfile .
```

## Deploy

```powershell
cd samples/ClusterScaling/deploy
$pw = Read-Host -AsSecureString -Prompt 'Admin password'
./deploy.ps1 `
  -ResourceGroup rg-clusterscaling `
  -Location eastus `
  -ContainerImage <myregistry>.azurecr.io/clusterscaling-silo:latest `
  -AdminPassword $pw `
  -MinReplicas 1 -MaxReplicas 10
```

`deploy.ps1` is idempotent. It provisions the managed identity, the Tables-only
storage account (shared-key access disabled), the role assignment, the Log
Analytics workspace, the Container Apps environment, and the container app with
the KEDA `metrics-api` scale rule (`valueLocation: scaleValue`, `targetValue: 1`,
`minReplicas`/`maxReplicas`). It prints the ingress FQDN and the exact
`drive-load.ps1` command to run next.

> If your registry needs credentials for ACA to pull, configure the container
> app's registry after the first deploy (`az containerapp registry set ...`) or
> grant the app's managed identity `AcrPull` on the registry. Public images and
> ACR-with-managed-identity need no extra step.

## Drive load and observe scale-out

```powershell
$pw = Read-Host -AsSecureString -Prompt 'Admin password'
./drive-load.ps1 `
  -ResourceGroup rg-clusterscaling `
  -AdminPassword $pw `
  -Rate 2000 -Duration 300
```

`drive-load.ps1` resolves the ingress FQDN, launches the bundled LoadDriver
(compute-axis load), and - while it runs - polls `az containerapp replica list`
to print a **replica-count timeline** interleaved with the driver's continuous
offered-load throughput. Example shape:

```
Replica-count timeline (offered-load lines come from the driver):
  [t=    0s] replicas = 1
  [t=   10s] replicas = 1
    t=  10.0s  offered=    20,000  offered/s=    2,000  completed=    19,880 ...
  [t=   40s] replicas = 3
  [t=   70s] replicas = 6
  ...
```

**Timing expectations.** Scale-out **lags** the load by tens of seconds. Three
delays stack between offered load and a new replica:

1. the KEDA polling interval (ACA default 30s),
2. the KEDA scale-down cooldown / stabilization window, and
3. the signal's producer-side EWMA smoothing.

That is by design (it prevents replica thrashing). Sustain the load past the
window - the 5 minute default is comfortable - then watch the count settle back
toward `minReplicas` after the driver stops:

```powershell
az containerapp replica list -g rg-clusterscaling -n <app> --query 'length(@)' -o tsv
```

The `minReplicas` floor keeps the scrape target reachable; the `maxReplicas`
ceiling is the hard cap the autoscaler never exceeds regardless of how high
`scaleValue` climbs.

## Verify the credential gate

An anonymous or wrong-password call to the data API is rejected. The load driver
fails fast with a clear message if `-AdminPassword` does not match what
`deploy.ps1` hashed into the secret, so a mismatch surfaces immediately rather
than as silent zero throughput.

## Teardown

```powershell
./teardown.ps1 -ResourceGroup rg-clusterscaling
```

Deletes the whole resource group. An idle deployment is **not free** even at
`minReplicas=1`: the always-on replica bills vCPU + memory per second, Log
Analytics bills for ingested logs, and the storage account bills for the tables
it retains. Tear down as soon as an experiment finishes.

## When to use / when not to use

**Use this sample when you want to:**

- See the compute-axis scaling signal drive real horizontal scale-out on live
  infrastructure, end to end.
- Copy a correct managed-identity ACA wiring for a multi-silo Lattice cluster
  (Azure clustering + reminders + grain state + WAL, no keys or connection
  strings) and a KEDA `metrics-api` scale rule against `/lattice/scale`.
- Understand the Basic-over-managed-TLS credential posture for the write-capable
  data API and the ACA-secret hash injection.

**Do not use this sample when:**

- You want to scale on the storage axis. It is advisory and never wired to
  replica count; relieving WAL pressure is a rebalancing action, not an
  autoscaling one.
- You need a local, dependency-free demo. Start with `HelloWorld` or, for the
  credential mechanism alone, `PasswordProtection` (single in-process silo, no
  Azure).
- You want a production deployment blueprint verbatim. This is a teaching
  sample: it uses external ingress so you can drive load from your workstation,
  a single storage account, and permissive storage network ACLs. A production
  deployment would scope ingress (IP allow-list or internal), separate the WAL
  account from clustering, and lock down the storage firewall.

## Layout

```
samples/ClusterScaling/
  README.md
  src/
    ClusterScaling.Silo/          # multi-silo host: clustering + WAL + data API gRPC + scale signal
      Program.cs
      BasicAdminDataApiAuthorizer.cs
      ClusterScaling.Silo.csproj
    ClusterScaling.LoadDriver/    # compute-axis gRPC load generator
      Program.cs
      LoadDriverOptions.cs
      ClusterScaling.LoadDriver.csproj
  deploy/
    main.bicep                    # identity, storage, role, ACA env + app, KEDA scale rule
    deploy.ps1                    # hash password, provision, print FQDN (idempotent)
    drive-load.ps1                # run LoadDriver + poll replica timeline
    teardown.ps1                  # delete the resource group
```
