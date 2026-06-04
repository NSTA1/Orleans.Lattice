# benchmark/vm - Phase 0 host for wedge-plan2.md

Single Linux VM that replaces ACI as the iteration host for the wedge
reliability investigation. Everything here is automated via Bicep +
PowerShell; no portal clicks required.

## Why this exists

`repro/wedge-orleans/wedge-plan2.md` section 0 catalogues why ACI is
the wrong tool for reliability iteration (log-scraper duplication,
60s `az container logs` buffer, no live attach for `dotnet-dump` /
`dotnet-counters`, cold-start variance, shared NIC, vCPU ceiling).
This folder is the deterministic alternative.

## What it provisions

- 1x Linux VM (Ubuntu 24.04 LTS), default `Standard_D2as_v5`
  - smallest SKU that supports **accelerated networking** (the B-series
	does not; the D-family 2 vCPU is the minimum)
  - AMD 2 vCPU / 8 GiB, Premium SSD OS disk
- VNet + subnet, Standard public IP with stable DNS label
- NSG that allows **only** TCP/22 from a single source address (your
  current public IP, auto-detected at deploy time) and denies
  everything else inbound
- SSH **public-key auth only**, password auth disabled
- Auto-shutdown schedule at **19:00 UTC daily** (DevTestLab schedule)
- StorageV2 account (Standard_LRS), shared-key auth **disabled**, TLS 1.2 min, no public blob access
- Role assignments granting the VM's system-assigned managed identity
  **Storage Table/Blob/Queue Data Contributor** on that storage account
  (no keys, no connection strings — the silo authenticates via IMDS)

## Files

| File | Purpose |
|---|---|
| `main.bicep` | Resource definitions (VM, NIC, NSG, VNet, PIP, auto-shutdown, storage, role assignments). |
| `cloud-init.yaml` | First-boot bootstrap (installs .NET 10 SDK + `dotnet-dump`/`dotnet-counters`/`dotnet-trace`/`dotnet-gcdump`, rsync, creates `/opt/lattice`). |
| `lattice-silo.service` | systemd unit template for the azure-throughput silo; placeholders are filled in by `update-vm.ps1`. |
| `lattice-producer.service` | systemd unit template for the producer; runs on `127.0.0.1:7000` against the local silo. |
| `bootstrap.sh` | Manual recovery / single source of truth for the VM bootstrap (also auto-invoked by `update-vm.ps1` if dotnet missing). |
| `vm.parameters.ps1` | Default parameter values, **committed**. |
| `vm.parameters.local.ps1` | Your local overrides (subscription, key path, etc.). **Gitignored.** |
| `deploy-vm.ps1` | One-shot idempotent infra deploy. |
| `update-vm.ps1` | Inner loop: tar source over ssh -> `dotnet publish` silo + producer on the VM -> `systemctl restart lattice-silo`. |
| `run-cohort.ps1` | Runs one cohort: applies env drop-ins, restarts silo, starts producer, waits for exit, extracts journals to `benchmark/.run/vm/`. |
| `vm.ps1` | Day-to-day helper: `start` / `stop` / `status` / `ssh` / `logs` / `refresh-ip`. |

## One-time setup

1. Generate an SSH key if you don't already have one:
   ```powershell
   ssh-keygen -t ed25519 -f $HOME/.ssh/id_ed25519 -N '""'
   ```
2. Copy the parameters template and edit:
   ```powershell
   Copy-Item benchmark/vm/vm.parameters.ps1 benchmark/vm/vm.parameters.local.ps1
   # edit vm.parameters.local.ps1: set SubscriptionId, Location (match the Tables account region)
   ```
3. Log in to Azure (CLI):
   ```powershell
   az login
   ```
4. Deploy:
   ```powershell
   ./benchmark/vm/deploy-vm.ps1
   ```
   The script auto-detects your public IP if `AllowedSshSourceAddress`
   is blank, creates the resource group if missing, and prints the SSH
   command on success.

## Daily workflow

```powershell
./benchmark/vm/vm.ps1 start      # ~30s
./benchmark/vm/update-vm.ps1     # tar+publish silo+producer, restart silo
./benchmark/vm/run-cohort.ps1 -Vehicles 4000 -TickHz 5 -DurationSec 45
./benchmark/vm/vm.ps1 logs       # journalctl -fu lattice-silo (live tail)
# ... run cohorts ...
./benchmark/vm/vm.ps1 stop       # deallocate; no compute charges
```

`run-cohort.ps1` writes silo + producer journals into `benchmark/.run/vm/silo-<cohort>.log` and `producer-<cohort>.log`. Each cohort gets a unique `BENCH_TREE_ID` automatically so log analysis can scope to one run.

`update-vm.ps1` flags:
- `-NoBuild` -- just bounce the service (no rsync, no publish).
- `-NoRestart` -- sync + publish, leave the service alone (inspect first).
- `-Clean` -- wipe `/opt/lattice/publish` before publishing (force full rebuild).
- `-SkipUnitSync` -- skip re-rendering the systemd unit when only source changed.

`vm.ps1 status` prints the current power state, public IP, FQDN, and
the SSH source-address allow-list. If your operator IP changes
(coffee shop, VPN, new ISP lease):

```powershell
./benchmark/vm/vm.ps1 refresh-ip  # rewrites the NSG rule
```

## Auto-shutdown safety net

The DevTestLab `shutdown-computevm-<vm>` schedule fires at 19:00 UTC
daily (configurable via `AutoShutdownTime` / `AutoShutdownTimeZone`
in the parameters file). If you forget to run `vm.ps1 stop`, the VM
deallocates automatically. Notifications are disabled by default;
enable them by editing `main.bicep` if you want a heads-up.

## Cost notes

- `Standard_D2as_v5` is roughly $0.10/hr compute (region-dependent).
- Public IP (Standard, static): roughly $4/month.
- Premium SSD 64 GiB: roughly $10/month.
- **Stopped = deallocated** via `vm.ps1 stop`, so compute is $0 while
  off; storage + PIP still bill at ~$14/month idle.

## Tearing down

```powershell
az group delete --name rg-lattice-wedge --yes --no-wait
```
