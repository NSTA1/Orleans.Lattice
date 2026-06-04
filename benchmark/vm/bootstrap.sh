#!/usr/bin/env bash
# One-shot bootstrap for the wedge VM. Idempotent. Used by update-vm.ps1
# as a fallback when cloud-init fails (cloud-init's default runner is dash,
# which historically tripped on `set -o pipefail`; the YAML form has been
# patched but this script is the manual recovery path / single source of
# truth for what the VM needs to look like).
set -euo pipefail

UBUNTU_VERSION=$(lsb_release -rs)
if ! command -v dotnet >/dev/null 2>&1; then
  curl -fsSL -o /tmp/packages-microsoft-prod.deb \
	"https://packages.microsoft.com/config/ubuntu/${UBUNTU_VERSION}/packages-microsoft-prod.deb"
  sudo dpkg -i /tmp/packages-microsoft-prod.deb
  rm -f /tmp/packages-microsoft-prod.deb
  sudo apt-get update -qq
  sudo apt-get install -y dotnet-sdk-10.0
fi

export DOTNET_CLI_TELEMETRY_OPTOUT=1 DOTNET_NOLOGO=1
for tool in dotnet-dump dotnet-counters dotnet-trace dotnet-gcdump; do
  dotnet tool install --global "$tool" 2>/dev/null || \
	dotnet tool update --global "$tool" 2>/dev/null || true
done

sudo mkdir -p /opt/lattice/src /opt/lattice/publish /opt/lattice/logs
sudo chown -R "$(id -un)":"$(id -gn)" /opt/lattice

sudo tee /etc/profile.d/lattice.sh > /dev/null <<'PROF'
export DOTNET_ROOT=/usr/share/dotnet
export PATH=$PATH:/usr/share/dotnet:$HOME/.dotnet/tools
export DOTNET_CLI_TELEMETRY_OPTOUT=1
export DOTNET_NOLOGO=1
PROF

echo "bootstrap done; dotnet $(dotnet --version)"
