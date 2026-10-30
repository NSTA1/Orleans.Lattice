# Self-hosted Actions runner image for the CI runner pool (tools/ci-runners).
#
# One image, two roles inside each Container Apps Job execution:
#   - init container `mint-jit`: runs mint-jit.py, which holds the GitHub App
#     key, mints a single-use JIT runner config and writes it to /jit/config.
#   - main container `runner`:   runs runner-entrypoint.sh, which starts the
#     runner with that config. It never sees the App key.
#
# Baked in so jobs do not download them into the replica's ephemeral disk:
# the .NET 10 SDK (setup-dotnet finds it already installed), Python 3, jq and
# git. Java is left to actions/setup-java, which pins the version through
# tools/tlc/toolchain.json.
#
# Also what the test legs expect from a GitHub-hosted runner and cannot get
# from a container here (Container Apps runs no Docker daemon):
# PowerShell (rig and tuning script tests), the Docker CLI with the compose v2
# plugin (`docker compose config` needs no daemon), and Node with Azurite,
# which a leg routed to the pool runs in place of the Azurite image.
FROM ghcr.io/actions/actions-runner:latest

USER root
RUN apt-get update \
 && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
      ca-certificates curl git jq procps unzip zip python3 python3-jwt python3-cryptography libicu-dev \
 && rm -rf /var/lib/apt/lists/*

# PowerShell (Microsoft's package feed) and the Docker CLI with compose v2.
RUN . /etc/os-release \
 && curl -fsSL "https://packages.microsoft.com/config/ubuntu/${VERSION_ID}/packages-microsoft-prod.deb" -o /tmp/ms.deb \
 && dpkg -i /tmp/ms.deb && rm /tmp/ms.deb \
 && install -m 0755 -d /etc/apt/keyrings \
 && curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc \
 && echo "deb [arch=amd64 signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/ubuntu ${VERSION_CODENAME} stable" > /etc/apt/sources.list.d/docker.list \
 && apt-get update \
 && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends powershell docker-ce-cli docker-compose-plugin \
 && rm -rf /var/lib/apt/lists/*

# Node 22 and Azurite, pinned to the version the workflows' service uses.
RUN curl -fsSL https://deb.nodesource.com/setup_22.x | bash - \
 && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends nodejs \
 && npm install -g azurite@3.36.0 \
 && npm cache clean --force \
 && rm -rf /var/lib/apt/lists/*

# .NET 10 SDK where actions/setup-dotnet looks, owned by the runner user so
# setup-dotnet can add a patch band without sudo.
ENV DOTNET_ROOT=/usr/share/dotnet \
    DOTNET_INSTALL_DIR=/usr/share/dotnet \
    DOTNET_CLI_TELEMETRY_OPTOUT=1 \
    DOTNET_NOLOGO=1
RUN curl -fsSL https://dot.net/v1/dotnet-install.sh -o /tmp/dotnet-install.sh \
 && bash /tmp/dotnet-install.sh --channel 10.0 --install-dir /usr/share/dotnet \
 && rm /tmp/dotnet-install.sh \
 && ln -sf /usr/share/dotnet/dotnet /usr/local/bin/dotnet \
 && chown -R runner:runner /usr/share/dotnet

COPY tools/ci-runners/mint-jit.py tools/ci-runners/runner-entrypoint.sh /opt/ci-runner/
RUN chmod 0755 /opt/ci-runner/runner-entrypoint.sh

USER runner
WORKDIR /home/runner
