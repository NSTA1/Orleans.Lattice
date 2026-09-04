# syntax=docker/dockerfile:1

# ---------------------------------------------------------------------------
# Repository-context embedding companion, ONNX Runtime edition.
#
# A sibling to apps/embedding (the Onyx-derived image), not a replacement. It
# serves the SAME wire contract on the SAME port, so it is a drop-in behind the
# unchanged OnyxEmbeddingProvider client:
#
#   GET  /api/health
#   POST /encoder/bi-encoder-embed      on port 9000
#
# What it buys over the Onyx image:
#   - one image serves CPU and NVIDIA, selected by the EMBED_PROVIDER env var
#     with an automatic CPU fallback (build the cuda flavour to include CUDA);
#   - no Python and no torch, so the image is roughly an order of magnitude
#     smaller;
#   - the same vectors. Run in fp32 this reproduces the Onyx server's output
#     numerically (measured at cosine 1.000000 over real repository chunks), so
#     swapping the image leaves already-stored embeddings valid.
#
# Build context is THIS DIRECTORY (the server has no ProjectReference into
# src/), which keeps the context tiny:
#
#   docker build -t orleans-lattice-embedding-onnx:local apps/embedding-onnx
#
# For an NVIDIA host, build the cuda flavour and run with --gpus all:
#
#   docker build --build-arg ONNX_FLAVOR=cuda \
#     -t orleans-lattice-embedding-onnx:cuda apps/embedding-onnx
#   docker run --gpus all -e EMBED_PROVIDER=cuda orleans-lattice-embedding-onnx:cuda
#
# Behind a private / corporate-proxy / offline NuGet feed, pass your own
# NuGet.Config with the same BuildKit secret convention the other images use:
#
#   docker build -t orleans-lattice-embedding-onnx:local \
#     --secret id=nugetcfg,src=$env:APPDATA\NuGet\NuGet.Config apps/embedding-onnx
# ---------------------------------------------------------------------------

# --- Model acquisition -----------------------------------------------------
# The weights are pinned to an exact HuggingFace revision AND verified by
# SHA-256, so a re-tag or a mutated file upstream cannot silently change the
# embedding space. This is the same revision the Onyx companion image loads,
# which is what makes the two images numerically interchangeable.
#
# To move the pin, update the revision and BOTH digests together.
FROM mcr.microsoft.com/dotnet/sdk:10.0 AS model
ARG MODEL_REPO=nomic-ai/nomic-embed-text-v1
ARG MODEL_REVISION=3ac47f125a41961d13b397d0332866be2f9152e1
ARG MODEL_SHA256=347440e93b5ec979fdcf6041b72721aade7b9680c16022e2830db7115ff6fd9f
WORKDIR /model

RUN set -eux; \
    curl -fsSL -o model.onnx \
      "https://huggingface.co/${MODEL_REPO}/resolve/${MODEL_REVISION}/onnx/model.onnx"; \
    echo "${MODEL_SHA256}  model.onnx" | sha256sum -c -

# --- Build stage -----------------------------------------------------------
FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src

# Which ONNX Runtime build to bind: cpu (default) or cuda. See the csproj for
# what each flavour costs and includes.
ARG ONNX_FLAVOR=cpu

# Publish for an explicit runtime identifier. ONNX Runtime's P/Invoke names the
# native library "onnxruntime.dll" on every platform, and a portable (RID-less)
# publish leaves it under runtimes/linux-x64/native/libonnxruntime.so where the
# default probing variants ("onnxruntime.dll.so", "libonnxruntime.dll.so", ...)
# never match it - the container starts and then dies with a DllNotFoundException
# on the first session. Publishing RID-specific places the native beside the
# assembly so it resolves. Kept framework-dependent so the chiseled ASP.NET
# runtime image is still used.
ARG TARGET_RID=linux-x64

COPY . .

RUN --mount=type=secret,id=nugetcfg \
    if [ -f /run/secrets/nugetcfg ]; then set -- --configfile /run/secrets/nugetcfg; fi && \
    dotnet restore Orleans.Lattice.Embedding.Onnx.Host.csproj \
      -r "${TARGET_RID}" \
      /p:OnnxRuntimeFlavor="${ONNX_FLAVOR}" "$@"

# The vocabulary is committed gzipped (it is model data, not prose, and it
# contains characters the repository's text-hygiene gates reject in tracked text
# files). Decompress it here, in a stage that has a shell, so the runtime stage
# copies a plain file.
RUN gzip -dc assets/vocab.txt.gz > /vocab.txt

RUN dotnet publish Orleans.Lattice.Embedding.Onnx.Host.csproj \
    -c Release \
    -o /app/publish \
    -r "${TARGET_RID}" \
    --self-contained false \
    --no-restore \
    /p:OnnxRuntimeFlavor="${ONNX_FLAVOR}" \
    /p:UseAppHost=false

# --- Runtime stage ---------------------------------------------------------
# Distroless, shell-less, non-root.
#
# The `-extra` chiseled variant is REQUIRED, not a convenience: it is the one
# that carries ICU. The WordPiece pipeline strips accents via Unicode NFD
# normalization, which is unavailable without ICU, and on an ICU-less runtime
# every word containing a non-ASCII letter silently tokenizes to a single [UNK].
# See the InvariantGlobalization comment in the csproj; GlobalizationGuard fails
# the container at startup if this base image is ever swapped for an ICU-less one.
FROM mcr.microsoft.com/dotnet/aspnet:10.0-noble-chiseled-extra AS runtime
WORKDIR /app

COPY --from=build /app/publish ./
COPY --from=model /model/model.onnx /app/assets/model.onnx
COPY --from=build /vocab.txt /app/assets/vocab.txt

# The model and vocabulary are baked into the image, so a cold container needs
# no network and works fully offline - matching the Onyx companion's behaviour.
ENV EMBED_MODEL_PATH=/app/assets/model.onnx \
    EMBED_VOCAB_PATH=/app/assets/vocab.txt \
    EMBED_PORT=9000 \
    EMBED_PROVIDER=cpu

EXPOSE 9000

# Exec-form health probe. The chiseled runtime has NO SHELL, so a shell-form
# HEALTHCHECK cannot work; the server instead answers --healthcheck by probing
# its own endpoint and exiting 0/1. This keeps `depends_on: service_healthy`
# working for a compose file that swaps this image in for the Onyx one.
#
# The start period covers model load (a few seconds on the CPU, longer on a
# cold CUDA context) so a slow start is not reported as a failure.
HEALTHCHECK --interval=30s --timeout=10s --start-period=90s --retries=5 \
    CMD ["dotnet", "Orleans.Lattice.Embedding.Onnx.Host.dll", "--healthcheck"]

# The chiseled ASP.NET image already runs as a non-root user. No shell, no
# package manager, no setuid binaries.
ENTRYPOINT ["dotnet", "Orleans.Lattice.Embedding.Onnx.Host.dll"]
