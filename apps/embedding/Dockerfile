# Orleans.Lattice repository-context embedding companion.
#
# This image is DERIVED from Onyx's published model-server image
# (onyxdotapp/onyx-model-server, built upstream from backend/Dockerfile.model_server).
# Onyx's backend/model_server tree sits outside the ee/ directories and is
# licensed MIT (Expat); see the NOTICE file in this directory for the retained
# copyright notice. We add no application code - we pin the upstream tag, keep the
# baked default embedding model, force a CPU-only default, and document the
# resulting contract. One job: text -> vector on port 9000.
#
# The upstream image already:
#   - serves the bi-encoder embed endpoint at POST /encoder/bi-encoder-embed
#   - exposes health at GET /api/health on port 9000
#   - bakes nomic-ai/nomic-embed-text-v1 (768-dim, 512-token) into an image layer
#     via `local_files_only`, so cold start needs no network and the container
#     works fully offline
#   - is CPU-capable; GPU device reservation is opt-in, never required
#
# Pin the tag AND the digest so a re-tag upstream cannot silently change the
# embedding space. The digest below is the v4.5.6 multi-arch index digest, so
# platform resolution still picks the right arch (amd64/arm64). To move the pin,
# update the tag and the digest together (they must match):
#   docker buildx imagetools inspect onyxdotapp/onyx-model-server:v4.5.6
ARG ONYX_MODEL_SERVER_REF=onyxdotapp/onyx-model-server:v4.5.6@sha256:2144deebdf0904106363a230e737c773c36b2cd95f9259bbf0c3017f1ae68082
FROM ${ONYX_MODEL_SERVER_REF}

# --- Embedding-only, CPU-default posture -----------------------------------
# The model server lazy-loads models: it only loads a model when an endpoint is
# called, and only the embedding model is baked into this image. We call only the
# embed endpoint, so in practice this serves embeddings only - the rerank/intent
# endpoints exist but load nothing unless invoked. We do not enable the broader
# Onyx app surface (no indexing pipeline, no web/API server).
#
# Force CPU: keep any CUDA/GPU device selection opt-in. Setting CUDA_VISIBLE_DEVICES
# to empty makes torch fall back to CPU even on a GPU host unless a caller overrides
# it, so the image runs on a laptop with no accelerator.
ENV CUDA_VISIBLE_DEVICES="" \
    ONYX_RUNNING_IN_DOCKER="true" \
    HF_HOME=/app/.cache/huggingface \
    HF_HUB_OFFLINE="0"

# The baked default lives here; mount a HuggingFace cache over this path to swap in
# a different model (non-default models download on first use). Changing the model
# is a NEW embedding space, not an in-place change - re-tag and re-embed.
VOLUME ["/app/.cache/huggingface"]

EXPOSE 9000

# Fail-fast readiness signal for compose / orchestrators. Matches the health path
# the RepoContext OnyxEmbeddingProvider probes.
HEALTHCHECK --interval=15s --timeout=5s --start-period=120s --retries=10 \
    CMD ["python", "-c", "import urllib.request,sys; sys.exit(0 if urllib.request.urlopen('http://localhost:9000/api/health').status==200 else 1)"]

# Inherit the upstream entrypoint (CMD ["python", "-m", "model_server"]); do not
# override it. The model server binds 0.0.0.0:9000 by default.
