# Repository-context embedding companion (`apps/embedding`)

A produced, embedding-only companion image for the Orleans.Lattice
repository-context MCP host. It turns text into vectors over HTTP so the MCP
container never embeds in-process and keeps its single, MCP-only listener. The
default `IEmbeddingProvider` in `Orleans.Lattice.Api.Mcp.RepoContext`
(`OnyxEmbeddingProvider`) is a thin client for the HTTP contract this image
defines.

> **This is no longer the default companion image.** The repository-context
> sample now brings up
> [`apps/embedding-onnx`](../embedding-onnx/README.md), which serves this same
> contract on the same port and emits numerically identical vectors from an image
> roughly a tenth the size, with no model download on first run. This image
> remains supported and is still the reference implementation the ONNX one is
> pinned against; select it with the sample's
> `docker-compose.onyx.yml` override. `OnyxEmbeddingProvider` keeps its name
> because it is a client for the contract, not for this particular image.

One job: `text -> vector`.

## What it is

The image is **derived from Onyx's published model server**
(`onyxdotapp/onyx-model-server`, built upstream from `Dockerfile.model_server`).
That component is MIT (Expat) licensed - see [`NOTICE`](./NOTICE) for the retained
copyright notice. We add only a thin derived `Dockerfile`: it pins the upstream
tag and digest (the `v4.5.6` multi-arch index digest, so platform resolution
still selects the right arch), forces a CPU-only default, documents the contract,
and adds a health check. No Onyx source is vendored.

## Contract

| Property | Value |
| --- | --- |
| Port | `9000` |
| Health | `GET /api/health` |
| Embed | `POST /encoder/bi-encoder-embed` |
| Default model | `nomic-ai/nomic-embed-text-v1` (baked into an image layer) |
| Dimension | `768` |
| Context length | `512` tokens |
| Normalization | L2-normalized (`normalize_embeddings: true`) |

Request body (`EmbedRequest`): `texts: string[]`, `model_name`,
`max_context_length`, `normalize_embeddings: true`, `text_type` = `passage` for
stored chunks / `query` for search vectors, `provider_type: null` (local model).
Response (`EmbedResponse`): `embeddings: float[][]`.

This is Onyx's **internal, versioned** model-server API, not a stable public one.
It is pinned to the image tag and wrapped behind `OnyxEmbeddingProvider`, so a
schema change is a single-file update in that client.

## CPU by default

The image runs without a GPU. `CUDA_VISIBLE_DEVICES` is set empty so torch falls
back to CPU even on a GPU host; GPU is an opt-in override (clear the env and add
the accelerator device reservation), never required.

## Offline cold start

The default model's weights, tokenizer, and metadata are baked into an image
layer at build time (upstream loads the default with `local_files_only`), so a
cold container needs no network download and works fully offline.

## Swapping the model

Override the baked default by mounting a HuggingFace cache over
`/app/.cache/huggingface` and/or passing a different `model_name` on the request
(non-default models download on first use). Changing the model is a **new
embedding space** (a different `EmbeddingSpace` model id / dimension), not an
in-place change - re-embed stored content against the new space.

## Build

```bash
docker build -t orleans-lattice-embedding:local apps/embedding
# move the pin (update tag and digest together):
docker build --build-arg ONYX_MODEL_SERVER_REF=onyxdotapp/onyx-model-server:v4.5.6@sha256:2144deebdf0904106363a230e737c773c36b2cd95f9259bbf0c3017f1ae68082 -t orleans-lattice-embedding:local apps/embedding
```

Compose wiring (bringing this up alongside the RepoContext MCP host with the
provider pre-pointed at it) is owned by the local compose sample, not this image.

## Image size (stated honestly)

This is a Python + torch runtime with a baked transformer model, so "minimal" is
relative to that base - not a chiseled/distroless .NET-sized image. The pinned
`onyxdotapp/onyx-model-server:v4.5.6` base is roughly **4.8 GB** for `linux/amd64`
(compressed download; larger uncompressed on disk), dominated by torch, the CUDA
userspace libraries torch ships with, and the baked model weights. We minimise by
baking exactly one model, serving embeddings only, and adding no layers of our
own - but we do not over-promise a small image on top of that base.
