# Repository-context embedding companion, ONNX Runtime edition (`apps/embedding-onnx`)

An embedding-only companion image for the Orleans.Lattice repository-context MCP
host, built on ONNX Runtime instead of Python and PyTorch.

It is a **sibling** to [`apps/embedding`](../embedding/README.md), not a
replacement. That image stays the default; this one is opt-in.

One job: `text -> vector`.

## Why it exists

The Onyx-derived default image is CPU by default with NVIDIA as a config option,
and that contract is preserved exactly here. What this image adds:

| | `apps/embedding` (Onyx) | `apps/embedding-onnx` (this) |
| --- | --- | --- |
| Runtime | Python + PyTorch | .NET + ONNX Runtime |
| Image size | roughly 13 GB on disk | roughly 0.8 GB (cpu flavour) |
| CPU default | yes | yes |
| NVIDIA | env + device reservation | env + device reservation (`cuda` flavour) |
| Other GPUs | no | DirectML (Windows host only, not in-container) |
| Vectors | reference | **identical** (see below) |

## Vector compatibility

Run in fp32 this reproduces the Onyx server's vectors **numerically**: measured
at cosine `1.000000` (minimum, not just mean) across a 48-chunk corpus of real
repository files, on both the CPU and DirectML providers.

That is what makes the image a drop-in: swapping it in leaves already-stored
embeddings valid, with no re-index.

Three things make that true, and all three are load-bearing:

1. **The same weights.** The Dockerfile pins the exact HuggingFace revision the
   Onyx image loads (`3ac47f12...`) and verifies the download by SHA-256.
2. **The same pipeline.** WordPiece tokenize (lower-cased, truncated), forward
   pass, mean-pool over the attention mask, L2-normalize - matching the model's
   `modules.json` (`Transformer -> Pooling(mean) -> Normalize`).
3. **The same tokens.** Tokenizer output is pinned against golden HuggingFace
   token ids by `WordPieceTokenizerGoldenTests`.

> **Do not serve the INT8 (`model_quantized.onnx`) weights from this image.**
> INT8 is roughly 2.4x faster on the CPU but measures at cosine **0.930** against
> the reference, so it silently defines a *different* embedding space. It is only
> viable if you re-embed everything.

### Known divergence

For an **unbroken run of more than 100 characters** (base64 blobs, minified
assets, long hashes), HuggingFace emits a single `[UNK]` where this tokenizer
sub-words the run instead, so such a chunk will not embed identically. Ordinary
source text is unaffected. The gap is pinned by an explicit test so it cannot
change silently.

## Contract

Identical to the Onyx companion, on the same port, so the
`OnyxEmbeddingProvider` client needs no change:

| Property | Value |
| --- | --- |
| Port | `9000` |
| Health | `GET /api/health` |
| Embed | `POST /encoder/bi-encoder-embed` |
| Model | `nomic-ai/nomic-embed-text-v1` (baked into an image layer) |
| Dimension | `768` |
| Context length | `512` tokens |
| Normalization | L2-normalized (`normalize_embeddings: true`) |

Request (`texts`, `model_name`, `max_context_length`, `normalize_embeddings`,
`text_type`, `provider_type`) and response (`embeddings`) match the Onyx shapes.
`model_name` and `provider_type` are accepted and ignored: the image hosts one
baked local model.

No asymmetric task prefix is applied. The Onyx server takes prefixes from
`manual_query_prefix` / `manual_passage_prefix`, which the repository-context
client never sends, so "no prefix" is the reference behaviour for this caller.

## Configuration

| Variable | Default | Meaning |
| --- | --- | --- |
| `EMBED_PROVIDER` | `cpu` | `cpu`, `cuda` (aliases `gpu`, `nvidia`), or `dml`. Unknown values fall back to `cpu`. |
| `EMBED_PORT` | `9000` | HTTP listen port. |
| `EMBED_MODEL_PATH` | `/app/assets/model.onnx` | ONNX weights. |
| `EMBED_VOCAB_PATH` | `/app/assets/vocab.txt` | WordPiece vocabulary (committed gzipped, decompressed at build time). |
| `EMBED_MAX_CONTEXT_LENGTH` | `512` | Hard token ceiling; a larger request is clamped. |
| `EMBED_INTRA_THREADS` | ONNX Runtime default | CPU intra-op threads. |
| `EMBED_DEVICE_ID` | `0` | Device ordinal for an accelerated provider. |

An unknown `EMBED_PROVIDER`, or an unparseable number, falls back to the default
rather than aborting startup: a container that boots on the CPU is strictly more
useful than one that refuses to boot. A missing or unreadable model or
vocabulary **is** fatal, because serving wrong vectors is worse than serving
none.

## Build

```bash
# CPU (default flavour)
docker build -t orleans-lattice-embedding-onnx:local apps/embedding-onnx

# NVIDIA
docker build --build-arg ONNX_FLAVOR=cuda \
  -t orleans-lattice-embedding-onnx:cuda apps/embedding-onnx
```

The build context is this directory (the server has no `ProjectReference` into
`src/`), so the context stays small. Behind a private NuGet feed, pass
`--secret id=nugetcfg,src=<path to NuGet.Config>` as the other images do.

The `cuda` flavour carries the CUDA native libraries and is much larger, but it
still includes the CPU provider, so one `cuda` image serves both GPU and CPU
hosts and `EMBED_PROVIDER` alone chooses between them.

## Run

```bash
# CPU
docker run --rm -p 9000:9000 orleans-lattice-embedding-onnx:local

# NVIDIA
docker run --rm --gpus all -e EMBED_PROVIDER=cuda -p 9000:9000 \
  orleans-lattice-embedding-onnx:cuda
```

`GET /api/health` reports the provider that actually bound, so an operator can
confirm the GPU was picked up without reading logs:

```json
{ "status": "ok", "provider": "Cuda", "model": "model.onnx", "dimension": 768 }
```

## Using it from the RepoContext sample

The sample keeps the Onyx companion as its default. To run it against this image
instead, add the override file next to the sample's compose file:

```bash
cd samples/RepoContextContainer
docker compose -f docker-compose.yml -f docker-compose.onnx.yml up -d
```

Nothing else changes: same service name, same port, so
`LATTICE_EMBEDDING_ENDPOINT: http://embedder:9000` is untouched.

## Offline cold start

The weights and vocabulary are baked into image layers at build time, so a cold
container needs no network - matching the Onyx companion. There is no
HuggingFace cache volume to mount.

## Health checking

The runtime image is chiseled: no shell, no package manager, non-root. A
shell-form `HEALTHCHECK` therefore cannot work, so the image uses an exec-form
probe that re-invokes the server with `--healthcheck`. That keeps
`depends_on: service_healthy` working for a compose file that swaps this image
in for the Onyx one.

## Tests

The covering tests live in [`tests/`](./tests) and run in CI's `apps` lane
whenever this directory changes. They need neither the model nor a network: the
golden token fixture and the vocabulary are committed.

```bash
dotnet test apps/embedding-onnx/tests/Orleans.Lattice.Embedding.Onnx.Host.Tests.csproj
```

The tokenizer-parity test is the load-bearing one. A tokenizer that mis-handles
source text - swallowing newlines and tabs, or dropping `=`, `<`, `>` and
backticks - still produces correctly shaped, correctly normalized, entirely
wrong vectors, and no structural check would catch it.
