---
agent_spec: "docs/agents/api/mcp.json"
---

# Configuration

The authoritative reference for configuring `Orleans.Lattice.Api.Mcp.RepoContext` in a host: the parameters of `AddRepoContextTools`, the two options classes, and the environment variables the package itself reads. The container host built on this package adds its own variables (durability profile, data root, ports, storage tuning); those are documented once, in the [container quickstart](container.md#configuration), and are not repeated here.

The MCP server the tools register on is configured separately; see [`Orleans.Lattice.Api.Mcp` configuration](../lattice.api.mcp/configuration.md). Multi-cluster replication of the store is configured by [`Orleans.Lattice.Api.Mcp.RepoContext.Replication`](../lattice.api.mcp.repocontext.replication/README.md).

## `AddRepoContextTools` parameters

`AddRepoContextTools(this IServiceCollection services, bool enableWrites = false, bool workspaceMode = false, string? workspaceRoot = null, bool registerAsApp = false)` decides which tools exist. The host must also call `AddLatticeMcp` and map the endpoint for any of them to be reachable. Which tool each combination contributes is listed in [Tools](tools.md).

| Parameter | Default | Effect |
|---|---|---|
| `enableWrites` | `false` | Contributes the mutating tools: repository onboarding and removal, memory capture and maintenance, claim management, and the workspace-mode index reset. Without it a data-permitted caller is offered the read-only surface. |
| `workspaceMode` | `false` | `false`: the onboarding tool is the single-repository `repocontext_bootstrap`. `true`: adds the read-only `repocontext_list_repos` and, with writes enabled, replaces `repocontext_bootstrap` with `repocontext_add_repo`, `repocontext_remove_repo` and `repocontext_reset_index`. |
| `workspaceRoot` | `null` | The read-only root every wire-supplied repository path must resolve under; a path escaping it through `..` or a symbolic link is refused. **Required by every path-taking tool**: without a non-empty root, neither onboarding tool nor the read-only `repocontext_changed` is contributed, because the guard registered in its place admits every path. When a git source is configured, the git staging root is admitted alongside it. A path guard the host registered before this call is kept. |
| `registerAsApp` | `false` | Also registers the store as the installable app `repo-context`: its embedded manifest is made installable (not installed) and the group's always-on read-only tools are additionally offered as `repo-context_{tool}`. The host remains responsible for `AddLatticeApps` and for an MCP authorizer that admits those names. See [As an installable app](README.md#as-an-installable-app-opt-in). |

## `OnyxEmbeddingOptions`

Bound by `AddOnyxEmbeddingProvider(Action<OnyxEmbeddingOptions>? configure = null)`, which registers the bundled client for the embedding companion container as the `IEmbeddingProvider` (with `TryAdd`, so a provider the host bound first is kept). The defaults match the model and endpoint the shipped companion images serve. An options validator is registered; it runs when the options are first resolved. Changing `ModelName`, `Dimension` or `NormalizeEmbeddings` selects a new embedding space - see [Embedding-space safety](semantic-search.md#embedding-space-safety).

| Option | Type | Default | Validation and meaning |
|---|---|---|---|
| `BaseAddress` | `Uri` | `http://localhost:9000` (`DefaultBaseAddress`) | The companion model server's base address. |
| `ModelName` | `string` | `nomic-ai/nomic-embed-text-v1` (`DefaultModelName`) | The model id the server embeds with; it must be one the container serves. |
| `Dimension` | `int` | `768` (`DefaultDimension`) | The vector dimension the model produces; a response of any other length is rejected. |
| `MaxContextLength` | `int` | `512` (`DefaultMaxContextLength`) | The maximum context length, in tokens, sent with each request. |
| `NormalizeEmbeddings` | `bool` | `true` | Whether the server L2-normalizes the returned vectors. |
| `RequestTimeout` | `TimeSpan?` | `null` (the `HttpClient` default) | When set, must be strictly positive and at most `int.MaxValue` milliseconds, or `Timeout.InfiniteTimeSpan` for no timeout; any other value fails validation. |

The provider's full behaviour is described in [Binding and configuring the bundled provider](semantic-search.md#binding-and-configuring-the-bundled-provider).

## `RepoContextTtlOptions`

The per-repository memory time-to-live policy, resolved through named options as `IOptionsMonitor<RepoContextTtlOptions>.Get(repoId)`. A repository with no named configuration resolves the type's defaults (the unnamed instance is never consulted), so apply a policy to every repository with `ConfigureAll` and override one repository by name. `AddRepoContextTools` registers a validator that runs when a repository's policy is first resolved - on the first memory write that creates an entry without an explicit TTL - and an invalid policy refuses that write. It is not checked at host startup. See [Per-repository TTL policy](memory-and-ttl.md#per-repository-ttl-policy).

| Option | Type | Default | Validation and meaning |
|---|---|---|---|
| `DefaultMemoryTtl` | `TimeSpan?` | `null` | The TTL a newly created memory entry receives when the writer supplies none; `null` leaves memory durable by default. When set it must be positive. |
| `StructuralRecordsNeverExpire` | `bool` | `true` | A declarative policy flag; no code in the package reads it. |

## Environment variables the package reads

The package reads the variables below from the process environment once - most of them while `AddRepoContextTools` registers its services - so a change takes effect on the next start. A malformed or unrecognised value falls back to its default. `RepoContextEnvironmentVariables` publishes exactly this set, and `DescribeResolvedSettings()` reports the parsed value in force for each (see [Public API](api.md#settings-and-durability-reporting)). Each variable's default, accepted range and effect is documented in the container quickstart; the names below link there.

| Group | Variables | Documented in |
|---|---|---|
| Background indexing cadence and retrieval mode (`IndexingKeys`) | `LATTICE_SELFINDEX_TICK_SECONDS`, `LATTICE_RECONCILE_INTERVAL_SECONDS`, `LATTICE_RECONCILE_JITTER_SECONDS`, `LATTICE_FULL_WALK_INTERVAL_SECONDS`, `LATTICE_EMBEDDING_GAP_SCAN_INTERVAL_SECONDS`, `LATTICE_COVERAGE_DIGEST_AUDIT_INTERVAL_SECONDS`, `LATTICE_VECTOR_CACHE_TTL_SECONDS`, `LATTICE_REPOCONTEXT_TOKENIZER`, `LATTICE_REPOCONTEXT_INDEXING_ROLE`, `LATTICE_REPOCONTEXT_SOURCE_INDEXING`, `LATTICE_REPOCONTEXT_SEMANTIC_RETRIEVAL`, `LATTICE_REPOCONTEXT_ANN_INDEX_SCHEDULING`, `LATTICE_REPOCONTEXT_ANN_INDEX_RECLAMATION`, `LATTICE_REPOCONTEXT_ANN_SWEEP_INTERVAL_SECONDS`, `LATTICE_REPOCONTEXT_PACING`, `LATTICE_REPOCONTEXT_PACING_SLICE_SECONDS`, `LATTICE_REPOCONTEXT_PACING_REST_SECONDS`, `LATTICE_REPOCONTEXT_PACING_MAX_DELAY_SECONDS` | [Container configuration](container.md#configuration) |
| Approximate-index slice bounds (`AnnKeys`) | `LATTICE_REPOCONTEXT_ANN_OPEN_SLICE_BUDGET_SECONDS`, `LATTICE_REPOCONTEXT_ANN_OPEN_SLICE_MAX_EXTENSIONS`, `LATTICE_REPOCONTEXT_ANN_INGEST_SLICE_BUDGET_SECONDS`, `LATTICE_REPOCONTEXT_ANN_OPEN_MAX_CONSECUTIVE_REFUSALS`, `LATTICE_REPOCONTEXT_ANN_OPEN_REFUSAL_TERMINAL_SECONDS` | [Container configuration](container.md#configuration) |
| Agent-memory archive (`MemoryArchiveKeys`) | `LATTICE_REPOCONTEXT_MEMORY_ARCHIVE_DIR`, `LATTICE_REPOCONTEXT_MEMORY_ARCHIVE_INTERVAL_SECONDS`, `LATTICE_REPOCONTEXT_MEMORY_ARCHIVE_RESTORE`, `LATTICE_REPOCONTEXT_MEMORY_ARCHIVE_STOP_TIMEOUT_SECONDS` | [Container configuration](container.md#configuration), [Memory durability](memory-durability.md) |
| Git index source (`GitSourceKeys`, plus the `LATTICE_REPOCONTEXT_GIT_` per-repository prefix family) | `LATTICE_REPOCONTEXT_GIT_REPOS`, `LATTICE_REPOCONTEXT_GIT_STAGING_ROOT`, and the per-repository settings `LATTICE_REPOCONTEXT_GIT_<ID>_*` (the repository id upper-cased, with each non-alphanumeric character folded to `_`) | [Configuring a git source](container.md#configuring-a-git-source) |

The indexing role used by multi-cluster deployments (`LATTICE_REPOCONTEXT_INDEXING_ROLE`, `hub` or `spoke`) is explained in [Indexing roles](../lattice.api.mcp.repocontext.replication/README.md#indexing-roles-hub-and-spoke), and memory-only operation (`LATTICE_REPOCONTEXT_SOURCE_INDEXING=off`) in [Memory-only mode](container.md#memory-only-mode).

## Source map

- [Tool registration](../../src/lattice.api.mcp.repocontext/LatticeMcpRepoContextServiceCollectionExtensions.cs)
- [Embedding provider registration](../../src/lattice.api.mcp.repocontext/Embedding/LatticeMcpRepoContextEmbeddingServiceCollectionExtensions.cs) and [options](../../src/lattice.api.mcp.repocontext/Embedding/OnyxEmbeddingOptions.cs)
- [TTL options](../../src/lattice.api.mcp.repocontext/RepoContextTtlOptions.cs)
- [Published environment variables](../../src/lattice.api.mcp.repocontext/RepoContextEnvironmentVariables.cs)

## Related

- [Public API](api.md)
- [Architecture](architecture.md)
- [Container quickstart](container.md), [Tools](tools.md), [Memory and TTL](memory-and-ttl.md), [Semantic search](semantic-search.md)
