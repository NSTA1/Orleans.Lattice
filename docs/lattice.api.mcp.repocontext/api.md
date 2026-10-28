---
agent_spec: "docs/agents/api/mcp.json"
---

# Public API

The complete public surface of `Orleans.Lattice.Api.Mcp.RepoContext`, all in the `Orleans.Lattice.Api.Mcp.RepoContext` namespace: the registration extension methods, the embedding seam, the host-integration seams, the readiness and settings-reporting helpers, and the structured records the `repocontext_*` tools return. What each tool does, and the arguments it takes, is in [Tools](tools.md); options and environment variables are in [Configuration](configuration.md); how the parts fit together is in [Architecture](architecture.md).

## Registration

| Method | Declared on | What it registers |
|---|---|---|
| `AddRepoContextTools(bool enableWrites = false, bool workspaceMode = false, string? workspaceRoot = null, bool registerAsApp = false)` | `LatticeMcpRepoContextServiceCollectionExtensions` | The `repocontext_*` tool group on an `AddLatticeMcp` server, the indexing and retrieval services, and the default seams below. Idempotent for the tool group. The parameters decide which tools exist; see [Configuration](configuration.md#addrepocontexttools-parameters). |
| `AddOnyxEmbeddingProvider(Action<OnyxEmbeddingOptions>? configure = null)` | `LatticeMcpRepoContextEmbeddingServiceCollectionExtensions` | The bundled HTTP client for the embedding companion container as the singleton `IEmbeddingProvider`, with `TryAdd`, so a provider the host bound first is kept. |

Both extend `IServiceCollection`. Multi-cluster replication of the store is a separate package: [`Orleans.Lattice.Api.Mcp.RepoContext.Replication`](../lattice.api.mcp.repocontext.replication/api.md).

## Embedding seam

Semantic search runs only when an `IEmbeddingProvider` is bound; without one, search answers by keyword. See [The embedding seam](semantic-search.md#the-embedding-seam).

| Type | Role |
|---|---|
| `IEmbeddingProvider` | `Space` (the `EmbeddingSpace` every vector it returns belongs to), `Task<bool> IsAvailableAsync(CancellationToken)`, and `Task<EmbeddingResult> EmbedAsync(IReadOnlyList<string> texts, EmbeddingTextType textType, CancellationToken)`. |
| `EmbeddingSpace` | The model id, vector `Dimension` and `Normalized` flag that identify a vector space; constructed as `new EmbeddingSpace(string modelId, int dimension, bool normalized)`. A stored vector is comparable only to vectors from the same space, so changing any of the three is a new space. |
| `EmbeddingResult` | Either `Success(space, vectors)` or `Failure(space, error)`: `Succeeded`, `Space`, `Vectors` and `Error`. The contract is fail-closed: `EmbedAsync` never throws for an unreachable or misbehaving provider and never returns wrong-space vectors; it returns an unsuccessful result so the caller falls back to keyword recall. |
| `EmbeddingTextType` | `Passage` (indexed content) or `Query` (a search query), so a provider backed by an asymmetric model can apply the matching prefix. |
| `OnyxEmbeddingOptions` | The bundled provider's options; see [Configuration](configuration.md#onyxembeddingoptions). |

## Host-integration seams

`AddRepoContextTools` registers a default for each with `TryAdd`, so a host that registers its own first keeps it.

| Type | Role |
|---|---|
| `IRepoIndexRunAuthority` | `LatticeCredential? Resolve()`: the fixed host credential every background indexing run, including a reminder-driven resume after a restart, writes under. The default resolves none, so a run keeps whatever ambient credential started it - correct for a host with no access gate. A host enforcing a fail-closed gate registers its own before calling `AddRepoContextTools`. |
| `IRepoContextRetrievalWarmup` | `Task<bool> TryWarmAsync(CancellationToken)`: issues a real query through the ordinary search path so a readiness probe reports demonstrated semantic capability rather than configuration. It runs under the caller's ambient credential. |
| `RepoContextRetrievalReadinessState` | The shared, thread-safe record of whether this host can serve semantic retrieval: `Phase`, `IsReady`, `Arming`, `TimeToReady` and `FaultHoldDown`. It is fed by the retrieval path every query took (`Observe`) and by a probe's out-of-band signals (`MarkServing`, `MarkKeywordOnly`, `MarkNothingRegistered`, `MarkUnavailable`, `MarkSaturationUnavailable`, `ClearSaturationUnavailable`, `ObserveArming`). Once serving, a fault must persist for `FaultHoldDown` (`DefaultFaultHoldDown`, 30 seconds) before the phase falls back to `Building`. `IsReadyPhase` and `PhaseTag` classify and name a phase; `SaturatedCause` and `ProbeCause` are the cause strings it records. |
| `RepoContextRetrievalReadinessPhase` | `Building`, `Serving`, `KeywordOnly`, `NothingRegistered`, `SaturatedUnavailable`. `Serving`, `KeywordOnly` and `NothingRegistered` are ready. |
| `RepoContextRetrievalArming` | `Unknown`, `Unarmed`, `Armed`: whether the approximate index was last seen answering from a trained partitioning. Separate from readiness, because an unarmed plane still serves complete-recall results. |

## Retrieval-path values

`RepoContextRetrievalPath` holds the values a search or context result reports in `RetrievalPath`, and that `repocontext_health` exposes: `SemanticExact` (`semantic.exact`), `SemanticApproximate` (`semantic.approximate`), `KeywordNoEmbedder` (`keyword.no_embedder`), `KeywordVectorPlaneUnavailable` (`keyword.vector_plane_unavailable`), `KeywordIndexDegraded` (`keyword.index_degraded`) and `KeywordExactFallbackSuppressed` (`keyword.exact_fallback_suppressed`). `IsSemantic(path)` reports whether a value is one of the two semantic paths, and `NormalizeSemantic(declared)` maps a semantic index's declared value onto one of them, failing closed: only the exact literal `semantic.exact` stays exact, and anything else - `null`, empty or unrecognised - becomes `semantic.approximate`, the weaker recall claim. What each path means is in [Which path answered](semantic-search.md#which-path-answered-retrievalpath).

## Settings and durability reporting

These let a host report the configuration this package actually resolved instead of restating it.

| Type | Role |
|---|---|
| `RepoContextEnvironmentVariables` | Publishes every environment variable the package itself reads: `IndexingKeys`, `GitSourceKeys`, `MemoryArchiveKeys`, `AnnKeys`, their union `All`, and the `Prefixes` family (`GitSourceSettingPrefix`, the per-repository git settings). `DescribeResolvedSettings()` returns one `RepoContextSettingSnapshot` per key in `All`, and `IsPrefixMatched(name)` reports whether a name belongs to a prefix family. The variables themselves are documented in [Configuration](configuration.md#environment-variables-the-package-reads). |
| `RepoContextSettingSnapshot` | `(Name, Resolved, Default, WasDeclared)`: the parsed value in force, its default, and whether the variable was set (blank counts as unset). |
| `RepoContextMemoryDurabilityReport` | `Describe(dataRoot, walDirectory, grainStorePath)` builds the startup statement of where durable agent memory lives and what protects it. |
| `RepoContextMemoryDurabilityStatement` | `(Lines, IsWarning, IsArchived, ArchiveSharesDataRoot)`: the statement lines, whether to log it as a warning, whether an archive is configured, and whether that archive sits inside the data root it is meant to outlive. See [Memory durability](memory-durability.md). |

## Tool results

Every tool result is an MCP payload serialized to the tool's JSON structured content by the SDK. Unless the indexing section below says otherwise, these records carry no Orleans serialization attributes.

| Record | Returned by |
|---|---|
| `RepoContextSearchResult` | `repocontext_search`: the `Mode`, the `RetrievalPath`, and ranked `Hits`. |
| `RepoContextSearchHit` | One hit: `Score`, the hydrated `Entry`, the matched `VectorId` on a semantic hit, and the `Reasons` it ranked. |
| `RepoContextContextResult` | `repocontext_context`: the packed `Entries`, the settled `Detail`, `BudgetTokens`, `ResponseTokens`, `TotalTokens`, `Truncated`, `RetryBudgetTokens`, the `Session` and the `Reused` acknowledgements. See [Retrieval economics](retrieval-economics.md). |
| `RepoContextContextEntry` | One packed file: `Path`, `Score`, `Reasons`, `TokenCount`, `FullReadTokenCount`, `Content`, `ContentHash` and its `Units`. |
| `RepoContextContextUnit` | One delivered unit: its opaque `Receipt`, `Kind`, optional `Symbol` and `TokenCount`. |
| `RepoContextContextDetail` | The `detail` levels of `repocontext_context`: `Auto`, `Paths`, `Outline`, `Slices`. `Auto` is a request, not an emitted level: the bundle tries the richest level that yields a non-empty bundle (slices, then outline, then paths) and reports the concrete level in the result's `Detail`. |
| `RepoContextReuseAck` | One suppressed unit or file the caller already held. |
| `RepoContextEntryView` | `repocontext_recall`, and each entry of a scan, a neighbor walk or a search hit: the key, family, flattened `Fields`, `Tags`, `Links`, the expiry fields, and the memory link-staleness fields (`Stale`, `StaleLinks`, `DanglingLinks`). Fields a bulk read does not evaluate are `null`. |
| `RepoContextScanResult` | `repocontext_scan`: one page of `Entries`, `ContinuationToken` and `HasMore`. |
| `RepoContextNeighborsResult` | `repocontext_neighbors` |
| `RepoContextTopicsResult` / `RepoContextTopicSummary` | `repocontext_list_topics`: one summary per topic with its `EntryCount`. |
| `RepoContextRepoListResult` / `RepoContextRepoSummary` | `repocontext_list_repos`: one summary per repository, including `IndexedRoot`, `LastIngested`, `FileCount`, `EmbeddedVectorCount`, `EmbeddedVectorCountPending`, `EmbeddedVectorCountAsOf` and `IndexedCommit`. |
| `RepoContextEmbeddedCount` | The memoised embedded-source count behind those summary fields: `Count`, `Pending` and `AsOf`, built by `Exact(count)` or `PendingRefresh(count)`. |
| `RepoContextOutlineResult` / `RepoContextOutlineSymbol` | `repocontext_outline` |
| `RepoContextRelatedResult` / `RepoContextRelatedEdge` | `repocontext_related` |
| `RepoContextChangedResult` | `repocontext_changed` |
| `RepoContextRememberResult` | `repocontext_remember` |
| `RepoContextUpdateResult` | `repocontext_update` |
| `RepoContextForgetResult` | `repocontext_forget` |
| `RepoContextClaimResult` | `repocontext_claim` and `repocontext_renew_claim` |
| `RepoContextClaimStatusResult` | `repocontext_claim_status`; its `Authoritative` is always `false`. |
| `RepoContextReleaseClaimResult` | `repocontext_release_claim` |
| `RepoContextRepoRemovalResult` | `repocontext_remove_repo` |
| `RepoContextIndexResetResult` | `repocontext_reset_index` |
| `RepoContextHealthResult` | `repocontext_health`; with a repository id, `Repository` carries a `RepoContextRepositoryReadiness` verdict (its `Authoritative` is always `false`). See [Repository readiness](tools.md#repository-readiness). |
| `RepoContextStatsResult` | `repocontext_stats` |
| `RepoContextBootstrapResult` | The counters of one completed indexing pass (files scanned, added, updated, removed and unchanged, symbols captured, elapsed time). The onboarding tools start the pass asynchronously and return `RepoIndexProgress` instead, so no tool returns this record directly. |

`RepoContextClaimConflictException` is the protocol error a claimed record's fencing check raises for a superseded, missing, released, unissued or wrong-region token. It derives from the MCP SDK's `McpException` and implements `ILatticeDomainFault`, and its `Key`, `Reason`, `PresentedFencingToken`, `CurrentFencingToken`, `Owner` and `Region` let an agent decide whether to re-claim. It is raised inside the MCP host and never crosses a grain boundary. See [How a claim is enforced](tools.md#how-a-claim-is-enforced).

## Indexing records

`repocontext_add_repo`, `repocontext_bootstrap` and `repocontext_index_status` return `RepoIndexProgress`. These records also cross the boundary of the per-repository indexing grain - the job request and its progress are persisted with the job so it can resume after a restart - so, unlike the tool results above, they carry Orleans serialization attributes.

| Type | Role |
|---|---|
| `RepoIndexProgress` | `Status`, `Phase`, the file, chunk and embedding counters (`FilesScanned` ... `SymbolsEmbedded`), the reset counters `TreesSwept` and `EntriesDeleted`, `Attempt`, the timestamps, `Error`, and the adaptive `Pacing` snapshot. See [Asynchronous indexing lifecycle](tools.md#asynchronous-indexing-lifecycle). |
| `RepoIndexStatus` | The job status (`None`, `Running`, `Completed`, `Failed`). |
| `RepoIndexPhase` | `Pending`, `Walking`, `Reconciling`, `Applying`, `Vectorising`, `Done`, `Resetting`. |
| `RepoIndexPacing` / `RepoIndexPaceState` | The pacer's `State` (`Disabled`, `Idle`, `Pacing`, `Backoff`, `Waiting`, `Resting`, `Yielding`), `Reason`, `BatchDelayMilliseconds`, `Since` and `ForegroundRequests`. See [Adaptive pacing](tools.md#adaptive-pacing). |
| `RepoIndexProgressUpdate` | A partial progress delta: every counter is optional, and `FilesEmbeddedDelta` (newly stored file embeddings since the previous report) is added to the durable job total, so a resumed drive neither erases nor undercounts the earlier one. |
| `RepoIndexJobRequest` | The durable inputs of a job: the resolved `RepoRoot`, `RepoId`, the include and exclude globs, `RespectGitignore`, `ExcludeBinary`, `AllowPrune`, `CommitSha` and `ForceEmbeddingGapScan`. `RepoRoot` has already passed the workspace guard. |

## Options

`RepoContextTtlOptions` (per-repository memory TTL policy) and `OnyxEmbeddingOptions` are documented in [Configuration](configuration.md).

## Related

- [Configuration](configuration.md)
- [Architecture](architecture.md)
- [Tools](tools.md), [Record model](record-model.md), [Semantic search](semantic-search.md)
- [`Orleans.Lattice.Api.Mcp` public API](../lattice.api.mcp/api.md) - the server these tools register on.
- [`Orleans.Lattice.Api.Mcp.RepoContext.Replication` public API](../lattice.api.mcp.repocontext.replication/api.md)

## Public declaration reference

The following inventory includes directly declared public members and overloads, enum values, and record positional members. Inherited framework members and compiler-generated record equality helpers are not additional package operations. Each source link identifies the declaration that defines its signature.

### `Orleans.Lattice.Api.Mcp.RepoContext.EmbeddingResult`

[Source](../../src/lattice.api.mcp.repocontext/Embedding/EmbeddingResult.cs) (line 13).

`public sealed record EmbeddingResult`

- `public bool Succeeded { get; }`
- `public EmbeddingSpace Space { get; }`
- `public IReadOnlyList<ReadOnlyMemory<float>> Vectors { get; }`
- `public string? Error { get; }`
- `public static EmbeddingResult Success(EmbeddingSpace space, IReadOnlyList<ReadOnlyMemory<float>> vectors)`
- `public static EmbeddingResult Failure(EmbeddingSpace space, string error)`

### `Orleans.Lattice.Api.Mcp.RepoContext.EmbeddingSpace`

[Source](../../src/lattice.api.mcp.repocontext/Embedding/EmbeddingSpace.cs) (line 17).

`public sealed record EmbeddingSpace`

- `public EmbeddingSpace(string modelId, int dimension, bool normalized)`
- `public string ModelId { get; }`
- `public int Dimension { get; }`
- `public bool Normalized { get; }`

### `Orleans.Lattice.Api.Mcp.RepoContext.EmbeddingTextType`

[Source](../../src/lattice.api.mcp.repocontext/Embedding/EmbeddingTextType.cs) (line 12).

`public enum EmbeddingTextType`

- `Passage = 0`
- `Query = 1`

### `Orleans.Lattice.Api.Mcp.RepoContext.IEmbeddingProvider`

[Source](../../src/lattice.api.mcp.repocontext/Embedding/IEmbeddingProvider.cs) (line 25).

`public interface IEmbeddingProvider`

- `EmbeddingSpace Space { get; }`
- `Task<bool> IsAvailableAsync(CancellationToken cancellationToken = default)`
- `Task<EmbeddingResult> EmbedAsync(IReadOnlyList<string> texts, EmbeddingTextType textType, CancellationToken cancellationToken = default)`

### `Orleans.Lattice.Api.Mcp.RepoContext.IRepoContextRetrievalWarmup`

[Source](../../src/lattice.api.mcp.repocontext/Retrieval/IRepoContextRetrievalWarmup.cs) (line 24).

`public interface IRepoContextRetrievalWarmup`

- `Task<bool> TryWarmAsync(CancellationToken cancellationToken)`

### `Orleans.Lattice.Api.Mcp.RepoContext.IRepoIndexRunAuthority`

[Source](../../src/lattice.api.mcp.repocontext/Indexing/IRepoIndexRunAuthority.cs) (line 29).

`public interface IRepoIndexRunAuthority`

- `LatticeCredential? Resolve()`

### `Orleans.Lattice.Api.Mcp.RepoContext.LatticeMcpRepoContextEmbeddingServiceCollectionExtensions`

[Source](../../src/lattice.api.mcp.repocontext/Embedding/LatticeMcpRepoContextEmbeddingServiceCollectionExtensions.cs) (line 12).

`public static class LatticeMcpRepoContextEmbeddingServiceCollectionExtensions`

- `public static IServiceCollection AddOnyxEmbeddingProvider(this IServiceCollection services, Action<OnyxEmbeddingOptions>? configure = null)`

### `Orleans.Lattice.Api.Mcp.RepoContext.LatticeMcpRepoContextServiceCollectionExtensions`

[Source](../../src/lattice.api.mcp.repocontext/LatticeMcpRepoContextServiceCollectionExtensions.cs) (line 34).

`public static class LatticeMcpRepoContextServiceCollectionExtensions`

- `public static IServiceCollection AddRepoContextTools(this IServiceCollection services, bool enableWrites = false, bool workspaceMode = false, string? workspaceRoot = null, bool registerAsApp = false)`

### `Orleans.Lattice.Api.Mcp.RepoContext.OnyxEmbeddingOptions`

[Source](../../src/lattice.api.mcp.repocontext/Embedding/OnyxEmbeddingOptions.cs) (line 19).

`public sealed class OnyxEmbeddingOptions`

- `public const string DefaultModelName`
- `public const int DefaultDimension`
- `public const int DefaultMaxContextLength`
- `public const string DefaultBaseAddress`
- `public Uri BaseAddress { get; set; }`
- `public string ModelName { get; set; }`
- `public int Dimension { get; set; }`
- `public int MaxContextLength { get; set; }`
- `public bool NormalizeEmbeddings { get; set; }`
- `public TimeSpan? RequestTimeout { get; set; }`

### `Orleans.Lattice.Api.Mcp.RepoContext.RepoContextBootstrapResult`

[Source](../../src/lattice.api.mcp.repocontext/Bootstrap/RepoContextBootstrapResult.cs) (line 15).

`public sealed record RepoContextBootstrapResult`

- `public required string RepoId { get; init; }`
- `public required int FilesScanned { get; init; }`
- `public required int FilesAdded { get; init; }`
- `public required int FilesUpdated { get; init; }`
- `public required int FilesRemoved { get; init; }`
- `public required int FilesUnchanged { get; init; }`
- `public required int SymbolsCaptured { get; init; }`
- `public required long ElapsedMilliseconds { get; init; }`

### `Orleans.Lattice.Api.Mcp.RepoContext.RepoContextChangedResult`

[Source](../../src/lattice.api.mcp.repocontext/Graph/RepoContextChangedResult.cs) (line 15).

`public sealed record RepoContextChangedResult`

- `public required string RepoId { get; init; }`
- `public required IReadOnlyList<string> Added { get; init; }`
- `public required IReadOnlyList<string> Updated { get; init; }`
- `public required IReadOnlyList<string> Removed { get; init; }`
- `public required IReadOnlyList<string> Dependents { get; init; }`

### `Orleans.Lattice.Api.Mcp.RepoContext.RepoContextClaimConflictException`

[Source](../../src/lattice.api.mcp.repocontext/Claims/RepoContextClaimConflictException.cs) (line 29).

`public sealed class RepoContextClaimConflictException : McpException, ILatticeDomainFault`

- `public RepoContextClaimConflictException()`
- `public RepoContextClaimConflictException(string message)`
- `public RepoContextClaimConflictException(string message, Exception innerException)`
- `public string Key { get; }`
- `public string Reason { get; }`
- `public long? PresentedFencingToken { get; }`
- `public long? CurrentFencingToken { get; }`
- `public string? Owner { get; }`
- `public string? Region { get; }`

### `Orleans.Lattice.Api.Mcp.RepoContext.RepoContextClaimResult`

[Source](../../src/lattice.api.mcp.repocontext/Claims/RepoContextClaimResult.cs) (line 12).

`public sealed record RepoContextClaimResult`

- `public required string Key { get; init; }`
- `public required string LockName { get; init; }`
- `public required bool Granted { get; init; }`
- `public long? FencingToken { get; init; }`
- `public string? Owner { get; init; }`
- `public string? Region { get; init; }`
- `public string? LeaseExpiresAtUtc { get; init; }`
- `public double? LeaseSeconds { get; init; }`
- `public string? PreviousLeaseExpiresAtUtc { get; init; }`
- `public bool? LeaseShortened { get; init; }`
- `public string? Reason { get; init; }`

### `Orleans.Lattice.Api.Mcp.RepoContext.RepoContextClaimStatusResult`

[Source](../../src/lattice.api.mcp.repocontext/Claims/RepoContextClaimStatusResult.cs) (line 22).

`public sealed record RepoContextClaimStatusResult`

- `public required string Key { get; init; }`
- `public required string LockName { get; init; }`
- `public required bool Claimed { get; init; }`
- `public required bool IsHeld { get; init; }`
- `public long? FencingToken { get; init; }`
- `public long? ReleasedFencingToken { get; init; }`
- `public string? Owner { get; init; }`
- `public string? Region { get; init; }`
- `public required long LockFencingToken { get; init; }`
- `public string? LeaseExpiresAtUtc { get; init; }`
- `public required int QueueDepth { get; init; }`
- `public bool Authoritative { get; }`
- `public required bool Exists { get; init; }`

### `Orleans.Lattice.Api.Mcp.RepoContext.RepoContextContextDetail`

[Source](../../src/lattice.api.mcp.repocontext/Retrieval/RepoContextContextDetail.cs) (line 15).

`public enum RepoContextContextDetail`

- `Auto = 0`
- `Paths = 1`
- `Outline = 2`
- `Slices = 3`

### `Orleans.Lattice.Api.Mcp.RepoContext.RepoContextContextEntry`

[Source](../../src/lattice.api.mcp.repocontext/Retrieval/RepoContextContextEntry.cs) (line 14).

`public sealed record RepoContextContextEntry`

- `public required string Path { get; init; }`
- `public required double Score { get; init; }`
- `public IReadOnlyList<string> Reasons { get; init; }`
- `public required int TokenCount { get; init; }`
- `public required int? FullReadTokenCount { get; init; }`
- `public required string Content { get; init; }`
- `public string? ContentHash { get; init; }`
- `public IReadOnlyList<RepoContextContextUnit> Units { get; init; }`

### `Orleans.Lattice.Api.Mcp.RepoContext.RepoContextContextResult`

[Source](../../src/lattice.api.mcp.repocontext/Retrieval/RepoContextContextResult.cs) (line 25).

`public sealed record RepoContextContextResult`

- `public required string RepoId { get; init; }`
- `public required string Task { get; init; }`
- `public required string Mode { get; init; }`
- `public required string RetrievalPath { get; init; }`
- `public required string Detail { get; init; }`
- `public required int BudgetTokens { get; init; }`
- `public required int TotalTokens { get; init; }`
- `public required int ResponseTokens { get; init; }`
- `public required bool Truncated { get; init; }`
- `public required int? RetryBudgetTokens { get; init; }`
- `public required IReadOnlyList<RepoContextContextEntry> Entries { get; init; }`
- `public string? Session { get; init; }`
- `public IReadOnlyList<RepoContextReuseAck> Reused { get; init; }`

### `Orleans.Lattice.Api.Mcp.RepoContext.RepoContextContextUnit`

[Source](../../src/lattice.api.mcp.repocontext/Retrieval/RepoContextContextUnit.cs) (line 31).

`public sealed record RepoContextContextUnit`

- `public required string Receipt { get; init; }`
- `public required string Kind { get; init; }`
- `public string? Symbol { get; init; }`
- `public required int TokenCount { get; init; }`

### `Orleans.Lattice.Api.Mcp.RepoContext.RepoContextEmbeddedCount`

[Source](../../src/lattice.api.mcp.repocontext/Retrieval/RepoContextEmbeddedCount.cs) (line 25).

`public readonly record struct RepoContextEmbeddedCount`

- `public long? Count { get; init; }`
- `public bool Pending { get; init; }`
- `public DateTimeOffset? AsOf { get; init; }`
- `public static RepoContextEmbeddedCount Exact(long count)`
- `public static RepoContextEmbeddedCount PendingRefresh(long? count)`

### `Orleans.Lattice.Api.Mcp.RepoContext.RepoContextEntryView`

[Source](../../src/lattice.api.mcp.repocontext/Capture/RepoContextEntryView.cs) (line 20).

`public sealed record RepoContextEntryView`

- `public required string Key { get; init; }`
- `public required bool Exists { get; init; }`
- `public required string Kind { get; init; }`
- `public required string RepoId { get; init; }`
- `public string? Path { get; init; }`
- `public string? FullyQualifiedName { get; init; }`
- `public string? Topic { get; init; }`
- `public string? Id { get; init; }`
- `public required IReadOnlyDictionary<string, string> Fields { get; init; }`
- `public required IReadOnlyList<string> Tags { get; init; }`
- `public required IReadOnlyDictionary<string, IReadOnlyList<string>> Links { get; init; }`
- `public bool? Expires { get; init; }`
- `public string? ExpiresAtUtc { get; init; }`
- `public double? RemainingSeconds { get; init; }`
- `public bool? HasExpired { get; init; }`
- `public bool? Stale { get; init; }`
- `public IReadOnlyList<string>? StaleLinks { get; init; }`
- `public IReadOnlyList<string>? DanglingLinks { get; init; }`

### `Orleans.Lattice.Api.Mcp.RepoContext.RepoContextEnvironmentVariables`

[Source](../../src/lattice.api.mcp.repocontext/RepoContextEnvironmentVariables.cs) (line 37).

`public static class RepoContextEnvironmentVariables`

- `public const string GitSourceSettingPrefix`
- `public static IReadOnlyList<string> IndexingKeys { get; }`
- `public static IReadOnlyList<string> GitSourceKeys { get; }`
- `public static IReadOnlyList<string> MemoryArchiveKeys { get; }`
- `public static IReadOnlyList<string> AnnKeys { get; }`
- `public static IReadOnlyList<string> All { get; }`
- `public static IReadOnlyList<string> Prefixes { get; }`
- `public static IReadOnlyList<RepoContextSettingSnapshot> DescribeResolvedSettings()`
- `public static bool IsPrefixMatched(string name)`

### `Orleans.Lattice.Api.Mcp.RepoContext.RepoContextForgetResult`

[Source](../../src/lattice.api.mcp.repocontext/Capture/RepoContextForgetResult.cs) (line 12).

`public sealed record RepoContextForgetResult`

- `public required string Key { get; init; }`
- `public required string Mode { get; init; }`
- `public required bool Existed { get; init; }`
- `public string? ExpiresAtUtc { get; init; }`
- `public bool Undecodable { get; init; }`

### `Orleans.Lattice.Api.Mcp.RepoContext.RepoContextHealthResult`

[Source](../../src/lattice.api.mcp.repocontext/RepoContextHealthResult.cs) (line 42).

`public sealed record RepoContextHealthResult`

- `public required bool Available { get; init; }`
- `public required string Group { get; init; }`
- `public required string Status { get; init; }`
- `public required bool RetrievalReady { get; init; }`
- `public required string RetrievalPhase { get; init; }`
- `public RepoContextRepositoryReadiness? Repository { get; init; }`

### `Orleans.Lattice.Api.Mcp.RepoContext.RepoContextIndexResetResult`

[Source](../../src/lattice.api.mcp.repocontext/Capture/RepoContextIndexResetResult.cs) (line 17).

`public sealed record RepoContextIndexResetResult`

- `public required string RepoId { get; init; }`
- `public required int EntriesDeleted { get; init; }`
- `public required long ElapsedMilliseconds { get; init; }`
- `public required IReadOnlyList<string> TreesSwept { get; init; }`
- `public required bool MemoryPreserved { get; init; }`
- `public required bool CensusCleared { get; init; }`

### `Orleans.Lattice.Api.Mcp.RepoContext.RepoContextMemoryDurabilityReport`

[Source](../../src/lattice.api.mcp.repocontext/Archive/RepoContextMemoryDurabilityReport.cs) (line 44).

`public static class RepoContextMemoryDurabilityReport`

- `public static RepoContextMemoryDurabilityStatement Describe(string? dataRoot, string? walDirectory, string? grainStorePath)`

### `Orleans.Lattice.Api.Mcp.RepoContext.RepoContextMemoryDurabilityStatement`

[Source](../../src/lattice.api.mcp.repocontext/Archive/RepoContextMemoryDurabilityReport.cs) (line 15).

`public sealed record RepoContextMemoryDurabilityStatement(IReadOnlyList<string> Lines, bool IsWarning, bool IsArchived, bool ArchiveSharesDataRoot)`

- `IReadOnlyList<string> Lines`
- `bool IsWarning`
- `bool IsArchived`
- `bool ArchiveSharesDataRoot`

### `Orleans.Lattice.Api.Mcp.RepoContext.RepoContextNeighborsResult`

[Source](../../src/lattice.api.mcp.repocontext/Capture/RepoContextNeighborsResult.cs) (line 13).

`public sealed record RepoContextNeighborsResult`

- `public required string Key { get; init; }`
- `public required bool Exists { get; init; }`
- `public required IReadOnlyList<RepoContextEntryView> Neighbors { get; init; }`
- `public required bool Truncated { get; init; }`

### `Orleans.Lattice.Api.Mcp.RepoContext.RepoContextOutlineResult`

[Source](../../src/lattice.api.mcp.repocontext/Graph/RepoContextOutlineResult.cs) (line 13).

`public sealed record RepoContextOutlineResult`

- `public required string RepoId { get; init; }`
- `public required string Path { get; init; }`
- `public required bool Exists { get; init; }`
- `public required int? FullReadTokenCount { get; init; }`
- `public required IReadOnlyList<RepoContextOutlineSymbol> Symbols { get; init; }`

### `Orleans.Lattice.Api.Mcp.RepoContext.RepoContextOutlineSymbol`

[Source](../../src/lattice.api.mcp.repocontext/Graph/RepoContextOutlineSymbol.cs) (line 12).

`public sealed record RepoContextOutlineSymbol`

- `public required string FullyQualifiedName { get; init; }`
- `public required string Kind { get; init; }`
- `public required string Signature { get; init; }`
- `public required long StartLine { get; init; }`
- `public required long EndLine { get; init; }`

### `Orleans.Lattice.Api.Mcp.RepoContext.RepoContextRelatedEdge`

[Source](../../src/lattice.api.mcp.repocontext/Graph/RepoContextRelatedEdge.cs) (line 13).

`public sealed record RepoContextRelatedEdge`

- `public required string Symbol { get; init; }`
- `public required string? Path { get; init; }`

### `Orleans.Lattice.Api.Mcp.RepoContext.RepoContextRelatedResult`

[Source](../../src/lattice.api.mcp.repocontext/Graph/RepoContextRelatedResult.cs) (line 15).

`public sealed record RepoContextRelatedResult`

- `public required string RepoId { get; init; }`
- `public required string Path { get; init; }`
- `public required bool Exists { get; init; }`
- `public required IReadOnlyList<string> Imports { get; init; }`
- `public required IReadOnlyList<RepoContextRelatedEdge> Dependents { get; init; }`
- `public required IReadOnlyList<RepoContextRelatedEdge> Tests { get; init; }`

### `Orleans.Lattice.Api.Mcp.RepoContext.RepoContextReleaseClaimResult`

[Source](../../src/lattice.api.mcp.repocontext/Claims/RepoContextReleaseClaimResult.cs) (line 11).

`public sealed record RepoContextReleaseClaimResult`

- `public required string Key { get; init; }`
- `public required string LockName { get; init; }`
- `public required bool Released { get; init; }`
- `public required long FencingToken { get; init; }`
- `public string? Reason { get; init; }`

### `Orleans.Lattice.Api.Mcp.RepoContext.RepoContextRememberResult`

[Source](../../src/lattice.api.mcp.repocontext/Capture/RepoContextRememberResult.cs) (line 12).

`public sealed record RepoContextRememberResult`

- `public required string Key { get; init; }`
- `public required string RepoId { get; init; }`
- `public required string Topic { get; init; }`
- `public required string Id { get; init; }`
- `public required bool Created { get; init; }`
- `public bool? Expires { get; init; }`
- `public string? ExpiresAtUtc { get; init; }`
- `public int LinksAdded { get; init; }`
- `public int LinksRemoved { get; init; }`

### `Orleans.Lattice.Api.Mcp.RepoContext.RepoContextRepoListResult`

[Source](../../src/lattice.api.mcp.repocontext/Capture/RepoContextRepoListResult.cs) (line 13).

`public sealed record RepoContextRepoListResult`

- `public required IReadOnlyList<RepoContextRepoSummary> Repos { get; init; }`
- `public required int Count { get; init; }`

### `Orleans.Lattice.Api.Mcp.RepoContext.RepoContextRepoRemovalResult`

[Source](../../src/lattice.api.mcp.repocontext/Capture/RepoContextRepoRemovalResult.cs) (line 13).

`public sealed record RepoContextRepoRemovalResult`

- `public required string RepoId { get; init; }`
- `public required int EntriesDeleted { get; init; }`

### `Orleans.Lattice.Api.Mcp.RepoContext.RepoContextRepoSummary`

[Source](../../src/lattice.api.mcp.repocontext/Capture/RepoContextRepoSummary.cs) (line 15).

`public sealed record RepoContextRepoSummary`

- `public required string RepoId { get; init; }`
- `public string? IndexedRoot { get; init; }`
- `public string? LastIngested { get; init; }`
- `public long? FileCount { get; init; }`
- `public long? EmbeddedVectorCount { get; init; }`
- `public bool EmbeddedVectorCountPending { get; init; }`
- `public DateTimeOffset? EmbeddedVectorCountAsOf { get; init; }`
- `public string? IndexedCommit { get; init; }`

### `Orleans.Lattice.Api.Mcp.RepoContext.RepoContextRepositoryReadiness`

[Source](../../src/lattice.api.mcp.repocontext/RepoContextRepositoryReadiness.cs) (line 10).

`public sealed record RepoContextRepositoryReadiness`

- `public required string RepoId { get; init; }`
- `public required RepoContextRetrievalReadinessPhase Verdict { get; init; }`
- `public string? Reason { get; init; }`
- `public bool Authoritative { get; }`
- `public RepoIndexProgress? Ingest { get; init; }`
- `public string? IngestReason { get; init; }`
- `public RepoContextEmbeddedCount VectorCoverage { get; init; }`
- `public EmbeddingSpace? EmbeddingSpace { get; init; }`
- `public VectorIndexBuildProgress? Ann { get; init; }`
- `public bool? AnnCanServe { get; init; }`
- `public bool BreakerOpen { get; init; }`
- `public TimeSpan? BreakerProbeDueIn { get; init; }`
- `public string? LastRetrievalPath { get; init; }`
- `public DateTimeOffset? LastQueryAt { get; init; }`
- `public RepoContextRetrievalReadinessPhase ContentPhase { get; init; }`
- `public string? ContentReason { get; init; }`
- `public DateTimeOffset? ContentObservedAt { get; init; }`

### `Orleans.Lattice.Api.Mcp.RepoContext.RepoContextRetrievalArming`

[Source](../../src/lattice.api.mcp.repocontext/Retrieval/RepoContextRetrievalArming.cs) (line 33).

`public enum RepoContextRetrievalArming`

- `Unknown = 0`
- `Unarmed = 1`
- `Armed = 2`

### `Orleans.Lattice.Api.Mcp.RepoContext.RepoContextRetrievalPath`

[Source](../../src/lattice.api.mcp.repocontext/Retrieval/RepoContextRetrievalPath.cs) (line 33).

`public static class RepoContextRetrievalPath`

- `public const string SemanticExact`
- `public const string SemanticApproximate`
- `public const string KeywordNoEmbedder`
- `public const string KeywordVectorPlaneUnavailable`
- `public const string KeywordIndexDegraded`
- `public const string KeywordExactFallbackSuppressed`
- `public static string NormalizeSemantic(string? declared)`
- `public static bool IsSemantic(string? path)`

### `Orleans.Lattice.Api.Mcp.RepoContext.RepoContextRetrievalReadinessPhase`

[Source](../../src/lattice.api.mcp.repocontext/Retrieval/RepoContextRetrievalReadinessPhase.cs) (line 9).

`public enum RepoContextRetrievalReadinessPhase`

- `Building = 0`
- `Serving = 1`
- `KeywordOnly = 2`
- `NothingRegistered = 3`
- `SaturatedUnavailable = 4`

### `Orleans.Lattice.Api.Mcp.RepoContext.RepoContextRetrievalReadinessState`

[Source](../../src/lattice.api.mcp.repocontext/Retrieval/RepoContextRetrievalReadinessState.cs) (line 37).

`public sealed class RepoContextRetrievalReadinessState : IDisposable`

- `public const string SaturatedCause`
- `public const string ProbeCause`
- `public static readonly TimeSpan DefaultFaultHoldDown`
- `public RepoContextRetrievalReadinessState(TimeProvider timeProvider, TimeSpan? faultHoldDown = null)`
- `public TimeSpan FaultHoldDown { get; }`
- `public RepoContextRetrievalReadinessPhase Phase { get; }`
- `public bool IsReady { get; }`
- `public static bool IsReadyPhase(RepoContextRetrievalReadinessPhase phase)`
- `public static string PhaseTag(RepoContextRetrievalReadinessPhase phase)`
- `public RepoContextRetrievalArming Arming { get; }`
- `public TimeSpan? TimeToReady { get; }`
- `public void Observe(string? retrievalPath)`
- `public void ObserveArming(RepoContextRetrievalArming arming)`
- `public void MarkServing()`
- `public bool MarkSaturationUnavailable()`
- `public bool ClearSaturationUnavailable()`
- `public void MarkKeywordOnly()`
- `public void MarkNothingRegistered()`
- `public void MarkUnavailable(string? cause = null)`
- `public void Dispose()`

### `Orleans.Lattice.Api.Mcp.RepoContext.RepoContextReuseAck`

[Source](../../src/lattice.api.mcp.repocontext/Retrieval/RepoContextReuseAck.cs) (line 22).

`public sealed record RepoContextReuseAck`

- `public required string Path { get; init; }`
- `public required string Kind { get; init; }`
- `public string? Receipt { get; init; }`
- `public string? ContentHash { get; init; }`
- `public string? Symbol { get; init; }`

### `Orleans.Lattice.Api.Mcp.RepoContext.RepoContextScanResult`

[Source](../../src/lattice.api.mcp.repocontext/Capture/RepoContextScanResult.cs) (line 19).

`public sealed record RepoContextScanResult`

- `public required IReadOnlyList<RepoContextEntryView> Entries { get; init; }`
- `public string? ContinuationToken { get; init; }`
- `public required bool HasMore { get; init; }`

### `Orleans.Lattice.Api.Mcp.RepoContext.RepoContextSearchHit`

[Source](../../src/lattice.api.mcp.repocontext/Retrieval/RepoContextSearchHit.cs) (line 14).

`public sealed record RepoContextSearchHit`

- `public required double Score { get; init; }`
- `public required RepoContextEntryView Entry { get; init; }`
- `public string? VectorId { get; init; }`
- `public IReadOnlyList<string> Reasons { get; init; }`

### `Orleans.Lattice.Api.Mcp.RepoContext.RepoContextSearchResult`

[Source](../../src/lattice.api.mcp.repocontext/Retrieval/RepoContextSearchResult.cs) (line 16).

`public sealed record RepoContextSearchResult`

- `public required string RepoId { get; init; }`
- `public required string Query { get; init; }`
- `public required string Mode { get; init; }`
- `public required string RetrievalPath { get; init; }`
- `public required IReadOnlyList<RepoContextSearchHit> Hits { get; init; }`

### `Orleans.Lattice.Api.Mcp.RepoContext.RepoContextSettingSnapshot`

[Source](../../src/lattice.api.mcp.repocontext/RepoContextSettingSnapshot.cs) (line 38).

`public readonly record struct RepoContextSettingSnapshot(string Name, string Resolved, string Default, bool WasDeclared)`

- `string Name`
- `string Resolved`
- `string Default`
- `bool WasDeclared`

### `Orleans.Lattice.Api.Mcp.RepoContext.RepoContextStatsResult`

[Source](../../src/lattice.api.mcp.repocontext/RepoContextStatsResult.cs) (line 15).

`public sealed record RepoContextStatsResult`

- `public required long Calls { get; init; }`
- `public required long ResponseTokens { get; init; }`
- `public required long ReadsReplacedTokens { get; init; }`
- `public required long NetSavedTokens { get; init; }`
- `public required double WindowSeconds { get; init; }`

### `Orleans.Lattice.Api.Mcp.RepoContext.RepoContextTopicSummary`

[Source](../../src/lattice.api.mcp.repocontext/Capture/RepoContextTopicsResult.cs) (line 11).

`public sealed record RepoContextTopicSummary`

- `public required string Topic { get; init; }`
- `public required int EntryCount { get; init; }`

### `Orleans.Lattice.Api.Mcp.RepoContext.RepoContextTopicsResult`

[Source](../../src/lattice.api.mcp.repocontext/Capture/RepoContextTopicsResult.cs) (line 29).

`public sealed record RepoContextTopicsResult`

- `public required string RepoId { get; init; }`
- `public required IReadOnlyList<RepoContextTopicSummary> Topics { get; init; }`

### `Orleans.Lattice.Api.Mcp.RepoContext.RepoContextTtlOptions`

[Source](../../src/lattice.api.mcp.repocontext/RepoContextTtlOptions.cs) (line 25).

`public sealed class RepoContextTtlOptions`

- `public TimeSpan? DefaultMemoryTtl { get; set; }`
- `public bool StructuralRecordsNeverExpire { get; set; }`

### `Orleans.Lattice.Api.Mcp.RepoContext.RepoContextUpdateResult`

[Source](../../src/lattice.api.mcp.repocontext/Capture/RepoContextUpdateResult.cs) (line 11).

`public sealed record RepoContextUpdateResult`

- `public required string Key { get; init; }`
- `public required string Kind { get; init; }`
- `public required int FieldsUpdated { get; init; }`
- `public required int TagsAdded { get; init; }`
- `public required int TagsRemoved { get; init; }`
- `public int LinksAdded { get; init; }`
- `public int LinksRemoved { get; init; }`

### `Orleans.Lattice.Api.Mcp.RepoContext.RepoIndexJobRequest`

[Source](../../src/lattice.api.mcp.repocontext/Indexing/RepoIndexJobRequest.cs) (line 21).

`public sealed record RepoIndexJobRequest`

- `public required string RepoRoot { get; init; }`
- `public required string RepoId { get; init; }`
- `public IReadOnlyList<string>? IncludeGlobs { get; init; }`
- `public IReadOnlyList<string>? ExcludeGlobs { get; init; }`
- `public bool RespectGitignore { get; init; }`
- `public bool ExcludeBinary { get; init; }`
- `public bool AllowPrune { get; init; }`
- `public string? CommitSha { get; init; }`
- `public bool ForceEmbeddingGapScan { get; init; }`

### `Orleans.Lattice.Api.Mcp.RepoContext.RepoIndexPaceState`

[Source](../../src/lattice.api.mcp.repocontext/Indexing/RepoIndexPaceState.cs) (line 10).

`public enum RepoIndexPaceState`

- `Disabled = 0`
- `Idle = 1`
- `Pacing = 2`
- `Backoff = 3`
- `Waiting = 4`
- `Resting = 5`
- `Yielding = 6`

### `Orleans.Lattice.Api.Mcp.RepoContext.RepoIndexPacing`

[Source](../../src/lattice.api.mcp.repocontext/Indexing/RepoIndexPacing.cs) (line 17).

`public sealed record RepoIndexPacing`

- `public required RepoIndexPaceState State { get; init; }`
- `public required string Reason { get; init; }`
- `public long BatchDelayMilliseconds { get; init; }`
- `public DateTimeOffset? Since { get; init; }`
- `public int ForegroundRequests { get; init; }`

### `Orleans.Lattice.Api.Mcp.RepoContext.RepoIndexPhase`

[Source](../../src/lattice.api.mcp.repocontext/Indexing/RepoIndexPhase.cs) (line 10).

`public enum RepoIndexPhase`

- `Pending = 0`
- `Walking = 1`
- `Reconciling = 2`
- `Applying = 3`
- `Vectorising = 4`
- `Done = 5`
- `Resetting = 6`

### `Orleans.Lattice.Api.Mcp.RepoContext.RepoIndexProgress`

[Source](../../src/lattice.api.mcp.repocontext/Indexing/RepoIndexProgress.cs) (line 15).

`public sealed record RepoIndexProgress`

- `public required string RepoId { get; init; }`
- `public required RepoIndexStatus Status { get; init; }`
- `public required RepoIndexPhase Phase { get; init; }`
- `public int FilesScanned { get; init; }`
- `public int FilesAdded { get; init; }`
- `public int FilesUpdated { get; init; }`
- `public int FilesRemoved { get; init; }`
- `public int FilesUnchanged { get; init; }`
- `public int ChunksTotal { get; init; }`
- `public int ChunksCommitted { get; init; }`
- `public int FilesEmbedded { get; init; }`
- `public int Attempt { get; init; }`
- `public DateTimeOffset? StartedAt { get; init; }`
- `public DateTimeOffset? UpdatedAt { get; init; }`
- `public DateTimeOffset? CompletedAt { get; init; }`
- `public long? ElapsedMilliseconds { get; init; }`
- `public string? Error { get; init; }`
- `public int FilesContentProjected { get; init; }`
- `public int SymbolsEmbedded { get; init; }`
- `public int TreesSwept { get; init; }`
- `public int EntriesDeleted { get; init; }`
- `public RepoIndexPacing? Pacing { get; init; }`

### `Orleans.Lattice.Api.Mcp.RepoContext.RepoIndexProgressUpdate`

[Source](../../src/lattice.api.mcp.repocontext/Indexing/RepoIndexProgressUpdate.cs) (line 12).

`public readonly record struct RepoIndexProgressUpdate`

- `public RepoIndexPhase? Phase { get; init; }`
- `public int? FilesScanned { get; init; }`
- `public int? FilesAdded { get; init; }`
- `public int? FilesUpdated { get; init; }`
- `public int? FilesRemoved { get; init; }`
- `public int? FilesUnchanged { get; init; }`
- `public int? ChunksTotal { get; init; }`
- `public int? ChunksCommitted { get; init; }`
- `public int? FilesEmbedded { get; init; }`
- `public int? FilesContentProjected { get; init; }`
- `public int? SymbolsEmbedded { get; init; }`
- `public int? FilesEmbeddedDelta { get; init; }`

### `Orleans.Lattice.Api.Mcp.RepoContext.RepoIndexStatus`

[Source](../../src/lattice.api.mcp.repocontext/Indexing/RepoIndexStatus.cs) (line 12).

`public enum RepoIndexStatus`

- `None = 0`
- `Running = 1`
- `Completed = 2`
- `Failed = 3`
