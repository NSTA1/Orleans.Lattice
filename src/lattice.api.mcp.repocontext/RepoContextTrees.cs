namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// The repository-context tree-name map: one dedicated named Lattice tree per
/// CRDT family. Each name is resolved by the host through
/// <c>IOptionsMonitor&lt;LatticeOptions&gt;.Get(treeName)</c>, so per-tree
/// options (replication, tombstone compaction, backup, and enumeration) can be
/// tuned independently per family. The map is part of the stable layout contract,
/// alongside the key grammar in <see cref="RepoContextKeys"/>.
/// <para>
/// Mixing CRDT types in a single tree is not a correctness problem (each key's
/// value is its own CRDT), but per-tree separation future-proofs selective
/// replication (replicate the store-of-record trees; treat rebuildable vector
/// projections as optionally local-only), independent time-to-live / garbage-
/// collection and backup policy per family, and clean single-tree enumeration for
/// derived-projection rebuilds. The three vector trees are reserved here so the
/// layout is fixed before the retrieval surface is built; this package writes
/// only the structural and memory trees today.
/// </para>
/// </summary>
internal static class RepoContextTrees
{
    /// <summary>
    /// Tree holding the structural nodes: repo, package, and file records (low
    /// churn; may keep the default per-tree options). Per-symbol records live in
    /// their own <see cref="Symbol"/> tree.
    /// </summary>
    internal const string Structural = "repo-context-structural";

    /// <summary>
    /// Tree holding the per-symbol structural records (type, member, and function
    /// declarations). Held apart from the other structural nodes so the symbol
    /// family can carry its own schema-version envelope and its own compaction
    /// policy: symbols churn on every re-index of a changed file, whereas the
    /// repo/package/file nodes are comparatively stable.
    /// </summary>
    internal const string Symbol = "repo-context-symbol";

    /// <summary>
    /// Tree holding agent-authored memory records (higher churn from re-write and
    /// forget cycles; the host configures finite tombstone compaction here).
    /// </summary>
    internal const string Memory = "repo-context-memory";

    /// <summary>
    /// Tree holding the per-file searchable-content projection: one record per
    /// text file carrying its bounded body text, keyed by
    /// <c>repo/{repoId}/content/{path}</c>. It is a rebuildable projection (like the
    /// vector trees), not store-of-record - it lets the keyword/degraded search path
    /// rank over file <b>content</b> rather than filenames and symbol names alone. It
    /// churns as files change and are pruned, so the host configures finite tombstone
    /// compaction here.
    /// </summary>
    internal const string Content = "repo-context-content";

    /// <summary>
    /// Tree holding the reverse cross-reference projection: one
    /// <see cref="CrossReferenceNode"/> per referenced simple type-name, keyed by
    /// <c>repo/{repoId}/xref/{name}</c>, recording which symbols reference that name
    /// (its dependents) and which test types cover it. It is a rebuildable projection
    /// (like the content and vector trees), not store-of-record - the symbol
    /// reconciler maintains it incrementally on every reconcile so the
    /// <c>repocontext_related</c> tool can answer inbound-dependent and test lookups
    /// without a full scan. It churns as symbols and their references change, so the
    /// host configures finite tombstone compaction here.
    /// </summary>
    internal const string CrossReference = "repo-context-xref";

    /// <summary>Reserved tree for vector membership (the retrieval surface, built later).</summary>
    internal const string VectorMembership = "repo-context-vector-membership";

    /// <summary>Reserved tree for vector payloads (the retrieval surface, built later).</summary>
    internal const string VectorPayload = "repo-context-vector-payload";

    /// <summary>Reserved tree for vector metadata (the retrieval surface, built later).</summary>
    internal const string VectorMetadata = "repo-context-vector-metadata";

    /// <summary>
    /// Every named tree in the layout contract, in a stable order. Includes the
    /// reserved vector trees so host wiring can enumerate the full set.
    /// </summary>
    internal static IReadOnlyList<string> All { get; } = new[]
    {
        Structural,
        Symbol,
        Memory,
        Content,
        CrossReference,
        VectorMembership,
        VectorPayload,
        VectorMetadata,
    };

    /// <summary>
    /// Resolves the named tree that stores records of the given
    /// <paramref name="kind"/>. Structural kinds (repo, package, file) map to
    /// <see cref="Structural"/>; <see cref="RepoContextRecordKind.Symbol"/> maps to
    /// its dedicated <see cref="Symbol"/> tree;
    /// <see cref="RepoContextRecordKind.Memory"/> maps to <see cref="Memory"/>; and
    /// the vector kinds map to their dedicated vector trees.
    /// </summary>
    /// <param name="kind">The record family to route.</param>
    /// <exception cref="ArgumentOutOfRangeException">The kind is not a known record kind.</exception>
    internal static string ForKind(RepoContextRecordKind kind) => kind switch
    {
        RepoContextRecordKind.Repo
            or RepoContextRecordKind.Package
            or RepoContextRecordKind.File => Structural,
        RepoContextRecordKind.Symbol => Symbol,
        RepoContextRecordKind.Memory => Memory,
        RepoContextRecordKind.Content => Content,
        RepoContextRecordKind.VectorMetadata => VectorMetadata,
        RepoContextRecordKind.VectorPayload => VectorPayload,
        RepoContextRecordKind.VectorMembership => VectorMembership,
        _ => throw new ArgumentOutOfRangeException(nameof(kind), kind, "Unknown repo-context record kind."),
    };
}
