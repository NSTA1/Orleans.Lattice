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
    /// Tree holding the persisted approximate nearest-neighbour index the semantic
    /// retrieval path queries instead of re-scanning the whole
    /// <see cref="VectorMetadata"/> prefix. It is a purely local, wholly derived
    /// accelerator over the other three vector trees: every record in it can be
    /// recomputed from them, and discarding the tree costs a rebuild and nothing
    /// else.
    /// <para>
    /// It is deliberately <b>absent from <see cref="All"/></b>, and that absence is
    /// the point rather than an oversight. <see cref="All"/> is the enrolment list
    /// the replication companion mirrors cross-cluster, and an index is the one
    /// thing that must not be mirrored: it is derived, each cluster builds its own
    /// far more cheaply than it could ship one, and a replicated index would
    /// interleave two clusters' generations under a layout whose recovery path
    /// deletes whole key ranges. It also holds no store-of-record data, so no
    /// backup or portability sweep needs to carry it.
    /// </para>
    /// </summary>
    internal const string VectorIndex = "repo-context-vector-index";

    /// <summary>
    /// Tree holding per-session context-bundle reuse bookkeeping: one
    /// <see cref="RepoContextSessionRecord"/> per <c>(repoId, sessionId)</c>, keyed
    /// by <c>repo/{repoId}/session/{sessionId}</c>, recording the opaque receipts of
    /// units already delivered to that session and the whole-file versions the
    /// session already possesses. It is a rebuildable, bounded, and <b>expirable</b>
    /// bookkeeping projection (never store-of-record): entries carry a finite
    /// time-to-live so an abandoned session's bookkeeping lapses on its own, and each
    /// record is a grow-only CRDT so concurrent bundle calls sharing a session id
    /// converge on merge. It churns as sessions are created and expire, so the host
    /// configures finite tombstone compaction here.
    /// </summary>
    internal const string Session = "repo-context-session";

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
        Session,
        VectorMembership,
        VectorPayload,
        VectorMetadata,
    };

    /// <summary>
    /// The fail-closed allow-list of derived vector-plane trees the self-healing
    /// re-derivation may reset when one falls terminally off its write-ahead log.
    /// It contains exactly the two <b>rebuildable</b> vector projections
    /// (<see cref="VectorMetadata"/> and <see cref="VectorMembership"/>) whose full
    /// content can be regenerated from the store-of-record structural, symbol, and
    /// memory trees plus the working files. The content-addressed, write-once
    /// <see cref="VectorPayload"/> tree is intentionally excluded (it has no
    /// in-place deletes and cannot be re-derived by a drop-and-re-embed), as are
    /// every store-of-record tree - resetting one of those would be real data loss.
    /// The set is the single authoritative classification the re-deriver consults;
    /// anything not listed here is refused.
    /// </summary>
    private static readonly IReadOnlySet<string> RebuildableVectorTrees =
        new HashSet<string>(StringComparer.Ordinal) { VectorMetadata, VectorMembership };

    /// <summary>
    /// Reports whether <paramref name="treeName"/> is a rebuildable derived
    /// vector-plane tree the self-healer is permitted to auto-reset. Fails closed:
    /// a null, empty, unknown, store-of-record, or write-once
    /// (<see cref="VectorPayload"/>) name returns <see langword="false"/>, so the
    /// re-derivation can never touch a tree that holds primary data. This is the
    /// narrowest classification seam and is checked against local constants only -
    /// never against a tree id parsed from a wire- or exception-supplied string.
    /// </summary>
    /// <param name="treeName">The tree name to classify. May be <see langword="null"/>.</param>
    /// <returns><see langword="true"/> only for <see cref="VectorMetadata"/> or <see cref="VectorMembership"/>.</returns>
    internal static bool IsRebuildableVectorTree(string? treeName)
        => treeName is not null && RebuildableVectorTrees.Contains(treeName);

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
