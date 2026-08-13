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
    /// Tree holding the structural nodes: repo, package, file, and symbol records
    /// (low churn; may keep the default per-tree options).
    /// </summary>
    internal const string Structural = "repo-context-structural";

    /// <summary>
    /// Tree holding agent-authored memory records (higher churn from re-write and
    /// forget cycles; the host configures finite tombstone compaction here).
    /// </summary>
    internal const string Memory = "repo-context-memory";

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
        Memory,
        VectorMembership,
        VectorPayload,
        VectorMetadata,
    };

    /// <summary>
    /// Resolves the named tree that stores records of the given
    /// <paramref name="kind"/>. Structural kinds (repo, package, file, symbol)
    /// map to <see cref="Structural"/>; <see cref="RepoContextRecordKind.Memory"/>
    /// maps to <see cref="Memory"/>; and the vector kinds map to their dedicated
    /// vector trees.
    /// </summary>
    /// <param name="kind">The record family to route.</param>
    /// <exception cref="ArgumentOutOfRangeException">The kind is not a known record kind.</exception>
    internal static string ForKind(RepoContextRecordKind kind) => kind switch
    {
        RepoContextRecordKind.Repo
            or RepoContextRecordKind.Package
            or RepoContextRecordKind.File
            or RepoContextRecordKind.Symbol => Structural,
        RepoContextRecordKind.Memory => Memory,
        RepoContextRecordKind.VectorMetadata => VectorMetadata,
        RepoContextRecordKind.VectorPayload => VectorPayload,
        RepoContextRecordKind.VectorMembership => VectorMembership,
        _ => throw new ArgumentOutOfRangeException(nameof(kind), kind, "Unknown repo-context record kind."),
    };
}
