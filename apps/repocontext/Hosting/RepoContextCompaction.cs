using Orleans.Hosting;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Host;

/// <summary>
/// The repository-context tree names the host references for per-tree option
/// wiring. These literals mirror the package-internal <c>RepoContextTrees</c>
/// constants (which are not part of the public surface, so they cannot be
/// referenced here): <c>repo-context-structural</c>, <c>repo-context-symbol</c>,
/// <c>repo-context-memory</c>, <c>repo-context-vector-membership</c>,
/// <c>repo-context-vector-metadata</c>, and <c>repo-context-vector-payload</c>.
/// If a package accessor is ever exposed, prefer it over these literals.
/// </summary>
public static class RepoContextHostTrees
{
    /// <summary>The structural tree (repo/package/file records).</summary>
    public const string Structural = "repo-context-structural";

    /// <summary>The per-symbol structural tree (type/member/function declarations).</summary>
    public const string Symbol = "repo-context-symbol";

    /// <summary>
    /// The schema-family id stamped into the per-value envelope of every
    /// <see cref="Symbol"/> tree record, and the target version that tree is opted in
    /// at. Versioning the symbol tree makes each symbol value self-describing, so a
    /// future change to the symbol record shape ships as a new target version with an
    /// upcaster rather than a breaking, in-place reinterpretation of stored bytes.
    /// Only the symbol tree is opted in; every other repository-context tree stays
    /// unversioned and byte-identical.
    /// </summary>
    public const uint SymbolSchemaId = 1;

    /// <summary>The current target schema version the <see cref="Symbol"/> tree is stamped at.</summary>
    public const uint SymbolSchemaVersion = 1;

    /// <summary>The agent-authored memory tree.</summary>
    public const string Memory = "repo-context-memory";

    /// <summary>The vector-membership projection tree.</summary>
    public const string VectorMembership = "repo-context-vector-membership";

    /// <summary>The vector-metadata projection tree.</summary>
    public const string VectorMetadata = "repo-context-vector-metadata";

    /// <summary>The content-addressed, write-once vector-payload tree.</summary>
    public const string VectorPayload = "repo-context-vector-payload";

    /// <summary>
    /// The churn trees whose re-embed / prune / forget cycles create tombstones
    /// that must be reaped: memory, the two vector projections, structural (which
    /// the bootstrap prunes), and the symbol tree (which the bootstrap re-writes
    /// and prunes on every re-index of a changed file). The content-addressed
    /// vector-payload tree is write-once with no in-place deletes, so it is
    /// excluded - it needs no aggressive compaction.
    /// </summary>
    public static IReadOnlyList<string> ChurnTrees { get; } = new[]
    {
        Memory,
        VectorMembership,
        VectorMetadata,
        Structural,
        Symbol,
    };

    /// <summary>Every repository-context tree the box grants the local agent access to.</summary>
    public static IReadOnlyList<string> All { get; } = new[]
    {
        Structural,
        Symbol,
        Memory,
        VectorMembership,
        VectorMetadata,
        VectorPayload,
    };

    /// <summary>
    /// The derived, re-derivable embedding-vector trees. Every one of these is a
    /// projection of the source repository's embeddings that a downstream gap-scan
    /// can re-ingest from scratch, so none holds authoritative state. They are the
    /// trees that opt in to
    /// <see cref="Orleans.Lattice.ProjectionRebuildPolicy.RebuildFromWalAcceptLoss"/>
    /// (issue #1453): a dormant leaf whose durable checkpoint fell off the shared
    /// WAL self-heals by rebuilding from the surviving suffix instead of wedging
    /// every activation with <c>LeafProjectionStaleException</c>. The store-of-record
    /// trees (structural, symbol, memory) are deliberately excluded - accepting loss
    /// on them would silently drop committed data - so they keep the fail-closed
    /// default policy.
    /// </summary>
    public static IReadOnlyList<string> VectorTrees { get; } = new[]
    {
        VectorMembership,
        VectorMetadata,
        VectorPayload,
    };
}

/// <summary>
/// Applies finite per-tree tombstone-compaction options to the repository-context
/// churn trees so re-embed and prune tombstones are reaped in every durability
/// profile. Leaving the churn trees at the library defaults (an effectively
/// unbounded grace with no ratio or size trigger) would let tombstones accumulate
/// forever on a busy tree - a durability-cost defect this host closes at wiring
/// time.
/// </summary>
public static class RepoContextCompaction
{
    /// <summary>The finite tombstone grace period applied to churn trees.</summary>
    public static readonly TimeSpan ChurnTombstoneGracePeriod = TimeSpan.FromHours(1);

    /// <summary>The tombstone-to-total ratio that pre-empts an out-of-cycle pass on a churn tree.</summary>
    public const double ChurnMinTombstoneRatio = 0.25;

    /// <summary>The leaf entry count that forces an out-of-cycle pass on a churn tree.</summary>
    public const int ChurnMaxLeafEntriesBeforeForcedCompaction = 2000;

    /// <summary>
    /// Registers named-options compaction overrides for every
    /// <see cref="RepoContextHostTrees.ChurnTrees"/> entry on the silo. Idempotent
    /// per tree; safe to call once during host wiring.
    /// </summary>
    /// <param name="silo">The Orleans silo builder.</param>
    /// <returns>The same <paramref name="silo"/> for chaining.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="silo"/> is null.</exception>
    public static ISiloBuilder ConfigureRepoContextCompaction(this ISiloBuilder silo)
    {
        ArgumentNullException.ThrowIfNull(silo);

        foreach (var tree in RepoContextHostTrees.ChurnTrees)
        {
            silo.ConfigureLattice(tree, options =>
            {
                options.TombstoneGracePeriod = ChurnTombstoneGracePeriod;
                options.MinTombstoneRatioForCompaction = ChurnMinTombstoneRatio;
                options.MaxLeafEntriesBeforeForcedCompaction = ChurnMaxLeafEntriesBeforeForcedCompaction;
            });
        }

        return silo;
    }

    /// <summary>
    /// Opts every <see cref="RepoContextHostTrees.VectorTrees"/> entry in to the
    /// <see cref="Orleans.Lattice.ProjectionRebuildPolicy.RebuildFromWalAcceptLoss"/>
    /// recovery policy so a derived embedding-vector leaf that falls off the shared
    /// WAL self-heals by rebuilding from the surviving suffix instead of wedging every
    /// activation with <c>LeafProjectionStaleException</c> (issue #1453). The discarded
    /// prefix is re-derived by the embedding gap-scan. Only the re-derivable vector
    /// trees are opted in; the store-of-record trees keep the fail-closed default.
    /// Idempotent per tree; safe to call once during host wiring.
    /// </summary>
    /// <param name="silo">The Orleans silo builder.</param>
    /// <returns>The same <paramref name="silo"/> for chaining.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="silo"/> is null.</exception>
    public static ISiloBuilder ConfigureRepoContextVectorProjectionRecovery(this ISiloBuilder silo)
    {
        ArgumentNullException.ThrowIfNull(silo);

        foreach (var tree in RepoContextHostTrees.VectorTrees)
        {
            silo.ConfigureLattice(tree, options =>
            {
                options.ProjectionRebuildPolicy = ProjectionRebuildPolicy.RebuildFromWalAcceptLoss;
            });
        }

        return silo;
    }
}
