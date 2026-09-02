using Orleans.Hosting;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Host;

/// <summary>
/// The repository-context tree names the host references for per-tree option
/// wiring. These literals mirror the package-internal <c>RepoContextTrees</c>
/// constants (which are not part of the public surface, so they cannot be
/// referenced here): <c>repo-context-structural</c>, <c>repo-context-symbol</c>,
/// <c>repo-context-memory</c>, <c>repo-context-vector-membership</c>,
/// <c>repo-context-vector-metadata</c>, <c>repo-context-vector-payload</c>,
/// <c>repo-context-content</c>, and <c>repo-context-vector-index</c>.
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

    /// <summary>The per-file searchable-content projection tree.</summary>
    public const string Content = "repo-context-content";

    /// <summary>The reverse cross-reference projection tree (inbound dependents and test linkage).</summary>
    public const string CrossReference = "repo-context-xref";

    /// <summary>The per-session context-bundle reuse-bookkeeping tree.</summary>
    public const string Session = "repo-context-session";

    /// <summary>
    /// The persisted approximate nearest-neighbour index the semantic retrieval
    /// path queries instead of re-scanning the whole vector-metadata prefix.
    /// <para>
    /// It is a wholly derived, local accelerator, so it is deliberately excluded
    /// from the package's own replication enrolment list - but it still needs a
    /// local-agent grant here, because the box runs a default-deny access gate and
    /// an ungranted tree would fail closed on every read and write the index makes.
    /// That failure would be invisible in the worst way: the plane would keep
    /// declining, retrieval would keep serving correctly through the exact scan,
    /// and the index would simply never finish building.
    /// </para>
    /// </summary>
    public const string VectorIndex = "repo-context-vector-index";

    /// <summary>
    /// The churn trees whose re-embed / prune / forget cycles create tombstones
    /// that must be reaped: memory, the two vector projections, structural (which
    /// the bootstrap prunes), the symbol tree (which the bootstrap re-writes
    /// and prunes on every re-index of a changed file), the content projection
    /// (which the bootstrap re-writes on a changed file and deletes on a removed
    /// file), the reverse cross-reference projection (which the symbol reconciler
    /// re-writes and deletes as references change), the per-session reuse
    /// bookkeeping (whose entries expire and are pruned as sessions lapse), and the
    /// approximate index (which rewrites a cell's chunks on every flush and range-
    /// deletes a whole superseded generation on every retrain or rebuild). The
    /// content-addressed vector-payload tree is write-once with no in-place deletes,
    /// so it is excluded - it needs no aggressive compaction.
    /// </summary>
    public static IReadOnlyList<string> ChurnTrees { get; } = new[]
    {
        Memory,
        VectorMembership,
        VectorMetadata,
        Structural,
        Symbol,
        Content,
        CrossReference,
        Session,
        VectorIndex,
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
        Content,
        CrossReference,
        Session,
        VectorIndex,
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
}
