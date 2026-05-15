using System.Diagnostics;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// <see cref="System.Diagnostics.Metrics"/> instrumentation for
/// <see cref="BPlusLeafGrain"/>, plus the lazy DI resolvers shared by
/// every partial of the class. Houses (a) <see cref="PersistAsync"/>,
/// which wraps the leaf's <c>IPersistentState.WriteStateAsync</c> in a
/// latency-capturing helper so the <see cref="LatticeMetrics.LeafWriteDuration"/>
/// histogram observes every state-row flush, (b) the lazy
/// <see cref="ICommitLogWriter"/> and <see cref="ILogger{T}"/>
/// resolvers consulted by the foreground commit and projection paths,
/// and (c) the per-step <see cref="RecordCommitStep"/> recorder for the
/// commit-pipeline latency histogram.
/// <para>
/// Post-WAL-first scope: the per-shard WAL is the durability boundary
/// for every foreground commit - saga / set / tombstone / backstop /
/// merge / compaction - so the surviving role of
/// <see cref="PersistAsync"/> is narrowed to two slices:
/// (i) persisting non-WAL-replayable topology and lifecycle metadata
/// (sibling pointers, split lifecycle fields, tree id, shard index,
/// key range, last-compaction version), and (ii) flushing the
/// projection-checkpoint snapshot consulted by the activation-time WAL
/// replay path. Every entry mutation in <c>MergeEntriesAsync</c>,
/// <c>MergeManyAsync</c>, and <c>CompactTombstonesAsync</c> now appends
/// a synthetic <see cref="LatticeMutation"/> through
/// <see cref="ICommitLogWriter"/> before mutating in-memory state, on
/// the same WAL-first contract as <c>SetCoreAsync</c> and the
/// cross-migration LWW backstop.
/// </para>
/// </summary>
internal sealed partial class BPlusLeafGrain
{
    /// <summary>
    /// Cached <see cref="ICommitLogWriter"/> resolved from
    /// <see cref="IGrainContext.ActivationServices"/> on first use.
    /// <see langword="null"/> when the host has not registered the
    /// commit-log adapter (i.e. no replication package is in the
    /// composition root), in which case the foreground commit paths
    /// that consult the resolver short-circuit to the legacy state-row
    /// persist. With the adapter registered, the WAL append is the
    /// durability boundary and the legacy state-row persist is reserved
    /// for the topology / lifecycle metadata and projection-checkpoint
    /// slices described on the class-level summary.
    /// </summary>
    private ICommitLogWriter? _commitLogWriter;

    /// <summary>
    /// <see langword="true"/> once the lazy resolution of
    /// <see cref="_commitLogWriter"/> has run. Caching the outcome
    /// (including the <see langword="null"/> result) avoids paying the
    /// service-provider lookup on every commit.
    /// </summary>
    private bool _commitLogWriterResolved;

    /// <summary>
    /// Cached typed logger resolved on first use. Used by the foreground
    /// commit paths that route through <see cref="ICommitLogWriter"/>
    /// to log diagnostic context when the optional post-WAL bookkeeping
    /// (e.g. metric tagging, observer dispatch) throws - the caller
    /// still observes success because the WAL append already established
    /// the durable boundary.
    /// </summary>
    private ILogger<BPlusLeafGrain>? _logger;

    /// <summary>
    /// Persists the leaf's state row and records the elapsed time on
    /// the <see cref="LatticeMetrics.LeafWriteDuration"/> histogram.
    /// Used by (a) the topology / lifecycle paths (sibling pointer
    /// updates, split lifecycle transitions, tree-id / shard-index /
    /// key-range assignment, last-compaction-version stamping) that
    /// persist metadata not carried by the per-shard WAL, and (b) the
    /// projection-checkpoint flush that snapshots <c>Entries</c> plus
    /// <c>ProjectionCheckpointOffset</c> for the activation-time WAL
    /// replayer. Every foreground entry mutation - set, delete, saga
    /// prepare / terminal, cross-migration LWW backstop, merge, and
    /// tombstone-reap compaction - now routes durability through
    /// <see cref="ICommitLogWriter"/> rather than through this helper.
    /// The tree-id tag is sourced from persisted state and may be empty
    /// when the tree has not yet been registered with this leaf
    /// (pre-<c>SetTreeIdAsync</c>).
    /// </summary>
    private async Task PersistAsync()
    {
        var startTicks = Stopwatch.GetTimestamp();
        try
        {
            await state.WriteStateAsync();
        }
        finally
        {
            var elapsedMs = (Stopwatch.GetTimestamp() - startTicks) * 1000.0 / Stopwatch.Frequency;
            LatticeMetrics.LeafWriteDuration.Record(elapsedMs,
                new KeyValuePair<string, object?>(LatticeMetrics.TagTree, state.State.TreeId ?? string.Empty));
        }
    }

    /// <summary>Builds the single-tree tag used by every leaf-level instrument.</summary>
    private KeyValuePair<string, object?> LeafTreeTag() =>
        new(LatticeMetrics.TagTree, state.State.TreeId ?? string.Empty);

    /// <summary>
    /// Lazily resolves the commit-log writer from the activation's
    /// service provider. Returns <see langword="null"/> when no adapter
    /// has been registered (the legacy-only commit path) <em>or</em>
    /// while the leaf's <see cref="LeafNodeState.TreeId"/> is still
    /// unset - a leaf created without going through
    /// <see cref="ILattice"/> (e.g. a unit-test harness that grabs a
    /// leaf grain by raw <see cref="Guid"/>) cannot dispatch to a WAL
    /// shard whose grain key requires a non-empty tree id. Once
    /// <see cref="SetTreeIdAsync"/> populates the tree id the
    /// resolver returns the cached writer normally.
    /// </summary>
    private ICommitLogWriter? ResolveCommitLogWriter()
    {
        if (string.IsNullOrEmpty(state.State.TreeId))
            return null;

        if (_commitLogWriterResolved)
            return _commitLogWriter;

        _commitLogWriterResolved = true;
        _commitLogWriter = context.ActivationServices?.GetService<ICommitLogWriter>();
        return _commitLogWriter;
    }

    /// <summary>
    /// Lazily resolves an <see cref="ILogger{BPlusLeafGrain}"/> from the
    /// activation's service provider, caching the result.
    /// </summary>
    private ILogger<BPlusLeafGrain>? ResolveLogger()
    {
        if (_logger is not null)
            return _logger;

        _logger = context.ActivationServices?
            .GetService<ILoggerFactory>()?
            .CreateLogger<BPlusLeafGrain>();
        return _logger;
    }

    /// <summary>
    /// Records the elapsed time since <paramref name="startTicks"/> on
    /// <see cref="LatticeMetrics.LeafCommitDuration"/> tagged with
    /// <see cref="LatticeMetrics.TagStep"/> = <paramref name="step"/>.
    /// </summary>
    private void RecordCommitStep(string step, long startTicks)
    {
        var elapsedMs = (Stopwatch.GetTimestamp() - startTicks) * 1000.0 / Stopwatch.Frequency;
        LatticeMetrics.LeafCommitDuration.Record(elapsedMs,
            new KeyValuePair<string, object?>(LatticeMetrics.TagTree, state.State.TreeId ?? string.Empty),
            new KeyValuePair<string, object?>(LatticeMetrics.TagStep, step));
    }
}