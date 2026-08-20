using System.Diagnostics;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// <see cref="System.Diagnostics.Metrics"/> instrumentation for
/// <see cref="Orleans.Lattice.BPlusTree.Grains.BPlusLeafGrain"/>, plus the lazy DI resolvers shared by
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
    /// <summary>
    /// Diagnostic gate for the c2-vi etag-race probe. Set
    /// <c>LATTICE_BENCH_TRACE_PERSIST=1</c> in the silo environment to
    /// emit one stdout line per <see cref="PersistAsync"/> call with
    /// the activation id, <c>RecordExists</c>,
    /// <c>Etag</c>, and a short caller-
    /// supplied tag. Read once at process start; flipping the env var
    /// mid-run has no effect. Default <c>false</c> so production and
    /// the unit-test harness pay zero cost.
    /// </summary>
    private static readonly bool _tracePersist =
        Environment.GetEnvironmentVariable("LATTICE_BENCH_TRACE_PERSIST") is { Length: > 0 } v
        && (v == "1" || string.Equals(v, "true", StringComparison.OrdinalIgnoreCase));

    private async Task PersistAsync([System.Runtime.CompilerServices.CallerMemberName] string caller = "")
    {
        if (_tracePersist)
        {
            var etag = state.Etag is null
                ? "<null>"
                : (state.Etag.Length > 32 ? state.Etag.Substring(0, 32) + ".." : state.Etag);
            Console.WriteLine($"[diag persist] kind=leaf caller={caller} gid={context.GrainId} treeId='{state.State.TreeId ?? "<null>"}' shard={state.State.ShardIndex} recordExists={state.RecordExists} etag={etag}");
        }
        // #1557: capture whether this write is an initial create BEFORE the
        // attempt. A brand-new leaf whose storage row does not yet exist
        // (RecordExists == false) issues its first WriteStateAsync as an
        // insert with a null/empty expected etag. On a fresh volume / cold
        // silo the grain-directory warmup can transiently materialise two
        // activations of this leaf's deterministic grain id, so both issue
        // that first insert and the loser of the storage insert
        // compare-and-swap throws InconsistentStateException with BOTH
        // etags empty. The per-activation _splitGate cannot serialise that
        // race - it is cross-activation. See the benign-race catch below.
        var creatingRow = !state.RecordExists;
        var startTicks = Stopwatch.GetTimestamp();
        try
        {
            await state.WriteStateAsync();
        }
        catch (Orleans.Storage.InconsistentStateException ex)
            when (creatingRow
                && string.IsNullOrEmpty(ex.StoredEtag)
                && string.IsNullOrEmpty(ex.CurrentEtag))
        {
            // Benign first-create lost race (#1557). This activation never
            // read an existing row (creatingRow), and BOTH etags are empty,
            // so this is provably an insert-vs-insert race, not a
            // stale-state conflict on an existing row (which carries
            // non-empty etags and must still surface - preserving the
            // #1560 fall-off-the-log fail-loud contract). The only writers
            // of a leaf's deterministic grain id are the shard root seeding
            // the same tree, and data mutations are gated behind TreeId (see
            // ResolveCommitLogWriter), so the very first state-row write is
            // always the idempotent topology seed. The winner's durably-
            // committed row therefore already satisfies the seed this
            // activation intended: adopt it by re-reading and converge,
            // rather than failing the cold-start bulk apply with a spurious
            // fail-level run abort that only self-heals on a retry.
            await state.ReadStateAsync();
            ResolveLogger()?.LogDebug(
                "Leaf {GrainId} converged a benign first-create write race (#1557) by adopting the concurrently-committed row.",
                context.GrainId);
        }
        finally
        {
            var elapsedMs = (Stopwatch.GetTimestamp() - startTicks) * 1000.0 / Stopwatch.Frequency;
            LatticeMetrics.LeafWriteDuration.Record(elapsedMs,
                new KeyValuePair<string, object?>(LatticeMetrics.TagTree, state.State.TreeId ?? string.Empty));
        }

        // Best-effort byte-footprint publish to the owning shard root so the
        // shard-level storage-usage rollup stays current without ever
        // walking the leaf chain on the read path. The shard root marks
        // PublishLeafByteFootprintAsync AlwaysInterleave and updates
        // activation-scoped fields only (no WriteStateAsync), so the
        // await here cannot block the leaf's saga progress on a foreground
        // shard-root etag CAS. Awaiting (rather than fire-and-forget)
        // means a subsequent GetStorageUsageAsync against the shard root
        // observes this leaf's contribution deterministically.
        await TryPublishByteFootprintAsync();
    }

    private long _lastPublishedStateBytes = long.MinValue;
    private long _lastPublishedSnapshotBytes = long.MinValue;
    private long _lastPublishedLiveKeys = long.MinValue;

    /// <summary>
    /// Awaitable best-effort byte-footprint publish to the owning shard
    /// root. Skipped when the leaf has no resolved <c>TreeId</c> / shard
    /// index yet (very early in activation, before the parent has wired
    /// the leaf in), when the grain-id key is not a Guid (unit-test
    /// activations only - production leaf keys are always Guids), and
    /// when the values are identical to the most recent successful
    /// publish. Errors are swallowed so a transient shard-root failure
    /// does not fail the user-visible mutation; the next publish (or the
    /// operator-driven <c>RefreshLeafByteFootprintsAsync</c>) re-anchors
    /// the totals.
    /// </summary>
    private async Task TryPublishByteFootprintAsync()
    {
        var treeId = state.State.TreeId;
        if (treeId is null || state.State.ShardIndex is not int shardIndex)
        {
            return;
        }

        var stateBytes = Cache.StateBytes;
        var snapshotBytes = _lastCapturedSnapshotBytes;
        var liveKeys = Cache.LiveCount;
        if (stateBytes == _lastPublishedStateBytes
            && snapshotBytes == _lastPublishedSnapshotBytes
            && liveKeys == _lastPublishedLiveKeys)
        {
            return;
        }

        Guid leafKey;
        try
        {
            leafKey = context.GrainId.GetGuidKey();
        }
        catch (ArgumentException)
        {
            return;
        }

        var footprint = new State.LeafByteFootprint
        {
            StateBytes = stateBytes,
            SnapshotBytes = snapshotBytes,
            LiveKeys = liveKeys,
        };
        try
        {
            var shard = grainFactory.GetGrain<IShardRootGrain>($"{treeId}/{shardIndex}");
            await shard.PublishLeafByteFootprintAsync(leafKey, footprint);
            // Only advance the "last published" watermark on a successful
            // hop; a transient failure must not silence future republishes
            // that would otherwise carry the same byte totals.
            _lastPublishedStateBytes = stateBytes;
            _lastPublishedSnapshotBytes = snapshotBytes;
            _lastPublishedLiveKeys = liveKeys;
        }
        catch
        {
            // Best-effort: a transient publish failure must not fail the
            // user-visible mutation. The next persist re-publishes; the
            // operator-driven RefreshStorageUsageAsync re-anchors.
        }
    }

    /// <summary>Builds the single-tree tag used by every leaf-level instrument.</summary>
    private KeyValuePair<string, object?> LeafTreeTag() =>
        new(LatticeMetrics.TagTree, state.State.TreeId ?? string.Empty);

    /// <summary>
    /// Resolves the ambient compaction-trigger tag for the current call,
    /// or <see langword="null"/> if no trigger scope is active. Returns
    /// the cached singleton from <see cref="LatticeMetrics"/> for known
    /// labels (<c>reminder</c> / <c>ratio</c> / <c>size</c> /
    /// <c>operator</c>), avoiding a per-call <c>KeyValuePair</c>
    /// construction; falls back to a freshly-built pair only for an
    /// unknown label (defence-in-depth - the producers in this
    /// repository never set anything else).
    /// </summary>
    private static KeyValuePair<string, object?>? CompactionTriggerTag()
    {
        var raw = LatticeCompactionTriggerContext.Current;
        return raw switch
        {
            null => null,
            TombstoneCompactionGrain.TriggerReminder => LatticeMetrics.TriggerReminderTag,
            TombstoneCompactionGrain.TriggerRatio => LatticeMetrics.TriggerRatioTag,
            TombstoneCompactionGrain.TriggerSize => LatticeMetrics.TriggerSizeTag,
            TombstoneCompactionGrain.TriggerOperator => LatticeMetrics.TriggerOperatorTag,
            _ => new KeyValuePair<string, object?>(LatticeMetrics.TagTrigger, raw),
        };
    }

    /// <summary>
    /// Resolves the ambient compaction walk-path tag for the current
    /// call, or <see langword="null"/> if no path scope is active.
    /// Returns the cached singleton from <see cref="LatticeMetrics"/>
    /// for known labels (<c>walk</c> / <c>dirty-set</c>), avoiding a
    /// per-call <c>KeyValuePair</c> construction; falls back to a
    /// freshly-built pair only for an unknown label.
    /// </summary>
    private static KeyValuePair<string, object?>? CompactionPathTag()
    {
        var raw = LatticeCompactionPathContext.Current;
        return raw switch
        {
            null => null,
            LatticeMetrics.PathWalk => LatticeMetrics.PathWalkTag,
            LatticeMetrics.PathDirtySet => LatticeMetrics.PathDirtySetTag,
            _ => new KeyValuePair<string, object?>(LatticeMetrics.TagPath, raw),
        };
    }

    /// <summary>
    /// Lazily resolves the commit-log writer from the activation's
    /// service provider. Returns <see langword="null"/> when no adapter
    /// has been registered (the legacy-only commit path) <em>or</em>
    /// while the leaf's <see cref="Orleans.Lattice.BPlusTree.State.LeafNodeState.TreeId"/> is still
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

    /// <summary>
    /// Per-activation gate that serialises every entry into
    /// <see cref="BPlusLeafGrain.SplitAsync"/> and
    /// <see cref="BPlusLeafGrain.CompleteSplitAsync"/>.
    /// <para>
    /// The leaf's mutation surface
    /// (<see cref="Orleans.Lattice.BPlusTree.IBPlusLeafGrain.SetAsync(string, byte[])"/>,
    /// <see cref="Orleans.Lattice.BPlusTree.IBPlusLeafGrain.SetManyAsync"/>,
    /// <see cref="Orleans.Lattice.BPlusTree.IBPlusLeafGrain.DeleteAsync"/>,
    /// <see cref="Orleans.Lattice.BPlusTree.IBPlusLeafGrain.MergeManyAsync"/>) is marked
    /// <c>[AlwaysInterleave]</c> so multiple producer turns can run on
    /// the same activation concurrently (U9p c2-iii). Orleans serialises
    /// synchronous code between awaits, so the per-key LWW merge,
    /// HLC tick, and projection-hash updates are race-free. The single
    /// remaining hazard is concurrent entry into <c>SplitAsync</c>:
    /// the split predicate (<c>Cache.Count &gt; MaxLeafKeys</c>) is
    /// observed by the caller before <c>SplitAsync</c> sets
    /// <see cref="Primitives.SplitState.SplitInProgress"/> at its first
    /// post-await line, so two interleaved turns can both observe
    /// overflow and both enter <c>SplitAsync</c>, double-flipping the
    /// state row, allocating two siblings, and corrupting the chain.
    /// </para>
    /// <para>
    /// This gate serialises every <c>SplitAsync</c> / <c>CompleteSplitAsync</c>
    /// call site on the grain. The expected critical-section
    /// occupancy is &lt;= 1 ms per turn under steady state (no split)
    /// and ~tens-of-ms during an actual split, both of which are
    /// dominated by the post-WAL await the caller is already paying
    /// for; the gate adds no measurable latency to the steady-state
    /// path.
    /// </para>
    /// </summary>
    private readonly SemaphoreSlim _splitGate = new(1, 1);

    /// <summary>
    /// Per-activation count of foreground commits (<see cref="CommitSetAsync"/>
    /// or <see cref="CommitSetManyAsync"/>) currently in flight on this
    /// leaf. Snapshotted at the moment a new commit enters the commit
    /// path, recorded on <see cref="LatticeMetrics.LeafCommitInFlight"/>,
    /// then incremented for the duration of the commit. The decrement
    /// runs unconditionally in the disposed scope so an exception
    /// midway through the commit cannot leak depth.
    /// <para>
    /// Under the shipping non-reentrant scheduling of
    /// <see cref="Orleans.Lattice.BPlusTree.IBPlusLeafGrain.SetAsync(string, byte[])"/> /
    /// <see cref="Orleans.Lattice.BPlusTree.IBPlusLeafGrain.SetManyAsync"/> the recorded value
    /// is always <c>0</c>: Orleans serialises grain calls and the
    /// next commit cannot enter until the current one returns. The
    /// histogram is a falsifiability instrument for the U9m benchmark
    /// probe: a steady pin at <c>0</c> proves
    /// the leaf turn queue is not the binding constraint and routes
    /// the next probe to WAL-side fan-in (U9n); a steady lift above
    /// <c>0</c> identifies the leaf grain as the binding constraint.
    /// </para>
    /// </summary>
    private int _commitInFlight;

    /// <summary>
    /// Opens a <see cref="CommitInFlightScope"/> that snapshots
    /// <see cref="_commitInFlight"/> on the
    /// <see cref="LatticeMetrics.LeafCommitInFlight"/> histogram and
    /// increments the counter for the lifetime of the returned scope.
    /// Callers must <see langword="using"/> the scope so the matching
    /// decrement runs in every commit-path exit (return, exception,
    /// or split).
    /// </summary>
    private CommitInFlightScope EnterCommitScope()
    {
        var depthBefore = Interlocked.Increment(ref _commitInFlight) - 1;
        LatticeMetrics.LeafCommitInFlight.Record(depthBefore, LeafTreeTag());
        return new CommitInFlightScope(this);
    }

    /// <summary>
    /// Decrements the leaf's in-flight commit counter on
    /// <see cref="IDisposable.Dispose"/>. The scope is paired with
    /// <see cref="EnterCommitScope"/> via a <c>using</c> statement so
    /// the decrement runs in the commit path's <c>finally</c>
    /// regardless of how the commit body exits (normal return,
    /// exception, or split-induced sibling promotion).
    /// </summary>
    private readonly struct CommitInFlightScope : IDisposable
    {
        private readonly BPlusLeafGrain _grain;
        public CommitInFlightScope(BPlusLeafGrain grain) => _grain = grain;
        public void Dispose() => Interlocked.Decrement(ref _grain._commitInFlight);
    }
}