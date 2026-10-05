using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Decision-purge guard (issue #4508). On a replicating host a saga's prepare
/// can outlive its decision in the write-ahead log: partitions trim
/// independently, so a prepare can still be retained (and re-shipped to a peer
/// that bootstraps) after its terminal's partition was trimmed. The snapshot
/// export ships a decision row for every saga the registry still stores, which
/// is what lets the peer settle such a prepare; once the row is purged, the
/// prepare strands. The guard therefore keeps an expired tombstone until no
/// prepare of its saga can still be retained.
/// <para>
/// It is exact and skew-free. <c>ForgetAsync</c> stamps the tombstone with the
/// current sample generation. A refresh, at most once per interval, takes a
/// sample: it allocates the next generation, then reads every partition's next
/// sequence. A tombstone of generation <c>g</c> may be purged once some sample
/// <c>s &gt; g</c> exists and every partition's lowest retained sequence is at
/// or above <c>s</c>'s tail for that partition. Sample <c>s</c> started after
/// the forget, every prepare of the saga was durably appended before the
/// decision, and the decision precedes the forget, so each prepare's sequence
/// is below <c>s</c>'s tail - and so already trimmed.
/// </para>
/// <para>
/// It fails closed: a failed read, a partition missing from a sample, or a
/// changed physical tree or partition count leaves the tombstone in place, and
/// a reactivated registry (whose samples were in memory) waits for a fresh
/// sample. The cleared generation itself is persisted, so it never regresses.
/// When the sample ring is full the guard stops sampling rather than drop a
/// sample: the oldest sample is the one closest to clearing. A held tombstone stays masked from readers exactly as before; only
/// its physical removal waits, and it still counts against
/// <see cref="LatticeOptions.TxRegistryAdmissionBudgetBytes"/>. A tree whose
/// WAL stops trimming therefore holds its tombstones until it trims again.
/// On a covered tree a <see cref="LatticeOptions.TxDecisionRetention"/> of
/// zero still tombstones the decision at forget time (instead of dropping it)
/// so the guard can hold it; the tombstone is masked from readers at once.
/// </para>
/// </summary>
internal sealed partial class TxRegistryGrain
{
    /// <summary>Upper bound on the in-memory samples the guard keeps.</summary>
    internal const int MaxWalPurgeSamples = 32;

    private readonly List<WalPurgeSample> _walPurgeSamples = [];
    private bool _walPurgeHeld;
    private DateTimeOffset _walPurgeLastRefresh = DateTimeOffset.MinValue;
    private bool _walPurgeRefreshInFlight;
    private ILatticeReplicationContext? _replicationContext;
    private LatticeOptionsResolver? _optionsResolver;

    /// <summary>
    /// Highest sample generation the guard has seen every partition trim past:
    /// a stamped tombstone below it may be purged.
    /// </summary>
    internal long WalClearedGeneration => state.State.WalClearedGeneration;

    /// <summary>
    /// <see langword="true"/> when the host runs replication, so this
    /// registry's tombstones are held until the WAL no longer retains their
    /// sagas. Every tree on such a host is covered, not only the trees
    /// replicated today: a tree that becomes replicated later would otherwise
    /// have purged decisions whose prepares its log still retains.
    /// </summary>
    private bool WalPurgeGuardApplies =>
        _replicationContext is { IsReplicationEnabled: true } && _optionsResolver is not null;

    /// <summary>
    /// How often the guard samples and re-checks the WAL: half the decision
    /// retention, between 50 milliseconds and 30 seconds.
    /// </summary>
    private TimeSpan WalPurgeRefreshInterval
    {
        get
        {
            var half = Retention / 2;
            if (half < TimeSpan.FromMilliseconds(50)) return TimeSpan.FromMilliseconds(50);
            return half > TimeSpan.FromSeconds(30) ? TimeSpan.FromSeconds(30) : half;
        }
    }

    private void ResolveWalPurgeGuardServices()
    {
        _replicationContext = context.ActivationServices?.GetService<ILatticeReplicationContext>();
        _optionsResolver = context.ActivationServices?.GetService<LatticeOptionsResolver>();
    }

    /// <summary>
    /// The generation a tombstone <c>ForgetAsync</c> creates now is stamped
    /// with, or <see langword="null"/> when the guard does not cover the tree.
    /// </summary>
    private long? CurrentWalStamp() => WalPurgeGuardApplies ? state.State.WalSampleGeneration : null;

    /// <summary>
    /// Whether the guard lets the expired tombstone of <paramref name="txid"/>
    /// be physically purged.
    /// </summary>
    private bool IsWalPurgeCleared(Guid txid)
    {
        if (!WalPurgeGuardApplies)
        {
            return true;
        }

        if (_walPurgeHeld)
        {
            return false;
        }

        var stamp = state.State.ForgetWalGenerations.TryGetValue(txid, out var g) ? g : 0L;
        return stamp < state.State.WalClearedGeneration;
    }

    /// <summary>
    /// Reads the tree's <see cref="IWalPurgeHoldGrain"/> immediately before a
    /// prune pass that could purge something (issue #4534). A trim forced past
    /// a replication consumer's unshipped cursor records a hold before it
    /// trims; while any hold is outstanding the peer still needs a re-seed,
    /// which can only settle a saga whose decision is still stored, so every
    /// purge waits. Read after the cleared generation was established, so a
    /// trim that evidence depends on has its hold visible here. A failed read
    /// holds (fail closed).
    /// </summary>
    private async Task RefreshWalPurgeHoldAsync()
    {
        if (!WalPurgeGuardApplies || !HasClearedTombstone())
        {
            _walPurgeHeld = false;
            return;
        }

        try
        {
            // The WAL GC records holds under the physical tree whose log it trims.
            var entry = await grainFactory.GetLatticeRegistry().GetEntryAsync(TreeId);
            var holds = await grainFactory.GetGrain<IWalPurgeHoldGrain>(entry?.PhysicalTreeId ?? TreeId).GetAsync();
            _walPurgeHeld = holds.Count > 0;
        }
        catch (Exception ex)
        {
            _walPurgeHeld = true;
            LogWalPurgeGuardRefreshFailed(logger, TreeId, ex);
        }
    }

    private bool HasClearedTombstone()
    {
        var cleared = state.State.WalClearedGeneration;
        foreach (var txid in state.State.ForgottenAt.Keys)
        {
            var stamp = state.State.ForgetWalGenerations.TryGetValue(txid, out var g) ? g : 0L;
            if (stamp < cleared)
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>
    /// Advances the guard: clears every sample the WAL has trimmed past, then
    /// takes a new sample when a tombstone is stamped at or above the newest
    /// one. Rate-limited and single-flight; never throws.
    /// </summary>
    private async Task RefreshWalPurgeGuardAsync()
    {
        if (_walPurgeRefreshInFlight || state.State.ForgottenAt.Count == 0 || !WalPurgeGuardApplies)
        {
            return;
        }

        var now = TimeProvider.GetUtcNow();
        if (now - _walPurgeLastRefresh < WalPurgeRefreshInterval)
        {
            return;
        }

        _walPurgeRefreshInFlight = true;
        _walPurgeLastRefresh = now;
        try
        {
            var entry = await grainFactory.GetLatticeRegistry().GetEntryAsync(TreeId);
            var physicalTreeId = entry?.PhysicalTreeId ?? TreeId;
            var partitions = await _optionsResolver!.GetWalPartitionsAsync(physicalTreeId);
            if (partitions <= 0)
            {
                return;
            }

            _walPurgeSamples.RemoveAll(s => s.PhysicalTreeId != physicalTreeId || s.Tails.Length != partitions);
            if (_walPurgeSamples.Count > 0)
            {
                var lowest = await ReadPartitionsAsync(
                    physicalTreeId, partitions, static (wal, ct) => wal.GetLowestRetainedSequenceAsync(ct));
                for (var i = _walPurgeSamples.Count - 1; i >= 0; i--)
                {
                    if (IsTrimmedPast(lowest, _walPurgeSamples[i].Tails))
                    {
                        // Persisted with the caller's next write; monotone.
                        state.State.WalClearedGeneration = Math.Max(state.State.WalClearedGeneration, _walPurgeSamples[i].Generation);
                        break;
                    }
                }

                var cleared = state.State.WalClearedGeneration;
                _walPurgeSamples.RemoveAll(s => s.Generation <= cleared);
            }

            var covered = _walPurgeSamples.Count == 0
                ? state.State.WalClearedGeneration
                : _walPurgeSamples[^1].Generation;
            if (_walPurgeSamples.Count >= MaxWalPurgeSamples || !HasTombstoneStampedAtOrAbove(covered))
            {
                // A full ring keeps its oldest samples, the ones closest to
                // clearing; newer tombstones wait for room.
                return;
            }

            // Allocate the generation BEFORE reading the tails: a forget that
            // interleaves with the reads is stamped with this generation and so
            // needs a later sample, which is what keeps every sample's tails
            // read after the forgets it can clear.
            var generation = ++state.State.WalSampleGeneration;
            var tails = await ReadPartitionsAsync(
                physicalTreeId, partitions, static (wal, ct) => wal.GetNextSequenceAsync(ct).AsTask());
            _walPurgeSamples.Add(new WalPurgeSample(generation, physicalTreeId, tails));
        }
        catch (Exception ex)
        {
            LogWalPurgeGuardRefreshFailed(logger, TreeId, ex);
        }
        finally
        {
            _walPurgeRefreshInFlight = false;
        }
    }

    private bool HasTombstoneStampedAtOrAbove(long generation)
    {
        foreach (var txid in state.State.ForgottenAt.Keys)
        {
            var stamp = state.State.ForgetWalGenerations.TryGetValue(txid, out var g) ? g : 0L;
            if (stamp >= generation)
            {
                return true;
            }
        }

        return false;
    }

    private static bool IsTrimmedPast(long[] lowest, long[] tails)
    {
        for (var p = 0; p < tails.Length; p++)
        {
            // -1: the partition retains nothing, so nothing below the tail is left.
            if (lowest[p] >= 0 && lowest[p] < tails[p])
            {
                return false;
            }
        }

        return true;
    }

    private async Task<long[]> ReadPartitionsAsync(
        string physicalTreeId, int partitions, Func<IWalShardGrain, CancellationToken, Task<long>> read)
    {
        var tasks = new Task<long>[partitions];
        for (var p = 0; p < partitions; p++)
        {
            tasks[p] = read(grainFactory.GetGrain<IWalShardGrain>($"{physicalTreeId}/{p}"), CancellationToken.None);
        }

        return await Task.WhenAll(tasks);
    }

    /// <summary>One WAL tail sample: its generation, the physical tree it read, and each partition's next sequence.</summary>
    private readonly record struct WalPurgeSample(long Generation, string PhysicalTreeId, long[] Tails);

    [LoggerMessage(
        Level = LogLevel.Debug,
        Message = "TxRegistry on tree {TreeId} could not refresh its decision-purge guard; expired tombstones stay until a later refresh succeeds.")]
    private static partial void LogWalPurgeGuardRefreshFailed(ILogger logger, string treeId, Exception exception);
}
