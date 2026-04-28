using System.Collections.Concurrent;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication.Grains;

namespace Orleans.Lattice.Replication;

/// <summary>
/// Default <see cref="ILatticeBootstrapCoordinator"/> implementation.
/// Stores per-tree state in a
/// <see cref="ConcurrentDictionary{TKey, TValue}"/> and serialises
/// concurrent <see cref="BootstrapAsync"/> calls per tree through a
/// per-tree non-blocking gate (a single-permit
/// <see cref="SemaphoreSlim"/> entered with a zero timeout) so a
/// concurrent invocation against the same tree throws immediately
/// rather than queueing.
/// </summary>
internal sealed class LatticeBootstrapCoordinator(
    IGrainFactory grainFactory,
    ISnapshotProvider snapshotProvider) : ILatticeBootstrapCoordinator
{
    private readonly IGrainFactory _grainFactory =
        grainFactory ?? throw new ArgumentNullException(nameof(grainFactory));
    private readonly ISnapshotProvider _snapshotProvider =
        snapshotProvider ?? throw new ArgumentNullException(nameof(snapshotProvider));

    private readonly ConcurrentDictionary<string, BootstrapTracker> _trackers =
        new(StringComparer.Ordinal);

    /// <inheritdoc />
    public LatticeBootstrapState GetState(string treeName)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeName);
        return _trackers.TryGetValue(treeName, out var tracker)
            ? tracker.GetState()
            : LatticeBootstrapState.Idle;
    }

    /// <inheritdoc />
    public async Task BootstrapAsync(
        string treeName,
        string sourceClusterId,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeName);
        ArgumentException.ThrowIfNullOrEmpty(sourceClusterId);
        cancellationToken.ThrowIfCancellationRequested();

        var tracker = _trackers.GetOrAdd(treeName, static _ => new BootstrapTracker());

        // Non-blocking per-tree mutual exclusion: a concurrent call
        // against the same tree fails fast rather than queueing,
        // matching the contract documented on the public surface.
        if (!await tracker.Gate.WaitAsync(0, cancellationToken).ConfigureAwait(false))
        {
            throw new InvalidOperationException(
                $"A bootstrap is already in progress for tree '{treeName}'.");
        }

        try
        {
            tracker.SetState(LatticeBootstrapState.RequestingSnapshot);
            SnapshotStream snapshot;
            try
            {
                snapshot = await _snapshotProvider
                    .ExportAsync(treeName, HybridLogicalClock.Zero, cancellationToken)
                    .ConfigureAwait(false);
            }
            catch
            {
                tracker.SetState(LatticeBootstrapState.Failed);
                throw;
            }

            tracker.SetState(LatticeBootstrapState.ApplyingSnapshot);
            try
            {
                var apply = _grainFactory.GetGrain<IReplicationApplyGrain>(treeName);
                await foreach (var entry in snapshot.Entries
                    .WithCancellation(cancellationToken)
                    .ConfigureAwait(false))
                {
                    if (entry.Value is null)
                    {
                        // Tombstones are not emitted by the default
                        // provider, but defend against custom providers
                        // that might surface them.
                        continue;
                    }

                    await apply.ApplySetAsync(
                        entry.Key,
                        entry.Value,
                        entry.Timestamp,
                        sourceClusterId,
                        sourceVectorClock: null,
                        expiresAtTicks: 0).ConfigureAwait(false);
                }
            }
            catch
            {
                tracker.SetState(LatticeBootstrapState.Failed);
                throw;
            }

            tracker.SetState(LatticeBootstrapState.IncrementalHandoff);
            try
            {
                var hwm = _grainFactory.GetGrain<IReplicationHighWaterMarkGrain>(treeName);
                await hwm
                    .PinSnapshotAsync(snapshot.AsOfHlc, snapshot.CausalStableFrontier, cancellationToken)
                    .ConfigureAwait(false);
            }
            catch
            {
                tracker.SetState(LatticeBootstrapState.Failed);
                throw;
            }

            tracker.SetState(LatticeBootstrapState.LiveIncremental);
        }
        finally
        {
            tracker.Gate.Release();
        }
    }

    /// <summary>
    /// Per-tree state holder. <see cref="Gate"/> serialises concurrent
    /// bootstraps; the backing state field is published via
    /// <see cref="Volatile"/> reads/writes so <see cref="GetState"/>
    /// observes a coherent value without taking the gate.
    /// </summary>
    private sealed class BootstrapTracker
    {
        public SemaphoreSlim Gate { get; } = new(initialCount: 1, maxCount: 1);

        private int _state = (int)LatticeBootstrapState.Idle;

        public LatticeBootstrapState GetState() =>
            (LatticeBootstrapState)Volatile.Read(ref _state);

        public void SetState(LatticeBootstrapState state) =>
            Volatile.Write(ref _state, (int)state);
    }
}
