using Orleans.Concurrency;

namespace Orleans.Lattice.Replication.Grains;

/// <summary>
/// The single cluster-wide <see cref="IReplicationConfigEpochGrain"/> activation.
/// Holds the epoch in memory only: each activation mints a fresh
/// <see cref="ReplicationConfigEpoch.Incarnation"/>, so a reactivation reads to
/// every silo as a configuration change and no storage provider is needed.
/// </summary>
/// <remarks>
/// Reentrant because neither method awaits: each reads or bumps the counter in one
/// synchronous step, so a read never queues behind another silo's advance.
/// </remarks>
[Reentrant]
internal sealed class ReplicationConfigEpochGrain : Grain, IReplicationConfigEpochGrain
{
    private readonly Guid _incarnation = Guid.NewGuid();
    private long _version;

    /// <inheritdoc />
    public Task<ReplicationConfigEpoch> GetAsync() =>
        Task.FromResult(new ReplicationConfigEpoch(_incarnation, Interlocked.Read(ref _version)));

    /// <inheritdoc />
    public Task<ReplicationConfigEpoch> AdvanceAsync() =>
        Task.FromResult(new ReplicationConfigEpoch(_incarnation, Interlocked.Increment(ref _version)));
}
