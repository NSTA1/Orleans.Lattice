using Orleans.Concurrency;

namespace Orleans.Lattice.Apps;

/// <summary>
/// The single cluster-wide <see cref="IAppRegistryEpochGrain"/> activation. Holds the epoch in memory
/// only: each activation mints a fresh <see cref="AppRegistryEpoch.Incarnation"/>, so a reactivation
/// reads to every silo as a registry change and no storage provider is needed.
/// </summary>
/// <remarks>
/// Reentrant because neither method awaits: each reads or bumps the counter in one synchronous step,
/// so a read never queues behind another silo's advance.
/// </remarks>
[Reentrant]
internal sealed class AppRegistryEpochGrain : Grain, IAppRegistryEpochGrain
{
    private readonly Guid _incarnation = Guid.NewGuid();
    private long _version;

    /// <inheritdoc />
    public Task<AppRegistryEpoch> GetAsync() =>
        Task.FromResult(new AppRegistryEpoch(_incarnation, Interlocked.Read(ref _version)));

    /// <inheritdoc />
    public Task<AppRegistryEpoch> AdvanceAsync() =>
        Task.FromResult(new AppRegistryEpoch(_incarnation, Interlocked.Increment(ref _version)));
}
