using Orleans.Concurrency;

namespace Orleans.Lattice.Membership;

/// <summary>
/// The single cluster-wide <see cref="IMembershipEpochGrain"/> activation. Holds the
/// epoch in memory only: each activation mints a fresh
/// <see cref="MembershipEpoch.Incarnation"/>, so a reactivation reads to every silo
/// as a membership change and no storage provider is needed.
/// </summary>
/// <remarks>
/// Reentrant because neither method awaits: each reads or bumps the counter in one
/// synchronous step, so a read never queues behind another silo's advance.
/// </remarks>
[Reentrant]
internal sealed class MembershipEpochGrain : Grain, IMembershipEpochGrain
{
    private readonly Guid _incarnation = Guid.NewGuid();
    private long _version;

    /// <inheritdoc />
    public Task<MembershipEpoch> GetAsync() =>
        Task.FromResult(new MembershipEpoch(_incarnation, Interlocked.Read(ref _version)));

    /// <inheritdoc />
    public Task<MembershipEpoch> AdvanceAsync() =>
        Task.FromResult(new MembershipEpoch(_incarnation, Interlocked.Increment(ref _version)));
}
