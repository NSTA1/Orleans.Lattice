using Orleans.Concurrency;

namespace Orleans.Lattice.Auth;

/// <summary>
/// The single cluster-wide <see cref="IAuthPolicyEpochGrain"/> activation. Holds
/// the epoch in memory only: each activation mints a fresh
/// <see cref="AuthPolicyEpoch.Incarnation"/>, so a reactivation reads to every
/// silo as a policy change and no storage provider is needed.
/// </summary>
/// <remarks>
/// Reentrant because neither method awaits: each reads or bumps the counter in one
/// synchronous step, so a read never queues behind another silo's advance.
/// </remarks>
[Reentrant]
internal sealed class AuthPolicyEpochGrain : Grain, IAuthPolicyEpochGrain
{
    private readonly Guid _incarnation = Guid.NewGuid();
    private long _version;

    /// <inheritdoc />
    public Task<AuthPolicyEpoch> GetAsync() =>
        Task.FromResult(new AuthPolicyEpoch(_incarnation, Interlocked.Read(ref _version)));

    /// <inheritdoc />
    public Task<AuthPolicyEpoch> AdvanceAsync() =>
        Task.FromResult(new AuthPolicyEpoch(_incarnation, Interlocked.Increment(ref _version)));
}
