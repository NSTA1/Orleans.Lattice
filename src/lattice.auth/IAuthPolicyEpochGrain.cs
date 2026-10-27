using Orleans.Concurrency;

namespace Orleans.Lattice.Auth;

/// <summary>
/// The cluster-wide record of how far the authorization policy tree has moved. A
/// single activation, addressed by <see cref="GrainKey"/>: the silo that commits a
/// policy write advances it, and every silo reads it to learn that a write it did
/// not observe itself has committed. The core change feed fires only on the silo
/// hosting the leaf that committed a write, so without it every other silo would
/// keep deciding from its pre-write snapshot.
/// </summary>
[Alias(AuthTypeAliases.IAuthPolicyEpochGrain)]
internal interface IAuthPolicyEpochGrain : IGrainWithStringKey
{
    /// <summary>The fixed key of the single cluster-wide activation.</summary>
    const string GrainKey = "sys-auth-policy-epoch";

    /// <summary>Returns the current epoch.</summary>
    /// <returns>The current epoch.</returns>
    [ReadOnly]
    Task<AuthPolicyEpoch> GetAsync();

    /// <summary>Advances the epoch after a committed policy write.</summary>
    /// <returns>The advanced epoch.</returns>
    Task<AuthPolicyEpoch> AdvanceAsync();
}
