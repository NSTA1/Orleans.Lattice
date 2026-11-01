using Orleans.Concurrency;

namespace Orleans.Lattice.Membership;

/// <summary>
/// The cluster-wide record of how far the <c>sys-membership-*</c> trees have moved.
/// A single activation, addressed by <see cref="GrainKey"/>: the silo that commits a
/// membership write advances it, and every silo reads it to learn that a write it
/// did not observe itself has committed. The core change feed fires only on the
/// silo hosting the leaf that committed a write, so without it every other silo
/// would keep serving cached resolutions from before the write.
/// </summary>
[Alias(MembershipTypeAliases.IMembershipEpochGrain)]
internal interface IMembershipEpochGrain : IGrainWithStringKey
{
    /// <summary>The fixed key of the single cluster-wide activation.</summary>
    const string GrainKey = "sys-membership-epoch";

    /// <summary>Returns the current epoch.</summary>
    /// <returns>The current epoch.</returns>
    [ReadOnly]
    Task<MembershipEpoch> GetAsync();

    /// <summary>Advances the epoch after a committed membership write.</summary>
    /// <returns>The advanced epoch.</returns>
    Task<MembershipEpoch> AdvanceAsync();
}
