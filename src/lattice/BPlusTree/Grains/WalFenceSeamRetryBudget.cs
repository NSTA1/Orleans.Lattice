using Microsoft.Extensions.Options;
using Orleans.Configuration;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Sets <see cref="WalFenceSeamRetry.Budget"/> from the silo's configured Orleans
/// response timeout as the silo is built (epic #5605), so a fence rejection is
/// retried for as long as the grain call carrying the operation can still answer,
/// and no longer.
/// </summary>
internal sealed class WalFenceSeamRetryBudget(IOptions<SiloMessagingOptions> messaging) : ILifecycleParticipant<ISiloLifecycle>
{
    /// <inheritdoc />
    public void Participate(ISiloLifecycle lifecycle)
    {
        ArgumentNullException.ThrowIfNull(messaging);
        WalFenceSeamRetry.Budget = messaging.Value.ResponseTimeout;
    }
}
