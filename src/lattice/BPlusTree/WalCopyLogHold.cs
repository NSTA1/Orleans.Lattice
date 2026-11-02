using Microsoft.Extensions.Logging;
using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// How copy builders drive the bounded <see cref="IWalCopyLogHoldGrain"/> on a
/// physical copy's WAL (issue #5471), with the bounds every builder shares. Every
/// call is best effort: the hold only spares the peers a re-seed, so a failure to
/// arm, publish or release it is logged and never fails the build.
/// </summary>
internal static class WalCopyLogHold
{
    /// <summary>
    /// How long an armed hold lasts without a renewal. A builder renews on every
    /// slice, so this bounds how long an abandoned or crashed build keeps the
    /// copy's log.
    /// </summary>
    internal static readonly TimeSpan BuildLease = TimeSpan.FromMinutes(30);

    /// <summary>
    /// The longest a hold lasts after its copy is cut over. It normally ends far
    /// sooner, as soon as the source's offset consumers have rebound to the copy;
    /// this bounds a consumer that never does.
    /// </summary>
    internal static readonly TimeSpan PublishGrace = TimeSpan.FromMinutes(10);

    /// <summary>Arms or renews the hold on <paramref name="copyPhysicalTreeId"/>; call before each write slice.</summary>
    public static async Task ArmAsync(
        IGrainFactory grainFactory, string copyPhysicalTreeId, string sourcePhysicalTreeId, ILogger logger)
    {
        try
        {
            await grainFactory.GetGrain<IWalCopyLogHoldGrain>(copyPhysicalTreeId)
                .ArmAsync(sourcePhysicalTreeId, BuildLease);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            logger.LogWarning(ex,
                "Could not hold the WAL of copy {Copy} of {Source} while it is built; a replication peer may need a re-seed after the cutover.",
                copyPhysicalTreeId, sourcePhysicalTreeId);
        }
    }

    /// <summary>Records that <paramref name="copyPhysicalTreeId"/> was cut over, starting the hold's grace.</summary>
    public static async Task PublishAsync(IGrainFactory grainFactory, string copyPhysicalTreeId, ILogger logger)
    {
        try
        {
            await grainFactory.GetGrain<IWalCopyLogHoldGrain>(copyPhysicalTreeId).PublishAsync(PublishGrace);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            logger.LogWarning(ex,
                "Could not record the cutover of copy {Copy} on its WAL hold; the hold lapses with its build lease instead.",
                copyPhysicalTreeId);
        }
    }

    /// <summary>Releases the hold on a discarded <paramref name="copyPhysicalTreeId"/>.</summary>
    public static async Task ReleaseAsync(IGrainFactory grainFactory, string copyPhysicalTreeId, ILogger logger)
    {
        try
        {
            await grainFactory.GetGrain<IWalCopyLogHoldGrain>(copyPhysicalTreeId).ReleaseAsync();
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            logger.LogWarning(ex,
                "Could not release the WAL hold of discarded copy {Copy}; it lapses with its lease.",
                copyPhysicalTreeId);
        }
    }
}
