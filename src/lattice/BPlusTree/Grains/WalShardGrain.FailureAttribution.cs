using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Attribution of the fault that opens a sticky-failure episode (issue #6116).
/// While the post-failure resync runs, every append rethrows the latched fault
/// from the append fast path, so a storm of identical refusals carries only
/// that frame. The original fault, with the provider frame that raised it, is
/// logged here once per episode instead.
/// </summary>
internal sealed partial class WalShardGrain
{
    private ILogger<WalShardGrain>? _logger;

    private ILogger<WalShardGrain>? ResolveLogger()
    {
        if (_logger is not null)
        {
            return _logger;
        }

        _logger = context.ActivationServices?
            .GetService<ILoggerFactory>()?
            .CreateLogger<WalShardGrain>();
        return _logger;
    }

    /// <summary>
    /// Logs the fault that latched the sticky failure: the failed window, the
    /// original exception, whether the provider reported its outcome unknown (with
    /// its attempt history), and the shard's abandoned windows still in motion.
    /// Runs only on the failure path.
    /// </summary>
    private void LogStickyEpisodeOpened(InFlightFlush failed, Exception fault)
    {
        if (ResolveLogger() is not { } logger || !logger.IsEnabled(LogLevel.Warning))
        {
            return;
        }

        long? lowestUnsettledStart;
        long? highestUnsettledEnd;
        lock (_stateGate)
        {
            var windows = AbandonedWindows();
            lowestUnsettledStart = windows?.LowestUnsettledStart();
            highestUnsettledEnd = windows?.HighestUnsettledEndExclusive();
        }

        var unknown = WalAppendOutcomeUnknown.Find(fault);
        LogStickyFailureLatched(
            logger,
            fault,
            _treeId,
            _shardIndex,
            failed.StartOffset,
            failed.EndOffsetExclusive,
            unknown is not null,
            unknown?.Attempts ?? 0,
            unknown?.ClientAbandonedAttempts ?? 0,
            lowestUnsettledStart,
            highestUnsettledEnd);
    }

    [LoggerMessage(
        Level = LogLevel.Warning,
        Message = "WAL shard {ShardIndex} of tree '{TreeId}' latched a sticky failure on the flush of offsets [{StartOffset},{EndOffsetExclusive}); "
            + "appends fail with this fault until the resync settles. Outcome unknown: {OutcomeUnknown} ({Attempts} provider attempts, "
            + "{ClientAbandonedAttempts} abandoned by the client). Abandoned windows still in motion: [{LowestUnsettledStart},{HighestUnsettledEndExclusive}).")]
    private static partial void LogStickyFailureLatched(
        ILogger logger,
        Exception fault,
        string treeId,
        int shardIndex,
        long startOffset,
        long endOffsetExclusive,
        bool outcomeUnknown,
        int attempts,
        int clientAbandonedAttempts,
        long? lowestUnsettledStart,
        long? highestUnsettledEndExclusive);
}
