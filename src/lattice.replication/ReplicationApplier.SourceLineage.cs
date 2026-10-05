using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Replication;

/// <summary>
/// The source-lineage admission seam of <see cref="ReplicationApplier"/> (issue
/// #4707). Every replicated apply entry - a pushed batch (<see cref="ApplyBatchAsync"/>
/// and the per-entry <see cref="ApplyAsync"/>), a causal-buffer drain
/// (<see cref="ApplyDrainedEntryAsync"/>) and a dead-letter replay (which calls
/// <see cref="ApplyAsync"/>) - passes through <see cref="AdmitSourceLineageAsync"/>,
/// so an entry a sender read under a source lineage this tree no longer holds is
/// refused on every path, not only at the transport that received it.
/// </summary>
internal sealed partial class ReplicationApplier
{
    /// <summary>
    /// The <see cref="ApplyResult"/> a lineage-refused entry or run returns.
    /// </summary>
    private static readonly ApplyResult SourceLineageRefusedResult = new()
    {
        Applied = false,
        HighWaterMark = HybridLogicalClock.Zero,
        SourceLineageRefused = true,
    };

    /// <summary>
    /// The <see cref="ApplyResult"/> a transiently refused entry or run returns:
    /// deferred, so the sender re-ships it.
    /// </summary>
    private static readonly ApplyResult SourceLineageDeferredResult = new()
    {
        Applied = false,
        HighWaterMark = HybridLogicalClock.Zero,
        Deferred = true,
    };

    /// <summary>
    /// Runs <see cref="ReplicationSourceLineageGate.AdmitAsync"/> for
    /// <paramref name="treeId"/>: the one source-lineage check every apply
    /// entry of this applier passes through.
    /// </summary>
    internal ValueTask<ReplicationSourceLineageGate.Verdict> AdmitSourceLineageAsync(
        string treeId,
        CancellationToken cancellationToken) =>
        ReplicationSourceLineageGate.AdmitAsync(grainFactory, treeId, _logger, cancellationToken);
    /// <summary>
    /// The result an apply entry returns for a verdict other than
    /// <see cref="ReplicationSourceLineageGate.Verdict.Apply"/>.
    /// </summary>
    private static ApplyResult SourceLineageRefusal(ReplicationSourceLineageGate.Verdict verdict) =>
        verdict == ReplicationSourceLineageGate.Verdict.RefuseLineage
            ? SourceLineageRefusedResult
            : SourceLineageDeferredResult;
}
