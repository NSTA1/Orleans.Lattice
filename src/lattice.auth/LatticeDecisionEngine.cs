using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Auth;

/// <summary>
/// The default <see cref="ILatticeDecisionEngine"/>. Reads the current compiled
/// snapshot from the <see cref="CompiledPolicySnapshotMaintainer"/> and evaluates
/// requests against it with <see cref="PolicyEvaluator"/>. Holds no mutable state
/// of its own; the snapshot lifecycle (build, swap, epoch) lives on the
/// maintainer.
/// </summary>
internal sealed class LatticeDecisionEngine(
    CompiledPolicySnapshotMaintainer maintainer,
    IOptionsMonitor<LatticeAuthOptions> options) : ILatticeDecisionEngine
{
    /// <inheritdoc />
    public long CurrentEpoch => maintainer.CurrentEpoch;

    /// <inheritdoc />
    public LatticeAccessDecision Evaluate(
        LatticeSubject subject,
        string treeId,
        LatticeOperation operation,
        string? key = null,
        string? rangeStart = null,
        string? rangeEnd = null)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        return PolicyEvaluator.Evaluate(
            maintainer.Current,
            options.CurrentValue,
            subject,
            treeId,
            operation,
            key,
            rangeStart,
            rangeEnd);
    }
}
