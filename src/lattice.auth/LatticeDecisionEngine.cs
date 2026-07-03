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

    /// <summary>
    /// Evaluates a request exactly as <see cref="Evaluate"/> does, additionally
    /// surfacing the winning <paramref name="match"/> so the enforcement gate can
    /// build an audit event without re-evaluating. Used only on the audit path;
    /// the decision itself is identical to the fast <see cref="Evaluate"/> path.
    /// </summary>
    /// <param name="subject">The requesting subject.</param>
    /// <param name="treeId">The target tree id. Must not be <c>null</c> or empty.</param>
    /// <param name="operation">The requested operation.</param>
    /// <param name="key">The exact key for a point request, or <c>null</c> for a collection request.</param>
    /// <param name="rangeStart">The inclusive range start, or <c>null</c>.</param>
    /// <param name="rangeEnd">The exclusive range end, or <c>null</c>.</param>
    /// <param name="match">The winning rule match, or a default (unmatched) value.</param>
    /// <returns>The access decision.</returns>
    /// <exception cref="ArgumentException"><paramref name="treeId"/> is <c>null</c> or empty.</exception>
    internal LatticeAccessDecision Evaluate(
        LatticeSubject subject,
        string treeId,
        LatticeOperation operation,
        string? key,
        string? rangeStart,
        string? rangeEnd,
        out PolicyMatch match)
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
            rangeEnd,
            out match);
    }
}
