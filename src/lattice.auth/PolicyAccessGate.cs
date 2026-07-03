using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Auth;

/// <summary>
/// The real <see cref="ILatticeAccessGate"/> that turns
/// <c>Orleans.Lattice.Auth</c> into the enforcement control point. It replaces
/// the core default <see cref="NullLatticeAccessGate"/> and answers every
/// data-plane authorization request from the in-memory compiled policy snapshot
/// via <see cref="ILatticeDecisionEngine"/>, plus a bootstrap root-of-trust.
/// </summary>
/// <remarks>
/// <para>
/// The core resolves the caller subject and hands it in on
/// <see cref="LatticeAccessRequest.Subject"/>, so this gate performs <b>no</b>
/// subject resolution and <b>no</b> storage I/O on the request path: it is a
/// bootstrap-set membership check followed by an in-memory
/// <see cref="ILatticeDecisionEngine.Evaluate"/>. This keeps the request path
/// allocation-light and, crucially, non-re-entrant - the gate never reads a
/// lattice tree, so it cannot recurse back through itself while a scan is in
/// flight.
/// </para>
/// <para>
/// The compiled snapshot is warmed on the first request (awaited once while the
/// engine's epoch is still zero) so a live cluster never evaluates against an
/// empty snapshot; every subsequent request takes the synchronous fast path.
/// </para>
/// </remarks>
internal sealed class PolicyAccessGate(
    ILatticeDecisionEngine engine,
    CompiledPolicySnapshotMaintainer maintainer,
    IOptionsMonitor<LatticeAuthOptions> options) : ILatticeAccessGate
{
    /// <inheritdoc />
    public ValueTask<LatticeAccessDecision> AuthorizeAsync(
        in LatticeAccessRequest request,
        CancellationToken cancellationToken = default)
    {
        // Root-of-trust: a bootstrap administrator is always Admin on every tree
        // and operation, so a policy misconfiguration can never lock every
        // operator out of the authorization tree itself. Checked before the
        // engine so it holds even against a cold snapshot.
        if (IsBootstrapAdministrator(request.Subject.SubjectId))
        {
            return new ValueTask<LatticeAccessDecision>(LatticeAccessDecision.Allow());
        }

        // Optional strict-consistency policy-epoch fence (issue #982), off by
        // default. Only user writes reach this gate at all - the core
        // short-circuits system-origin and replication-applied writes before the
        // gate is consulted - so this can only ever fence a user write. When no
        // tree is opted into strict consistency the check is skipped with a single
        // null/empty test, leaving the eventual path byte-for-byte unchanged and
        // zero-cost. Placed after the bootstrap-admin bypass so the break-glass
        // root of trust is never fenced out of repairing policy under a stale
        // epoch.
        if (ShouldFence(in request, out var fenceReason))
        {
            return new ValueTask<LatticeAccessDecision>(LatticeAccessDecision.Deny(fenceReason));
        }

        // Warm fast path: once any rebuild has advanced the epoch, evaluation is
        // synchronous and in-memory, so complete without allocating a state
        // machine.
        if (maintainer.CurrentEpoch > 0)
        {
            return new ValueTask<LatticeAccessDecision>(Evaluate(in request));
        }

        // Cold path (first request on this silo): warm the snapshot once, then
        // evaluate. The request is copied by value into the async helper because
        // an async method cannot take an 'in' parameter.
        return WarmThenEvaluateAsync(request, cancellationToken);
    }

    private async ValueTask<LatticeAccessDecision> WarmThenEvaluateAsync(
        LatticeAccessRequest request,
        CancellationToken cancellationToken)
    {
        await maintainer.EnsureWarmAsync(cancellationToken).ConfigureAwait(false);
        return Evaluate(in request);
    }

    private LatticeAccessDecision Evaluate(in LatticeAccessRequest request) =>
        engine.Evaluate(
            request.Subject,
            request.TreeId,
            request.Operation,
            request.Key,
            request.RangeStart,
            request.RangeEnd);

    private bool IsBootstrapAdministrator(string subjectId)
    {
        var admins = options.CurrentValue.BootstrapAdministrators;
        return admins.Count > 0 && admins.Contains(subjectId);
    }

    /// <summary>
    /// The operation bits that count as a write for the strict-consistency fence:
    /// everything that mutates a tree. Pure reads (<see cref="LatticeOperation.Read"/>,
    /// <see cref="LatticeOperation.RangeRead"/>) are deliberately excluded so a
    /// read is never fenced.
    /// </summary>
    private const LatticeOperation FenceWriteMask =
        LatticeOperation.Write
        | LatticeOperation.Delete
        | LatticeOperation.RangeDelete
        | LatticeOperation.CrdtApply
        | LatticeOperation.AtomicWrite
        | LatticeOperation.BulkLoad
        | LatticeOperation.Admin;

    /// <summary>
    /// Decides whether the strict-consistency epoch fence rejects this request.
    /// Returns <c>false</c> (and leaves <paramref name="reason"/> empty) for the
    /// zero-cost eventual path: no strict tree configured, a non-write operation,
    /// a tree that is not opted in, no ambient epoch floor, or a local epoch that
    /// has already caught up to the floor.
    /// </summary>
    private bool ShouldFence(in LatticeAccessRequest request, out string reason)
    {
        reason = string.Empty;

        var strictTrees = options.CurrentValue.StrictConsistencyTrees;
        if (strictTrees is null || strictTrees.Count == 0)
        {
            // Eventual path: no strict trees configured, nothing to fence.
            return false;
        }

        if ((request.Operation & FenceWriteMask) == 0)
        {
            // Reads are never fenced.
            return false;
        }

        if (!strictTrees.Contains(request.TreeId))
        {
            return false;
        }

        var required = LatticePolicyEpochFenceContext.RequiredEpoch;
        if (required is not long floor)
        {
            // No caller-supplied floor -> eventual for this write.
            return false;
        }

        var localEpoch = engine.CurrentEpoch;
        if (localEpoch >= floor)
        {
            // Local policy has already converged to (or past) the required floor.
            return false;
        }

        reason =
            $"Strict-consistency fence: a write to tree '{request.TreeId}' requires policy epoch "
            + $">= {floor} but this cluster's compiled policy is at epoch {localEpoch}. The write is "
            + "rejected until cross-cluster replication and the policy change-feed settle.";
        return true;
    }
}
