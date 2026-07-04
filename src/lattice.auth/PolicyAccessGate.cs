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
    LatticeDecisionEngine engine,
    CompiledPolicySnapshotMaintainer maintainer,
    LatticeAuthDecisionObserver observer,
    IOptionsMonitor<LatticeAuthOptions> options) : ILatticeAccessGate, ILatticeReadGrantProbe
{
    /// <inheritdoc />
    public ValueTask<LatticeAccessDecision> AuthorizeAsync(
        in LatticeAccessRequest request,
        CancellationToken cancellationToken = default)
    {
        var start = LatticeAuthDecisionObserver.CaptureStart();

        // Root-of-trust: a bootstrap administrator is always Admin on every tree
        // and operation, so a policy misconfiguration can never lock every
        // operator out of the authorization tree itself. Checked before the
        // engine so it holds even against a cold snapshot.
        if (IsBootstrapAdministrator(request.Subject.SubjectId))
        {
            var allow = LatticeAccessDecision.Allow();
            observer.Observe(in request, in allow, default, maintainer.CurrentEpoch, start);
            return new ValueTask<LatticeAccessDecision>(allow);
        }

        // Control-plane isolation (issue #1103). The reserved authorization
        // namespace (sys-auth-*) governs the gate itself - membership and policy -
        // so its access decision must be independent of the data-plane
        // DefaultEffect. A non-bootstrap caller only reaches this point because it
        // is not a break-glass administrator; since no rule may be scoped at the
        // reserved namespace (the store rejects it), an unmatched request MUST
        // fail closed to Deny even under DefaultEffect=Allow. Without this, an
        // unmatched admin request would inherit Allow and any caller (including an
        // anonymous one) could rewrite membership and policy - a full
        // control-plane takeover. The infrastructure's own reads and writes of
        // this namespace run system-origin and never reach the gate, so they are
        // unaffected; only a genuine external control-plane request is governed
        // here. An explicit matched Allow (a future separately-modelled grant) is
        // still honoured.
        if (LatticeAuthReservedTrees.IsReserved(request.TreeId))
        {
            var controlPlane = EvaluateControlPlane(in request);
            observer.Observe(in request, in controlPlane, default, maintainer.CurrentEpoch, start);
            return new ValueTask<LatticeAccessDecision>(controlPlane);
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
            var deny = LatticeAccessDecision.Deny(fenceReason);
            observer.Observe(in request, in deny, default, engine.CurrentEpoch, start);
            return new ValueTask<LatticeAccessDecision>(deny);
        }

        // Warm fast path: once any rebuild has advanced the epoch, evaluation is
        // synchronous and in-memory, so complete without allocating a state
        // machine.
        if (maintainer.CurrentEpoch > 0)
        {
            return new ValueTask<LatticeAccessDecision>(EvaluateAndObserve(in request, start));
        }

        // Cold path (first request on this silo): warm the snapshot once, then
        // evaluate. The request is copied by value into the async helper because
        // an async method cannot take an 'in' parameter.
        return WarmThenEvaluateAsync(request, start, cancellationToken);
    }

    private async ValueTask<LatticeAccessDecision> WarmThenEvaluateAsync(
        LatticeAccessRequest request,
        long start,
        CancellationToken cancellationToken)
    {
        await maintainer.EnsureWarmAsync(cancellationToken).ConfigureAwait(false);
        return EvaluateAndObserve(in request, start);
    }

    /// <summary>
    /// Computes the decision and emits post-decision observability. On the
    /// audit-enabled path the detailed evaluation additionally surfaces the
    /// winning rule so the audit event can name it; on the default (audit-off)
    /// path the byte-for-byte-unchanged fast evaluation is used and only the
    /// (listener-guarded) metrics are recorded.
    /// </summary>
    private LatticeAccessDecision EvaluateAndObserve(in LatticeAccessRequest request, long start)
    {
        LatticeAccessDecision decision;
        PolicyMatch match = default;
        if (observer.IsAuditEnabled)
        {
            decision = engine.Evaluate(
                request.Subject,
                request.TreeId,
                request.Operation,
                request.Key,
                request.RangeStart,
                request.RangeEnd,
                out match);
        }
        else
        {
            decision = Evaluate(in request);
        }

        observer.Observe(in request, in decision, in match, maintainer.CurrentEpoch, start);
        return decision;
    }

    private LatticeAccessDecision Evaluate(in LatticeAccessRequest request) =>
        engine.Evaluate(
            request.Subject,
            request.TreeId,
            request.Operation,
            request.Key,
            request.RangeStart,
            request.RangeEnd);

    /// <summary>
    /// Evaluates a request that targets the reserved authorization namespace
    /// (<c>sys-auth-*</c>) with <b>control-plane isolation</b>: the decision is
    /// forced closed (Deny) on every outcome that is not an explicit matched
    /// Allow, so the data-plane <see cref="LatticeAuthOptions.DefaultEffect"/> can
    /// never grant control of the gate. Bootstrap administrators never reach here
    /// (they are allowed earlier), so this governs only non-bootstrap callers,
    /// which - absent a rule that can be scoped at the reserved namespace - always
    /// resolve to Deny.
    /// </summary>
    private LatticeAccessDecision EvaluateControlPlane(in LatticeAccessRequest request)
    {
        var decision = engine.Evaluate(
            request.Subject,
            request.TreeId,
            request.Operation,
            request.Key,
            request.RangeStart,
            request.RangeEnd,
            out var match);

        if (match.Matched && match.Effect == LatticeEffect.Allow && decision.Allowed)
        {
            return decision;
        }

        return LatticeAccessDecision.Deny(
            "Control-plane isolation: the reserved authorization namespace is governed only by "
            + "bootstrap administrators (or an explicit matched allow rule); an unmatched request is "
            + "denied independently of the data-plane default effect.");
    }

    private bool IsBootstrapAdministrator(string subjectId)
    {
        var admins = options.CurrentValue.BootstrapAdministrators;
        return admins.Count > 0 && admins.Contains(subjectId);
    }

    /// <inheritdoc />
    public ValueTask<bool> HasAnyGrantAsync(
        string treeId,
        LatticeSubject subject,
        LatticeOperation operation,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);

        // An unauthenticated caller can never read any key.
        if (subject.IsAnonymous)
        {
            return new ValueTask<bool>(false);
        }

        // Root-of-trust: a bootstrap administrator can read every tree.
        if (IsBootstrapAdministrator(subject.SubjectId))
        {
            return new ValueTask<bool>(true);
        }

        // Control-plane isolation: the reserved authorization namespace is never
        // visible to a non-bootstrap caller by default effect - only an explicit
        // matched allow grant makes it so. Mirror the enforcement path so a caller
        // that cannot administer the namespace also cannot learn it exists.
        if (LatticeAuthReservedTrees.IsReserved(treeId))
        {
            var reserved = engine.Evaluate(subject, treeId, operation, key: null, rangeStart: null, rangeEnd: null, out var match);
            return new ValueTask<bool>(match.Matched && match.Effect == LatticeEffect.Allow && reserved.Allowed);
        }

        return new ValueTask<bool>(engine.HasAnyGrant(subject, treeId, operation));
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
