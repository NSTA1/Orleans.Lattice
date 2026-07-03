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
}
