using Microsoft.Extensions.DependencyInjection;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Access-gate enforcement wiring for the <see cref="LatticeGrain"/> data-plane
/// choke point. This partial resolves the caller subject, consults the
/// registered <see cref="ILatticeAccessGate"/> once per call, and honours the
/// system-origin bypass so internal machinery (replication-apply, saga legs,
/// maintenance) never self-filters.
/// </summary>
/// <remarks>
/// <para>
/// This issue wires only the <b>read-path key-filter</b>: the
/// <see cref="LatticeAccessDecision.KeyFilter"/> a gate returns is applied
/// server-side, during enumeration, so unauthorized keys are pruned before any
/// value crosses the grain boundary to the caller. Allow/deny enforcement for
/// writes, deletes, CRDT apply, atomic writes, range-delete, and lifecycle is a
/// later step; <see cref="AuthorizeAsync"/> is written generally (it returns the
/// full decision) so that step can reuse it, but only the range-read surfaces
/// consult it here.
/// </para>
/// <para>
/// <b>Zero-cost default.</b> With only <c>AddLattice</c> registered the gate is
/// <see cref="NullLatticeAccessGate"/> (a cached, synchronously-completed
/// <see cref="LatticeAccessDecision.Allow"/> with a <c>null</c>
/// <see cref="LatticeAccessDecision.KeyFilter"/>) and the membership context is
/// <see cref="NullLatticeMembershipContext"/> (a cached anonymous subject). Both
/// awaits complete synchronously, so the <c>async ValueTask</c> state machine
/// never suspends and never heap-allocates, and the per-key scan loop sees a
/// <c>null</c> filter and does no per-key work.
/// </para>
/// </remarks>
internal sealed partial class LatticeGrain
{
    private ILatticeAccessGate? _accessGate;
    private bool _accessGateResolved;
    private ILatticeMembershipContext? _membershipContext;
    private bool _membershipContextResolved;

    /// <summary>
    /// The registered access gate, resolved once per activation. Always
    /// non-<c>null</c> in a normally configured host because <c>AddLattice</c>
    /// registers <see cref="NullLatticeAccessGate"/>; falls back to the null
    /// gate if the service is somehow unregistered so the read path never
    /// throws on a missing gate.
    /// </summary>
    private ILatticeAccessGate AccessGate
    {
        get
        {
            if (!_accessGateResolved)
            {
                _accessGate = services.GetService<ILatticeAccessGate>();
                _accessGateResolved = true;
            }
            return _accessGate ?? NullAccessGateFallback;
        }
    }

    /// <summary>
    /// The registered membership context, resolved once per activation, or
    /// <c>null</c> when unregistered (in which case the subject resolver yields
    /// <see cref="LatticeSubject.Anonymous"/>).
    /// </summary>
    private ILatticeMembershipContext? MembershipContext
    {
        get
        {
            if (!_membershipContextResolved)
            {
                _membershipContext = services.GetService<ILatticeMembershipContext>();
                _membershipContextResolved = true;
            }
            return _membershipContext;
        }
    }

    private static readonly ILatticeAccessGate NullAccessGateFallback = new NullLatticeAccessGate();

    /// <summary>
    /// Resolves the caller subject and consults the access gate for a single
    /// data-plane request shape, returning the full
    /// <see cref="LatticeAccessDecision"/>. Skips the gate entirely (returning
    /// an allow-all decision) when the ambient turn is a system-origin call.
    /// </summary>
    /// <param name="operation">The operation shape being authorized.</param>
    /// <param name="key">The single key touched, or <c>null</c> for range / whole-set shapes.</param>
    /// <param name="rangeStart">The inclusive range start, or <c>null</c>.</param>
    /// <param name="rangeEnd">The range end, or <c>null</c>.</param>
    /// <param name="cancellationToken">Cancels subject resolution and authorization.</param>
    /// <returns>The gate's decision, or <see cref="LatticeAccessDecision.Allow"/> for a system-origin turn.</returns>
    private async ValueTask<LatticeAccessDecision> AuthorizeAsync(
        LatticeOperation operation,
        string? key,
        string? rangeStart,
        string? rangeEnd,
        CancellationToken cancellationToken)
    {
        // System-origin bypass: internal machinery (replication-apply, saga
        // legs, maintenance) never self-filters. One RequestContext lookup.
        if (LatticeAccessGateContext.IsSystemOrigin)
        {
            return LatticeAccessDecision.Allow();
        }

        var subject = await LatticeAccessGateSubjectResolver
            .ResolveAsync(MembershipContext, cancellationToken);
        var request = new LatticeAccessRequest(TreeId, operation, subject, key, rangeStart, rangeEnd);
        return await AccessGate.AuthorizeAsync(in request, cancellationToken);
    }

    /// <summary>
    /// Resolves the read-path key-filter for a range scan
    /// (<see cref="LatticeOperation.RangeRead"/>) over the half-open range
    /// [<paramref name="startInclusive"/>, <paramref name="endExclusive"/>).
    /// Returns <c>null</c> when no per-key filtering is required (a plain allow,
    /// or a system-origin turn), in which case the scan admits every key with no
    /// per-key work. A returned predicate keeps a key when it returns <c>true</c>.
    /// </summary>
    private async ValueTask<Func<string, bool>?> ResolveRangeReadKeyFilterAsync(
        string? startInclusive,
        string? endExclusive,
        CancellationToken cancellationToken)
    {
        var decision = await AuthorizeAsync(
            LatticeOperation.RangeRead, key: null, startInclusive, endExclusive, cancellationToken);
        return decision.KeyFilter;
    }

    /// <summary>
    /// Resolves the read-path key-filter for a multi-key point read
    /// (<see cref="LatticeOperation.Read"/>, <c>GetMany</c>). A single request is
    /// issued for the tree and the returned filter (if any) is applied across the
    /// requested keys. Returns <c>null</c> when no per-key filtering is required.
    /// </summary>
    private async ValueTask<Func<string, bool>?> ResolveMultiReadKeyFilterAsync(
        CancellationToken cancellationToken)
    {
        var decision = await AuthorizeAsync(
            LatticeOperation.Read, key: null, rangeStart: null, rangeEnd: null, cancellationToken);
        return decision.KeyFilter;
    }
}
