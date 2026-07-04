using Microsoft.Extensions.DependencyInjection;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Access-gate enforcement wiring for the durable cursor grain. The live
/// key / entry cursor paths page through the public
/// <see cref="ILattice.KeysAsync"/> / <see cref="ILattice.EntriesAsync"/>
/// surface, which already applies the caller's read-path key-filter (the
/// caller's identity propagates on <see cref="Orleans.Runtime.RequestContext"/>
/// from the client through the cursor grain into that call), so those paths
/// need no additional wiring here. This partial closes the two gaps that do not
/// go through the public filtered surface:
/// <list type="bullet">
///   <item><description>the <b>delete-range</b> cursor, which is enforced with
///   a <see cref="LatticeOperation.RangeDelete"/> hard-deny over the cursor's
///   full effective range up front, so a partially authorized range deletes
///   nothing across every step;</description></item>
///   <item><description>the <b>snapshot</b> cursor, which reads snapshot leaf
///   grains directly and therefore has the read-path key-filter re-applied at
///   page emit here.</description></item>
/// </list>
/// </summary>
internal sealed partial class LatticeCursorGrain
{
    private ILatticeAccessGate? _accessGate;
    private bool _accessGateResolved;
    private ILatticeMembershipContext? _membershipContext;
    private bool _membershipContextResolved;

    private static readonly ILatticeAccessGate CursorNullGateFallback = new NullLatticeAccessGate();

    /// <summary>
    /// The registered access gate, resolved once per activation. Falls back to
    /// the null gate if unregistered so the cursor never throws on a missing
    /// service.
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

            return _accessGate ?? CursorNullGateFallback;
        }
    }

    /// <summary>
    /// The registered membership context, resolved once per activation, or
    /// <c>null</c> when unregistered (the subject resolver then yields
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

    /// <summary>
    /// Fail-closed <see cref="LatticeOperation.RangeDelete"/> hard-deny over the
    /// cursor's effective range. Throws <see cref="LatticeAuthorizationDeniedException"/>
    /// when the caller is not authorized to delete the whole range (a plain deny
    /// or a partial-coverage allow), so a delete-range cursor never tombstones a
    /// subset of a range the policy only partially admits. Zero-cost under the
    /// default null gate / system-origin turn.
    /// </summary>
    private ValueTask EnforceCursorRangeDeleteAsync(string? startInclusive, string? endExclusive) =>
        LatticeAccessGateEnforcement.EnforceRangeDeleteAsync(
            AccessGate, MembershipContext, state.State.TreeId, startInclusive, endExclusive, CancellationToken.None);

    /// <summary>
    /// Resolves the fail-closed read-path key-filter for a snapshot cursor page
    /// over the cursor's effective range. Returns <c>null</c> when no filtering
    /// is required (default null gate, system-origin, or a plain allow), a
    /// reject-all predicate on a full deny, or the gate's per-key filter on a
    /// partial allow.
    /// </summary>
    private ValueTask<Func<string, bool>?> ResolveSnapshotKeyFilterAsync(string? startInclusive, string? endExclusive) =>
        LatticeAccessGateEnforcement.ResolveRangeReadFilterAsync(
            AccessGate, MembershipContext, state.State.TreeId, startInclusive, endExclusive, CancellationToken.None);
}
