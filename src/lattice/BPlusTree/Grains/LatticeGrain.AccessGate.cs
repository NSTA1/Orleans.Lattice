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
/// <b>Zero-cost default.</b> With only <c>AddLattice</c> registered the gate
/// is <see cref="NullLatticeAccessGate"/>, so <see cref="AuthorizeAsync"/>
/// returns a cached allow-all decision <em>without</em> resolving the caller
/// subject or allocating a request: the default read path is byte-for-byte
/// identical to the pre-gate behaviour, and the per-key scan loop sees a
/// <c>null</c> filter and does no per-key work. Subject resolution and the
/// gate call happen only once a real gate is registered.
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
        // Gate-bypass: internal machinery (replication-apply, saga legs,
        // maintenance) and authorised view-maintenance traffic never
        // self-filter. One RequestContext lookup.
        if (LatticeAccessGateContext.IsGateBypassed)
        {
            return LatticeAccessDecision.Allow();
        }

        // No authorization configured: with only the default null gate there is
        // no decision to make and no filter to apply, so skip subject resolution
        // entirely. This keeps the default path byte-for-byte identical to the
        // pre-gate behaviour (no membership resolution, no request allocation)
        // and - crucially - avoids re-entrancy: an infrastructure add-on that
        // dogfoods a tree (for example the membership directory) reads that tree
        // through the public scan surface, and resolving the caller subject here
        // would recurse back into the very directory being read.
        var gate = AccessGate;
        if (gate is NullLatticeAccessGate)
        {
            return LatticeAccessDecision.Allow();
        }

        // Caller-identity resolution is itself infrastructure: the membership
        // context resolves the subject by reading its dogfooded directory trees
        // through the public scan surface, which would otherwise re-enter this
        // gate and recurse. Resolve the subject under a system-origin scope so
        // those internal directory reads bypass the gate; the gate is then
        // consulted for the caller's actual request outside that scope.
        LatticeSubject subject;
        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            subject = await LatticeAccessGateSubjectResolver
                .ResolveAsync(MembershipContext, cancellationToken);
        }

        var request = new LatticeAccessRequest(TreeId, operation, subject, key, rangeStart, rangeEnd);
        return await gate.AuthorizeAsync(in request, cancellationToken);
    }

    /// <summary>
    /// Fail-closed enforcement for a single-key mutation
    /// (<see cref="LatticeOperation.Write"/> / <see cref="LatticeOperation.Delete"/>
    /// / <see cref="LatticeOperation.CrdtApply"/>). Throws
    /// <see cref="LatticeAuthorizationDeniedException"/> when the gate denies the
    /// key. Inherits the null-gate / system-origin zero-cost short-circuits, so it
    /// is allocation-free and never throws under the default null gate.
    /// </summary>
    private ValueTask EnforcePointAsync(LatticeOperation operation, string key, CancellationToken cancellationToken) =>
        LatticeAccessGateEnforcement.EnforcePointAsync(AccessGate, MembershipContext, TreeId, operation, key, cancellationToken);

    /// <summary>
    /// Fail-closed enforcement for a batch of single-key mutations, resolving the
    /// caller subject once and throwing on the first denied key. Used to authorize
    /// every key of a batch (and every leg of an atomic batch) before any write is
    /// applied.
    /// </summary>
    private ValueTask EnforceManyPointsAsync(LatticeOperation operation, IReadOnlyList<string> keys, CancellationToken cancellationToken) =>
        LatticeAccessGateEnforcement.EnforceManyPointsAsync(AccessGate, MembershipContext, TreeId, operation, keys, cancellationToken);

    /// <summary>
    /// Fail-closed hard-deny enforcement for a <see cref="LatticeOperation.RangeDelete"/>:
    /// a plain deny or a partial-coverage (filtered) allow both throw and delete
    /// nothing; only a uniform whole-range allow proceeds.
    /// </summary>
    private ValueTask EnforceRangeDeleteAsync(string? startInclusive, string? endExclusive, CancellationToken cancellationToken) =>
        LatticeAccessGateEnforcement.EnforceRangeDeleteAsync(AccessGate, MembershipContext, TreeId, startInclusive, endExclusive, cancellationToken);

    /// <summary>
    /// Fail-closed enforcement for a whole-tree operation carrying no key or range
    /// (<see cref="LatticeOperation.Admin"/> / <see cref="LatticeOperation.BulkLoad"/>).
    /// </summary>
    private ValueTask EnforceWholeTreeAsync(LatticeOperation operation, CancellationToken cancellationToken) =>
        LatticeAccessGateEnforcement.EnforceWholeTreeAsync(AccessGate, MembershipContext, TreeId, operation, cancellationToken);

    /// <summary>
    /// Fail-closed authorization check for a single-key point read
    /// (<see cref="LatticeOperation.Read"/>). Returns <c>true</c> when the caller
    /// may observe <paramref name="key"/>; a denied key returns <c>false</c> so
    /// the read surface can report the key as absent (not-found / empty) rather
    /// than throwing, matching point-read semantics. Inherits the null-gate /
    /// system-origin zero-cost short-circuit, so the default path returns a
    /// synchronously-completed <c>true</c> without resolving the subject.
    /// </summary>
    private async ValueTask<bool> IsPointReadAllowedAsync(string key, CancellationToken cancellationToken)
    {
        var decision = await AuthorizeAsync(
            LatticeOperation.Read, key, rangeStart: null, rangeEnd: null, cancellationToken);
        if (!decision.Allowed)
        {
            return false;
        }

        // A filtered allow (should not occur for a point request, but honour it
        // defensively) admits the key only when its predicate keeps it.
        return decision.KeyFilter is null || decision.KeyFilter(key);
    }

    /// <summary>
    /// Fail-closed enforcement for a batch of entry writes, authorizing every
    /// entry's key (plus any <paramref name="additionalDeleteKeys"/>) before any
    /// write is applied. Materializes the key list only when a real gate is
    /// active, so the default null-gate path allocates nothing and returns
    /// synchronously. Used by the multi-key and atomic write surfaces so a single
    /// denied key aborts the whole batch before commit.
    /// </summary>
    private ValueTask EnforceEntryWritesAsync(
        IReadOnlyList<KeyValuePair<string, byte[]>> entries,
        IReadOnlyList<string>? additionalDeleteKeys,
        CancellationToken cancellationToken)
    {
        var gate = AccessGate;
        var deleteCount = additionalDeleteKeys?.Count ?? 0;
        if (LatticeAccessGateEnforcement.SkipsEnforcement(gate) || (entries.Count == 0 && deleteCount == 0))
        {
            return ValueTask.CompletedTask;
        }

        var keys = new List<string>(entries.Count + deleteCount);
        for (var i = 0; i < entries.Count; i++)
        {
            keys.Add(entries[i].Key);
        }

        if (additionalDeleteKeys is not null)
        {
            for (var i = 0; i < additionalDeleteKeys.Count; i++)
            {
                keys.Add(additionalDeleteKeys[i]);
            }
        }

        return LatticeAccessGateEnforcement.EnforceManyPointsAsync(
            gate, MembershipContext, TreeId, LatticeOperation.Write, keys, cancellationToken);
    }

    /// <summary>
    /// Fail-closed hard-deny enforcement for a read that cannot be narrowed by a
    /// per-key filter (a per-shard count aggregate or a content digest): a plain
    /// deny or a partial-coverage (filtered) allow both throw; only a uniform
    /// whole-range allow proceeds.
    /// </summary>
    private ValueTask EnforceUniformRangeReadAsync(string? startInclusive, string? endExclusive, CancellationToken cancellationToken) =>
        LatticeAccessGateEnforcement.EnforceUniformRangeReadAsync(AccessGate, MembershipContext, TreeId, startInclusive, endExclusive, cancellationToken);

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
        // Fail-closed: a plain deny carries no key-filter, so translate it into a
        // reject-all predicate here rather than returning null (which the scan
        // surface reads as "no filtering required" and would fail OPEN, admitting
        // every key of a range the caller is not authorized to read).
        if (!decision.Allowed)
        {
            return static _ => false;
        }

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
        // Fail-closed: see ResolveRangeReadKeyFilterAsync. A uniform deny must
        // prune every requested key, not fall through to an unfiltered read.
        if (!decision.Allowed)
        {
            return static _ => false;
        }

        return decision.KeyFilter;
    }
}
