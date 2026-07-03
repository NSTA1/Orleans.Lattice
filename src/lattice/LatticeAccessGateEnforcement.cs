namespace Orleans.Lattice;

/// <summary>
/// The shared, fail-closed enforcement primitive over an
/// <see cref="ILatticeAccessGate"/>. It resolves the caller subject once (under a
/// system-origin scope so identity resolution never re-enters the gate), consults
/// the gate, and throws <see cref="LatticeAuthorizationDeniedException"/> when the
/// request is not allowed. Used at every user-originated mutation choke point:
/// the data-plane <c>LatticeGrain</c>, the durable cursor grain, and the
/// cross-tree atomic-write coordinator.
/// </summary>
/// <remarks>
/// <para>
/// <b>Zero-cost default.</b> Every method short-circuits (returns without
/// resolving the subject or allocating a request) when the ambient turn is
/// system-origin, or when the registered gate is the default
/// <see cref="NullLatticeAccessGate"/>. The default host (only <c>AddLattice</c>
/// registered) therefore pays nothing: no membership resolution, no request
/// struct, no gate call, and never a throw.
/// </para>
/// <para>
/// <b>No storage I/O on the request path.</b> The only work a real gate does is
/// resolve the subject (a cached membership lookup) and evaluate the in-memory
/// compiled policy snapshot. The enforcer performs no reads of the governed tree
/// itself.
/// </para>
/// </remarks>
internal static class LatticeAccessGateEnforcement
{
    /// <summary>
    /// <c>true</c> when enforcement can be skipped entirely for the current turn:
    /// the turn is system-origin, or the registered gate is the default no-op.
    /// Call sites use it to avoid materializing a per-key list (or any other
    /// per-request work) on the zero-cost default path before delegating to an
    /// enforcement method.
    /// </summary>
    public static bool SkipsEnforcement(ILatticeAccessGate gate) =>
        LatticeAccessGateContext.IsGateBypassed || gate is NullLatticeAccessGate;

    /// <summary>
    /// Enforces a single-key operation (<see cref="LatticeOperation.Write"/>,
    /// <see cref="LatticeOperation.Delete"/>, <see cref="LatticeOperation.CrdtApply"/>).
    /// Throws when the gate denies the key, or returns an allow whose per-key
    /// filter excludes the key (fail-closed).
    /// </summary>
    public static async ValueTask EnforcePointAsync(
        ILatticeAccessGate gate,
        ILatticeMembershipContext? membership,
        string treeId,
        LatticeOperation operation,
        string key,
        CancellationToken cancellationToken)
    {
        if (LatticeAccessGateContext.IsGateBypassed || gate is NullLatticeAccessGate)
        {
            return;
        }

        var subject = await ResolveSubjectAsync(membership, cancellationToken);
        var request = new LatticeAccessRequest(treeId, operation, subject, key, rangeStart: null, rangeEnd: null);
        var decision = await gate.AuthorizeAsync(in request, cancellationToken);
        ThrowIfPointDenied(in decision, treeId, operation, subject, key);
    }

    /// <summary>
    /// Enforces the same single-key <paramref name="operation"/> for every key in
    /// <paramref name="keys"/>, resolving the caller subject exactly once. Throws
    /// on the first key the gate denies. Used to authorize a batch write and,
    /// crucially, every leg of an atomic batch <b>before</b> any leg is applied,
    /// so a single denied key aborts the whole batch with no partial writes.
    /// </summary>
    public static async ValueTask EnforceManyPointsAsync(
        ILatticeAccessGate gate,
        ILatticeMembershipContext? membership,
        string treeId,
        LatticeOperation operation,
        IReadOnlyList<string> keys,
        CancellationToken cancellationToken)
    {
        if (LatticeAccessGateContext.IsGateBypassed || gate is NullLatticeAccessGate || keys.Count == 0)
        {
            return;
        }

        var subject = await ResolveSubjectAsync(membership, cancellationToken);
        for (var i = 0; i < keys.Count; i++)
        {
            var key = keys[i];
            var request = new LatticeAccessRequest(treeId, operation, subject, key, rangeStart: null, rangeEnd: null);
            var decision = await gate.AuthorizeAsync(in request, cancellationToken);
            ThrowIfPointDenied(in decision, treeId, operation, subject, key);
        }
    }

    /// <summary>
    /// Enforces a <see cref="LatticeOperation.RangeDelete"/> over the half-open
    /// range with <b>hard-deny</b> semantics: the delete is all-or-nothing, so a
    /// plain deny <em>and</em> a partial-coverage (filtered) allow both throw and
    /// delete nothing. Only a uniform whole-range allow (no key-filter) proceeds.
    /// </summary>
    public static async ValueTask EnforceRangeDeleteAsync(
        ILatticeAccessGate gate,
        ILatticeMembershipContext? membership,
        string treeId,
        string? rangeStart,
        string? rangeEnd,
        CancellationToken cancellationToken)
    {
        if (LatticeAccessGateContext.IsGateBypassed || gate is NullLatticeAccessGate)
        {
            return;
        }

        var subject = await ResolveSubjectAsync(membership, cancellationToken);
        var request = new LatticeAccessRequest(
            treeId, LatticeOperation.RangeDelete, subject, key: null, rangeStart, rangeEnd);
        var decision = await gate.AuthorizeAsync(in request, cancellationToken);

        if (!decision.Allowed)
        {
            throw Denied(treeId, LatticeOperation.RangeDelete, subject, decision.Reason);
        }

        // Partial-coverage allow: a range delete may never narrow to an
        // authorized subset. Deny the whole operation and delete nothing.
        if (decision.KeyFilter is not null)
        {
            throw new LatticeAuthorizationDeniedException(
                treeId,
                LatticeOperation.RangeDelete,
                subject.SubjectId,
                decision.Reason ?? "Range delete is not fully authorized over the requested range; "
                    + "a range delete is all-or-nothing and is refused rather than narrowed.");
        }
    }

    /// <summary>
    /// Enforces a whole-tree <see cref="LatticeOperation"/> that carries no key or
    /// range (for example <see cref="LatticeOperation.Admin"/>,
    /// <see cref="LatticeOperation.BulkLoad"/>). Throws when the gate denies, or
    /// returns a per-key filter (a whole-tree operation cannot be narrowed, so a
    /// filtered allow is treated as a deny, fail-closed).
    /// </summary>
    public static async ValueTask EnforceWholeTreeAsync(
        ILatticeAccessGate gate,
        ILatticeMembershipContext? membership,
        string treeId,
        LatticeOperation operation,
        CancellationToken cancellationToken)
    {
        if (LatticeAccessGateContext.IsGateBypassed || gate is NullLatticeAccessGate)
        {
            return;
        }

        var subject = await ResolveSubjectAsync(membership, cancellationToken);
        var request = new LatticeAccessRequest(treeId, operation, subject, key: null, rangeStart: null, rangeEnd: null);
        var decision = await gate.AuthorizeAsync(in request, cancellationToken);

        if (!decision.Allowed)
        {
            throw Denied(treeId, operation, subject, decision.Reason);
        }

        if (decision.KeyFilter is not null)
        {
            throw new LatticeAuthorizationDeniedException(
                treeId,
                operation,
                subject.SubjectId,
                decision.Reason ?? "Operation is not fully authorized over the whole tree; "
                    + "a whole-tree operation cannot be narrowed and is refused.");
        }
    }

    /// <summary>
    /// Enforces a read that cannot be meaningfully narrowed by a per-key filter -
    /// a per-shard count aggregate or a content digest over a range - with
    /// <b>hard-deny</b> semantics: a plain deny <em>and</em> a partial-coverage
    /// (filtered) allow both throw; only a uniform whole-range allow proceeds.
    /// Refusing rather than narrowing avoids leaking structural information (the
    /// physical shard count and per-shard key distribution, or a content-digest
    /// oracle) about keys the caller is not authorized to read.
    /// </summary>
    public static async ValueTask EnforceUniformRangeReadAsync(
        ILatticeAccessGate gate,
        ILatticeMembershipContext? membership,
        string treeId,
        string? rangeStart,
        string? rangeEnd,
        CancellationToken cancellationToken)
    {
        if (LatticeAccessGateContext.IsGateBypassed || gate is NullLatticeAccessGate)
        {
            return;
        }

        var subject = await ResolveSubjectAsync(membership, cancellationToken);
        var request = new LatticeAccessRequest(
            treeId, LatticeOperation.RangeRead, subject, key: null, rangeStart, rangeEnd);
        var decision = await gate.AuthorizeAsync(in request, cancellationToken);

        if (!decision.Allowed)
        {
            throw Denied(treeId, LatticeOperation.RangeRead, subject, decision.Reason);
        }

        // Partial-coverage allow: a per-shard count or a content digest cannot be
        // narrowed to an authorized key subset without leaking structure, so a
        // filtered allow is refused rather than narrowed (fail-closed).
        if (decision.KeyFilter is not null)
        {
            throw new LatticeAuthorizationDeniedException(
                treeId,
                LatticeOperation.RangeRead,
                subject.SubjectId,
                decision.Reason ?? "Read is not fully authorized over the requested range; "
                    + "a per-shard count or content digest cannot be narrowed per key and is refused.");
        }
    }

    /// <summary>
    /// Resolves the read-path key-filter for a <see cref="LatticeOperation.RangeRead"/>
    /// over the half-open range, fail-closed. Returns <c>null</c> when no per-key
    /// filtering is required (a plain allow, the default null gate, or a
    /// system-origin turn); a reject-all predicate when the caller is denied the
    /// range outright; or the gate's per-key filter for a partial (filtered)
    /// allow. Used by the snapshot cursor paths, which read snapshot leaf grains
    /// directly and therefore bypass the public filtered scan surface.
    /// </summary>
    public static async ValueTask<Func<string, bool>?> ResolveRangeReadFilterAsync(
        ILatticeAccessGate gate,
        ILatticeMembershipContext? membership,
        string treeId,
        string? rangeStart,
        string? rangeEnd,
        CancellationToken cancellationToken)
    {
        if (LatticeAccessGateContext.IsGateBypassed || gate is NullLatticeAccessGate)
        {
            return null;
        }

        var subject = await ResolveSubjectAsync(membership, cancellationToken);
        var request = new LatticeAccessRequest(
            treeId, LatticeOperation.RangeRead, subject, key: null, rangeStart, rangeEnd);
        var decision = await gate.AuthorizeAsync(in request, cancellationToken);
        if (!decision.Allowed)
        {
            return static _ => false;
        }

        return decision.KeyFilter;
    }

    private static async ValueTask<LatticeSubject> ResolveSubjectAsync(
        ILatticeMembershipContext? membership,
        CancellationToken cancellationToken)
    {
        // Resolve the caller identity under a system-origin scope so the
        // membership directory's own dogfooded reads bypass the gate and cannot
        // re-enter this enforcer (the OC-2 non-recursion guarantee).
        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            return await LatticeAccessGateSubjectResolver.ResolveAsync(membership, cancellationToken);
        }
    }

    private static void ThrowIfPointDenied(
        in LatticeAccessDecision decision,
        string treeId,
        LatticeOperation operation,
        in LatticeSubject subject,
        string key)
    {
        if (!decision.Allowed)
        {
            throw Denied(treeId, operation, subject, decision.Reason);
        }

        // A point request should resolve to a uniform allow/deny; if a custom
        // gate returns a key-filter, honour it for this one key, failing closed
        // when it excludes the key.
        if (decision.KeyFilter is not null && !decision.KeyFilter(key))
        {
            throw new LatticeAuthorizationDeniedException(
                treeId,
                operation,
                subject.SubjectId,
                decision.Reason ?? $"Key '{key}' is excluded by the access gate's per-key filter.");
        }
    }

    private static LatticeAuthorizationDeniedException Denied(
        string treeId,
        LatticeOperation operation,
        in LatticeSubject subject,
        string? reason) =>
        new(treeId, operation, subject.SubjectId, reason ?? "Denied by access gate.");
}
