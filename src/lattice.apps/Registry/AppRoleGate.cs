using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Apps;

/// <summary>
/// One manifest role compiled for one install: exactly what the app-owned <c>app:{slug}:</c> rules the role
/// compiler writes for the role confer - the role's operations intersected with the install's consented
/// ceiling, its scopes resolved to effective (tenant-composed) tree ids, and the membership groups the install
/// binds to it. It is the single definition of "holds an app role": the app workspace, the app MCP tool surface
/// and the app bridge all derive from it, so they cannot disagree about who holds a role.
/// </summary>
/// <remarks>
/// <para>
/// <b>Held by binding, not by capability.</b> A caller holds the role when its transitive group closure
/// contains a group the install binds to the role, and the role's compiled rules confer something (at least one
/// operation and at least one scope). Rights the caller holds through any other rule never make it hold an app
/// role, so a caller with broad operator rights of its own is not shown controls the bridge would then deny.
/// </para>
/// <para>
/// <b>Fail closed.</b> The anonymous subject, a subject with no id, and a subject with no group closure hold no
/// role. A binding that is null or carries no group id binds nobody. Because the gate is compiled from one
/// registry record revision, re-binding a role produces a new revision and therefore a new gate, so the change
/// is seen on the next evaluation.
/// </para>
/// <para>
/// <b>A deny still wins.</b> <see cref="IsHeld"/> is the binding alone. <see cref="IsHeldAsync"/> is the rule every
/// surface reports: the binding grants the role and the access gate can then only take it away, when it refuses
/// the role's operations, so an explicit deny rule on a bound member is honoured; it can never add a role. The app
/// workspace and the app MCP tool gate use it. The app bridge gets the same deny semantics from the data path
/// itself, which authorizes every call under the caller's own identity.
/// </para>
/// </remarks>
internal sealed class AppRoleGate
{
    /// <summary>Initializes a new <see cref="AppRoleGate"/>.</summary>
    /// <param name="operations">The operations the role's compiled rules confer.</param>
    /// <param name="scopes">The role's scopes, resolved to effective tree ids.</param>
    /// <param name="groupIds">The membership groups the install binds to the role.</param>
    /// <exception cref="ArgumentNullException"><paramref name="scopes"/> or <paramref name="groupIds"/> is null.</exception>
    public AppRoleGate(LatticeOperation operations, LatticeScope[] scopes, string[] groupIds)
    {
        ArgumentNullException.ThrowIfNull(scopes);
        ArgumentNullException.ThrowIfNull(groupIds);
        Operations = operations;
        Scopes = scopes;
        GroupIds = groupIds;
    }

    /// <summary>
    /// The operations the role's compiled rules confer: the role's declared operations intersected with the
    /// install's consented ceiling and the operations a role may ever carry.
    /// </summary>
    public LatticeOperation Operations { get; }

    /// <summary>The role's scopes, resolved to effective (tenant-composed) tree ids.</summary>
    public LatticeScope[] Scopes { get; }

    /// <summary>The distinct membership groups the install binds to the role, in binding order.</summary>
    public string[] GroupIds { get; }

    /// <summary>Whether the role's compiled rules confer anything: an operation, a scope and a bound group.</summary>
    public bool ConfersGrant => Operations != LatticeOperation.None && Scopes.Length > 0 && GroupIds.Length > 0;

    /// <summary>Evaluates whether <paramref name="subject"/> holds the role by binding.</summary>
    /// <param name="subject">The resolved caller.</param>
    /// <returns><c>true</c> when the caller is a member of a group bound to a role that confers a grant.</returns>
    public bool IsHeld(in LatticeSubject subject)
    {
        if (!ConfersGrant
            || string.IsNullOrEmpty(subject.SubjectId)
            || subject.IsAnonymous
            || subject.GroupIds is not { Count: > 0 } groups)
        {
            return false;
        }

        foreach (var groupId in GroupIds)
        {
            if (IsMember(groups, groupId))
                return true;
        }

        return false;
    }

    /// <summary>
    /// Evaluates whether <paramref name="subject"/> holds the role by binding and the access gate does not refuse
    /// it. The binding decides who <em>can</em> hold the role; the gate can only take it away - an explicit deny
    /// rule on the caller (on one of the role's trees, or cluster-wide) wins over the compiled app rule exactly as
    /// it does on the data path. The gate is asked only once the binding holds, so a caller's own rights can
    /// never add a role.
    /// </summary>
    /// <param name="gate">The shared access gate.</param>
    /// <param name="subject">The resolved caller.</param>
    /// <param name="cancellationToken">Cancels the evaluation.</param>
    /// <returns>
    /// <c>true</c> when the caller is bound to the role and, on at least one of its scopes, the gate refuses none
    /// of its operations.
    /// </returns>
    /// <exception cref="ArgumentNullException"><paramref name="gate"/> is null.</exception>
    /// <remarks>
    /// Each operation bit is asked in the scope's own shape: with its key for a key scope, and as a whole-tree
    /// request otherwise. A key-filtered answer is resolved at a representative key of the scope - the key itself,
    /// the prefix itself, or the empty key for a whole tree - because the question is only whether the gate
    /// refuses the role there. That probe cannot widen anything (#3863 concerned a filter <em>granting</em> a
    /// prefix): it is reached only after the binding holds, and it can only turn the answer to <c>false</c>. A
    /// deny narrower than the scope (one key under a tree scope, say) leaves the role held and is enforced by the
    /// data path itself.
    /// </remarks>
    public async ValueTask<bool> IsHeldAsync(ILatticeAccessGate gate, LatticeSubject subject, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(gate);
        if (!IsHeld(subject))
            return false;

        foreach (var scope in Scopes)
        {
            if (!await IsRefusedAsync(gate, subject, scope, cancellationToken).ConfigureAwait(false))
                return true;
        }

        return false;
    }

    private async ValueTask<bool> IsRefusedAsync(
        ILatticeAccessGate gate,
        LatticeSubject subject,
        LatticeScope scope,
        CancellationToken cancellationToken)
    {
        var key = scope.Kind == LatticeScopeKind.Key ? scope.KeyOrPrefix : null;
        var probe = scope.KeyOrPrefix ?? string.Empty;
        var remaining = (int)Operations;
        while (remaining != 0)
        {
            var bit = remaining & -remaining;
            remaining &= remaining - 1;

            var request = new LatticeAccessRequest(scope.TreeId, (LatticeOperation)bit, subject, key);
            var decision = await gate.AuthorizeAsync(in request, cancellationToken).ConfigureAwait(false);
            if (!decision.Allowed || (decision.KeyFilter is { } filter && !filter(probe)))
                return true;
        }

        return false;
    }

    /// <summary>
    /// Whether the transitive group closure <paramref name="groups"/> contains <paramref name="groupId"/>. A set
    /// answers with its own lookup; any other collection is compared ordinally.
    /// </summary>
    /// <param name="groups">The caller's transitive group closure.</param>
    /// <param name="groupId">The bound group.</param>
    /// <returns><c>true</c> when the closure contains the group.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="groups"/> is null.</exception>
    public static bool IsMember(IReadOnlyCollection<string> groups, string groupId)
    {
        ArgumentNullException.ThrowIfNull(groups);
        if (groups is IReadOnlySet<string> set)
            return set.Contains(groupId);
        if (groups is string[] array)
            return Array.IndexOf(array, groupId) >= 0;

        foreach (var group in groups)
        {
            if (string.Equals(group, groupId, StringComparison.Ordinal))
                return true;
        }

        return false;
    }
}
