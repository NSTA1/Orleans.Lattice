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
