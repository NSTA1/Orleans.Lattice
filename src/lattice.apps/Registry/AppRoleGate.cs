using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Apps;

/// <summary>
/// One manifest role compiled for one tenant install, exactly as the role compiler writes it into the
/// app-owned <c>app:{slug}:</c> rules: the operations those rules confer, the role's scopes resolved to
/// effective (tenant-composed) tree ids, and the membership groups the role is bound to. It is the single
/// definition of "holds an app role": the app workspace, the app MCP tool gate and the app bridge all derive
/// from it (see <see cref="AppRoleGrantEvaluator"/>), so no two of them can disagree about who holds a role.
/// </summary>
/// <remarks>
/// <para>
/// <b>Held by binding, not by capability.</b> A caller holds the role if and only if the app-owned rules
/// compiled for the role's bindings grant it: the caller is a member of a group bound to the role. Rights the
/// caller holds under any other rule - a cluster-wide allow, a grant on the app's trees, a key-filtered allow
/// that happens to spell a prefix (#3863) - never make the caller hold an app role, and the access gate is
/// not consulted. What the caller may then actually do is still enforced on the data path under the
/// caller's own identity.
/// </para>
/// <para>
/// <b>Fail closed.</b> A role confers nothing, and so is never held, when its operations intersected with the
/// install's consented ceiling are empty, when it has no scope, or when it has no readable binding (a null
/// binding or one with no group id is ignored). A caller that is anonymous, or whose membership resolved no
/// group, holds no role.
/// </para>
/// </remarks>
internal sealed class AppRoleGate
{
    /// <summary>Initializes a new <see cref="AppRoleGate"/>.</summary>
    /// <param name="operations">The operations the app-owned rules confer: the role's, within the ceiling.</param>
    /// <param name="scopes">The role's scopes, resolved to effective tree ids.</param>
    /// <param name="groupIds">The distinct membership groups the role is bound to.</param>
    /// <exception cref="ArgumentNullException"><paramref name="scopes"/> or <paramref name="groupIds"/> is null.</exception>
    public AppRoleGate(LatticeOperation operations, LatticeScope[] scopes, string[] groupIds)
    {
        ArgumentNullException.ThrowIfNull(scopes);
        ArgumentNullException.ThrowIfNull(groupIds);
        Operations = operations;
        Scopes = scopes;
        GroupIds = groupIds;
    }

    /// <summary>The operations the app-owned rules confer: the role's operations within the consented ceiling.</summary>
    public LatticeOperation Operations { get; }

    /// <summary>The role's scopes, resolved to effective (tenant-composed) tree ids.</summary>
    public LatticeScope[] Scopes { get; }

    /// <summary>The distinct membership groups the role is bound to; empty when the role is unbound.</summary>
    public string[] GroupIds { get; }

    /// <summary>Whether the role confers anything: it has operations, a scope and a bound group.</summary>
    public bool ConfersAnything => Operations != LatticeOperation.None && Scopes.Length != 0 && GroupIds.Length != 0;

    /// <summary>Evaluates whether <paramref name="subject"/> holds the role.</summary>
    /// <param name="subject">The resolved caller.</param>
    /// <returns><c>true</c> when the caller is a member of a group the role is bound to and the role confers anything.</returns>
    public bool IsHeldBy(LatticeSubject subject)
    {
        if (!ConfersAnything
            || string.IsNullOrEmpty(subject.SubjectId)
            || subject.IsAnonymous
            || subject.GroupIds is not { Count: > 0 } groups)
        {
            return false;
        }

        foreach (var groupId in GroupIds)
        {
            if (IsMember(groups, groupId))
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>Whether <paramref name="groupId"/> is in the caller's group closure <paramref name="groups"/>.</summary>
    /// <param name="groups">The caller's transitive group closure, or null.</param>
    /// <param name="groupId">The group to look for.</param>
    /// <returns><c>true</c> when the closure contains the group (ordinal).</returns>
    public static bool IsMember(IReadOnlyCollection<string>? groups, string groupId)
    {
        if (groups is null || groups.Count == 0 || string.IsNullOrEmpty(groupId))
        {
            return false;
        }

        if (groups is IReadOnlySet<string> set)
        {
            return set.Contains(groupId);
        }

        foreach (var group in groups)
        {
            if (string.Equals(group, groupId, StringComparison.Ordinal))
            {
                return true;
            }
        }

        return false;
    }
}
