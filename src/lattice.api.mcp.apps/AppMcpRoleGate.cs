using Orleans.Lattice.Apps;

namespace Orleans.Lattice.Api.Mcp.Apps;

/// <summary>
/// The app tool surface's per-tool gate: whether the caller holds the role a tool declares. It is the single
/// decision point both the advertisement path and the invocation path consult, so the two cannot drift apart,
/// and it delegates the decision itself to the shared <see cref="AppRoleGate"/> compiled by
/// <see cref="AppRoleGrantEvaluator"/> - the same definition the app workspace reports and the app bridge
/// enforces. A caller holds a role by binding (membership of a group bound to it), never by rights it holds
/// outside the app's own rules.
/// </summary>
internal static class AppMcpRoleGate
{
    /// <summary>Evaluates whether <paramref name="subject"/> holds <paramref name="role"/>.</summary>
    /// <param name="role">The tool's declared role, compiled for the install.</param>
    /// <param name="subject">The resolved caller.</param>
    /// <returns><c>true</c> when the caller is a member of a group bound to the role.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="role"/> is null.</exception>
    public static bool IsHeld(AppRoleGate role, LatticeSubject subject)
    {
        ArgumentNullException.ThrowIfNull(role);
        return role.IsHeldBy(subject);
    }
}