using Orleans.Lattice.Apps;

namespace Orleans.Lattice.Api.Mcp.Apps;

/// <summary>
/// The app tool surface's per-tool gate: whether the caller holds the role a tool declares. It is the single
/// decision point both the advertisement path and the invocation path consult, so the two cannot drift apart,
/// and it delegates the evaluation itself to the shared <see cref="AppRoleGate"/> of
/// <see cref="AppRoleGrantEvaluator"/> - the same definition the app workspace reports and the app bridge's
/// grants are built from. A role is held by binding, never by the caller's other rights.
/// </summary>
internal static class AppMcpRoleGate
{
    /// <summary>Evaluates whether <paramref name="subject"/> holds <paramref name="role"/>.</summary>
    /// <param name="role">The tool's declared role, compiled for the install.</param>
    /// <param name="subject">The resolved caller.</param>
    /// <returns>
    /// <c>true</c> when the caller is a member of a group the install binds to the role. The evaluation reads
    /// only the compiled binding and the resolved group closure, so the task always completes synchronously
    /// and allocates nothing.
    /// </returns>
    /// <exception cref="ArgumentNullException"><paramref name="role"/> is null.</exception>
    public static ValueTask<bool> IsHeldAsync(AppRoleGate role, in LatticeSubject subject)
    {
        ArgumentNullException.ThrowIfNull(role);
        return new ValueTask<bool>(role.IsHeld(subject));
    }
}
