using Orleans.Lattice.Apps;

namespace Orleans.Lattice.Api.Mcp.Apps;

/// <summary>
/// The app tool surface's per-tool gate: whether the caller holds the role a tool declares. It is the single
/// decision point both the advertisement path and the invocation path consult, so the two cannot drift apart,
/// and it delegates the evaluation itself to the shared <see cref="AppRoleGate"/> of
/// <see cref="AppRoleGrantEvaluator"/>. A role is held by binding - the same definition the app workspace
/// reports and the app bridge's grants are built from - and the shared access gate can then only take it away:
/// an explicit deny rule on a bound member withholds the tool, because an app tool runs app code the data path
/// may never see.
/// </summary>
internal static class AppMcpRoleGate
{
    /// <summary>Evaluates whether <paramref name="subject"/> holds <paramref name="role"/>.</summary>
    /// <param name="role">The tool's declared role, compiled for the install.</param>
    /// <param name="gate">The shared access gate, which can only refuse a role the binding confers.</param>
    /// <param name="subject">The resolved caller.</param>
    /// <param name="cancellationToken">Cancels the evaluation.</param>
    /// <returns>
    /// <c>true</c> when the caller is a member of a group the install binds to the role and the gate refuses
    /// none of the role's operations on at least one of its scopes.
    /// </returns>
    /// <exception cref="ArgumentNullException"><paramref name="role"/> or <paramref name="gate"/> is null.</exception>
    public static ValueTask<bool> IsHeldAsync(
        AppRoleGate role,
        ILatticeAccessGate gate,
        LatticeSubject subject,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(role);
        ArgumentNullException.ThrowIfNull(gate);
        return role.IsHeldAsync(gate, subject, cancellationToken);
    }
}
