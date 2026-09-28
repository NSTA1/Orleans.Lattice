using Orleans.Lattice.Apps;

namespace Orleans.Lattice.Api.Mcp.Apps;

/// <summary>
/// The app tool surface's per-tool gate: whether the caller holds the role a tool declares. It is the single
/// decision point both the advertisement path and the invocation path consult, so the two cannot drift apart,
/// and it delegates the evaluation itself to the shared <see cref="AppRoleGate"/> of
/// <see cref="AppRoleGrantEvaluator"/>, the same evaluation the app workspace gates on.
/// </summary>
internal static class AppMcpRoleGate
{
    /// <summary>Evaluates whether <paramref name="subject"/> holds <paramref name="role"/>.</summary>
    /// <param name="role">The tool's declared role, compiled for the install's tenant.</param>
    /// <param name="gate">The shared access gate.</param>
    /// <param name="subject">The resolved caller.</param>
    /// <param name="cancellationToken">Cancels the evaluation.</param>
    /// <returns><c>true</c> when the caller holds the role on at least one of its scopes.</returns>
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
