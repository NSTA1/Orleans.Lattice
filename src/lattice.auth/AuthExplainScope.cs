namespace Orleans.Lattice.Auth;

/// <summary>
/// Marks the current asynchronous flow as an administrator's explain: a gate
/// evaluation in another subject's name that is not that subject's access attempt.
/// While it is marked, <see cref="LatticeAuthDecisionObserver"/> neither counts the
/// decision nor audits it, so an explain cannot write a decision into the audit
/// trail that names a subject who never made the request.
/// <para>
/// The mark lives in an <see cref="AsyncLocal{T}"/>, which does not cross a grain
/// call, so it covers only the in-process gate evaluation. Call <see cref="Enter"/>
/// only from inside an <see langword="async"/> method: the method's own execution
/// context carries the mark, and it is discarded when the method returns, so it
/// never leaks into the caller's flow.
/// </para>
/// </summary>
internal static class AuthExplainScope
{
    private static readonly AsyncLocal<bool> Active = new();

    /// <summary>Whether the current flow is an explain evaluation.</summary>
    internal static bool IsActive => Active.Value;

    /// <summary>Marks the current flow as an explain evaluation.</summary>
    internal static void Enter() => Active.Value = true;
}
