namespace Orleans.Lattice;

/// <summary>
/// Public seam that lets a co-hosted, trusted infrastructure extension - such as
/// one of the bundled API bindings or the Model Context Protocol server - run a
/// scoped <em>system-origin</em> operation that the access gate admits without a
/// user identity, and detect whether the current turn is already inside such a
/// scope.
/// </summary>
/// <remarks>
/// <para>
/// A system-origin scope positively marks the current turn as an
/// infrastructure-authored call (for example a trusted, read-only permission
/// introspection performed on a caller's behalf) so the access-gate enforcement
/// point skips authorization for it and never self-blocks. The marker flows
/// across grain calls with the ambient request context, so a single scope at an
/// infrastructure path's entry marks every nested call it fans out.
/// </para>
/// <para>
/// This is a deliberately narrow, trusted seam: entering a scope bypasses the
/// access gate for its lifetime, so only co-hosted infrastructure that already
/// runs inside the silo's trust boundary should use it. It exists so such an
/// extension does not need to reach into the library's internals to perform the
/// bypass. The richer internal ambient signals (for example materialised-view
/// maintenance scopes) remain internal.
/// </para>
/// </remarks>
public static class LatticeSystemOrigin
{
    /// <summary>
    /// Gets a value indicating whether a system-origin scope is active on the
    /// ambient request context. The default outside any scope is <c>false</c>
    /// (a user-origin call).
    /// </summary>
    public static bool IsActive => LatticeAccessGateContext.IsSystemOrigin;

    /// <summary>
    /// Marks the ambient context as a system-origin scope for the lifetime of
    /// the returned scope, restoring the prior value on
    /// <see cref="System.IDisposable.Dispose"/>. Safe to nest; disposal is
    /// idempotent.
    /// </summary>
    /// <returns>A scope that clears the marker when disposed.</returns>
    public static System.IDisposable Enter() => LatticeAccessGateContext.EnterSystemOrigin();
}
