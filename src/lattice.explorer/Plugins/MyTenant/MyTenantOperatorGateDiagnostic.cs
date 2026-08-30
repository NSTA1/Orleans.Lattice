using Orleans.Lattice.Explorer.Core.Tenancy;

namespace Orleans.Lattice.Explorer.Plugins.MyTenant;

/// <summary>
/// The plugin's registration-order diagnostic for the Explorer's
/// platform-operator gate.
/// <para>
/// <c>AddExplorerTenantView()</c> registers a fail-closed placeholder
/// <see cref="IExplorerTenantOperatorGate"/> with <c>TryAdd</c>, and an
/// administrative plugin registers the real one. <c>TryAdd</c> means the
/// <em>first</em> registration wins, so a head that calls
/// <c>AddExplorerTenantView()</c> before <c>AddExplorerAccess()</c> keeps the
/// placeholder: nobody ever validates as a platform operator, every tenant
/// switch silently changes nothing, and a cross-tenant request degrades to the
/// active tenant with no explanation.
/// </para>
/// <para>
/// Failing closed is right; failing closed <em>silently</em> is not. A head has
/// no way to tell a correctly-ordered deployment with no operators from a
/// misordered one, so this reports the difference and the surface says so.
/// </para>
/// </summary>
/// <remarks>
/// The placeholder is <see langword="internal"/> to the navigation core, so it
/// cannot be named here. It is identified by the only property that is stable
/// and true of it alone: it is the sole
/// <see cref="IExplorerTenantOperatorGate"/> the core itself ships, and every
/// real gate is supplied by a plugin package. Comparing the implementation's
/// assembly to the contract's therefore identifies the placeholder exactly,
/// without reaching past an access modifier.
/// </remarks>
public static class MyTenantOperatorGateDiagnostic
{
    /// <summary>
    /// The message describing a head that is still running on the fail-closed
    /// placeholder gate.
    /// </summary>
    public const string PlaceholderGateMessage =
        "No platform-operator gate is registered, so nobody validates as an operator and switching "
        + "tenant will change nothing. A head that supports operators must call AddExplorerAccess() "
        + "before AddExplorerTenantView(): the navigation core registers a fail-closed placeholder "
        + "gate with TryAdd, so the first registration wins.";

    /// <summary>
    /// Whether <paramref name="gate"/> is the navigation core's own fail-closed
    /// placeholder rather than a real head-supplied gate.
    /// </summary>
    /// <param name="gate">The resolved operator gate, or <see langword="null"/> when none is registered.</param>
    /// <returns>
    /// <see langword="true"/> when the gate is the core's placeholder.
    /// <see langword="false"/> for a real gate, and for
    /// <see langword="null"/> - a head that registered no gate at all never
    /// called <c>AddExplorerTenantView()</c>, which is the non-tenant posture and
    /// not a misordering.
    /// </returns>
    public static bool IsFailClosedPlaceholder(IExplorerTenantOperatorGate? gate) =>
        gate is not null && gate.GetType().Assembly == typeof(IExplorerTenantOperatorGate).Assembly;

    /// <summary>
    /// The diagnostic for <paramref name="gate"/>, or <see langword="null"/>
    /// when the head supplied a real one and there is nothing to report.
    /// </summary>
    /// <param name="gate">The resolved operator gate, or <see langword="null"/>.</param>
    /// <returns>The diagnostic message, or <see langword="null"/>.</returns>
    public static string? Describe(IExplorerTenantOperatorGate? gate) =>
        IsFailClosedPlaceholder(gate) ? PlaceholderGateMessage : null;
}
