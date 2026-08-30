namespace Orleans.Lattice.Explorer.Plugins.Telemetry;

/// <summary>
/// The series-label names the panels read, and the sentinel the platform's own
/// trees are attributed to.
/// </summary>
/// <remarks>
/// <para>
/// <b>Bound to the emitting constants, not restated.</b> These forward to
/// <see cref="LatticeTenantLabel"/>, which is what the cluster actually tags a
/// measurement with, so a rename there breaks the build here instead of quietly
/// leaving a panel matching on a label nothing emits.
/// </para>
/// <para>
/// <b>These are read, never written.</b> The tenant label is derived
/// server-side from the authenticated caller and is never accepted from a
/// request, so a panel reading it is reading the facade's own answer. Nothing
/// here narrows a result by tenant: the tree filter narrows within whatever
/// scope the facade already served, and dropping it shows everything the facade
/// returned.
/// </para>
/// </remarks>
public static class TelemetryLabelNames
{
    /// <summary>The logical tree a series belongs to.</summary>
    public const string Tree = "tree";

    /// <summary>
    /// The tenant a series was attributed to, derived server-side. Always
    /// emitted, so there is one catalogue and one set of panels whether or not
    /// the tenancy add-on is installed.
    /// </summary>
    public const string Tenant = LatticeTenantLabel.TagTenant;

    /// <summary>
    /// The sentinel the platform's own internal trees carry in the
    /// <see cref="Tenant"/> label, so a panel can say "platform" rather than
    /// rendering a reserved id as though it were a tenant.
    /// </summary>
    public const string PlatformTenant = LatticeTenantLabel.PlatformTenant;

    /// <summary>
    /// The tenant a series carries on a deployment with no tenancy add-on, so
    /// the label is present and meaningless rather than absent.
    /// </summary>
    /// <remarks>
    /// Named for the deployment rather than for the word "default" on purpose:
    /// <c>TelemetryTenantNeutralityTests</c> forbids a member here whose name
    /// suggests the seam decides a tenant, and that guard is worth more than the
    /// tidier name. This is a value the cluster emits, read and never written.
    /// </remarks>
    public const string TenancyOffTenant = LatticeTenantLabel.DefaultTenant;

    /// <summary>
    /// The display text for <paramref name="tenant"/>: the platform sentinel
    /// and the tenancy-off value both read as something a person recognises
    /// rather than as a reserved id.
    /// </summary>
    /// <param name="tenant">The tenant label value a series carried.</param>
    /// <returns>The text to render.</returns>
    public static string DisplayTenant(string? tenant) => tenant switch
    {
        null or "" => "unattributed",
        PlatformTenant => "platform",
        TenancyOffTenant => "default",
        _ => tenant,
    };
}
