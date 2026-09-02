using Orleans.Lattice.Explorer.Core.Vocabulary;

namespace Orleans.Lattice.Explorer.Core.Tenancy;

/// <summary>
/// One thing the shell owes the caller about their tenant scope: that a switch
/// took effect, that it was refused, that the tenant asked for cannot be reached,
/// or that a remembered tenant had to be abandoned.
/// </summary>
/// <remarks>
/// <para>
/// This type exists because the control seam's fail-closed
/// <see cref="bool"/> results used to be discarded, so a refusal and a success
/// looked identical - nothing happened and nothing was said. Every outcome now
/// becomes one of these and is announced, so a denial is explained rather than
/// merely enacted.
/// </para>
/// <para>
/// Composed on a user action or an abandoned restore, never per render, so the
/// string work happens once per outcome.
/// </para>
/// </remarks>
/// <param name="Kind">Which outcome this describes.</param>
/// <param name="Message">The sentence to show and announce. Never empty.</param>
public sealed record ExplorerTenantScopeNotice(ExplorerTenantNoticeKind Kind, string Message)
{
    /// <summary>
    /// Whether this reports something the caller could not do, rather than
    /// something that happened. Assertive announcement and the denial help tone;
    /// <see cref="ExplorerTenantNoticeKind.Applied"/> and
    /// <see cref="ExplorerTenantNoticeKind.RestoreAbandoned"/> are polite,
    /// because in both cases the scope is now valid and merely needs stating.
    /// </summary>
    public bool IsDenial =>
        Kind is ExplorerTenantNoticeKind.Refused or ExplorerTenantNoticeKind.Unknown;

    /// <summary>
    /// The scope is now <paramref name="tenant"/>, stated in the shell's settled
    /// active-tenant wording.
    /// </summary>
    /// <param name="tenant">The tenant now in effect.</param>
    /// <returns>The composed notice.</returns>
    public static ExplorerTenantScopeNotice Applied(ExplorerTenantId tenant) =>
        new(ExplorerTenantNoticeKind.Applied, ExplorerVocabulary.FormatActiveTenant(tenant.Value));

    /// <summary>The all-tenant view is now on or off, stated in the settled wording.</summary>
    /// <param name="allTenants">Whether the view now spans every reachable tenant.</param>
    /// <returns>The composed notice.</returns>
    public static ExplorerTenantScopeNotice VisibilityApplied(bool allTenants) =>
        allTenants ? AllTenantsOn : AllTenantsOff;

    /// <summary>
    /// The change was refused fail-closed. Carries the shared access copy for the
    /// tenant scope control, so the refusal states its remedy rather than only
    /// its refusal.
    /// </summary>
    /// <returns>The composed notice.</returns>
    public static ExplorerTenantScopeNotice Refused() => RefusedNotice;

    /// <summary>
    /// <paramref name="tenant"/> is not among the tenants this caller can reach,
    /// so the scope was left alone.
    /// </summary>
    /// <param name="tenant">The tenant that could not be reached.</param>
    /// <returns>The composed notice.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="tenant"/> is <see langword="null"/>.</exception>
    public static ExplorerTenantScopeNotice Unknown(string tenant)
    {
        ArgumentNullException.ThrowIfNull(tenant);

        // The shared denial copy, named for the specific tenant: from the
        // caller's side a revoked grant, a suspended tenant and a deleted one are
        // the same fact - that tenant cannot be read as - and carry the same
        // remedy.
        var denied = ExplorerAccessCopy.Denied(
            ExplorerGlossary.Get(ExplorerTermIds.Tenant).Label + " '" + tenant + "'");

        return new ExplorerTenantScopeNotice(
            ExplorerTenantNoticeKind.Unknown,
            ExplorerAccessCopy.Describe(denied));
    }

    /// <summary>
    /// A remembered tenant no longer resolves, so the scope fell back to
    /// <paramref name="fallback"/>. <paramref name="explanation"/> is the
    /// preference contract's own sentence for the abandoned value, so the shell
    /// explains a forgotten preference the same way everywhere.
    /// </summary>
    /// <param name="explanation">The preference contract's explanation. Must not be <see langword="null"/> or empty.</param>
    /// <param name="fallback">The tenant now in effect instead.</param>
    /// <returns>The composed notice.</returns>
    /// <exception cref="ArgumentException"><paramref name="explanation"/> is <see langword="null"/> or empty.</exception>
    public static ExplorerTenantScopeNotice RestoreAbandoned(string explanation, ExplorerTenantId fallback)
    {
        ArgumentException.ThrowIfNullOrEmpty(explanation);
        return new ExplorerTenantScopeNotice(
            ExplorerTenantNoticeKind.RestoreAbandoned,
            explanation + " " + ExplorerVocabulary.FormatActiveTenant(fallback.Value) + ".");
    }

    // Pre-composed: the outcomes whose wording does not depend on a tenant id are
    // the same string every time, so a refusal costs no allocation.
    private static readonly ExplorerTenantScopeNotice RefusedNotice = new(
        ExplorerTenantNoticeKind.Refused,
        ExplorerAccessCopy.Describe(
            ExplorerAccessCopy.Denied(ExplorerVocabulary.TenantAdministrationArea)));

    private static readonly ExplorerTenantScopeNotice AllTenantsOn = new(
        ExplorerTenantNoticeKind.Applied,
        ExplorerVocabulary.AllTenantsLabel + ": on.");

    private static readonly ExplorerTenantScopeNotice AllTenantsOff = new(
        ExplorerTenantNoticeKind.Applied,
        ExplorerVocabulary.AllTenantsLabel + ": off.");
}
