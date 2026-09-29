namespace Orleans.Lattice.Explorer.Shell.Areas.Tenancy;

/// <summary>
/// What the caller may do in the Tenancy area, as proven for this circuit's
/// identity: whether it holds platform-operator standing (the directory and
/// every tenant's administration), and whether it administers its own tenant.
/// </summary>
/// <remarks>
/// The standing only decides what the area offers. The cluster authorizes every
/// call again, so a stale standing can only show a control that is then refused,
/// never grant anything.
/// </remarks>
/// <param name="IsOperator">Whether the caller validates as a platform operator.</param>
/// <param name="CurrentTenant">The tenant the caller's credential resolves to.</param>
/// <param name="ActiveTenant">The tenant the Explorer is scoped to, or <see langword="null"/> when none is established.</param>
/// <param name="IsTenantAdmin">Whether the caller holds admin authority over <see cref="Workspace"/>.</param>
internal sealed record TenancyStanding(bool IsOperator, string CurrentTenant, string? ActiveTenant, bool IsTenantAdmin)
{
    /// <summary>The tenant "my tenant" describes: the active tenant, or the credential's own.</summary>
    public string Workspace => ActiveTenant ?? CurrentTenant;

    /// <summary>Whether the caller may use the area at all.</summary>
    public bool MaySee => IsOperator || IsTenantAdmin;
}
