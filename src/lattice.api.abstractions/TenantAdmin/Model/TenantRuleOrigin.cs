namespace Orleans.Lattice.Api.TenantAdmin;

/// <summary>
/// Where a rule shown to a tenant administrator comes from, which decides how
/// much of it the tenant may see. It refines <see cref="TenantRuleLayer"/>: every
/// origin other than <see cref="Tenant"/> is in the
/// <see cref="TenantRuleLayer.Platform"/> layer.
/// </summary>
/// <remarks>
/// <para>
/// A <see cref="PlatformWide"/> rule (a cluster-wide <c>Tree:*</c> rule) and an
/// <see cref="AppRole"/> rule (compiled from an installed app's role bindings)
/// are never listed to a tenant. When one of them decides an explanation, or
/// applies to a subject's effective permissions, it is reported by rule id and
/// effect only: its subject, scope, and operations are withheld (see
/// <see cref="TenantRuleView.SubjectWithheld"/>).
/// </para>
/// <para>
/// The zero value is <see cref="PlatformTree"/>, a read-only origin, so an origin
/// that was never set never reads as the tenant's own editable rule.
/// </para>
/// </remarks>
[GenerateSerializer]
[Alias(ApiTenantAdminTypeAliases.TenantRuleOrigin)]
public enum TenantRuleOrigin
{
    /// <summary>An operator rule scoped to one of the tenant's own trees. Listed to the tenant, read-only.</summary>
    PlatformTree = 0,

    /// <summary>A cluster-wide operator rule (<c>Tree:*</c>). Never listed; reported by id and effect only.</summary>
    PlatformWide = 1,

    /// <summary>A rule compiled from an installed app's role bindings. Never listed; reported by id and effect only.</summary>
    AppRole = 2,

    /// <summary>A tenant-tier rule authored by the tenant's own administrators. Listed and editable.</summary>
    Tenant = 3,
}
