namespace Orleans.Lattice.Api.TenantAdmin;

/// <summary>
/// The result of <see cref="ILatticeTenantPolicyAdmin.EffectivePermissionsAsync"/>:
/// the rules of both layers that name a subject directly or through one of its
/// groups and govern the tenant's trees, grants and denies alike.
/// </summary>
/// <remarks>
/// <para>
/// It does not collapse the rules into a per-key verdict - use
/// <see cref="ILatticeTenantPolicyAdmin.ExplainAsync"/> for a verdict on a concrete
/// operation. Each rule carries its <see cref="TenantRuleView.Layer"/>, so a caller
/// can see which grants a platform rule makes final.
/// </para>
/// <para>
/// A platform-wide rule or an app role rule that applies is reported by id and
/// effect only (see <see cref="TenantRuleView.SubjectWithheld"/>).
/// </para>
/// </remarks>
[GenerateSerializer]
[Alias(ApiTenantAdminTypeAliases.TenantEffectivePermissions)]
[Immutable]
public sealed record TenantEffectivePermissions
{
    /// <summary>The tenant id the permissions were resolved in.</summary>
    [Id(0)] public required string TenantId { get; init; }

    /// <summary>The subject id the permissions were resolved for.</summary>
    [Id(1)] public required string SubjectId { get; init; }

    /// <summary>The kind of principal <see cref="SubjectId"/> names.</summary>
    [Id(2)] public TenantSubjectKind SubjectKind { get; init; }

    /// <summary>The tenant-local tree name the listing was narrowed to, or <see langword="null"/> for every tree of the tenant.</summary>
    [Id(3)] public string? TreeName { get; init; }

    /// <summary>The rules naming the subject, ordered with the platform layer first, then by tree name and rule id.</summary>
    [Id(4)] public IReadOnlyList<TenantRuleView> Rules { get; init; } = Array.Empty<TenantRuleView>();
}
