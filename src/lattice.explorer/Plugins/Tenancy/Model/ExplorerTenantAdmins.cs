namespace Orleans.Lattice.Explorer.Plugins.Tenancy;

/// <summary>
/// The subjects that hold tenant-admin authority over one tenant.
/// </summary>
public sealed record ExplorerTenantAdmins
{
    /// <summary>The tenant the subjects administer.</summary>
    public required string TenantId { get; init; }

    /// <summary>
    /// The live admin-subject ids, in ordinal order. Never
    /// <see langword="null"/>. A tenant always retains at least one, because
    /// removing the last is refused server-side.
    /// </summary>
    public required IReadOnlyList<string> Subjects { get; init; }
}
