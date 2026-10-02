namespace Orleans.Lattice.Explorer.UI.Areas.Tenancy;

/// <summary>One entry of a tenant's admin set, with what it names.</summary>
/// <param name="SubjectId">The entry as recorded: a user id, a cluster group id, or a full tenant group id.</param>
/// <param name="Kind">What the entry names.</param>
internal sealed record TenancyAdminEntry(string SubjectId, TenancyAdminEntryKind Kind)
{
    /// <summary>The entry's kind as shown.</summary>
    public string KindLabel => Label(Kind);

    /// <summary>Whether the entry names a group, which counts as one admin entry however many members it has.</summary>
    public bool IsGroup => Kind is TenancyAdminEntryKind.ClusterGroup or TenancyAdminEntryKind.TenantGroup;

    /// <summary>The label of <paramref name="kind"/>.</summary>
    /// <param name="kind">The kind.</param>
    /// <returns>The label.</returns>
    public static string Label(TenancyAdminEntryKind kind) => kind switch
    {
        TenancyAdminEntryKind.User => "User",
        TenancyAdminEntryKind.ClusterGroup => "Cluster group",
        TenancyAdminEntryKind.TenantGroup => "Tenant group",
        TenancyAdminEntryKind.OtherTenantGroup => "Another tenant's group, not counted",
        _ => "User or group",
    };
}
