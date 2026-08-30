namespace Orleans.Lattice.Explorer.Plugins.Tenancy;

/// <summary>
/// The outcome of granting or revoking one subject's tenant-admin authority,
/// carrying the resulting admin-subject set so a panel refreshes from the write
/// rather than re-reading.
/// </summary>
public sealed record ExplorerTenantAdminChange
{
    /// <summary>The tenant whose admin subjects were edited.</summary>
    public required string TenantId { get; init; }

    /// <summary>The subject the call named.</summary>
    public required string SubjectId { get; init; }

    /// <summary>
    /// <see langword="true"/> when the call changed the set. The operations are
    /// idempotent, so granting an existing member or revoking a non-member
    /// reports <see langword="false"/> rather than failing.
    /// </summary>
    public required bool Changed { get; init; }

    /// <summary>
    /// The resulting live admin-subject ids, in ordinal order. Never
    /// <see langword="null"/>.
    /// </summary>
    public required IReadOnlyList<string> Subjects { get; init; }
}
