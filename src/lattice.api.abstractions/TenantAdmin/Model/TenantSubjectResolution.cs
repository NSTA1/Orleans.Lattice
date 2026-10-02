namespace Orleans.Lattice.Api.TenantAdmin;

/// <summary>
/// The result of <see cref="ILatticeTenantDirectoryAdmin.ResolveSubjectAsync"/>:
/// whether a subject is an admin or a member of a tenant, and through which
/// admin-set and member-set entries. An entry matches when it names the subject
/// itself or one of the subject's resolved transitive groups.
/// </summary>
/// <remarks>
/// Admins are implicitly members, so <see cref="IsMember"/> is
/// <see langword="true"/> whenever <see cref="IsAdmin"/> is, even when
/// <see cref="MemberEntries"/> is empty. A subject matched by no entry is neither,
/// and may not act as the tenant.
/// </remarks>
[GenerateSerializer]
[Alias(ApiTenantAdminTypeAliases.TenantSubjectResolution)]
[Immutable]
public sealed record TenantSubjectResolution
{
    /// <summary>The tenant id the subject was resolved against.</summary>
    [Id(0)] public required string TenantId { get; init; }

    /// <summary>The subject id that was resolved.</summary>
    [Id(1)] public required string SubjectId { get; init; }

    /// <summary>The kind of principal <see cref="SubjectId"/> names.</summary>
    [Id(2)] public TenantSubjectKind SubjectKind { get; init; }

    /// <summary><see langword="true"/> when an admin-set entry matches the subject.</summary>
    [Id(3)] public bool IsAdmin { get; init; }

    /// <summary><see langword="true"/> when the subject may act as the tenant: it is an admin, or a member-set entry matches it.</summary>
    [Id(4)] public bool IsMember { get; init; }

    /// <summary>The admin-set entries that match the subject, in ordinal order.</summary>
    [Id(5)] public IReadOnlyList<TenantMemberEntry> AdminEntries { get; init; } = Array.Empty<TenantMemberEntry>();

    /// <summary>The member-set entries that match the subject, in ordinal order.</summary>
    [Id(6)] public IReadOnlyList<TenantMemberEntry> MemberEntries { get; init; } = Array.Empty<TenantMemberEntry>();
}
