namespace Orleans.Lattice.Api.TenantAdmin;

/// <summary>
/// The result of adding or removing a single tenant-admin subject: the tenant
/// and subject the call targeted, whether the call actually wrote to the
/// membership set, and the resulting live subject set. Both mutations are
/// idempotent, so <see cref="Changed"/> reports <see langword="false"/> when the
/// subject was already a member (on add) or already absent (on remove) and no
/// registry write was made.
/// </summary>
[GenerateSerializer]
[Alias(ApiTenantAdminTypeAliases.TenantAdminSubjectChangeResult)]
[Immutable]
public sealed record TenantAdminSubjectChangeResult
{
    /// <summary>The tenant id whose admin-subject set the call targeted.</summary>
    [Id(0)] public required string TenantId { get; init; }

    /// <summary>The subject id the call added or removed.</summary>
    [Id(1)] public required string SubjectId { get; init; }

    /// <summary><see langword="true"/> when the call wrote to the membership set; <see langword="false"/> for an idempotent no-op.</summary>
    [Id(2)] public required bool Changed { get; init; }

    /// <summary>
    /// The tenant's live admin-subject ids as committed, in ordinal order. This is
    /// the <em>converged</em> set the registry's CRDT merge produced, so it also
    /// reflects any concurrent membership write from another replica rather than
    /// only this call's own change.
    /// </summary>
    [Id(3)] public required IReadOnlyList<string> Subjects { get; init; }
}
