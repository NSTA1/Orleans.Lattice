namespace Orleans.Lattice.Api.State;

/// <summary>
/// Paging request for <see cref="ILatticeStateQuery.ScanTagMembersAsync"/>: the
/// members of a single tag across every subject tree a tag index covers.
/// </summary>
/// <remarks>
/// Members are enumerated in a deterministic, stable order (ascending by
/// <c>(tree id, key)</c>). <see cref="PageToken"/> is the exclusive cursor: pass
/// the <c>NextPageToken</c> returned by the previous page to fetch the next one.
/// A request with a <see langword="null"/> token starts from the beginning.
/// </remarks>
[GenerateSerializer]
[Alias(ApiStateTypeAliases.TagMemberScanRequest)]
[Immutable]
public sealed record TagMemberScanRequest
{
    /// <summary>Default page size used when <see cref="PageSize"/> is unset.</summary>
    public const int DefaultPageSize = 100;

    /// <summary>Largest page size honoured; larger values are clamped down.</summary>
    public const int MaxPageSize = 1000;

    /// <summary>The logical tag-index name whose members are scanned.</summary>
    [Id(0)] public required string IndexName { get; init; }

    /// <summary>The tag whose member keys are returned.</summary>
    [Id(1)] public required string Tag { get; init; }

    /// <summary>
    /// Maximum number of members to return in a single page. Values below
    /// <c>1</c> fall back to <see cref="DefaultPageSize"/>; values above
    /// <see cref="MaxPageSize"/> are clamped to it.
    /// </summary>
    [Id(2)] public int PageSize { get; init; } = DefaultPageSize;

    /// <summary>
    /// Exclusive continuation cursor: the opaque <c>(tree id, key)</c> token of
    /// the last member on the previous page. <see langword="null"/> (the
    /// default) starts from the beginning.
    /// </summary>
    [Id(3)] public string? PageToken { get; init; }

    /// <summary>The effective, clamped page size derived from <see cref="PageSize"/>.</summary>
    public int EffectivePageSize => PageSize switch
    {
        < 1 => DefaultPageSize,
        > MaxPageSize => MaxPageSize,
        _ => PageSize,
    };
}
