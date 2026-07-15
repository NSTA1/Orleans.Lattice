namespace Orleans.Lattice.Api.State;

/// <summary>
/// Paging request for the per-tree dead-letter-queue listing
/// (<see cref="ILatticeStateQuery.ListDeadLettersAsync"/>). The queue is
/// enumerated in append (time) order; <see cref="PageToken"/> is the opaque
/// cursor returned by the previous page, and a <see langword="null"/> token
/// starts from the oldest entry.
/// </summary>
[GenerateSerializer]
[Alias(ApiStateTypeAliases.DeadLetterQueueRequest)]
[Immutable]
public sealed record DeadLetterQueueRequest
{
    /// <summary>Default page size used when <see cref="PageSize"/> is unset.</summary>
    public const int DefaultPageSize = 100;

    /// <summary>Largest page size honoured; larger values are clamped down.</summary>
    public const int MaxPageSize = 1000;

    /// <summary>The governed tree whose dead-letter queue is listed.</summary>
    [Id(0)] public required string TreeId { get; init; }

    /// <summary>
    /// Maximum number of entries to return in a single page. Values below
    /// <c>1</c> fall back to <see cref="DefaultPageSize"/>; values above
    /// <see cref="MaxPageSize"/> are clamped to it.
    /// </summary>
    [Id(1)] public int PageSize { get; init; } = DefaultPageSize;

    /// <summary>
    /// Opaque continuation cursor returned by the previous page.
    /// <see langword="null"/> (the default) starts from the oldest entry.
    /// </summary>
    [Id(2)] public string? PageToken { get; init; }

    /// <summary>The effective, clamped page size derived from <see cref="PageSize"/>.</summary>
    public int EffectivePageSize => PageSize switch
    {
        < 1 => DefaultPageSize,
        > MaxPageSize => MaxPageSize,
        _ => PageSize,
    };
}
