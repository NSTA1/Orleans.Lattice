namespace Orleans.Lattice.Explorer.Access;

/// <summary>
/// A read result for an Access-area listing (users, groups, direct or transitive
/// members, or rules). Carries the outcome <see cref="Status"/>, an optional
/// <see cref="Message"/> (populated on a denial or failure), the
/// <see cref="Entries"/> page, and the paging <see cref="NextPageToken"/> when
/// the underlying surface is paged (<see langword="null"/> for the un-paged
/// membership lists). The services fold a server denial or a transport failure
/// into a non-success view rather than throwing, so the UI never leaks an
/// exception even when the advisory capability map believed the read was allowed.
/// </summary>
/// <typeparam name="T">The element type of the listing.</typeparam>
public sealed record AccessListView<T>
{
    /// <summary>The outcome category of the read.</summary>
    public required AccessOperationStatus Status { get; init; }

    /// <summary>A human-readable message, populated on a denial or failure.</summary>
    public string Message { get; init; } = string.Empty;

    /// <summary>The entries on this page. Empty on a denial or failure.</summary>
    public IReadOnlyList<T> Entries { get; init; } = Array.Empty<T>();

    /// <summary>
    /// The continuation cursor for the next page, or <see langword="null"/> when
    /// this is the last page or the surface is not paged.
    /// </summary>
    public string? NextPageToken { get; init; }

    /// <summary><see langword="true"/> when the read succeeded.</summary>
    public bool IsSuccess => Status == AccessOperationStatus.Succeeded;
}
