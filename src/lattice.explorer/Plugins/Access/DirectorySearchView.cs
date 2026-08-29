using Orleans.Lattice.Api.Auth;
using Orleans.Lattice.Membership;

namespace Orleans.Lattice.Explorer.Access;

/// <summary>
/// A read result for an Access-area identity-directory search. Carries the
/// outcome <see cref="Status"/>, an optional <see cref="Message"/> (populated on
/// a denial or failure), the matched <see cref="Principals"/> page, the paging
/// <see cref="NextPageToken"/>, and an <see cref="Available"/> flag distinguishing
/// 'no principals matched' from 'no identity directory is configured'. The service
/// folds a server denial or a transport failure into a non-success view rather
/// than throwing, and folds a directory-unavailable backend into a successful but
/// <see cref="Available"/>-<see langword="false"/> view, so the UI never leaks an
/// exception even when the advisory capability map believed the read was allowed.
/// </summary>
public sealed record DirectorySearchView
{
    /// <summary>The outcome category of the search.</summary>
    public required AccessOperationStatus Status { get; init; }

    /// <summary>A human-readable message, populated on a denial or failure.</summary>
    public string Message { get; init; } = string.Empty;

    /// <summary>The matched principals on this page. Empty on a denial, failure, or when no directory is configured.</summary>
    public IReadOnlyList<DirectoryPrincipalDescriptor> Principals { get; init; }
        = Array.Empty<DirectoryPrincipalDescriptor>();

    /// <summary>
    /// The continuation cursor for the next page, or <see langword="null"/> when
    /// this is the last page.
    /// </summary>
    public string? NextPageToken { get; init; }

    /// <summary>
    /// <see langword="true"/> when a searchable identity directory is configured
    /// and produced this page; <see langword="false"/> when no directory is
    /// available, in which case <see cref="Principals"/> is always empty.
    /// </summary>
    public bool Available { get; init; }

    /// <summary><see langword="true"/> when the read succeeded (whether or not a directory is available).</summary>
    public bool IsSuccess => Status == AccessOperationStatus.Succeeded;

    /// <summary>
    /// The clean view shown when no searchable identity directory is configured:
    /// a successful read with no principals and <see cref="Available"/>
    /// <see langword="false"/>.
    /// </summary>
    public static DirectorySearchView Unavailable { get; } = new()
    {
        Status = AccessOperationStatus.Succeeded,
        Available = false,
    };
}
