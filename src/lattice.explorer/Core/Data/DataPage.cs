namespace Orleans.Lattice.Explorer.Core.Data;

/// <summary>
/// A snapshot-isolated page of entries plus the continuation token that resumes
/// the same scan cursor (or <see langword="null"/> when the scan is drained).
/// </summary>
public sealed record DataPage
{
    /// <summary>The entries in this page, in the scan's key order.</summary>
    public IReadOnlyList<DataEntry> Entries { get; init; } = Array.Empty<DataEntry>();

    /// <summary>The token that resumes the scan against the same snapshot, or <see langword="null"/> when drained.</summary>
    public string? ContinuationToken { get; init; }

    /// <summary>Whether a further page is available.</summary>
    public bool HasMore => !string.IsNullOrEmpty(ContinuationToken);

    /// <summary>An empty page with no continuation.</summary>
    public static DataPage Empty { get; } = new();
}
