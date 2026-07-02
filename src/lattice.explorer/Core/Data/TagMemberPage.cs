namespace Orleans.Lattice.Explorer.Core.Data;

/// <summary>
/// A page of a tag's live members across a tag index plus the continuation
/// token that resumes the same scan (or <see langword="null"/> when drained).
/// </summary>
public sealed record TagMemberPage
{
    /// <summary>The members in this page, ordered by <c>(tree id, key)</c> ordinal.</summary>
    public IReadOnlyList<TagMemberRow> Members { get; init; } = Array.Empty<TagMemberRow>();

    /// <summary>The token that resumes the scan, or <see langword="null"/> when drained.</summary>
    public string? ContinuationToken { get; init; }

    /// <summary>Whether a further page is available.</summary>
    public bool HasMore => !string.IsNullOrEmpty(ContinuationToken);

    /// <summary>An empty page with no continuation.</summary>
    public static TagMemberPage Empty { get; } = new();
}
