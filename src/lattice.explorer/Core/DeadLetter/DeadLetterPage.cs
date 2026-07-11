namespace Orleans.Lattice.Explorer.Core.DeadLetter;

/// <summary>
/// One page of a tree's strict-mode dead-letter queue (append/time order) plus
/// the continuation token that resumes enumeration (or <see langword="null"/>
/// when the queue is drained).
/// </summary>
public sealed record DeadLetterPage
{
    /// <summary>The dead-letter entries in this page, in append (time) order.</summary>
    public IReadOnlyList<DeadLetterEntry> Entries { get; init; } = Array.Empty<DeadLetterEntry>();

    /// <summary>The token that resumes the listing, or <see langword="null"/> when drained.</summary>
    public string? ContinuationToken { get; init; }

    /// <summary>Whether a further page is available.</summary>
    public bool HasMore => !string.IsNullOrEmpty(ContinuationToken);

    /// <summary>An empty page with no continuation.</summary>
    public static DeadLetterPage Empty { get; } = new();
}
