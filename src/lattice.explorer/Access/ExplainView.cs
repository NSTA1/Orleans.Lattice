using Orleans.Lattice.Api.Auth;

namespace Orleans.Lattice.Explorer.Access;

/// <summary>
/// The result of an Explain query: the outcome <see cref="Status"/>, an optional
/// <see cref="Message"/> (populated on a denial or failure), and the
/// <see cref="Explanation"/> when the query succeeded. The verdict inside
/// <see cref="Explanation"/> is produced by the same access gate the data plane
/// consults, so the UI renders it verbatim and never re-derives a decision.
/// </summary>
public sealed record ExplainView
{
    /// <summary>The outcome category of the query.</summary>
    public required AccessOperationStatus Status { get; init; }

    /// <summary>A human-readable message, populated on a denial or failure.</summary>
    public string Message { get; init; } = string.Empty;

    /// <summary>The facade's explanation, or <see langword="null"/> on a denial or failure.</summary>
    public AuthExplanation? Explanation { get; init; }

    /// <summary><see langword="true"/> when the query succeeded.</summary>
    public bool IsSuccess => Status == AccessOperationStatus.Succeeded;
}
