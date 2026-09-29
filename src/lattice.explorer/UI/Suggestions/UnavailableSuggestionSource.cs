using Orleans.Lattice.Explorer.UI.Design.Components;

namespace Orleans.Lattice.Explorer.UI.Suggestions;

/// <summary>
/// The source for a picker whose values this head cannot list at all: it always
/// answers unavailable, so the field accepts what is typed and says why.
/// </summary>
internal sealed class UnavailableSuggestionSource : ILtSuggestionSource
{
    /// <summary>The note shown.</summary>
    public const string Reason = "Suggestions are not available here, so the value is used as typed.";

    private static readonly LtSuggestionSet Answer = LtSuggestionSet.Unavailable(Reason);

    private UnavailableSuggestionSource()
    {
    }

    /// <summary>The one instance.</summary>
    public static UnavailableSuggestionSource Instance { get; } = new();

    /// <inheritdoc />
    public ValueTask<LtSuggestionSet> SuggestAsync(string text, int limit, CancellationToken cancellationToken) =>
        ValueTask.FromResult(Answer);
}
