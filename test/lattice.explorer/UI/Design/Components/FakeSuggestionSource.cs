using Orleans.Lattice.Explorer.UI.Design.Components;

namespace Orleans.Lattice.Explorer.Tests.UI.Design.Components;

/// <summary>
/// A suggestion source a test drives: it records every query and its token, and
/// answers at once from <see cref="Values"/>, or - when <see cref="Gated"/> - only
/// when the test releases that query, so a test can hold a query in flight
/// without any timer.
/// </summary>
internal sealed class FakeSuggestionSource : ILtSuggestionSource
{
    /// <summary>Creates the source over <paramref name="values"/>.</summary>
    /// <param name="values">The existing values.</param>
    public FakeSuggestionSource(params string[] values) => Values = [.. values];

    /// <summary>The existing values, matched by prefix, exact match first.</summary>
    public List<string> Values { get; }

    /// <summary>Every query, in order.</summary>
    public List<Query> Queries { get; } = [];

    /// <summary>Whether each query waits for the test to release it.</summary>
    public bool Gated { get; set; }

    /// <summary>Whether a gated query keeps running when its token is cancelled.</summary>
    public bool IgnoresCancellation { get; set; }

    /// <summary>When set, every query answers unavailable with this reason.</summary>
    public string? Unavailable { get; set; }

    /// <summary>When set, every query throws it.</summary>
    public Exception? Throws { get; set; }

    /// <inheritdoc />
    public ValueTask<LtSuggestionSet> SuggestAsync(string text, int limit, CancellationToken cancellationToken)
    {
        var query = new Query(text, limit, cancellationToken);
        Queries.Add(query);
        if (Throws is { } exception)
        {
            return ValueTask.FromException<LtSuggestionSet>(exception);
        }

        if (!Gated)
        {
            return ValueTask.FromResult(Answer(text, limit));
        }

        if (!IgnoresCancellation)
        {
            cancellationToken.Register(() => query.Gate.TrySetCanceled(cancellationToken));
        }

        return new ValueTask<LtSuggestionSet>(query.Gate.Task);
    }

    /// <summary>The answer the source gives for <paramref name="text"/>.</summary>
    public LtSuggestionSet Answer(string text, int limit)
    {
        if (Unavailable is { } reason)
        {
            return LtSuggestionSet.Unavailable(reason);
        }

        var exact = Values.Where(value => value == text);
        var starts = Values.Where(value => value != text && value.StartsWith(text, StringComparison.OrdinalIgnoreCase));
        var all = exact.Concat(starts).ToList();
        return LtSuggestionSet.Of([.. all.Take(limit).Select(value => new LtSuggestion(value, "Tree"))], all.Count > limit);
    }

    /// <summary>One query and the gate a gated query waits on.</summary>
    internal sealed record Query(string Text, int Limit, CancellationToken Token)
    {
        /// <summary>Released by the test with an answer.</summary>
        public TaskCompletionSource<LtSuggestionSet> Gate { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
    }
}
