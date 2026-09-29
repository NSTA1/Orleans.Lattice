using Orleans.Lattice.Explorer.UI.Navigation;

namespace Orleans.Lattice.Explorer.Tests.UI.Navigation;

/// <summary>
/// A completion source a test drives: it answers immediately, answers when the
/// test completes its <see cref="Gate"/>, throws, or never answers at all, and
/// records every query and whether it was cancelled.
/// </summary>
internal sealed class FakeCompletionSource : IAddressCompletionSource
{
    private readonly Func<AddressQuery, CancellationToken, ValueTask<IReadOnlyList<AddressCompletion>>> _answer;

    /// <summary>Creates a source that answers with <paramref name="answer"/>.</summary>
    /// <param name="answer">How to answer.</param>
    public FakeCompletionSource(Func<AddressQuery, CancellationToken, ValueTask<IReadOnlyList<AddressCompletion>>> answer) =>
        _answer = answer;

    /// <summary>The queries asked, in order.</summary>
    public List<AddressQuery> Queries { get; } = [];

    /// <summary>The tokens the source was handed, in order.</summary>
    public List<CancellationToken> Tokens { get; } = [];

    /// <summary>A source that answers <paramref name="completions"/> at once.</summary>
    /// <param name="completions">The answer.</param>
    public static FakeCompletionSource Answering(params AddressCompletion[] completions) =>
        new((_, _) => ValueTask.FromResult<IReadOnlyList<AddressCompletion>>(completions));

    /// <summary>A source that throws.</summary>
    public static FakeCompletionSource Throwing() =>
        new((_, _) => throw new InvalidOperationException("The source is broken."));

    /// <summary>A source that answers only when <paramref name="gate"/> completes, ignoring cancellation.</summary>
    /// <param name="gate">The answer's completion.</param>
    public static FakeCompletionSource Gated(TaskCompletionSource<IReadOnlyList<AddressCompletion>> gate) =>
        new((_, _) => new ValueTask<IReadOnlyList<AddressCompletion>>(gate.Task));

    /// <inheritdoc />
    public ValueTask<IReadOnlyList<AddressCompletion>> CompleteAsync(AddressQuery query, CancellationToken cancellationToken)
    {
        Queries.Add(query);
        Tokens.Add(cancellationToken);
        return _answer(query, cancellationToken);
    }
}
