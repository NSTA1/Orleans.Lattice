using Orleans.Lattice.Explorer.UI.Appearance;

namespace Orleans.Lattice.Explorer.Tests.Appearance;

/// <summary>
/// An <see cref="IExplorerAppearanceApplier"/> that records what it was asked to
/// put on the document, instead of reaching one.
/// </summary>
/// <remarks>
/// It completes synchronously, exactly as the real applier does when interop is
/// unavailable, so a test that raises a synchronous event and then asserts on
/// <see cref="Applied"/> is observing a completed effect rather than racing one.
/// </remarks>
internal sealed class FakeExplorerAppearanceApplier : IExplorerAppearanceApplier
{
    /// <summary>Every state applied, in order.</summary>
    public List<ExplorerAppearanceState> Applied { get; } = [];

    /// <summary>The most recently applied state.</summary>
    public ExplorerAppearanceState Last => Applied[^1];

    /// <summary>When set, the applier faults, standing in for a broken document.</summary>
    public Exception? Fault { get; init; }

    /// <inheritdoc />
    public ValueTask ApplyAsync(ExplorerAppearanceState state, CancellationToken cancellationToken = default)
    {
        Applied.Add(state);

        return Fault is null
            ? ValueTask.CompletedTask
            : ValueTask.FromException(Fault);
    }
}
