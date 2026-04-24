using Microsoft.Extensions.Logging.Abstractions;

namespace Orleans.Lattice.Tests.Fakes;

/// <summary>
/// Test helpers for building <see cref="MutationObserverDispatcher"/>
/// instances without a DI container. Most grain unit tests do not care
/// about mutation observers — <see cref="NoObservers"/> returns a zero-
/// observer dispatcher that satisfies the ctor requirement and
/// short-circuits on the hot path.
/// </summary>
internal static class TestMutationObservers
{
    /// <summary>
    /// Creates a <see cref="MutationObserverDispatcher"/> with no registered
    /// observers. Mutations published through it are dropped.
    /// </summary>
    public static MutationObserverDispatcher NoObservers() =>
        new([], NullLogger<MutationObserverDispatcher>.Instance);

    /// <summary>
    /// Creates a <see cref="MutationObserverDispatcher"/> over the supplied
    /// observers. Useful when a test wants to assert that a specific
    /// mutation was published.
    /// </summary>
    public static MutationObserverDispatcher With(params IMutationObserver[] observers) =>
        new(observers, NullLogger<MutationObserverDispatcher>.Instance);
}

/// <summary>
/// Collects every mutation it observes into an in-memory list, for use in
/// unit tests that assert on the publish side-effect.
/// </summary>
internal sealed class RecordingMutationObserver : IMutationObserver
{
    private readonly List<LatticeMutation> _mutations = [];
    private readonly object _lock = new();

    /// <summary>The mutations captured so far, in publish order.</summary>
    public IReadOnlyList<LatticeMutation> Mutations
    {
        get { lock (_lock) return _mutations.ToArray(); }
    }

    /// <inheritdoc />
    public Task OnMutationAsync(LatticeMutation mutation, CancellationToken cancellationToken)
    {
        lock (_lock) _mutations.Add(mutation);
        return Task.CompletedTask;
    }
}

/// <summary>
/// Throws a configurable exception on every call, for testing the
/// dispatcher's swallow-and-log semantics.
/// </summary>
internal sealed class ThrowingMutationObserver(Exception? toThrow = null) : IMutationObserver
{
    private readonly Exception _toThrow = toThrow ?? new InvalidOperationException("test observer failure");

    /// <inheritdoc />
    public Task OnMutationAsync(LatticeMutation mutation, CancellationToken cancellationToken) =>
        throw _toThrow;
}
