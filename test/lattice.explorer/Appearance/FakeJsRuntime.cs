using Microsoft.JSInterop;

namespace Orleans.Lattice.Explorer.Tests.Appearance;

/// <summary>
/// A recording <see cref="IJSRuntime"/>, so a test can assert exactly what was
/// asked of the browser without one.
/// </summary>
internal sealed class FakeJsRuntime : IJSRuntime
{
    /// <summary>Every invocation, in order.</summary>
    public List<(string Identifier, object?[]? Arguments)> Calls { get; } = [];

    /// <summary>When set, every invocation faults with it.</summary>
    public Exception? Fault { get; init; }

    /// <inheritdoc />
    public ValueTask<TValue> InvokeAsync<TValue>(string identifier, object?[]? args) =>
        InvokeAsync<TValue>(identifier, CancellationToken.None, args);

    /// <inheritdoc />
    public ValueTask<TValue> InvokeAsync<TValue>(
        string identifier,
        CancellationToken cancellationToken,
        object?[]? args)
    {
        Calls.Add((identifier, args));

        return Fault is null
            ? ValueTask.FromResult<TValue>(default!)
            : ValueTask.FromException<TValue>(Fault);
    }
}
