using Microsoft.Extensions.Logging;

namespace Orleans.Lattice.GrainIndex.Tests.Registry;

/// <summary>
/// An <see cref="ILogger{TCategoryName}"/> that records every entry it is
/// handed, so a test can assert the level and content a branch is required to
/// log at without depending on a logging provider.
/// </summary>
/// <typeparam name="TCategory">The logger's category type.</typeparam>
internal sealed class CapturingLogger<TCategory> : ILogger<TCategory>
{
    /// <summary>Every entry logged, in order.</summary>
    internal List<(LogLevel Level, string Message)> Entries { get; } = [];

    /// <summary>The messages logged at <paramref name="level"/>, in order.</summary>
    internal IReadOnlyList<string> MessagesAt(LogLevel level) =>
        Entries.Where(e => e.Level == level).Select(e => e.Message).ToArray();

    /// <inheritdoc />
    public IDisposable? BeginScope<TState>(TState state)
        where TState : notnull => null;

    /// <inheritdoc />
    public bool IsEnabled(LogLevel logLevel) => true;

    /// <inheritdoc />
    public void Log<TState>(
        LogLevel logLevel,
        EventId eventId,
        TState state,
        Exception? exception,
        Func<TState, Exception?, string> formatter)
    {
        ArgumentNullException.ThrowIfNull(formatter);
        Entries.Add((logLevel, formatter(state, exception)));
    }
}
