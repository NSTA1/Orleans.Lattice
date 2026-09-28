using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;

namespace Orleans.Lattice.Api.Mcp.Tests;

/// <summary>
/// A logger provider that records every log entry at every level, so a test can
/// assert which level a message was written at and whether it carried an
/// exception.
/// </summary>
internal sealed class CapturingLoggerProvider : ILoggerProvider, ILoggerFactory
{
    private readonly ConcurrentQueue<CapturedLogEntry> _entries = new();

    /// <summary>Every entry written so far, in write order.</summary>
    public IReadOnlyList<CapturedLogEntry> Entries => _entries.ToArray();

    /// <inheritdoc />
    public ILogger CreateLogger(string categoryName) => new CapturingLogger(categoryName, _entries);

    /// <inheritdoc />
    public void AddProvider(ILoggerProvider provider)
    {
    }

    /// <inheritdoc />
    public void Dispose()
    {
    }

    private sealed class CapturingLogger(string category, ConcurrentQueue<CapturedLogEntry> entries) : ILogger
    {
        public IDisposable? BeginScope<TState>(TState state)
            where TState : notnull
            => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(
            LogLevel logLevel,
            EventId eventId,
            TState state,
            Exception? exception,
            Func<TState, Exception?, string> formatter)
            => entries.Enqueue(new CapturedLogEntry(category, logLevel, eventId, formatter(state, exception), exception));
    }
}

/// <summary>One captured log entry.</summary>
/// <param name="Category">The logger category.</param>
/// <param name="Level">The level it was written at.</param>
/// <param name="EventId">Its event id.</param>
/// <param name="Message">The formatted message.</param>
/// <param name="Exception">The exception it carried, if any.</param>
internal sealed record CapturedLogEntry(
    string Category, LogLevel Level, EventId EventId, string Message, Exception? Exception);
