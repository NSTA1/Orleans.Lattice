using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Harness;

/// <summary>
/// An in-memory <see cref="ILoggerProvider"/> that records every log entry so a
/// test can assert on the lines a component emits. Registered as a singleton
/// <see cref="ILoggerProvider"/> through
/// <see cref="RepoContextMcpHarnessOptions.ConfigureServices"/>, it is picked up by
/// the harness's <see cref="ILoggerFactory"/> alongside (or instead of) the
/// console provider. Thread-safe: entries are held in a
/// <see cref="ConcurrentQueue{T}"/> because the MCP request pipeline logs off the
/// test thread.
/// </summary>
public sealed class CapturingLoggerProvider : ILoggerProvider
{
    private readonly ConcurrentQueue<CapturedLogEntry> _entries = new();

    /// <summary>The entries captured so far, in log order.</summary>
    public IReadOnlyCollection<CapturedLogEntry> Entries => _entries.ToArray();

    /// <inheritdoc />
    public ILogger CreateLogger(string categoryName)
        => new CapturingLogger(categoryName, _entries);

    /// <inheritdoc />
    public void Dispose()
    {
    }

    private sealed class CapturingLogger(string category, ConcurrentQueue<CapturedLogEntry> sink)
        : ILogger
    {
        public IDisposable? BeginScope<TState>(TState state)
            where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(
            LogLevel logLevel,
            EventId eventId,
            TState state,
            Exception? exception,
            Func<TState, Exception?, string> formatter)
        {
            ArgumentNullException.ThrowIfNull(formatter);
            sink.Enqueue(new CapturedLogEntry(
                category, logLevel, formatter(state, exception), exception));
        }
    }
}
