using Microsoft.Extensions.Logging;
using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Auth.Tests;

/// <summary>
/// Unit tests for <see cref="LoggerLatticeAuthAuditSink"/>: a denied decision is
/// written at <see cref="LogLevel.Warning"/> (the security-relevant signal) and
/// an allowed decision at <see cref="LogLevel.Debug"/>.
/// </summary>
[TestFixture]
public sealed class LoggerLatticeAuthAuditSinkTests
{
    private static LatticeAuthDecisionEvent Event(LatticeEffect effect) =>
        new("alice", LatticeOperation.Write, "orders", effect, policyEpoch: 3, DateTimeOffset.UtcNow, key: "k");

    [Test]
    public async Task Denied_decision_is_logged_at_warning()
    {
        var logger = new CapturingLogger<LoggerLatticeAuthAuditSink>();
        var sink = new LoggerLatticeAuthAuditSink(logger);

        await sink.WriteAsync(Event(LatticeEffect.Deny));

        Assert.That(logger.Entries, Has.Count.EqualTo(1));
        Assert.That(logger.Entries.Single().Level, Is.EqualTo(LogLevel.Warning));
    }

    [Test]
    public async Task Allowed_decision_is_logged_at_debug()
    {
        var logger = new CapturingLogger<LoggerLatticeAuthAuditSink>();
        var sink = new LoggerLatticeAuthAuditSink(logger);

        await sink.WriteAsync(Event(LatticeEffect.Allow));

        Assert.That(logger.Entries, Has.Count.EqualTo(1));
        Assert.That(logger.Entries.Single().Level, Is.EqualTo(LogLevel.Debug));
    }

    [Test]
    public async Task Allowed_decision_is_not_logged_when_debug_is_disabled()
    {
        var logger = new CapturingLogger<LoggerLatticeAuthAuditSink> { MinLevel = LogLevel.Information };
        var sink = new LoggerLatticeAuthAuditSink(logger);

        await sink.WriteAsync(Event(LatticeEffect.Allow));

        Assert.That(logger.Entries, Is.Empty, "an allowed audit must not be written when Debug is off");
    }

    private sealed class CapturingLogger<T> : ILogger<T>
    {
        private readonly List<(LogLevel Level, string Message)> _entries = new();

        public LogLevel MinLevel { get; init; } = LogLevel.Trace;

        public IReadOnlyList<(LogLevel Level, string Message)> Entries => _entries;

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => logLevel >= MinLevel;

        public void Log<TState>(
            LogLevel logLevel,
            EventId eventId,
            TState state,
            Exception? exception,
            Func<TState, Exception?, string> formatter)
        {
            if (IsEnabled(logLevel))
            {
                _entries.Add((logLevel, formatter(state, exception)));
            }
        }
    }
}
