using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Auth.Tests;

/// <summary>
/// Unit coverage for <see cref="AuthPostureLogger"/>: the start-up hosted service
/// must emit exactly one informational line carrying the deployment's effective
/// authorization posture (default effect plus both opt-in tier flags), so the
/// otherwise-silent disabled tiers are discoverable in a log an operator already
/// reads.
/// </summary>
[TestFixture]
public sealed class AuthPostureLoggerTests
{
    [Test]
    public async Task StartAsync_logs_the_posture_with_both_flags_and_default_effect()
    {
        var logger = new CapturingLogger<AuthPostureLogger>();
        var options = new LatticeAuthOptions
        {
            DefaultEffect = LatticeEffect.Deny,
            AllTreesGrantsEnabled = true,
            AccessAdministrationDelegationEnabled = false,
        };
        var sut = new AuthPostureLogger(logger, new StaticOptionsMonitor<LatticeAuthOptions>(options));

        await sut.StartAsync(CancellationToken.None);

        Assert.That(logger.Entries, Has.Count.EqualTo(1));
        var entry = logger.Entries[0];
        Assert.Multiple(() =>
        {
            Assert.That(entry.Level, Is.EqualTo(LogLevel.Information));
            Assert.That(entry.Message, Does.Contain("DefaultEffect"));
            Assert.That(entry.Message, Does.Contain(nameof(LatticeEffect.Deny)));
            Assert.That(entry.Message, Does.Contain("AllTreesGrantsEnabled"));
            Assert.That(entry.Message, Does.Contain("AccessAdministrationDelegationEnabled"));
            Assert.That(entry.Message, Does.Contain("True"));
            Assert.That(entry.Message, Does.Contain("False"));
        });
    }

    [Test]
    public async Task StopAsync_is_a_no_op_and_logs_nothing()
    {
        var logger = new CapturingLogger<AuthPostureLogger>();
        var sut = new AuthPostureLogger(
            logger,
            new StaticOptionsMonitor<LatticeAuthOptions>(new LatticeAuthOptions()));

        await sut.StopAsync(CancellationToken.None);

        Assert.That(logger.Entries, Is.Empty);
    }

    private sealed record LogEntry(LogLevel Level, string Message);

    private sealed class CapturingLogger<T> : ILogger<T>
    {
        public List<LogEntry> Entries { get; } = [];

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(
            LogLevel logLevel,
            EventId eventId,
            TState state,
            Exception? exception,
            Func<TState, Exception?, string> formatter) =>
            Entries.Add(new LogEntry(logLevel, formatter(state, exception)));
    }

    private sealed class StaticOptionsMonitor<TOptions>(TOptions value) : IOptionsMonitor<TOptions>
    {
        public TOptions CurrentValue { get; } = value;

        public TOptions Get(string? name) => CurrentValue;

        public IDisposable? OnChange(Action<TOptions, string?> listener) => null;
    }
}
