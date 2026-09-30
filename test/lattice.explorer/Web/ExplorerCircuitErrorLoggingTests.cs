using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Hosting.Internal;
using Microsoft.Extensions.Logging;
using Orleans.Lattice.Explorer.Web;

namespace Orleans.Lattice.Explorer.Tests.Web;

/// <summary>
/// The web head's circuit-fault logging (issue #4011): in Development an unhandled circuit
/// exception always reaches the log with its stack trace, whatever the host's own filters
/// say; outside Development the host's filters are left alone.
/// </summary>
[TestFixture]
public sealed class ExplorerCircuitErrorLoggingTests
{
    private const string CircuitHost = ExplorerCircuitErrorLogging.CircuitCategory + ".CircuitHost";

    [Test]
    public void In_development_a_circuit_fault_is_logged_with_its_exception_even_when_the_host_silences_microsoft()
    {
        var sink = new RecordingProvider();
        using var provider = Build(Environments.Development, sink, logging => logging.AddFilter("Microsoft", LogLevel.None));
        var factory = provider.GetRequiredService<ILoggerFactory>();
        var fault = new InvalidOperationException("boom");

        factory.CreateLogger(CircuitHost).LogError(fault, "Unhandled exception in circuit '{CircuitId}'.", "c-1");

        Assert.Multiple(() =>
        {
            Assert.That(sink.Records, Has.Count.EqualTo(1));
            Assert.That(sink.Records[0].Level, Is.EqualTo(LogLevel.Error));
            Assert.That(sink.Records[0].Exception, Is.SameAs(fault), "the stack trace travels with the record");
            Assert.That(factory.CreateLogger(CircuitHost).IsEnabled(LogLevel.Warning), Is.False, "only faults are let through");
            Assert.That(factory.CreateLogger("Microsoft.AspNetCore.Routing").IsEnabled(LogLevel.Error), Is.False, "every other category keeps the host's rule");
        });
    }

    [Test]
    public void Outside_development_the_hosts_filters_are_left_alone()
    {
        using var provider = Build(Environments.Production, new RecordingProvider(), logging => logging.AddFilter("Microsoft", LogLevel.None));

        Assert.That(provider.GetRequiredService<ILoggerFactory>().CreateLogger(CircuitHost).IsEnabled(LogLevel.Error), Is.False);
    }

    [Test]
    public void A_host_that_already_shows_the_circuit_category_keeps_its_own_level()
    {
        using var provider = Build(
            Environments.Development,
            new RecordingProvider(),
            logging => logging.AddFilter("Microsoft", LogLevel.None).AddFilter(ExplorerCircuitErrorLogging.CircuitCategory, LogLevel.Debug));

        Assert.That(provider.GetRequiredService<ILoggerFactory>().CreateLogger(CircuitHost).IsEnabled(LogLevel.Debug), Is.True);
    }

    [Test]
    public void Without_a_host_environment_nothing_is_added()
    {
        var options = new LoggerFilterOptions();

        new ExplorerCircuitErrorLogging().Configure(options);

        Assert.That(options.Rules, Is.Empty);
    }

    [Test]
    public void In_development_the_rule_is_added_once_at_error()
    {
        var options = new LoggerFilterOptions();

        new ExplorerCircuitErrorLogging(new HostingEnvironment { EnvironmentName = Environments.Development }).Configure(options);

        Assert.Multiple(() =>
        {
            Assert.That(options.Rules, Has.Count.EqualTo(1));
            Assert.That(options.Rules[0].CategoryName, Is.EqualTo(ExplorerCircuitErrorLogging.CircuitCategory));
            Assert.That(options.Rules[0].LogLevel, Is.EqualTo(LogLevel.Error));
            Assert.That(options.Rules[0].ProviderName, Is.Null, "every provider receives it");
        });
    }

    [Test]
    public void Configure_null_options_throws()
    {
        Assert.That(() => new ExplorerCircuitErrorLogging().Configure(null!), Throws.ArgumentNullException);
    }

    private static ServiceProvider Build(string environment, RecordingProvider sink, Action<ILoggingBuilder> configure)
    {
        var services = new ServiceCollection();
        services.AddSingleton<IHostEnvironment>(new HostingEnvironment { EnvironmentName = environment });
        services.AddLogging(logging =>
        {
            logging.ClearProviders();
            logging.AddProvider(sink);
            configure(logging);
        });
        services.AddLatticeExplorerWeb();
        return services.BuildServiceProvider();
    }

    private sealed class RecordingProvider : ILoggerProvider
    {
        public List<(LogLevel Level, Exception? Exception)> Records { get; } = [];

        public ILogger CreateLogger(string categoryName) => new Sink(this);

        public void Dispose()
        {
        }

        private sealed class Sink(RecordingProvider owner) : ILogger
        {
            public IDisposable? BeginScope<TState>(TState state)
                where TState : notnull => null;

            public bool IsEnabled(LogLevel logLevel) => true;

            public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter) =>
                owner.Records.Add((logLevel, exception));
        }
    }
}
