using Microsoft.AspNetCore.Components.Server;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Hosting.Internal;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Samples.Explorer.Tests;

/// <summary>
/// The console region's circuit-fault diagnostics (issue #4011): a terminated circuit
/// leaves its stack trace in the terminal and nothing else is logged there; in
/// Development the browser is sent the detail too.
/// </summary>
[TestFixture]
public sealed class SampleCircuitDiagnosticsTests
{
    private const string CircuitHost = SampleCircuitDiagnostics.CircuitCategory + ".CircuitHost";

    [Test]
    public void A_circuit_fault_is_logged_with_its_exception_and_nothing_else_is()
    {
        var sink = new RecordingProvider();
        using var provider = new ServiceCollection()
            .AddLogging(logging =>
            {
                logging.ClearProviders();
                SampleCircuitDiagnostics.ConfigureLogging(logging);
                logging.AddProvider(sink);
            })
            .BuildServiceProvider();
        var factory = provider.GetRequiredService<ILoggerFactory>();
        var fault = new ObjectDisposedException("CancellationTokenSource");

        factory.CreateLogger(CircuitHost).LogError(fault, "Unhandled exception in circuit '{CircuitId}'.", "c-1");
        factory.CreateLogger(CircuitHost).LogWarning("a warning");
        factory.CreateLogger("Orleans.Runtime.Silo").LogError("a silo error");

        Assert.Multiple(() =>
        {
            Assert.That(sink.Records, Has.Count.EqualTo(1));
            Assert.That(sink.Records[0].Exception, Is.SameAs(fault), "the stack trace travels with the record");
            Assert.That(provider.GetServices<ILoggerProvider>().OfType<Microsoft.Extensions.Logging.Console.ConsoleLoggerProvider>(), Is.Not.Empty, "the record reaches the terminal");
        });
    }

    [Test]
    public void In_development_the_browser_is_sent_the_detail()
    {
        Assert.Multiple(() =>
        {
            Assert.That(DetailedErrors(Environments.Development), Is.True);
            Assert.That(DetailedErrors(Environments.Production), Is.False);
        });
    }

    [Test]
    public void Null_arguments_throw()
    {
        Assert.Multiple(() =>
        {
            Assert.That(() => SampleCircuitDiagnostics.ConfigureLogging(null!), Throws.ArgumentNullException);
            Assert.That(() => SampleCircuitDiagnostics.ConfigureCircuits(null!, new HostingEnvironment()), Throws.ArgumentNullException);
            Assert.That(() => SampleCircuitDiagnostics.ConfigureCircuits(new ServiceCollection(), null!), Throws.ArgumentNullException);
        });
    }

    private static bool DetailedErrors(string environment)
    {
        var services = new ServiceCollection();
        services.AddOptions();
        SampleCircuitDiagnostics.ConfigureCircuits(services, new HostingEnvironment { EnvironmentName = environment });
        using var provider = services.BuildServiceProvider();
        return provider.GetRequiredService<IOptions<CircuitOptions>>().Value.DetailedErrors;
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
