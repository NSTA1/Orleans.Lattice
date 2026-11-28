using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Configuration;
using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Auth.Tests;

/// <summary>
/// Unit coverage for <see cref="GatewayTrustBoundaryWarner"/> (issue #5346): the
/// start-up hosted service must warn when the Orleans client gateway has no inbound
/// connection middleware (no TLS, so no client-certificate authentication), log that
/// the certificate mode cannot be verified when some middleware is present, and stay
/// silent when the silo exposes no gateway.
/// </summary>
[TestFixture]
public sealed class GatewayTrustBoundaryWarnerTests
{
    [Test]
    public async Task StartAsync_warns_when_gateway_has_no_inbound_middleware()
    {
        var logger = new CapturingLogger<GatewayTrustBoundaryWarner>();
        var sut = CreateSut(logger, gatewayPort: 30000, new SiloConnectionOptions());

        await sut.StartAsync(CancellationToken.None);

        Assert.That(logger.Entries, Has.Count.EqualTo(1));
        Assert.Multiple(() =>
        {
            Assert.That(logger.Entries[0].Level, Is.EqualTo(LogLevel.Warning));
            Assert.That(logger.Entries[0].Message, Does.Contain("30000"));
            Assert.That(logger.Entries[0].Message, Does.Contain("#5346"));
        });
    }

    [Test]
    public async Task StartAsync_logs_unverifiable_information_when_gateway_middleware_is_configured()
    {
        var connectionOptions = new SiloConnectionOptions();
        connectionOptions.ConfigureGatewayInboundConnection(builder => builder.Use(next => next));
        var logger = new CapturingLogger<GatewayTrustBoundaryWarner>();
        var sut = CreateSut(logger, gatewayPort: 30000, connectionOptions);

        await sut.StartAsync(CancellationToken.None);

        Assert.That(logger.Entries, Has.Count.EqualTo(1));
        Assert.Multiple(() =>
        {
            Assert.That(logger.Entries[0].Level, Is.EqualTo(LogLevel.Information));
            Assert.That(logger.Entries[0].Message, Does.Contain("cannot verify"));
        });
    }

    [Test]
    public async Task StartAsync_logs_nothing_when_the_silo_exposes_no_gateway()
    {
        var logger = new CapturingLogger<GatewayTrustBoundaryWarner>();
        var sut = CreateSut(logger, gatewayPort: 0, new SiloConnectionOptions());

        await sut.StartAsync(CancellationToken.None);

        Assert.That(logger.Entries, Is.Empty);
    }

    [Test]
    public async Task StartAsync_does_not_throw_when_the_gateway_pipeline_cannot_be_inspected()
    {
        var connectionOptions = new SiloConnectionOptions();
        connectionOptions.ConfigureGatewayInboundConnection(
            _ => throw new InvalidOperationException("probe failure"));
        var logger = new CapturingLogger<GatewayTrustBoundaryWarner>();
        var sut = CreateSut(logger, gatewayPort: 30000, connectionOptions);

        await sut.StartAsync(CancellationToken.None);

        Assert.That(logger.Entries, Has.Count.EqualTo(1));
        Assert.That(logger.Entries[0].Level, Is.EqualTo(LogLevel.Debug));
    }

    [Test]
    public async Task StopAsync_is_a_no_op_and_logs_nothing()
    {
        var logger = new CapturingLogger<GatewayTrustBoundaryWarner>();
        var sut = CreateSut(logger, gatewayPort: 30000, new SiloConnectionOptions());

        await sut.StopAsync(CancellationToken.None);

        Assert.That(logger.Entries, Is.Empty);
    }

    private static GatewayTrustBoundaryWarner CreateSut(
        CapturingLogger<GatewayTrustBoundaryWarner> logger,
        int gatewayPort,
        SiloConnectionOptions connectionOptions) =>
        new(
            logger,
            Options.Create(new EndpointOptions { GatewayPort = gatewayPort }),
            Options.Create(connectionOptions),
            new EmptyServiceProvider());

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

    private sealed class EmptyServiceProvider : IServiceProvider
    {
        public object? GetService(Type serviceType) => null;
    }
}
