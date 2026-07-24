using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Orleans.Lattice.ReferenceArchitecture.Hosting;

namespace Orleans.Lattice.ReferenceArchitecture.Hosting.Tests;

/// <summary>
/// Coverage of the path-scoped health-probe log suppression wired by
/// <see cref="HealthProbeRequestLoggingExtensions.SuppressHealthProbeRequestLogs"/>.
/// The decorator is exercised through a recording inner provider so the exact set
/// of entries that survive filtering can be asserted.
/// </summary>
[TestFixture]
public sealed class HealthProbeRequestLoggingTests
{
    private const string HealthPath = FrontDoorOriginLockApplicationBuilderExtensions.HealthPath;

    private static ILogger BuildLogger(RecordingLoggerProvider recorder, IHttpContextAccessor accessor)
    {
        var services = new ServiceCollection();
        services.AddSingleton<IHttpContextAccessor>(accessor);
        services.AddLogging(logging =>
        {
            logging.ClearProviders();
            logging.Services.AddSingleton<ILoggerProvider>(recorder);
            logging.SuppressHealthProbeRequestLogs();
        });

        var provider = services.BuildServiceProvider();
        return provider.GetRequiredService<ILoggerFactory>().CreateLogger("Microsoft.AspNetCore.Hosting.Diagnostics");
    }

    private static ILogger BuildLogger(
        RecordingLoggerProvider recorder,
        IHttpContextAccessor accessor,
        params string[] probePaths)
    {
        var services = new ServiceCollection();
        services.AddSingleton<IHttpContextAccessor>(accessor);
        services.AddLogging(logging =>
        {
            logging.ClearProviders();
            logging.Services.AddSingleton<ILoggerProvider>(recorder);
            logging.SuppressProbeRequestLogs(probePaths);
        });

        var provider = services.BuildServiceProvider();
        return provider.GetRequiredService<ILoggerFactory>().CreateLogger("Microsoft.AspNetCore.Hosting.Diagnostics");
    }

    private static IHttpContextAccessor AccessorFor(string? path, int statusCode = StatusCodes.Status200OK)
    {
        var accessor = new HttpContextAccessor();
        if (path is not null)
        {
            var context = new DefaultHttpContext();
            context.Request.Path = path;
            context.Response.StatusCode = statusCode;
            accessor.HttpContext = context;
        }

        return accessor;
    }

    [Test]
    public void Suppresses_informational_logs_while_serving_the_health_path()
    {
        var recorder = new RecordingLoggerProvider();
        var logger = BuildLogger(recorder, AccessorFor(HealthPath));

        logger.LogInformation("Request starting HEAD /health");

        Assert.That(recorder.Entries, Is.Empty);
    }

    [Test]
    public void Keeps_informational_logs_for_a_non_health_request()
    {
        var recorder = new RecordingLoggerProvider();
        var logger = BuildLogger(recorder, AccessorFor("/trees"));

        logger.LogInformation("Request starting GET /trees");

        Assert.That(recorder.Entries, Has.Count.EqualTo(1));
    }

    [Test]
    public void Keeps_warnings_and_errors_even_on_the_health_path()
    {
        var recorder = new RecordingLoggerProvider();
        var logger = BuildLogger(recorder, AccessorFor(HealthPath));

        logger.LogWarning("health check degraded");
        logger.LogError("health check failed");

        Assert.That(recorder.Entries, Has.Count.EqualTo(2));
    }

    [Test]
    public void Keeps_informational_logs_written_outside_any_request()
    {
        var recorder = new RecordingLoggerProvider();
        var logger = BuildLogger(recorder, AccessorFor(path: null));

        logger.LogInformation("silo started");

        Assert.That(recorder.Entries, Has.Count.EqualTo(1));
    }

    [Test]
    public void Does_not_treat_a_sibling_path_as_the_health_path()
    {
        var recorder = new RecordingLoggerProvider();
        var logger = BuildLogger(recorder, AccessorFor("/healthz"));

        logger.LogInformation("Request starting GET /healthz");

        Assert.That(recorder.Entries, Has.Count.EqualTo(1));
    }

    private const string DigestProbePath =
        "/orleans.lattice.replication.LatticeReplication/ProbeDigest";

    [Test]
    public void Suppresses_informational_logs_while_serving_a_configured_engine_transport_path()
    {
        var recorder = new RecordingLoggerProvider();
        var logger = BuildLogger(
            recorder,
            AccessorFor(DigestProbePath),
            "/orleans.lattice.replication.LatticeReplication");

        logger.LogInformation("Request finished HTTP/2 POST /...ProbeDigest - 200");

        Assert.That(recorder.Entries, Is.Empty);
    }

    [Test]
    public void Suppresses_across_multiple_configured_probe_paths()
    {
        var recorder = new RecordingLoggerProvider();
        var logger = BuildLogger(
            recorder,
            AccessorFor(HealthPath),
            HealthPath,
            "/orleans.lattice.replication.LatticeReplication");

        logger.LogInformation("Request starting HEAD /health");

        Assert.That(recorder.Entries, Is.Empty);
    }

    [Test]
    public void Keeps_informational_logs_for_a_probe_path_that_drew_a_non_success_status()
    {
        var recorder = new RecordingLoggerProvider();
        var logger = BuildLogger(
            recorder,
            AccessorFor(DigestProbePath, StatusCodes.Status500InternalServerError),
            "/orleans.lattice.replication.LatticeReplication");

        logger.LogInformation("Request finished HTTP/2 POST /...ProbeDigest - 500");

        Assert.That(recorder.Entries, Has.Count.EqualTo(1));
    }

    [Test]
    public void Keeps_informational_logs_for_a_health_probe_that_drew_a_non_success_status()
    {
        var recorder = new RecordingLoggerProvider();
        var logger = BuildLogger(
            recorder,
            AccessorFor(HealthPath, StatusCodes.Status503ServiceUnavailable),
            HealthPath);

        logger.LogInformation("Request finished HEAD /health - 503");

        Assert.That(recorder.Entries, Has.Count.EqualTo(1));
    }

    [Test]
    public void SuppressProbeRequestLogs_rejects_an_empty_path_set()
    {
        var services = new ServiceCollection();
        services.AddLogging(logging =>
            Assert.Throws<ArgumentException>(() => logging.SuppressProbeRequestLogs()));
    }

    private sealed class RecordingLoggerProvider : ILoggerProvider
    {
        public List<string> Entries { get; } = [];

        public ILogger CreateLogger(string categoryName) => new RecordingLogger(Entries);

        public void Dispose()
        {
        }

        private sealed class RecordingLogger(List<string> entries) : ILogger
        {
            public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

            public bool IsEnabled(LogLevel logLevel) => true;

            public void Log<TState>(
                LogLevel logLevel,
                EventId eventId,
                TState state,
                Exception? exception,
                Func<TState, Exception?, string> formatter) => entries.Add(formatter(state, exception));
        }
    }
}
