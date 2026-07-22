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

    private static IHttpContextAccessor AccessorFor(string? path)
    {
        var accessor = new HttpContextAccessor();
        if (path is not null)
        {
            var context = new DefaultHttpContext();
            context.Request.Path = path;
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
