using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;

namespace Orleans.Lattice.ReferenceArchitecture.Hosting;

/// <summary>
/// A logger provider decorator that suppresses verbose (below
/// <see cref="LogLevel.Warning"/>) log entries written while serving a request
/// whose path matches any of the configured high-frequency probe paths. See
/// <see cref="HealthProbeRequestLoggingExtensions.SuppressProbeRequestLogs"/>.
/// </summary>
internal sealed class ProbeRequestFilteringLoggerProvider : ILoggerProvider, ISupportExternalScope
{
    private readonly ILoggerProvider _inner;
    private readonly IHttpContextAccessor _httpContextAccessor;
    private readonly IReadOnlyList<string> _paths;

    public ProbeRequestFilteringLoggerProvider(
        ILoggerProvider inner,
        IHttpContextAccessor httpContextAccessor,
        IReadOnlyList<string> paths)
    {
        _inner = inner;
        _httpContextAccessor = httpContextAccessor;
        _paths = paths;
    }

    public ILogger CreateLogger(string categoryName) =>
        new ProbeRequestFilteringLogger(_inner.CreateLogger(categoryName), _httpContextAccessor, _paths);

    public void SetScopeProvider(IExternalScopeProvider scopeProvider)
    {
        if (_inner is ISupportExternalScope supportsScope)
        {
            supportsScope.SetScopeProvider(scopeProvider);
        }
    }

    public void Dispose() => _inner.Dispose();

    private sealed class ProbeRequestFilteringLogger : ILogger
    {
        private readonly ILogger _inner;
        private readonly IHttpContextAccessor _httpContextAccessor;
        private readonly IReadOnlyList<string> _paths;

        public ProbeRequestFilteringLogger(
            ILogger inner,
            IHttpContextAccessor httpContextAccessor,
            IReadOnlyList<string> paths)
        {
            _inner = inner;
            _httpContextAccessor = httpContextAccessor;
            _paths = paths;
        }

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull =>
            _inner.BeginScope(state);

        public bool IsEnabled(LogLevel logLevel) => _inner.IsEnabled(logLevel);

        public void Log<TState>(
            LogLevel logLevel,
            EventId eventId,
            TState state,
            Exception? exception,
            Func<TState, Exception?, string> formatter)
        {
            if (IsProbeNoise(logLevel))
            {
                return;
            }

            _inner.Log(logLevel, eventId, state, exception, formatter);
        }

        private bool IsProbeNoise(LogLevel logLevel)
        {
            // Always let warnings and errors through, even on a probe path, so a
            // genuine failure is never hidden.
            if (logLevel >= LogLevel.Warning)
            {
                return false;
            }

            // Logs written outside an HTTP request (startup, background workers) are
            // never probe noise.
            var context = _httpContextAccessor.HttpContext;
            if (context is null)
            {
                return false;
            }

            var path = context.Request.Path;
            for (var i = 0; i < _paths.Count; i++)
            {
                if (path.StartsWithSegments(_paths[i], StringComparison.OrdinalIgnoreCase))
                {
                    // Only a SUCCESSFUL probe is noise. If this request drew a
                    // non-success response, keep its full request-pipeline logging so
                    // a failing probe is never hidden. The status is the framework's
                    // default 200 until the response starts, so the pre-response
                    // "Request starting"/routing lines of a call that later fails are
                    // suppressed, but the terminal "Request finished - <code>" line
                    // and any error carry the failure through.
                    return IsSuccessStatusCode(context.Response.StatusCode);
                }
            }

            return false;
        }

        private static bool IsSuccessStatusCode(int statusCode) =>
            statusCode is >= 200 and <= 299;
    }
}
