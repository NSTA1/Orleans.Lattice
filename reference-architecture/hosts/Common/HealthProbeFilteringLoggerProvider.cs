using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;

namespace Orleans.Lattice.ReferenceArchitecture.Hosting;

/// <summary>
/// A logger provider decorator that suppresses verbose (below
/// <see cref="LogLevel.Warning"/>) log entries written while serving a request to
/// the configured health-probe path. See
/// <see cref="HealthProbeRequestLoggingExtensions.SuppressHealthProbeRequestLogs"/>.
/// </summary>
internal sealed class HealthProbeFilteringLoggerProvider : ILoggerProvider, ISupportExternalScope
{
    private readonly ILoggerProvider _inner;
    private readonly IHttpContextAccessor _httpContextAccessor;
    private readonly string _healthPath;

    public HealthProbeFilteringLoggerProvider(
        ILoggerProvider inner,
        IHttpContextAccessor httpContextAccessor,
        string healthPath)
    {
        _inner = inner;
        _httpContextAccessor = httpContextAccessor;
        _healthPath = healthPath;
    }

    public ILogger CreateLogger(string categoryName) =>
        new HealthProbeFilteringLogger(_inner.CreateLogger(categoryName), _httpContextAccessor, _healthPath);

    public void SetScopeProvider(IExternalScopeProvider scopeProvider)
    {
        if (_inner is ISupportExternalScope supportsScope)
        {
            supportsScope.SetScopeProvider(scopeProvider);
        }
    }

    public void Dispose() => _inner.Dispose();

    private sealed class HealthProbeFilteringLogger : ILogger
    {
        private readonly ILogger _inner;
        private readonly IHttpContextAccessor _httpContextAccessor;
        private readonly string _healthPath;

        public HealthProbeFilteringLogger(
            ILogger inner,
            IHttpContextAccessor httpContextAccessor,
            string healthPath)
        {
            _inner = inner;
            _httpContextAccessor = httpContextAccessor;
            _healthPath = healthPath;
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
            if (IsHealthProbeNoise(logLevel))
            {
                return;
            }

            _inner.Log(logLevel, eventId, state, exception, formatter);
        }

        private bool IsHealthProbeNoise(LogLevel logLevel)
        {
            // Always let warnings and errors through, even on the health path, so a
            // genuine probe failure is never hidden.
            if (logLevel >= LogLevel.Warning)
            {
                return false;
            }

            // Logs written outside an HTTP request (startup, background workers) are
            // never health-probe noise.
            var context = _httpContextAccessor.HttpContext;
            if (context is null)
            {
                return false;
            }

            return context.Request.Path.StartsWithSegments(_healthPath, StringComparison.OrdinalIgnoreCase);
        }
    }
}
