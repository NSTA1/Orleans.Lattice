using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Orleans.Lattice.ReferenceArchitecture.Hosting;

/// <summary>
/// Wires path-scoped suppression of framework request-pipeline log output for
/// high-frequency probe paths.
/// <para>
/// Two classes of caller probe a silo host continuously and would otherwise
/// dominate log storage with successful-request framework noise
/// (<c>Microsoft.AspNetCore.Hosting.Diagnostics</c> "Request starting"/"Request
/// finished", routing "Executing/Executed endpoint", and - when a probe is
/// challenged - auth noise), none of which carries diagnostic value:
/// </para>
/// <list type="bullet">
/// <item>Azure Front Door and Container Apps probe <c>/health</c> several times a
/// second per replica.</item>
/// <item>The cross-region replication engine transport (the anti-entropy digest
/// probe, peer high-water-mark polling, Merkle walk, and the live push/snapshot
/// channels) fires silo-to-silo on a fixed cadence regardless of activity.</item>
/// </list>
/// <para>
/// This helper drops the verbose (below <see cref="LogLevel.Warning"/>) log
/// entries written while <b>successfully</b> serving a request to one of the
/// configured probe paths, so real (non-probe) requests keep full logging, a
/// probe that draws a non-success response keeps full logging, and any genuine
/// warning/error still surfaces.
/// </para>
/// </summary>
public static class HealthProbeRequestLoggingExtensions
{
    /// <summary>
    /// Decorates every registered logger provider so that informational and more
    /// verbose log entries written while successfully serving a request whose path
    /// starts with any of <paramref name="probePaths"/> are suppressed. Warnings
    /// and errors, requests that drew a non-success (non-2xx) response, and all
    /// logs outside a request (background/startup), are left untouched.
    /// </summary>
    /// <param name="builder">The logging builder.</param>
    /// <param name="probePaths">
    /// The probe path prefixes to suppress. Matched on segment boundaries, so
    /// <c>/health</c> matches <c>/health</c> but not <c>/healthz</c>.
    /// </param>
    /// <returns>The logging builder, for chaining.</returns>
    public static ILoggingBuilder SuppressProbeRequestLogs(
        this ILoggingBuilder builder,
        params string[] probePaths)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(probePaths);
        if (probePaths.Length == 0)
        {
            throw new ArgumentException("At least one probe path is required.", nameof(probePaths));
        }

        foreach (var path in probePaths)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(path);
        }

        var paths = (string[])probePaths.Clone();

        // The decorator needs the ambient HttpContext to know which request is being
        // served; AddHttpContextAccessor is idempotent (TryAdd) so this is safe even
        // when the host already registers it.
        builder.Services.AddHttpContextAccessor();

        // Wrap each already-registered ILoggerProvider in the filtering decorator.
        // Providers are registered as an enumerable of ILoggerProvider (console,
        // debug, etc.); replacing each descriptor in place preserves its lifetime.
        for (var i = 0; i < builder.Services.Count; i++)
        {
            var descriptor = builder.Services[i];
            if (descriptor.ServiceType != typeof(ILoggerProvider))
            {
                continue;
            }

            builder.Services[i] = ServiceDescriptor.Describe(
                typeof(ILoggerProvider),
                sp =>
                {
                    var inner = (ILoggerProvider)Materialize(sp, descriptor);
                    var accessor = sp.GetRequiredService<IHttpContextAccessor>();
                    return new ProbeRequestFilteringLoggerProvider(inner, accessor, paths);
                },
                descriptor.Lifetime);
        }

        return builder;
    }

    /// <summary>
    /// Suppresses successful-request framework log noise for the platform
    /// liveness/health-probe path. A thin convenience over
    /// <see cref="SuppressProbeRequestLogs"/> for the common single-path case.
    /// </summary>
    /// <param name="builder">The logging builder.</param>
    /// <param name="healthPath">
    /// The health-probe path prefix to suppress (defaults to
    /// <see cref="FrontDoorOriginLockApplicationBuilderExtensions.HealthPath"/>).
    /// </param>
    /// <returns>The logging builder, for chaining.</returns>
    public static ILoggingBuilder SuppressHealthProbeRequestLogs(
        this ILoggingBuilder builder,
        string healthPath = FrontDoorOriginLockApplicationBuilderExtensions.HealthPath)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentException.ThrowIfNullOrWhiteSpace(healthPath);
        return builder.SuppressProbeRequestLogs(healthPath);
    }

    private static object Materialize(IServiceProvider serviceProvider, ServiceDescriptor descriptor)
    {
        if (descriptor.ImplementationInstance is not null)
        {
            return descriptor.ImplementationInstance;
        }

        if (descriptor.ImplementationFactory is not null)
        {
            return descriptor.ImplementationFactory(serviceProvider);
        }

        return ActivatorUtilities.CreateInstance(serviceProvider, descriptor.ImplementationType!);
    }
}
