using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Orleans.Lattice.ReferenceArchitecture.Hosting;

/// <summary>
/// Wires path-scoped suppression of framework request-pipeline log output for the
/// platform liveness/health-probe path.
/// <para>
/// Azure Front Door and Container Apps probe <c>/health</c> several times a second
/// per replica. Every probe would otherwise emit a burst of informational
/// framework log lines (<c>Microsoft.AspNetCore.Hosting.Diagnostics</c> "Request
/// starting"/"Request finished", routing, and - when the probe is challenged - auth
/// noise) that carry no diagnostic value and dominate log storage at scale. This
/// helper drops the verbose (below <see cref="LogLevel.Warning"/>) log entries that
/// are written while serving a request to the health path, so real (non-probe)
/// requests keep full logging and any genuine warning/error on the health endpoint
/// still surfaces.
/// </para>
/// </summary>
public static class HealthProbeRequestLoggingExtensions
{
    /// <summary>
    /// Decorates every registered logger provider so that informational and more
    /// verbose log entries written while serving a request whose path starts with
    /// <paramref name="healthPath"/> are suppressed. Warnings and errors, and all
    /// logs outside a request (background/startup), are left untouched.
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
                    return new HealthProbeFilteringLoggerProvider(inner, accessor, healthPath);
                },
                descriptor.Lifetime);
        }

        return builder;
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
