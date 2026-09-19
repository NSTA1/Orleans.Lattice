using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Host;

/// <summary>
/// Registers the health-publication seam: the signal that holds the container's own
/// health verdict, the meter that puts that verdict onto the existing scrape, and the
/// publisher that refreshes both on a background cadence.
/// </summary>
/// <remarks>
/// <para>
/// Issue #2868 recorded 43 minutes during which this container's grain-liveness check
/// correctly reported unhealthy - it reads the reserved <c>sys-auth-policy</c> tree,
/// and that tree was wedged - while <c>/metrics</c> answered 200 with a full scrape
/// and every MCP call needing authorization returned 500. Detection was never the
/// defect. The verdict simply reached no consumer: it lived only in the container's
/// health log, and the scrape, which is the only externally reachable surface and the
/// one every dashboard and probe is pointed at, carried no health series at all.
/// </para>
/// <para>
/// Registration is factored out of the host builder so that the wiring is assertable
/// without standing up a silo. A publisher that is written but never registered
/// produces exactly the failure this seam exists to remove - an absent series that
/// reads as a healthy zero - so the registration is the part most worth a test.
/// </para>
/// </remarks>
internal static class RepoContextHealthPublication
{
    /// <summary>
    /// How long after startup the first background publication runs.
    /// </summary>
    /// <remarks>
    /// Deliberately not zero. The grain-liveness component grades a failing probe as
    /// degraded rather than unhealthy while the silo is still joining, so publishing
    /// immediately would spend the first observation on a verdict that is known in
    /// advance to be provisional.
    /// </remarks>
    internal static readonly TimeSpan PublishDelay = TimeSpan.FromSeconds(15);

    /// <summary>
    /// The interval between background publications.
    /// </summary>
    /// <remarks>
    /// The cadence is load-bearing, not a default left implicit. Health components are
    /// evaluated on demand, so without a timer the grain-liveness check - the only
    /// component that touches the authorization seam - runs solely when something
    /// probes it over HTTP. A deployment whose liveness probe is pointed at
    /// <c>/metrics</c>, which is the natural choice because it is the only exposed
    /// port, would otherwise never exercise that seam at all. It is deliberately no
    /// faster than the container healthcheck's own interval: the probe is a point
    /// read, but doubling its rate buys no detection latency that the healthcheck does
    /// not already provide.
    /// </remarks>
    internal static readonly TimeSpan PublishPeriod = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Adds the health signal, its meter, and the publisher that drives them.
    /// </summary>
    /// <param name="services">The container being built.</param>
    /// <returns>The same collection, for chaining.</returns>
    /// <remarks>
    /// The component set is read from the health-check registrations rather than
    /// listed here, so a component registered under a deployment-conditional branch is
    /// covered without this site knowing about it, and a component added later cannot
    /// be silently unreported.
    /// </remarks>
    internal static IServiceCollection AddRepoContextHealthPublication(this IServiceCollection services)
    {
        ArgumentNullException.ThrowIfNull(services);

        services.AddSingleton(sp => new RepoContextHealthSignal(
            sp.GetRequiredService<IOptions<HealthCheckServiceOptions>>()
                .Value.Registrations.Select(registration => registration.Name)));
        services.AddSingleton<RepoContextHealthMeter>();
        services.AddSingleton<IHealthCheckPublisher, RepoContextHealthPublisher>();
        services.Configure<HealthCheckPublisherOptions>(options =>
        {
            options.Delay = PublishDelay;
            options.Period = PublishPeriod;
        });

        return services;
    }
}
