using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Tenancy;

/// <summary>
/// Logs the silo's tenancy posture once at start-up: the opt-in
/// <see cref="LatticeTenancyOptions.DelegatedAccessAdministrationEnabled"/> flag,
/// alongside the authorization posture line the auth add-on writes. The flag is off
/// by default and a disabled feature is otherwise silent - tenant groups, member
/// entries and tenant-tier rules are inert and unauthorable - so surfacing it in a
/// line an operator already reads makes the deployment's opt-in state discoverable
/// without inspecting configuration.
/// </summary>
/// <param name="logger">The logger the posture line is written to.</param>
/// <param name="options">The tenancy options.</param>
internal sealed class TenancyPostureLogger(
    ILogger<TenancyPostureLogger> logger,
    IOptionsMonitor<LatticeTenancyOptions> options) : IHostedService
{
    /// <inheritdoc />
    public Task StartAsync(CancellationToken cancellationToken)
    {
        logger.LogInformation(
            "Lattice tenancy posture: DelegatedAccessAdministrationEnabled={DelegatedAccessAdministrationEnabled}. "
                + "The flag is opt-in and off by default; while off, tenant groups, tenant member entries and "
                + "tenant-tier rules are inert and unauthorable, and active-tenant validation is the exact-id admin check.",
            options.CurrentValue.DelegatedAccessAdministrationEnabled);

        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}
