using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Auth;

/// <summary>
/// Logs the silo's effective authorization posture once at start-up: the default
/// effect and the two opt-in tier flags
/// (<see cref="LatticeAuthOptions.AllTreesGrantsEnabled"/> and
/// <see cref="LatticeAuthOptions.AccessAdministrationDelegationEnabled"/>). Both
/// tiers are off by default, and a disabled tier is otherwise silent - an
/// all-trees grant is inert and a delegation grant is unauthorable - so surfacing
/// the posture in a line an operator already reads makes the deployment's opt-in
/// state discoverable without inspecting configuration.
/// </summary>
internal sealed class AuthPostureLogger(
    ILogger<AuthPostureLogger> logger,
    IOptionsMonitor<LatticeAuthOptions> options) : IHostedService
{
    /// <inheritdoc />
    public Task StartAsync(CancellationToken cancellationToken)
    {
        var o = options.CurrentValue;
        logger.LogInformation(
            "Lattice authorization posture: DefaultEffect={DefaultEffect}, AllTreesGrantsEnabled={AllTreesGrantsEnabled}, "
                + "AccessAdministrationDelegationEnabled={AccessAdministrationDelegationEnabled}. "
                + "Both tier flags are opt-in and off by default; while off, an all-trees ('*') data grant is inert "
                + "and an access-administration delegation grant is unauthorable.",
            o.DefaultEffect,
            o.AllTreesGrantsEnabled,
            o.AccessAdministrationDelegationEnabled);

        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}
