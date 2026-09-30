using Orleans.Lattice.Explorer.Core.Configuration;
using Orleans.Lattice.Explorer.Core.Connection;

namespace Orleans.Lattice.Explorer.UI.Session;

/// <summary>
/// The default <see cref="IConnectionTester"/>: a throwaway Core
/// <see cref="LatticeStateConnection"/>, configured, probed and disposed.
/// </summary>
/// <remarks>
/// <para>
/// The probe dials, from the head, an address the visitor typed, so it runs only
/// when the head accepts browser-driven endpoint configuration
/// (<see cref="SessionEndpointConfigurationOptions"/>). On any other head it is
/// refused before anything is dialled, whichever caller asked for it.
/// </para>
/// <para>
/// The probe is anonymous on purpose. The endpoint under test is whatever the
/// operator has just typed, and sending the circuit's current credential to an
/// unconfirmed address would hand it to whoever answers there. An
/// authentication refusal still proves the endpoint is up, so it is reported as
/// <see cref="ConnectionTestOutcome.SignInRequired"/>, not as a failure. The
/// configuration's metadata headers are dropped with the credential.
/// </para>
/// <para>
/// The configuration's non-secret transport headers (a fronting proxy may refuse a
/// request without its origin header) belong to the endpoint the head is
/// configured for. They are kept only when the probe targets that same endpoint,
/// and dropped for any other, so an operator's origin-lock header is never sent to
/// a host the visitor chose.
/// </para>
/// <para>
/// The result carries only an outcome. The endpoint's own status text never leaves
/// the tester.
/// </para>
/// </remarks>
/// <param name="explorer">The circuit's session, whose current configuration names the configured endpoint.</param>
/// <param name="options">Whether the head accepts interactive endpoint configuration.</param>
internal sealed class LatticeConnectionTester(IExplorerSession explorer, SessionEndpointConfigurationOptions options) : IConnectionTester
{
    /// <summary>The message a refused probe throws with.</summary>
    internal const string RefusalMessage = "This Explorer does not accept endpoint configuration from the browser, so it does not test endpoints either.";

    private static readonly TimeSpan ProbeBudget = TimeSpan.FromSeconds(15);

    /// <inheritdoc />
    public async Task<ConnectionTestResult> TestAsync(ExplorerConfiguration configuration, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(configuration);

        if (!options.AllowInteractiveEndpointConfiguration)
        {
            throw new InvalidOperationException(RefusalMessage);
        }

        var settings = ProbeSettings(configuration, explorer.Current?.Endpoint);

        using var budget = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        budget.CancelAfter(ProbeBudget);

        await using var connection = new LatticeStateConnection();
        try
        {
            await connection.ConfigureAsync(settings, budget.Token);
        }
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
        {
            return new ConnectionTestResult(ConnectionTestOutcome.Unreachable);
        }

        return Classify(connection.Status);
    }

    /// <summary>
    /// The settings a probe of <paramref name="candidate"/> is sent with: anonymous,
    /// and carrying the transport headers only when the candidate is the configured
    /// endpoint.
    /// </summary>
    /// <param name="candidate">The configuration under test.</param>
    /// <param name="configuredEndpoint">The endpoint the head is configured for, or <see langword="null"/> when none is.</param>
    /// <returns>The probe's connection settings.</returns>
    internal static LatticeConnectionSettings ProbeSettings(ExplorerConfiguration candidate, string? configuredEndpoint)
    {
        ArgumentNullException.ThrowIfNull(candidate);

        var settings = candidate.ToConnectionSettings() with { Authentication = null };
        return IsSameEndpoint(candidate.Endpoint, configuredEndpoint)
            ? settings
            : settings with { TransportHeaders = null };
    }

    /// <summary>Classifies the status a probe left behind, discarding its message.</summary>
    /// <param name="status">The throwaway connection's status after its first probe.</param>
    /// <returns>The test result.</returns>
    internal static ConnectionTestResult Classify(LatticeConnectionStatus status)
    {
        ArgumentNullException.ThrowIfNull(status);

        return status switch
        {
            { State: LatticeConnectionState.Connected } => new ConnectionTestResult(ConnectionTestOutcome.Reachable),
            { RequiresAuthentication: true } => new ConnectionTestResult(ConnectionTestOutcome.SignInRequired),
            _ => new ConnectionTestResult(ConnectionTestOutcome.Unreachable),
        };
    }

    // Deliberately conservative, like the sign-in's endpoint binding: anything not
    // recognisably the same endpoint - including an absent one - counts as different.
    private static bool IsSameEndpoint(string? candidate, string? configured)
    {
        if (string.IsNullOrWhiteSpace(candidate) || string.IsNullOrWhiteSpace(configured))
        {
            return false;
        }

        return string.Equals(
            candidate.Trim().TrimEnd('/'),
            configured.Trim().TrimEnd('/'),
            StringComparison.OrdinalIgnoreCase);
    }
}
