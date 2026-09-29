using Orleans.Lattice.Explorer.Core.Configuration;
using Orleans.Lattice.Explorer.Core.Connection;

namespace Orleans.Lattice.Explorer.UI.Session;

/// <summary>
/// The default <see cref="IConnectionTester"/>: a throwaway Core
/// <see cref="LatticeStateConnection"/>, configured, probed and disposed.
/// </summary>
/// <remarks>
/// <para>
/// The probe is anonymous on purpose. The endpoint under test is whatever the
/// operator has just typed, and sending the circuit's current credential to an
/// unconfirmed address would hand it to whoever answers there. An
/// authentication refusal still proves the endpoint is up, so it is reported as
/// <see cref="ConnectionTestOutcome.SignInRequired"/>, not as a failure.
/// </para>
/// <para>
/// The configuration's non-secret transport headers are kept (a fronting proxy
/// may refuse a request without its origin header), but its metadata headers
/// are dropped with the credential.
/// </para>
/// </remarks>
internal sealed class LatticeConnectionTester : IConnectionTester
{
    private static readonly TimeSpan ProbeBudget = TimeSpan.FromSeconds(15);

    /// <inheritdoc />
    public async Task<ConnectionTestResult> TestAsync(ExplorerConfiguration configuration, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(configuration);

        var settings = configuration.ToConnectionSettings() with { Authentication = null };

        using var budget = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        budget.CancelAfter(ProbeBudget);

        await using var connection = new LatticeStateConnection();
        try
        {
            await connection.ConfigureAsync(settings, budget.Token);
        }
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
        {
            return new ConnectionTestResult(ConnectionTestOutcome.Unreachable, "The endpoint did not answer in time.");
        }

        return Classify(connection.Status);
    }

    /// <summary>Classifies the status a probe left behind.</summary>
    /// <param name="status">The throwaway connection's status after its first probe.</param>
    /// <returns>The test result.</returns>
    internal static ConnectionTestResult Classify(LatticeConnectionStatus status)
    {
        ArgumentNullException.ThrowIfNull(status);

        return status switch
        {
            { State: LatticeConnectionState.Connected } => new ConnectionTestResult(ConnectionTestOutcome.Reachable),
            { RequiresAuthentication: true } => new ConnectionTestResult(ConnectionTestOutcome.SignInRequired, status.Message),
            _ => new ConnectionTestResult(ConnectionTestOutcome.Unreachable, status.Message),
        };
    }
}
