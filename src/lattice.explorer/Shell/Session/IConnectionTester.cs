using Orleans.Lattice.Explorer.Core.Configuration;

namespace Orleans.Lattice.Explorer.Shell.Session;

/// <summary>
/// Tests whether a candidate connection configuration reaches a Lattice API
/// endpoint, without persisting it or disturbing the circuit's live connection.
/// </summary>
internal interface IConnectionTester
{
    /// <summary>
    /// Probes the endpoint <paramref name="configuration"/> names, anonymously,
    /// and reports what it answered.
    /// </summary>
    /// <param name="configuration">The candidate configuration. Its endpoint has already passed transport validation.</param>
    /// <param name="cancellationToken">Cancels the probe.</param>
    /// <returns>What the endpoint answered.</returns>
    Task<ConnectionTestResult> TestAsync(ExplorerConfiguration configuration, CancellationToken cancellationToken = default);
}
