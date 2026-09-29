using Orleans.Lattice.Explorer.Core.Configuration;
using Orleans.Lattice.Explorer.UI.Session;

namespace Orleans.Lattice.Explorer.Tests.UI.Session;

/// <summary>A connection tester that answers a fixed result and records what it was asked to test.</summary>
internal sealed class FakeConnectionTester : IConnectionTester
{
    /// <summary>The result every test answers.</summary>
    public ConnectionTestResult Result { get; set; } = new(ConnectionTestOutcome.Reachable);

    /// <summary>When set, every test throws it.</summary>
    public Exception? Failure { get; set; }

    /// <summary>Every configuration tested.</summary>
    public List<ExplorerConfiguration> Tested { get; } = [];

    /// <inheritdoc />
    public Task<ConnectionTestResult> TestAsync(ExplorerConfiguration configuration, CancellationToken cancellationToken = default)
    {
        Tested.Add(configuration);
        return Failure is not null ? Task.FromException<ConnectionTestResult>(Failure) : Task.FromResult(Result);
    }
}
