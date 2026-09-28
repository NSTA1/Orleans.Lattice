using Orleans.Lattice.Explorer.Core.Session;

namespace Orleans.Lattice.Explorer.Tests.Shell.Session;

/// <summary>A fixed preference scope: one user on one cluster.</summary>
internal sealed class FixedPreferenceScopeProvider : IExplorerPreferenceScopeProvider
{
    /// <inheritdoc />
    public ExplorerPreferenceScopeIdentity Current { get; } = new("alice", "https://cluster.example:443");

    /// <inheritdoc />
    public event Action? ScopeChanged
    {
        add { }
        remove { }
    }
}
