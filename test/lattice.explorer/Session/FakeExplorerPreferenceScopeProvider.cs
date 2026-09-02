using Orleans.Lattice.Explorer.Core.Session;

namespace Orleans.Lattice.Explorer.Tests.Session;

/// <summary>
/// A directly driven <see cref="IExplorerPreferenceScopeProvider"/>, so a test
/// moves between identities explicitly rather than through a sign-in or a
/// configuration apply.
/// </summary>
internal sealed class FakeExplorerPreferenceScopeProvider : IExplorerPreferenceScopeProvider
{
    /// <inheritdoc />
    public ExplorerPreferenceScopeIdentity Current { get; private set; } =
        new("alice", "https://cluster-a");

    /// <inheritdoc />
    public event Action? ScopeChanged;

    /// <summary>Moves to a new identity and announces it.</summary>
    /// <param name="user">The signed-in user.</param>
    /// <param name="cluster">The connected cluster.</param>
    public void MoveTo(string user, string cluster)
    {
        Current = new ExplorerPreferenceScopeIdentity(user, cluster);
        ScopeChanged?.Invoke();
    }
}
