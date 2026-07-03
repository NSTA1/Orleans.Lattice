using Orleans.Lattice.Explorer.Core.Connection;

namespace Orleans.Lattice.Explorer.Tests.Connection;

/// <summary>
/// A controllable <see cref="ILatticeCallCredentialProvider"/> test double: counts
/// header fetches and forced refreshes and lets a test choose whether a forced
/// refresh succeeds, so the connection's silent-refresh-then-retry path can be
/// driven deterministically.
/// </summary>
internal sealed class FakeCredentialProvider : ILatticeCallCredentialProvider
{
    public int HeaderCount { get; private set; }

    public int RefreshCount { get; private set; }

    public bool RefreshResult { get; set; } = true;

    public string? Header { get; set; } = "Bearer token";

    public ValueTask<string?> GetAuthorizationHeaderAsync(CancellationToken cancellationToken = default)
    {
        HeaderCount++;
        return new ValueTask<string?>(Header);
    }

    public ValueTask<bool> RefreshAsync(CancellationToken cancellationToken = default)
    {
        RefreshCount++;
        return new ValueTask<bool>(RefreshResult);
    }
}
