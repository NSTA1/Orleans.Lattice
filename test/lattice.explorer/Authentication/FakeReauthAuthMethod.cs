using Orleans.Lattice.Explorer.Core.Authentication;
using Orleans.Lattice.Explorer.Core.Connection;

namespace Orleans.Lattice.Explorer.Tests.Authentication;

/// <summary>
/// A credential provider that also exposes the re-authentication signal, so a
/// test can drive the revoked transition deterministically and assert that the
/// auth session re-raises it.
/// </summary>
internal sealed class FakeReauthCredentialProvider : ILatticeCallCredentialProvider, IReauthRequiredSource, IDisposable
{
    public bool Disposed { get; private set; }

    public event Action? ReauthRequired;

    public ValueTask<string?> GetAuthorizationHeaderAsync(CancellationToken cancellationToken = default)
        => new((string?)"******");

    public ValueTask<bool> RefreshAsync(CancellationToken cancellationToken = default)
        => new(true);

    /// <summary>Simulates the provider latching into its revoked state.</summary>
    public void TriggerReauth() => ReauthRequired?.Invoke();

    public void Dispose() => Disposed = true;
}

/// <summary>
/// A token-based auth method whose sign-in wraps a
/// <see cref="FakeReauthCredentialProvider"/>, so the session's hook of the
/// provider's re-authentication signal can be exercised.
/// </summary>
internal sealed class FakeReauthAuthMethod : IExplorerAuthMethod
{
    public FakeReauthAuthMethod(string schemeId) => SchemeId = schemeId;

    public string SchemeId { get; }

    /// <summary>The provider handed to the most recent sign-in.</summary>
    public FakeReauthCredentialProvider? LastProvider { get; private set; }

    public bool CanHandle(string advertisedScheme)
        => string.Equals(advertisedScheme, SchemeId, StringComparison.OrdinalIgnoreCase);

    public Task<ExplorerAuthSignIn> ChallengeAsync(ExplorerAuthChallengeContext context, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(context);
        var provider = new FakeReauthCredentialProvider();
        LastProvider = provider;
        return Task.FromResult(new ExplorerAuthSignIn
        {
            SchemeId = SchemeId,
            DisplayName = "reauth-user",
            Authentication = LatticeCallAuthentication.Bearer(provider),
        });
    }
}
