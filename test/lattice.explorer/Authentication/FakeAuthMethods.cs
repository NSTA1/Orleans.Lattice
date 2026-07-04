using Orleans.Lattice.Explorer.Core.Authentication;
using Orleans.Lattice.Explorer.Core.Connection;
using Orleans.Lattice.Explorer.Tests.Connection;

namespace Orleans.Lattice.Explorer.Tests.Authentication;

/// <summary>
/// A token-based <see cref="IExplorerAuthMethod"/> test double for a bespoke
/// third scheme, proving a new mechanism plugs into the explorer without any
/// change to core code. Its challenge yields a live bearer credential.
/// </summary>
internal sealed class FakeTokenAuthMethod : IExplorerAuthMethod
{
    public FakeTokenAuthMethod(string schemeId) => SchemeId = schemeId;

    public string SchemeId { get; }

    public int ChallengeCount { get; private set; }

    public bool CanHandle(string advertisedScheme)
        => string.Equals(advertisedScheme, SchemeId, StringComparison.OrdinalIgnoreCase);

    public Task<ExplorerAuthSignIn> ChallengeAsync(ExplorerAuthChallengeContext context, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(context);
        ChallengeCount++;
        return Task.FromResult(new ExplorerAuthSignIn
        {
            SchemeId = SchemeId,
            DisplayName = "custom-user",
            Authentication = LatticeCallAuthentication.Bearer(new FakeCredentialProvider()),
        });
    }
}

/// <summary>A configurable <see cref="IExplorerAuthSchemeProbe"/> test double.</summary>
internal sealed class FakeSchemeProbe : IExplorerAuthSchemeProbe
{
    public ExplorerAuthSchemeAdvertisement Result { get; set; } = ExplorerAuthSchemeAdvertisement.Empty;

    public int ProbeCount { get; private set; }

    public Task<ExplorerAuthSchemeAdvertisement> ProbeAsync(
        string address,
        bool allowUnencryptedHttp2 = false,
        CancellationToken cancellationToken = default)
    {
        ProbeCount++;
        return Task.FromResult(Result);
    }
}
