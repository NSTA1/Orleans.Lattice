using Azure.Core;

namespace Orleans.Lattice.Membership.Entra.Graph.Tests;

/// <summary>
/// A <see cref="TokenCredential"/> test double that returns a fixed token without
/// contacting Azure, so the secret-less authentication path can be exercised
/// without a live managed identity.
/// </summary>
internal sealed class FakeTokenCredential : TokenCredential
{
    private readonly AccessToken _token = new("fake-token", DateTimeOffset.MaxValue);

    /// <inheritdoc />
    public override AccessToken GetToken(TokenRequestContext requestContext, CancellationToken cancellationToken)
        => _token;

    /// <inheritdoc />
    public override ValueTask<AccessToken> GetTokenAsync(TokenRequestContext requestContext, CancellationToken cancellationToken)
        => new(_token);
}
