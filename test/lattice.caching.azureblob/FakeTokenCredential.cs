using Azure.Core;

namespace Orleans.Lattice.Caching.AzureBlob.Tests;

/// <summary>
/// A no-op <see cref="TokenCredential"/> for option-validation and
/// client-construction tests that never issue a real request.
/// </summary>
internal sealed class FakeTokenCredential : TokenCredential
{
    public override AccessToken GetToken(TokenRequestContext requestContext, CancellationToken cancellationToken) =>
        new("fake", DateTimeOffset.MaxValue);

    public override ValueTask<AccessToken> GetTokenAsync(TokenRequestContext requestContext, CancellationToken cancellationToken) =>
        new(new AccessToken("fake", DateTimeOffset.MaxValue));
}
