using Azure.Core;

namespace Orleans.Lattice.Api.Mcp.Tests;

/// <summary>
/// A test <see cref="TokenCredential"/> that hands out caller-controlled access
/// tokens, counts synchronous acquisitions, records the scopes it was asked for,
/// and can be made to throw so a test can exercise the fail-closed path.
/// </summary>
internal sealed class FakeTokenCredential : TokenCredential
{
    private readonly Func<int, AccessToken> _tokenFactory;
    private readonly bool _throwOnAcquire;

    public FakeTokenCredential(Func<int, AccessToken> tokenFactory, bool throwOnAcquire = false)
    {
        _tokenFactory = tokenFactory;
        _throwOnAcquire = throwOnAcquire;
    }

    /// <summary>The number of times a token was acquired.</summary>
    public int CallCount { get; private set; }

    /// <summary>The scopes requested on the most recent acquisition.</summary>
    public string[] LastScopes { get; private set; } = [];

    public override AccessToken GetToken(TokenRequestContext requestContext, CancellationToken cancellationToken)
    {
        LastScopes = requestContext.Scopes;
        if (_throwOnAcquire)
        {
            throw new InvalidOperationException("credential unavailable");
        }

        var token = _tokenFactory(CallCount);
        CallCount++;
        return token;
    }

    public override ValueTask<AccessToken> GetTokenAsync(
        TokenRequestContext requestContext,
        CancellationToken cancellationToken)
        => new(GetToken(requestContext, cancellationToken));
}
