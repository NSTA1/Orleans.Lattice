using Azure.Core;

namespace Orleans.Lattice.Api.Mcp.Telemetry.Azure.Tests;

/// <summary>
/// A test <see cref="TokenCredential"/> that hands out caller-controlled access
/// tokens, counts acquisitions, records the scopes it was asked for, and can be
/// gated to block a pending acquisition so a test can exercise concurrent callers.
/// </summary>
internal sealed class FakeTokenCredential : TokenCredential
{
    private readonly Func<int, AccessToken> _tokenFactory;
    private readonly TaskCompletionSource? _gate;

    public FakeTokenCredential(Func<int, AccessToken> tokenFactory, TaskCompletionSource? gate = null)
    {
        _tokenFactory = tokenFactory;
        _gate = gate;
    }

    /// <summary>The number of times a token was acquired.</summary>
    public int CallCount { get; private set; }

    /// <summary>The scopes requested on the most recent acquisition.</summary>
    public string[] LastScopes { get; private set; } = [];

    public override async ValueTask<AccessToken> GetTokenAsync(
        TokenRequestContext requestContext,
        CancellationToken cancellationToken)
    {
        if (_gate is not null)
        {
            await _gate.Task.WaitAsync(cancellationToken).ConfigureAwait(false);
        }

        return Issue(requestContext);
    }

    public override AccessToken GetToken(
        TokenRequestContext requestContext,
        CancellationToken cancellationToken)
    {
        _gate?.Task.GetAwaiter().GetResult();
        return Issue(requestContext);
    }

    private AccessToken Issue(TokenRequestContext requestContext)
    {
        LastScopes = requestContext.Scopes;
        var token = _tokenFactory(CallCount);
        CallCount++;
        return token;
    }
}
