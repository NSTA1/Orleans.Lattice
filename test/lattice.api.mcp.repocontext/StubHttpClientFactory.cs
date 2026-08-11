namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests;

/// <summary>
/// A test <see cref="IHttpClientFactory"/> that hands out
/// <see cref="HttpClient"/> instances backed by a supplied stub handler, so the
/// provider's factory-per-call pattern can be exercised in isolation.
/// </summary>
internal sealed class StubHttpClientFactory : IHttpClientFactory
{
    private readonly HttpMessageHandler _handler;

    /// <summary>
    /// Creates the factory over a single shared stub handler.
    /// </summary>
    /// <param name="handler">The handler every created client uses.</param>
    public StubHttpClientFactory(HttpMessageHandler handler)
    {
        ArgumentNullException.ThrowIfNull(handler);
        _handler = handler;
    }

    /// <inheritdoc />
    public HttpClient CreateClient(string name) => new(_handler, disposeHandler: false);
}
