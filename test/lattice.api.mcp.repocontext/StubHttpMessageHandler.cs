namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests;

/// <summary>
/// A test <see cref="HttpMessageHandler"/> that answers every request from a
/// caller-supplied function, so provider tests exercise the wire contract and the
/// fail-closed paths without a real endpoint.
/// </summary>
internal sealed class StubHttpMessageHandler : HttpMessageHandler
{
    private readonly Func<HttpRequestMessage, HttpResponseMessage> _responder;

    /// <summary>
    /// Creates the handler.
    /// </summary>
    /// <param name="responder">Produces the response (or throws) for each request.</param>
    public StubHttpMessageHandler(Func<HttpRequestMessage, HttpResponseMessage> responder)
    {
        ArgumentNullException.ThrowIfNull(responder);
        _responder = responder;
    }

    /// <inheritdoc />
    protected override Task<HttpResponseMessage> SendAsync(
        HttpRequestMessage request, CancellationToken cancellationToken)
        => Task.FromResult(_responder(request));
}
