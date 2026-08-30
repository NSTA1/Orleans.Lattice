using System.Net;

namespace Orleans.Lattice.Api.Mcp.Telemetry.Azure.Tests;

/// <summary>
/// A test <see cref="HttpMessageHandler"/> that replies with a canned response and
/// snapshots what the telemetry proxy put on the wire - the request URI and every
/// header, flattened to name/value pairs - so a credential-isolation test can
/// assert exactly where a minted backend token did and did not appear.
/// </summary>
/// <remarks>
/// The snapshot is taken inside <see cref="SendAsync"/> rather than by holding the
/// <see cref="HttpRequestMessage"/>, because the proxy disposes the request as soon
/// as the call returns. Copying the values out keeps the assertions independent of
/// that lifetime.
/// </remarks>
internal sealed class CapturingHttpMessageHandler : HttpMessageHandler
{
    private readonly string _responseJson;
    private readonly HttpStatusCode _statusCode;

    public CapturingHttpMessageHandler(
        string responseJson = "{\"status\":\"success\",\"data\":{\"resultType\":\"vector\",\"result\":[]}}",
        HttpStatusCode statusCode = HttpStatusCode.OK)
    {
        _responseJson = responseJson;
        _statusCode = statusCode;
    }

    /// <summary>The number of requests the handler observed.</summary>
    public int RequestCount { get; private set; }

    /// <summary>The absolute URI of the most recent request, or null if none was sent.</summary>
    public Uri? LastRequestUri { get; private set; }

    /// <summary>
    /// Every header name/value pair on the most recent request, one entry per
    /// value, so a test can scan the whole header set rather than a single header.
    /// </summary>
    public IReadOnlyList<KeyValuePair<string, string>> LastHeaders { get; private set; } = [];

    /// <summary>
    /// The value of the most recent request's <c>Authorization</c> header rendered
    /// as it went on the wire (<c>scheme parameter</c>), or null when unset.
    /// </summary>
    public string? LastAuthorization { get; private set; }

    /// <inheritdoc />
    protected override Task<HttpResponseMessage> SendAsync(
        HttpRequestMessage request,
        CancellationToken cancellationToken)
    {
        RequestCount++;
        LastRequestUri = request.RequestUri;
        LastAuthorization = request.Headers.Authorization is { } auth
            ? $"{auth.Scheme} {auth.Parameter}"
            : null;

        var headers = new List<KeyValuePair<string, string>>();
        foreach (var header in request.Headers)
        {
            foreach (var value in header.Value)
            {
                headers.Add(new KeyValuePair<string, string>(header.Key, value));
            }
        }

        LastHeaders = headers;

        return Task.FromResult(new HttpResponseMessage(_statusCode)
        {
            Content = new StringContent(_responseJson),
        });
    }
}
