using System.Net;

namespace Orleans.Lattice.Api.Mcp.Telemetry.Tests;

/// <summary>
/// A test <see cref="HttpMessageHandler"/> that records every outgoing request
/// and replies with a canned response, so a test can assert what the
/// <see cref="PrometheusQueryClient"/> put on the wire without a real network.
/// </summary>
internal sealed class CapturingHttpMessageHandler : HttpMessageHandler
{
    private readonly string _responseJson;
    private readonly HttpStatusCode _statusCode;

    public CapturingHttpMessageHandler(
        string responseJson = "{\"status\":\"success\",\"data\":{}}",
        HttpStatusCode statusCode = HttpStatusCode.OK)
    {
        _responseJson = responseJson;
        _statusCode = statusCode;
    }

    /// <summary>The most recent request the handler observed.</summary>
    public HttpRequestMessage? LastRequest { get; private set; }

    /// <summary>
    /// The number of requests the handler observed, so a test can assert that a
    /// rejected request never reached the backend at all.
    /// </summary>
    public int RequestCount { get; private set; }

    /// <summary>
    /// The most recent request's <c>Authorization</c> header rendered as it went on
    /// the wire (<c>scheme parameter</c>), or null when unset. Captured here rather
    /// than read off <see cref="LastRequest"/> because the proxy disposes the
    /// request as soon as the call returns.
    /// </summary>
    public string? LastAuthorization { get; private set; }

    /// <inheritdoc />
    protected override Task<HttpResponseMessage> SendAsync(
        HttpRequestMessage request,
        CancellationToken cancellationToken)
    {
        LastRequest = request;
        RequestCount++;
        LastAuthorization = request.Headers.Authorization is { } auth
            ? $"{auth.Scheme} {auth.Parameter}"
            : null;

        return Task.FromResult(new HttpResponseMessage(_statusCode)
        {
            Content = new StringContent(_responseJson),
        });
    }
}
