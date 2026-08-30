using System.Net;

namespace Orleans.Lattice.Api.Telemetry.Tests;

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

    /// <inheritdoc />
    protected override Task<HttpResponseMessage> SendAsync(
        HttpRequestMessage request,
        CancellationToken cancellationToken)
    {
        LastRequest = request;
        return Task.FromResult(new HttpResponseMessage(_statusCode)
        {
            Content = new StringContent(_responseJson),
        });
    }
}
