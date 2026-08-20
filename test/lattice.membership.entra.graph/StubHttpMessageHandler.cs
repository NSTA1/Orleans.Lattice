using System.Net;
using System.Text;

namespace Orleans.Lattice.Membership.Entra.Graph.Tests;

/// <summary>
/// A deterministic <see cref="HttpMessageHandler"/> test double that serves a queue
/// of canned HTTP responses (status + JSON body) in order, so a real
/// <c>GraphServiceClient</c> can be driven through its genuine request/serialize/
/// deserialize path without any live Microsoft Graph call. Each queued entry is
/// returned once, in FIFO order; every handled request is recorded for assertions.
/// </summary>
internal sealed class StubHttpMessageHandler : HttpMessageHandler
{
    private readonly Queue<(HttpStatusCode Status, string Json)> _responses = new();

    /// <summary>Every request the handler served, in order.</summary>
    public List<HttpRequestMessage> Requests { get; } = new();

    /// <summary>
    /// The request body text captured at send time for each served request (the
    /// request content is disposed by the SDK once the call returns, so it is read
    /// here rather than after the fact). <c>null</c> for a bodyless request.
    /// </summary>
    public List<string?> RequestBodies { get; } = new();

    /// <summary>The most recently served request, or <c>null</c> when none.</summary>
    public HttpRequestMessage? LastRequest => Requests.Count == 0 ? null : Requests[^1];

    /// <summary>The most recently served request's captured body, or <c>null</c>.</summary>
    public string? LastRequestBody => RequestBodies.Count == 0 ? null : RequestBodies[^1];

    /// <summary>Enqueues a JSON response body served with the given status code.</summary>
    public StubHttpMessageHandler Enqueue(HttpStatusCode status, string json)
    {
        _responses.Enqueue((status, json));
        return this;
    }

    /// <inheritdoc />
    protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
    {
        Requests.Add(request);
        RequestBodies.Add(request.Content is null
            ? null
            : await request.Content.ReadAsStringAsync(cancellationToken));

        var (status, json) = _responses.Count > 0
            ? _responses.Dequeue()
            : (HttpStatusCode.OK, "{}");

        var response = new HttpResponseMessage(status)
        {
            Content = new StringContent(json, Encoding.UTF8, "application/json"),
        };
        return response;
    }
}
