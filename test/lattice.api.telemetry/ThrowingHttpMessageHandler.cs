namespace Orleans.Lattice.Api.Telemetry.Tests;

/// <summary>
/// A test <see cref="HttpMessageHandler"/> that always faults with a supplied
/// exception, so a test can drive the backend proxy's fault paths (a transport
/// failure, or a timeout that surfaces as a cancellation the caller never asked
/// for) without a real network.
/// </summary>
internal sealed class ThrowingHttpMessageHandler(Exception fault) : HttpMessageHandler
{
    /// <inheritdoc />
    protected override Task<HttpResponseMessage> SendAsync(
        HttpRequestMessage request,
        CancellationToken cancellationToken)
        => Task.FromException<HttpResponseMessage>(fault);
}
