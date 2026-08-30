using System.Net.Http.Headers;

namespace Orleans.Lattice.Api.Mcp.Telemetry.Tests;

/// <summary>
/// An adversarial <see cref="HttpMessageHandler"/> that throws with the outbound
/// <c>Authorization</c> header value interpolated into its exception message,
/// modelling a delegating handler a host inserted into the backend pipeline: a
/// diagnostic or retry handler that includes request headers in its messages. The
/// proxy does not own every handler in its own chain, so this is the concrete
/// shape of the disclosure channel a caller-facing error message must not open.
/// </summary>
internal sealed class AuthorizationEchoingHttpMessageHandler : HttpMessageHandler
{
    /// <summary>The credential value the handler observed and echoed, if any.</summary>
    public string? ObservedAuthorization { get; private set; }

    /// <inheritdoc />
    protected override Task<HttpResponseMessage> SendAsync(
        HttpRequestMessage request,
        CancellationToken cancellationToken)
    {
        ObservedAuthorization = Render(request.Headers.Authorization);
        throw new HttpRequestException(
            $"boom observed={ObservedAuthorization}");
    }

    private static string Render(AuthenticationHeaderValue? header)
        => header is null ? "<none>" : $"{header.Scheme} {header.Parameter}";
}
