using System.Globalization;
using System.Net;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Api.Mcp.Telemetry;

/// <summary>
/// The default <see cref="IPrometheusQueryClient"/>: an <see cref="HttpClient"/>
/// binding over the Prometheus HTTP API. Every request is built against the
/// configured backend base address and carries the configured <b>backend</b>
/// credential (bearer or basic stamped as an <c>Authorization</c> header;
/// mutual-TLS presented on the transport handler). The client takes no
/// dependency on any Lattice credential source, so the caller's Lattice
/// credential can never be forwarded to the backend.
/// </summary>
internal sealed class PrometheusQueryClient : IPrometheusQueryClient
{
    private readonly HttpClient _http;
    private readonly IOptions<LatticeApiMcpTelemetryOptions> _options;
    private readonly ITelemetryBackendTokenProvider? _tokenProvider;
    private readonly ILogger<PrometheusQueryClient>? _logger;

    /// <summary>
    /// Creates the client over a preconfigured <paramref name="http"/> (whose base
    /// address and timeout the registration wires from the options) and the
    /// telemetry <paramref name="options"/> the per-request backend credential is
    /// read from. The optional <paramref name="tokenProvider"/> supplies the
    /// rotating backend token when
    /// <see cref="LatticeApiMcpTelemetryOptions.AuthMode"/> is
    /// <see cref="LatticeTelemetryBackendAuthMode.DynamicBearer"/>; it is a backend
    /// credential seam and can never carry a Lattice caller credential.
    /// </summary>
    /// <param name="http">The HTTP client pointed at the backend base address.</param>
    /// <param name="options">The telemetry options carrying the backend credential.</param>
    /// <param name="tokenProvider">
    /// The dynamic backend-token source consulted only in
    /// <see cref="LatticeTelemetryBackendAuthMode.DynamicBearer"/> mode. Left
    /// <see langword="null"/> for every static auth mode.
    /// </param>
    /// <param name="logger">
    /// The optional server-side sink for backend-request faults. The MCP tool
    /// result deliberately carries a fixed, non-interpolated error message so the
    /// backend credential can never ride out on exception text, which makes this
    /// the only place the operator-facing detail is retained. Left
    /// <see langword="null"/> when the host registered no logging.
    /// </param>
    public PrometheusQueryClient(
        HttpClient http,
        IOptions<LatticeApiMcpTelemetryOptions> options,
        ITelemetryBackendTokenProvider? tokenProvider = null,
        ILogger<PrometheusQueryClient>? logger = null)
    {
        ArgumentNullException.ThrowIfNull(http);
        ArgumentNullException.ThrowIfNull(options);
        _http = http;
        _options = options;
        _tokenProvider = tokenProvider;
        _logger = logger;
    }

    /// <inheritdoc />
    public async Task<PrometheusQueryResponse> InstantQueryAsync(
        string query,
        DateTimeOffset? time,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(query);

        var uri = new StringBuilder("api/v1/query?query=").Append(Uri.EscapeDataString(query));
        if (time is { } at)
        {
            uri.Append("&time=").Append(FormatTimestamp(at));
        }

        using var document = await SendAsync(uri.ToString(), cancellationToken).ConfigureAwait(false);
        return ReadQueryEnvelope(document);
    }

    /// <inheritdoc />
    public async Task<PrometheusQueryResponse> RangeQueryAsync(
        string query,
        DateTimeOffset start,
        DateTimeOffset end,
        TimeSpan step,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(query);

        var uri = new StringBuilder("api/v1/query_range?query=")
            .Append(Uri.EscapeDataString(query))
            .Append("&start=").Append(FormatTimestamp(start))
            .Append("&end=").Append(FormatTimestamp(end))
            .Append("&step=").Append(
                step.TotalSeconds.ToString("0.###", CultureInfo.InvariantCulture));

        using var document = await SendAsync(uri.ToString(), cancellationToken).ConfigureAwait(false);
        return ReadQueryEnvelope(document);
    }

    /// <inheritdoc />
    public async Task<IReadOnlyList<string>> ListMetricNamesAsync(CancellationToken cancellationToken)
    {
        using var document = await SendAsync(
            "api/v1/label/__name__/values", cancellationToken).ConfigureAwait(false);

        if (!document.RootElement.TryGetProperty("data", out var data)
            || data.ValueKind != JsonValueKind.Array)
        {
            return [];
        }

        var names = new List<string>(data.GetArrayLength());
        foreach (var element in data.EnumerateArray())
        {
            if (element.GetString() is { } name)
            {
                names.Add(name);
            }
        }

        return names;
    }

    /// <inheritdoc />
    public async Task<PrometheusMetadataResponse> MetricMetadataAsync(
        string? metric,
        CancellationToken cancellationToken)
    {
        var path = metric is null
            ? "api/v1/metadata"
            : "api/v1/metadata?metric=" + Uri.EscapeDataString(metric);

        using var document = await SendAsync(path, cancellationToken).ConfigureAwait(false);
        return new PrometheusMetadataResponse(ReadStatus(document), CloneData(document));
    }

    private async Task<JsonDocument> SendAsync(string relativeUri, CancellationToken cancellationToken)
    {
        using var request = new HttpRequestMessage(HttpMethod.Get, relativeUri);

        try
        {
            // Inside the try: stamping resolves the backend credential and can
            // itself fault (an unregistered dynamic-token provider, an empty
            // minted token, or the provider's own auth failure). Those are pure
            // misconfigurations, and the caller-facing message now points the
            // operator at these logs - so they must actually land here.
            await StampBackendCredentialAsync(request, cancellationToken).ConfigureAwait(false);

            using var response = await _http
                .SendAsync(request, HttpCompletionOption.ResponseHeadersRead, cancellationToken)
                .ConfigureAwait(false);
            response.EnsureSuccessStatusCode();

            await using var stream = await response.Content
                .ReadAsStreamAsync(cancellationToken).ConfigureAwait(false);
            return await JsonDocument.ParseAsync(stream, cancellationToken: cancellationToken)
                .ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            // The only place the fault detail is retained. The MCP tool result
            // carries a fixed, non-interpolated message precisely because
            // exception text from an unowned handler in this pipeline can echo
            // the outbound Authorization header, so the detail must stay on the
            // server side of the trust boundary. A 404 is not a fault - the
            // metadata tool degrades it to an empty result - so it is logged at
            // debug rather than warning.
            //
            // Cold path only: the request-path substring is the one allocation
            // here and is computed only once a logger is present and the level
            // is actually enabled, so a disabled or absent logger costs nothing.
            var level = ex is HttpRequestException { StatusCode: HttpStatusCode.NotFound }
                ? LogLevel.Debug
                : LogLevel.Warning;
            if (_logger?.IsEnabled(level) == true)
            {
                _logger.Log(
                    level,
                    ex,
                    "Telemetry backend request to '{RequestPath}' failed.",
                    RequestPathOf(relativeUri));
            }

            throw;
        }
    }

    /// <summary>
    /// The path portion of a request URI, with the query string dropped so the
    /// log line names the backend endpoint without echoing the caller's PromQL
    /// expression. Allocates a substring only when the URI carries a query, and
    /// is reached only from the cold fault path.
    /// </summary>
    private static string RequestPathOf(string relativeUri)
    {
        var query = relativeUri.IndexOf('?', StringComparison.Ordinal);
        return query < 0 ? relativeUri : relativeUri[..query];
    }

    private async Task StampBackendCredentialAsync(
        HttpRequestMessage request,
        CancellationToken cancellationToken)
    {
        var options = _options.Value;
        switch (options.AuthMode)
        {
            case LatticeTelemetryBackendAuthMode.Bearer:
                if (!string.IsNullOrEmpty(options.Credential?.BearerToken))
                {
                    request.Headers.Authorization =
                        new AuthenticationHeaderValue("Bearer", options.Credential.BearerToken);
                }

                break;
            case LatticeTelemetryBackendAuthMode.Basic:
                if (!string.IsNullOrEmpty(options.Credential?.BasicUsername))
                {
                    var raw = $"{options.Credential.BasicUsername}:{options.Credential.BasicPassword}";
                    var token = Convert.ToBase64String(Encoding.UTF8.GetBytes(raw));
                    request.Headers.Authorization = new AuthenticationHeaderValue("Basic", token);
                }

                break;
            case LatticeTelemetryBackendAuthMode.DynamicBearer:
                if (_tokenProvider is null)
                {
                    throw new InvalidOperationException(
                        $"{nameof(LatticeTelemetryBackendAuthMode.DynamicBearer)} requires an "
                        + $"{nameof(ITelemetryBackendTokenProvider)} to be registered, but none was "
                        + "resolved. Register one (for Azure managed Prometheus, call "
                        + "AddAzureTelemetryBackendToken from the "
                        + "Orleans.Lattice.Api.Mcp.Telemetry.Azure package).");
                }

                var dynamicToken = await _tokenProvider
                    .GetAccessTokenAsync(cancellationToken).ConfigureAwait(false);
                if (string.IsNullOrEmpty(dynamicToken))
                {
                    throw new InvalidOperationException(
                        $"The registered {nameof(ITelemetryBackendTokenProvider)} returned an empty "
                        + $"backend token in {nameof(LatticeTelemetryBackendAuthMode.DynamicBearer)} mode. "
                        + "A telemetry query must not be sent to the backend without the bearer token it "
                        + "requires, so the request is failed closed rather than sent unauthenticated.");
                }

                request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", dynamicToken);

                break;
            case LatticeTelemetryBackendAuthMode.None:
            case LatticeTelemetryBackendAuthMode.MutualTls:
                // None: no header. MutualTls: the client certificate is presented
                // on the transport handler, not as a request header.
                break;
        }
    }

    private static PrometheusQueryResponse ReadQueryEnvelope(JsonDocument document)
        => new(ReadStatus(document), CloneData(document));

    private static string ReadStatus(JsonDocument document)
        => document.RootElement.TryGetProperty("status", out var status)
            ? status.GetString() ?? string.Empty
            : string.Empty;

    private static JsonElement CloneData(JsonDocument document)
        => document.RootElement.TryGetProperty("data", out var data)
            ? data.Clone()
            : default;

    private static string FormatTimestamp(DateTimeOffset value)
        => (value.ToUnixTimeMilliseconds() / 1000.0)
            .ToString("0.###", CultureInfo.InvariantCulture);
}
