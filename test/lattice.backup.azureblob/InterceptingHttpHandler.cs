namespace Orleans.Lattice.Backup.AzureBlob.Tests;

/// <summary>
/// An <see cref="HttpMessageHandler"/> that lets a test intercept the blob-service
/// requests the Azure SDK issues, either short-circuiting one with a synthetic
/// response, rewriting its response body, or pausing it, before falling through to
/// the live emulator.
/// <para>
/// The sink's tolerance paths - a manifest deleted between listing and read, a chunk
/// length prefix split across two network reads, a second caller arriving while the
/// container is still being created - are unreachable through ordinary emulator
/// traffic. They depend on a single call behaving differently while everything around
/// it succeeds, which is exactly what intercepting at the transport reproduces, and
/// deterministically rather than by racing clients and hoping.
/// </para>
/// <para>
/// Requests the interceptor declines (by returning <see langword="null"/>) are
/// forwarded unchanged, so every other call still talks to the real emulator and the
/// assertions remain end-to-end.
/// </para>
/// </summary>
internal sealed class InterceptingHttpHandler : DelegatingHandler
{
    public InterceptingHttpHandler()
        : base(new HttpClientHandler())
    {
    }

    /// <summary>
    /// Invoked for every outbound request. Return a response to short-circuit the
    /// call, or <see langword="null"/> to let it reach the emulator. The delegate may
    /// await before returning <see langword="null"/>, which pauses the real call and
    /// is how the container-initialisation race is made deterministic.
    /// </summary>
    public Func<HttpRequestMessage, CancellationToken, Task<HttpResponseMessage?>>? Interceptor { get; set; }

    /// <summary>
    /// Invoked for every response the emulator returns. When it yields true, the
    /// response body is re-wrapped so it is delivered a byte at a time.
    /// </summary>
    public Func<HttpRequestMessage, bool>? DribbleResponseBody { get; set; }

    /// <summary>True when the request is the SDK's create-container call.</summary>
    public static bool IsContainerCreate(HttpRequestMessage request) =>
        request.Method == HttpMethod.Put
        && (request.RequestUri?.Query.Contains("restype=container", StringComparison.Ordinal) ?? false);

    /// <summary>
    /// True when the request downloads the blob at <paramref name="blobName"/>, as
    /// opposed to listing the container or reading blob properties.
    /// </summary>
    public static bool IsBlobDownload(HttpRequestMessage request, string blobName) =>
        request.Method == HttpMethod.Get
        && (request.RequestUri?.AbsolutePath.EndsWith('/' + blobName, StringComparison.Ordinal) ?? false)
        && !(request.RequestUri?.Query.Contains("comp=", StringComparison.Ordinal) ?? false);

    /// <summary>Builds a synthetic storage error response.</summary>
    public static HttpResponseMessage StorageError(System.Net.HttpStatusCode status, string errorCode)
    {
        var response = new HttpResponseMessage(status) { Content = new StringContent(string.Empty) };
        response.Headers.TryAddWithoutValidation("x-ms-error-code", errorCode);
        return response;
    }

    protected override async Task<HttpResponseMessage> SendAsync(
        HttpRequestMessage request,
        CancellationToken cancellationToken)
    {
        var interceptor = Interceptor;
        if (interceptor is not null)
        {
            var injected = await interceptor(request, cancellationToken).ConfigureAwait(false);
            if (injected is not null)
            {
                return injected;
            }
        }

        var response = await base.SendAsync(request, cancellationToken).ConfigureAwait(false);

        if (DribbleResponseBody?.Invoke(request) == true && response.IsSuccessStatusCode)
        {
            var body = await response.Content.ReadAsStreamAsync(cancellationToken).ConfigureAwait(false);
            var dribbled = new StreamContent(new DribbleStream(body));
            foreach (var header in response.Content.Headers)
            {
                dribbled.Headers.TryAddWithoutValidation(header.Key, header.Value);
            }

            response.Content = dribbled;
        }

        return response;
    }
}
