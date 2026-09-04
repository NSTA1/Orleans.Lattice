namespace Orleans.Lattice.Caching.AzureBlob.Tests;

/// <summary>
/// An <see cref="HttpMessageHandler"/> that lets a test intercept the blob-service
/// requests the Azure SDK issues, either short-circuiting one with a synthetic
/// response or pausing it, before falling through to the live emulator.
/// <para>
/// The cache's best-effort paths - the sliding-metadata renewal and the eviction of
/// an entry found expired on read - swallow <see cref="Azure.RequestFailedException"/>
/// on purpose, because a concurrent delete or rewrite must not fail the read that
/// triggered them. Those handlers cannot be reached by ordinary emulator traffic:
/// the only way in is for a specific storage call to fail while the surrounding
/// operation succeeds. Intercepting at the transport reproduces exactly that, and
/// deterministically, rather than by racing two clients and hoping.
/// </para>
/// <para>
/// Requests the interceptor declines (by returning <see langword="null"/>) are
/// forwarded unchanged, so every other call in the test still talks to the real
/// emulator and the assertions remain end-to-end.
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

    /// <summary>True when the request is the SDK's set-blob-metadata call.</summary>
    public static bool IsSetMetadata(HttpRequestMessage request) =>
        request.Method == HttpMethod.Put
        && (request.RequestUri?.Query.Contains("comp=metadata", StringComparison.Ordinal) ?? false);

    /// <summary>True when the request is a blob delete.</summary>
    public static bool IsBlobDelete(HttpRequestMessage request) =>
        request.Method == HttpMethod.Delete
        && !(request.RequestUri?.Query.Contains("restype=container", StringComparison.Ordinal) ?? false);

    /// <summary>True when the request is the SDK's create-container call.</summary>
    public static bool IsContainerCreate(HttpRequestMessage request) =>
        request.Method == HttpMethod.Put
        && (request.RequestUri?.Query.Contains("restype=container", StringComparison.Ordinal) ?? false);

    /// <summary>
    /// Builds a synthetic storage error response. A status the retry policy does not
    /// treat as transient keeps the test fast and the failure count exact.
    /// </summary>
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

        return await base.SendAsync(request, cancellationToken).ConfigureAwait(false);
    }
}
