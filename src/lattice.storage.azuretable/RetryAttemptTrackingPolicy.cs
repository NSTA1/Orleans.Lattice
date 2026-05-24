using System.Globalization;
using Azure.Core;
using Azure.Core.Pipeline;

namespace Orleans.Lattice.Storage.AzureTable;

/// <summary>
/// Per-retry <see cref="HttpPipelinePolicy"/> that increments
/// <see cref="LatticeMetrics.ProviderRetryAttempts"/> exactly once per
/// retry attempt the Azure SDK performs on a request - regardless of
/// whether the retry ultimately succeeds.
/// <para>
/// <b>Why.</b> The Phase A scaling diagnostic
/// (<c>scaling.md</c> &#8594; Phase A &#8212; Outcomes) found a 5-100x gap
/// between caller-observed wall p99 (700-1,700 ms) and Azure Tables
/// server-timing p99 (10-130 ms) on the WAL hot path. That signature
/// is the canonical fingerprint of retry storms whose retries
/// eventually succeed - so they never increment the existing
/// <see cref="LatticeMetrics.ProviderRetryExhausted"/> counter, which
/// only fires when the SDK gives up. This policy fills the gap by
/// recording <i>attempted</i> retries with the HTTP status that
/// triggered each retry, so dashboards can attribute wall-time
/// inflation to SDK backoff directly.
/// </para>
/// <para>
/// <b>How.</b> Registered at
/// <see cref="HttpPipelinePosition.PerRetry"/>, the policy is invoked
/// once per attempt including the initial try. The policy uses
/// <see cref="HttpMessage.SetProperty(string, object)"/> /
/// <see cref="HttpMessage.TryGetProperty(string, out object)"/> to
/// track attempt count off the message itself - the first invocation
/// stamps a marker and is a no-op for the counter; every subsequent
/// invocation sees the marker, captures the previous attempt's
/// status from <see cref="HttpMessage.Response"/>, and increments the
/// counter. Transport-level retries that surface no HTTP response
/// (e.g. a network exception before any header bytes arrived) are
/// tagged with <c>0</c> so the counter still increments and is
/// distinguishable in dashboards.
/// </para>
/// <para>
/// <b>Cardinality.</b> Tags only by HTTP status (small bounded set);
/// per-tree / per-shard correlation is intentionally omitted to keep
/// the counter's cardinality low on multi-tenant silos. Per-shard
/// failure attribution is covered by
/// <see cref="LatticeMetrics.ProviderRetryExhausted"/>, which fires
/// rarely and already carries shard tags.
/// </para>
/// </summary>
public sealed class RetryAttemptTrackingPolicy : HttpPipelinePolicy
{
    /// <summary>
    /// Property name used to stash the attempt marker on the
    /// <see cref="HttpMessage"/>. Lives in the message's property
    /// bag; the SDK creates a fresh <see cref="HttpMessage"/> per
    /// pipeline invocation, so the marker's lifetime is exactly one
    /// request's full retry chain.
    /// </summary>
    private const string AttemptMarkerProperty = "Orleans.Lattice.RetryAttemptTrackingPolicy.Attempted";

    /// <summary>
    /// Shared instance. The policy is stateless across messages (it
    /// keys all per-attempt state off the <see cref="HttpMessage"/>
    /// itself), so a single instance is safe to attach to every
    /// <see cref="HttpPipeline"/> the provider builds.
    /// </summary>
    public static readonly RetryAttemptTrackingPolicy Instance = new();

    /// <inheritdoc/>
    public override void Process(HttpMessage message, ReadOnlyMemory<HttpPipelinePolicy> pipeline)
    {
        RecordIfRetry(message);
        ProcessNext(message, pipeline);
    }

    /// <inheritdoc/>
    public override async ValueTask ProcessAsync(HttpMessage message, ReadOnlyMemory<HttpPipelinePolicy> pipeline)
    {
        RecordIfRetry(message);
        await ProcessNextAsync(message, pipeline).ConfigureAwait(false);
    }

    private static void RecordIfRetry(HttpMessage message)
    {
        // First attempt for this message: stamp the marker and return.
        // The PerRetry pipeline position guarantees we are re-invoked
        // for every retry attempt on the same HttpMessage instance, so
        // subsequent invocations observe the marker and treat the call
        // as a retry attempt.
        if (!message.TryGetProperty(AttemptMarkerProperty, out _))
        {
            message.SetProperty(AttemptMarkerProperty, BoxedTrue);
            return;
        }

        // The previous attempt's Response is what the SDK's retry
        // policy inspected to decide a retry was warranted. A
        // transport-level failure with no HTTP exchange leaves
        // HasResponse false; tag those with status 0 so the counter
        // still increments and dashboards can separate transport
        // retries from HTTP-status retries.
        var status = message.HasResponse ? message.Response.Status : 0;
        LatticeMetrics.ProviderRetryAttempts.Add(1,
            new KeyValuePair<string, object?>(LatticeMetrics.TagStatus,
                status.ToString(CultureInfo.InvariantCulture)));
    }

    /// <summary>
    /// Pre-boxed <see langword="true"/> so the per-message
    /// <see cref="HttpMessage.SetProperty(string, object)"/> stamp
    /// allocates zero on every first-attempt invocation. The marker
    /// value is opaque to the policy; only its presence matters.
    /// </summary>
    private static readonly object BoxedTrue = true;
}
