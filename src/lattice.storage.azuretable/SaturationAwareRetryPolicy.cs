using System.Globalization;
using Azure.Core;
using Azure.Core.Pipeline;

namespace Orleans.Lattice.Storage.AzureTable;

/// <summary>
/// Per-retry <see cref="HttpPipelinePolicy"/> that short-circuits the
/// Azure SDK's retry loop when the silo-scoped WAL saturation signal
/// reports <see cref="WalSaturationState.Saturated"/>. Closes the
/// consumer-coverage gap that the Azure SDK's internal retry policy
/// ignores cancellation once a call has handed off to the underlying
/// <c>Socket.SendAsync</c>: under the canonical Azure Tables
/// single-account 409-Conflict regime the SDK retries occupy a WAL slot
/// for the duration of the configured retry budget even after the
/// per-tree saturation classifier escalates to
/// <see cref="WalSaturationState.Saturated"/>, polluting the silo's
/// drain wall-clock and the stall-watchdog with hundreds of parked
/// async frames whose dominant headers are
/// <c>RetryAttemptTrackingPolicy.ProcessAsync</c> -&gt;
/// <c>RedirectPolicy.ProcessAsync</c> -&gt;
/// <c>ResponseBodyPolicy.ProcessAsync</c> -&gt;
/// <c>HttpClient.SendAsync</c> -&gt;
/// <c>HttpConnection.SendAsync</c>.
/// <para>
/// <b>How.</b> Registered at
/// <see cref="HttpPipelinePosition.PerRetry"/> alongside
/// <see cref="RetryAttemptTrackingPolicy"/>, the policy stamps a marker
/// on the <see cref="HttpMessage"/>'s property bag on first invocation
/// and then, on every subsequent invocation (i.e. retry attempts),
/// consults the silo-scoped <see cref="IWalSaturationSignal"/>. When
/// the aggregate state across every observed tree is
/// <see cref="WalSaturationState.Saturated"/>, the policy abandons the
/// retry by stamping a synthetic 503 <see cref="Response"/> with a
/// zero <c>Retry-After</c> onto the message and returning without
/// invoking the rest of the pipeline. The Azure SDK's outer retry
/// loop observes the 503 + <c>Retry-After</c> and exits the retry
/// chain deterministically; the caller's catch site (the provider's
/// phase-1 / phase-2 transaction await) sees a
/// <see cref="Azure.RequestFailedException"/> as if the SDK had
/// exhausted its retries, which the writer's existing provider-
/// failure-count path attributes to the third Saturated classifier
/// input. The signal therefore loops back through the saturation
/// classifier instead of waiting out the full SDK retry budget.
/// </para>
/// <para>
/// <b>Aggregate vs per-tree signal.</b> The HTTP pipeline does not
/// carry a per-call tree id (the policy sits below the
/// <see cref="Azure.Data.Tables.TableClient"/> abstraction), so the
/// policy consults
/// <see cref="IWalSaturationSignal.GetAggregateState"/> rather than
/// <see cref="IWalSaturationSignal.GetCurrentState"/>. For a single-
/// tree silo, aggregate equals per-tree exactly. For a multi-tree
/// silo sharing one Azure Tables account, saturation episodes are
/// almost always correlated because the storage account is the shared
/// resource that throttles; a stray per-tree precision loss on a
/// cross-tree multi-account deployment is acceptable for closing the
/// drain-wall-clock / stall-watchdog signature this policy targets.
/// </para>
/// <para>
/// <b>First attempts are never short-circuited.</b> A fresh request
/// always reaches the network at least once even under
/// <see cref="WalSaturationState.Saturated"/>; only retries are
/// abandoned. This preserves the SDK's contract that a successful-
/// first-attempt request never observes a synthetic failure, and
/// keeps the policy purely additive on the healthy steady-state path.
/// </para>
/// <para>
/// <b>Opt-out.</b> Hosts that prefer the historical unguarded retry
/// behaviour set
/// <see cref="AzureTableWalStorageOptions.HonorSaturationSignal"/> to
/// <see langword="false"/>; the provider then never attaches this
/// policy and the SDK's internal retry policy runs unmodified.
/// </para>
/// </summary>
public sealed class SaturationAwareRetryPolicy : HttpPipelinePolicy
{
    /// <summary>
    /// Property name used to stash the attempt marker on the
    /// <see cref="HttpMessage"/>. Distinct from
    /// <see cref="RetryAttemptTrackingPolicy"/>'s marker so the two
    /// policies do not race on the same property bag entry.
    /// </summary>
    private const string AttemptMarkerProperty = "Orleans.Lattice.SaturationAwareRetryPolicy.Attempted";

    private readonly IWalSaturationSignal _signal;

    /// <summary>
    /// Constructs a policy that consults <paramref name="signal"/> on
    /// every retry attempt. The signal is the silo-scoped singleton
    /// registered by <c>AddLattice</c>; a null signal disables the
    /// short-circuit (the provider's registration extension skips
    /// attaching this policy in that case).
    /// </summary>
    /// <param name="signal">The silo-scoped WAL saturation signal.</param>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="signal"/> is <c>null</c>.</exception>
    public SaturationAwareRetryPolicy(IWalSaturationSignal signal)
    {
        ArgumentNullException.ThrowIfNull(signal);
        _signal = signal;
    }

    /// <inheritdoc/>
    public override void Process(HttpMessage message, ReadOnlyMemory<HttpPipelinePolicy> pipeline)
    {
        if (ShouldShortCircuit(message))
        {
            ApplySyntheticSaturatedResponse(message);
            return;
        }
        ProcessNext(message, pipeline);
    }

    /// <inheritdoc/>
    public override async ValueTask ProcessAsync(HttpMessage message, ReadOnlyMemory<HttpPipelinePolicy> pipeline)
    {
        if (ShouldShortCircuit(message))
        {
            ApplySyntheticSaturatedResponse(message);
            return;
        }
        await ProcessNextAsync(message, pipeline).ConfigureAwait(false);
    }

    private bool ShouldShortCircuit(HttpMessage message)
    {
        // First attempt for this message: stamp the marker and pass
        // through. The PerRetry pipeline position guarantees we are
        // re-invoked for every retry attempt on the same HttpMessage
        // instance, so subsequent invocations observe the marker and
        // treat the call as a retry.
        if (!message.TryGetProperty(AttemptMarkerProperty, out _))
        {
            message.SetProperty(AttemptMarkerProperty, BoxedTrue);
            return false;
        }

        // Retry attempt: consult the aggregate saturation signal and
        // short-circuit on Saturated. Healthy / Throttled fall through
        // to the inner pipeline so transient transport faults still
        // retry per the SDK's default policy.
        return _signal.GetAggregateState() == WalSaturationState.Saturated;
    }

    private static void ApplySyntheticSaturatedResponse(HttpMessage message)
    {
        message.Response = new SaturatedResponse();
        LatticeMetrics.ProviderRetryShortCircuited.Add(1,
            new KeyValuePair<string, object?>(LatticeMetrics.TagStatus,
                SaturatedResponse.SyntheticStatus.ToString(CultureInfo.InvariantCulture)));
    }

    /// <summary>
    /// Pre-boxed <see langword="true"/> so the per-message
    /// <see cref="HttpMessage.SetProperty(string, object)"/> stamp
    /// allocates zero on every first-attempt invocation. The marker
    /// value is opaque to the policy; only its presence matters.
    /// </summary>
    private static readonly object BoxedTrue = true;

    /// <summary>
    /// Synthetic <see cref="Response"/> stamped onto a saturated retry.
    /// Carries a 503 status with an empty content stream and a
    /// <c>Retry-After: 0</c> header so the SDK's outer retry policy
    /// observes a deterministic "service unavailable, do not retry
    /// immediately" signal and exits the retry chain rather than
    /// burning more attempts. The caller's catch site sees the same
    /// <see cref="Azure.RequestFailedException"/> shape as a
    /// retry-exhausted failure.
    /// </summary>
    private sealed class SaturatedResponse : Azure.Response
    {
        internal const int SyntheticStatus = 503;
        private const string RetryAfterHeaderName = "Retry-After";
        private const string RetryAfterHeaderValue = "0";
        private const string ReasonPhraseValue =
            "WAL saturation signal short-circuited the SDK retry; the silo's saturation classifier will surface this failure via the provider-failure-count path.";

        public override int Status => SyntheticStatus;
        public override string ReasonPhrase => ReasonPhraseValue;
        public override Stream? ContentStream { get => Stream.Null; set { } }
        public override string ClientRequestId { get => string.Empty; set { } }
        public override void Dispose() { }

        protected override bool ContainsHeader(string name) =>
            string.Equals(name, RetryAfterHeaderName, StringComparison.OrdinalIgnoreCase);

        protected override IEnumerable<Azure.Core.HttpHeader> EnumerateHeaders()
        {
            yield return new Azure.Core.HttpHeader(RetryAfterHeaderName, RetryAfterHeaderValue);
        }

#pragma warning disable CS8765 // SDK declares non-nullable out; synthetic response sets value only on hit.
        protected override bool TryGetHeader(string name, out string? value)
        {
            if (string.Equals(name, RetryAfterHeaderName, StringComparison.OrdinalIgnoreCase))
            {
                value = RetryAfterHeaderValue;
                return true;
            }
            value = null;
            return false;
        }

        protected override bool TryGetHeaderValues(string name, out IEnumerable<string>? values)
        {
            if (string.Equals(name, RetryAfterHeaderName, StringComparison.OrdinalIgnoreCase))
            {
                values = new[] { RetryAfterHeaderValue };
                return true;
            }
            values = null;
            return false;
        }
#pragma warning restore CS8765
    }
}
