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
    private readonly TimeSpan _cooldown;
    private readonly TimeProvider _timeProvider;

    // Wall-clock ticks (UTC) of the most recent observation of
    // WalSaturationState.Saturated by this policy. Long-typed so the
    // read/write pair can be observed atomically across the per-
    // pipeline retry threads without a lock; Interlocked.Read /
    // Interlocked.Exchange guarantee tear-free reads on 32-bit hosts.
    // Sentinel long.MinValue means "never observed Saturated", chosen
    // so the cooldown predicate (now - lastSat >= cooldown) is trivially
    // satisfied on a fresh policy with any non-negative cooldown.
    private long _lastSaturatedObservationTicks = long.MinValue;

    /// <summary>
    /// Constructs a policy that consults <paramref name="signal"/> on
    /// every retry attempt. The signal is the silo-scoped singleton
    /// registered by <c>AddLattice</c>; a null signal disables the
    /// short-circuit (the provider's registration extension skips
    /// attaching this policy in that case). Uses
    /// <see cref="AzureTableWalStorageOptions.DefaultSaturationShortCircuitCooldown"/>
    /// for the sticky-window duration and the system clock.
    /// </summary>
    /// <param name="signal">The silo-scoped WAL saturation signal.</param>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="signal"/> is <c>null</c>.</exception>
    public SaturationAwareRetryPolicy(IWalSaturationSignal signal)
        : this(signal, AzureTableWalStorageOptions.DefaultSaturationShortCircuitCooldown, TimeProvider.System)
    {
    }

    /// <summary>
    /// Constructs a policy with an explicit cooldown duration and
    /// clock source. Used by <see cref="AzureTableWalStorageOptions.BuildServiceClient"/>
    /// to plumb the configured cooldown, and by the test suite to
    /// inject a deterministic clock.
    /// </summary>
    /// <param name="signal">The silo-scoped WAL saturation signal.</param>
    /// <param name="cooldown">Sticky-window duration after a
    /// Saturated observation during which subsequent retries are
    /// short-circuited regardless of the signal's current state.
    /// Must be non-negative; <see cref="TimeSpan.Zero"/> disables the
    /// sticky window (only the present state is consulted).</param>
    /// <param name="timeProvider">Clock source for the sticky window.</param>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="signal"/> or <paramref name="timeProvider"/> is <c>null</c>.</exception>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when <paramref name="cooldown"/> is negative.</exception>
    public SaturationAwareRetryPolicy(IWalSaturationSignal signal, TimeSpan cooldown, TimeProvider timeProvider)
    {
        ArgumentNullException.ThrowIfNull(signal);
        ArgumentNullException.ThrowIfNull(timeProvider);
        if (cooldown < TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(cooldown), cooldown, "cooldown must be non-negative");
        }
        _signal = signal;
        _cooldown = cooldown;
        _timeProvider = timeProvider;
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

        // Retry attempt. Two short-circuit paths:
        //
        // 1. Present-state Saturated: the silo's classifier just told
        //    us the storage account is exhausted. Stamp the
        //    observation timestamp (for the cooldown predicate below)
        //    and short-circuit.
        //
        // 2. Recent-observation cooldown: even when the present
        //    aggregate state has decayed to Throttled or Healthy,
        //    we keep short-circuiting for SaturationShortCircuitCooldown
        //    after the last Saturated observation. This bridges the
        //    gap between the sampler's 200 ms tick (which is the
        //    effective lifetime of any single Saturated observation
        //    under a transient burst regime) and the SDK's
        //    exponential-backoff retry spacing of 800 ms - 3.2 s.
        //    Without it, the SDK retry that would otherwise fire 800
        //    ms after we observed Saturated almost always lands in
        //    a Throttled or Healthy window and passes through to the
        //    network, burning storage-side capacity the classifier
        //    just told us is gone.
        //
        // Healthy + no recent Saturated observation: fall through to
        // the inner pipeline so transient transport faults still
        // retry per the SDK's default policy.
        var state = _signal.GetAggregateState();
        if (state == WalSaturationState.Saturated)
        {
            Interlocked.Exchange(ref _lastSaturatedObservationTicks, _timeProvider.GetUtcNow().UtcTicks);
            return true;
        }

        if (_cooldown > TimeSpan.Zero)
        {
            var lastObsTicks = Interlocked.Read(ref _lastSaturatedObservationTicks);
            if (lastObsTicks != long.MinValue)
            {
                var elapsed = _timeProvider.GetUtcNow().UtcTicks - lastObsTicks;
                if (elapsed < _cooldown.Ticks)
                {
                    return true;
                }
            }
        }

        return false;
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
