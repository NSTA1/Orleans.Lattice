using System.Diagnostics.Metrics;
using Azure;
using Azure.Core;
using Azure.Core.Pipeline;
using NSubstitute;

namespace Orleans.Lattice.Storage.AzureTable.Tests;

/// <summary>
/// White-box unit tests for <see cref="SaturationAwareRetryPolicy"/>:
/// the per-retry pipeline policy that short-circuits the Azure SDK
/// retry loop when the silo-scoped <see cref="IWalSaturationSignal"/>
/// reports <see cref="WalSaturationState.Saturated"/>. The tests stub
/// the signal and the downstream pipeline so the policy is exercised
/// in isolation against a deterministic transcript of attempts.
/// </summary>
[TestFixture]
public class SaturationAwareRetryPolicyTests
{
    private const string CounterName = "orleans.lattice.provider.retry.short_circuited";

    private sealed class ShortCircuitRecorder : IDisposable
    {
        private readonly MeterListener _listener;
        public List<(long Value, string Status)> Records { get; } = new();

        public ShortCircuitRecorder()
        {
            _listener = new MeterListener
            {
                InstrumentPublished = (inst, l) =>
                {
                    if (ReferenceEquals(inst.Meter, LatticeMetrics.Meter)
                        && inst.Name == CounterName)
                    {
                        l.EnableMeasurementEvents(inst);
                    }
                },
            };
            _listener.SetMeasurementEventCallback<long>(OnLong);
            _listener.Start();
        }

        private void OnLong(Instrument instrument, long value,
            ReadOnlySpan<KeyValuePair<string, object?>> tags, object? state)
        {
            string status = "(missing)";
            foreach (var tag in tags)
            {
                if (tag.Key == LatticeMetrics.TagStatus)
                {
                    status = tag.Value as string ?? "(null)";
                    break;
                }
            }
            lock (Records)
            {
                Records.Add((value, status));
            }
        }

        public void Dispose() => _listener.Dispose();
    }

    /// <summary>
    /// Stub downstream policy that always succeeds and tracks the
    /// invocation count, so tests can assert whether the inner
    /// pipeline was reached.
    /// </summary>
    private sealed class CountingNextPolicy : HttpPipelinePolicy
    {
        public int Invocations { get; private set; }

        public override void Process(HttpMessage message, ReadOnlyMemory<HttpPipelinePolicy> pipeline)
        {
            Invocations++;
            message.Response = new StubResponse(200);
        }

        public override ValueTask ProcessAsync(HttpMessage message, ReadOnlyMemory<HttpPipelinePolicy> pipeline)
        {
            Process(message, pipeline);
            return ValueTask.CompletedTask;
        }
    }

    private sealed class StubResponse(int status) : Response
    {
        public override int Status => status;
        public override string ReasonPhrase => string.Empty;
        public override Stream? ContentStream { get => null; set { } }
        public override string ClientRequestId { get => string.Empty; set { } }
        public override void Dispose() { }
        protected override bool ContainsHeader(string name) => false;
        protected override IEnumerable<HttpHeader> EnumerateHeaders() => Array.Empty<HttpHeader>();
#pragma warning disable CS8765
        protected override bool TryGetHeader(string name, out string? value) { value = null; return false; }
        protected override bool TryGetHeaderValues(string name, out IEnumerable<string>? values) { values = null; return false; }
#pragma warning restore CS8765
    }

    private sealed class StubRequest : Azure.Core.Request
    {
        public override RequestMethod Method { get; set; } = RequestMethod.Post;
        public override RequestContent? Content { get; set; }
        public override string ClientRequestId { get; set; } = string.Empty;
        protected override bool ContainsHeader(string name) => false;
        protected override IEnumerable<HttpHeader> EnumerateHeaders() => Array.Empty<HttpHeader>();
        protected override void AddHeader(string name, string value) { }
        protected override bool RemoveHeader(string name) => false;
#pragma warning disable CS8765
        protected override bool TryGetHeader(string name, out string? value) { value = null; return false; }
        protected override bool TryGetHeaderValues(string name, out IEnumerable<string>? values) { values = null; return false; }
#pragma warning restore CS8765
        public override void Dispose() { }
    }

    private static HttpMessage NewMessage() => new(new StubRequest(), new ResponseClassifier());

    [Test]
    public void Ctor_throws_when_signal_is_null()
    {
        Assert.That(() => new SaturationAwareRetryPolicy(null!), Throws.ArgumentNullException);
    }

    [Test]
    public async Task First_attempt_under_saturated_signal_passes_through_to_inner_pipeline()
    {
        var signal = Substitute.For<IWalSaturationSignal>();
        signal.GetAggregateState().Returns(WalSaturationState.Saturated);

        var policy = new SaturationAwareRetryPolicy(signal);
        var next = new CountingNextPolicy();
        var pipeline = new HttpPipelinePolicy[] { next };
        var message = NewMessage();

        using var recorder = new ShortCircuitRecorder();
        await policy.ProcessAsync(message, pipeline);

        Assert.That(next.Invocations, Is.EqualTo(1),
            "the first attempt must always reach the network, even under Saturated");
        Assert.That(message.Response.Status, Is.EqualTo(200));
        lock (recorder.Records)
        {
            Assert.That(recorder.Records, Is.Empty,
                "the short-circuit counter only fires on abandoned retries, never on first attempts");
        }
    }

    [Test]
    public async Task Retry_under_saturated_signal_stamps_synthetic_503_and_skips_inner_pipeline()
    {
        var signal = Substitute.For<IWalSaturationSignal>();
        signal.GetAggregateState().Returns(WalSaturationState.Saturated);

        var policy = new SaturationAwareRetryPolicy(signal);
        var next = new CountingNextPolicy();
        var pipeline = new HttpPipelinePolicy[] { next };
        var message = NewMessage();

        using var recorder = new ShortCircuitRecorder();

        // First attempt: under Saturated but first-attempts pass through.
        await policy.ProcessAsync(message, pipeline);
        Assert.That(next.Invocations, Is.EqualTo(1));

        // Retry: same HttpMessage instance. Saturated -> short-circuit.
        await policy.ProcessAsync(message, pipeline);

        Assert.That(next.Invocations, Is.EqualTo(1),
            "the inner pipeline must not be invoked on a saturated retry");
        Assert.That(message.Response.Status, Is.EqualTo(503));
        Assert.That(message.Response.Headers.TryGetValue("Retry-After", out var retryAfter), Is.True);
        Assert.That(retryAfter, Is.EqualTo("0"));

        lock (recorder.Records)
        {
            Assert.That(recorder.Records.Count, Is.EqualTo(1));
            Assert.That(recorder.Records[0].Value, Is.EqualTo(1L));
            Assert.That(recorder.Records[0].Status, Is.EqualTo("503"));
        }
    }

    [Test]
    public async Task Retry_under_healthy_signal_passes_through_to_inner_pipeline()
    {
        var signal = Substitute.For<IWalSaturationSignal>();
        signal.GetAggregateState().Returns(WalSaturationState.Healthy);

        var policy = new SaturationAwareRetryPolicy(signal);
        var next = new CountingNextPolicy();
        var pipeline = new HttpPipelinePolicy[] { next };
        var message = NewMessage();

        using var recorder = new ShortCircuitRecorder();
        await policy.ProcessAsync(message, pipeline);
        await policy.ProcessAsync(message, pipeline);

        Assert.That(next.Invocations, Is.EqualTo(2),
            "Healthy retries must reach the network so transient transport faults still retry per the SDK default");
        lock (recorder.Records)
        {
            Assert.That(recorder.Records, Is.Empty);
        }
    }

    [Test]
    public async Task Retry_under_throttled_signal_passes_through_to_inner_pipeline()
    {
        var signal = Substitute.For<IWalSaturationSignal>();
        signal.GetAggregateState().Returns(WalSaturationState.Throttled);

        // Cooldown disabled (TimeSpan.Zero) so the sticky-window path
        // never fires. Without an explicit cooldown the policy keeps
        // short-circuiting for 2 s after any Saturated observation,
        // but in this test the signal is steady-Throttled and the
        // last-observation sentinel is long.MinValue, so even with
        // a non-zero cooldown Throttled would still fall through.
        // The explicit zero documents intent: this test asserts the
        // present-state branch alone.
        var policy = new SaturationAwareRetryPolicy(signal, TimeSpan.Zero, TimeProvider.System);
        var next = new CountingNextPolicy();
        var pipeline = new HttpPipelinePolicy[] { next };
        var message = NewMessage();

        await policy.ProcessAsync(message, pipeline);
        await policy.ProcessAsync(message, pipeline);

        Assert.That(next.Invocations, Is.EqualTo(2),
            "Throttled is an advisory state, not a refusal: retries pass through to the inner pipeline");
    }

    [Test]
    public async Task Retry_within_cooldown_after_saturated_short_circuits_even_under_healthy_signal()
    {
        // Drive the signal Saturated on the first retry, then Healthy
        // on the second. With a 2-second cooldown and a fake clock
        // advanced only 500 ms between observations, retry 2 must
        // STILL short-circuit because the cooldown predicate fires
        // off the recorded last-Saturated timestamp, independent of
        // the signal's present state.
        var states = new Queue<WalSaturationState>(new[]
        {
            WalSaturationState.Saturated, // retry 1 - stamps timestamp + short-circuits
            WalSaturationState.Healthy,   // retry 2 - within cooldown, still short-circuits
        });
        var signal = new SequenceSignal(states);
        var fakeTime = new ManualTimeProvider(new DateTimeOffset(2025, 1, 1, 0, 0, 0, TimeSpan.Zero));

        var policy = new SaturationAwareRetryPolicy(signal, TimeSpan.FromSeconds(2), fakeTime);
        var next = new CountingNextPolicy();
        var pipeline = new HttpPipelinePolicy[] { next };
        var message = NewMessage();

        using var recorder = new ShortCircuitRecorder();

        await policy.ProcessAsync(message, pipeline); // first attempt - pass through, next.Invocations=1
        Assert.That(next.Invocations, Is.EqualTo(1));

        await policy.ProcessAsync(message, pipeline); // retry 1: Saturated, short-circuit, stamp ts
        Assert.That(next.Invocations, Is.EqualTo(1),
            "retry 1 must not reach the inner pipeline (Saturated)");
        Assert.That(message.Response.Status, Is.EqualTo(503));

        fakeTime.Advance(TimeSpan.FromMilliseconds(500));

        await policy.ProcessAsync(message, pipeline); // retry 2: signal Healthy, but within cooldown
        Assert.That(next.Invocations, Is.EqualTo(1),
            "retry 2 must still short-circuit because the cooldown window has not yet elapsed");
        Assert.That(message.Response.Status, Is.EqualTo(503));

        lock (recorder.Records)
        {
            Assert.That(recorder.Records.Count, Is.EqualTo(2),
                "both retry 1 and retry 2 should increment the short-circuit counter");
        }
    }

    [Test]
    public async Task Retry_after_cooldown_elapsed_passes_through_to_inner_pipeline()
    {
        // Same setup as the cooldown test above, but advance the
        // fake clock past the cooldown so the second retry's sticky-
        // window predicate fails and the pass-through path runs.
        var states = new Queue<WalSaturationState>(new[]
        {
            WalSaturationState.Saturated, // retry 1
            WalSaturationState.Healthy,   // retry 2 - cooldown elapsed, must pass through
        });
        var signal = new SequenceSignal(states);
        var fakeTime = new ManualTimeProvider(new DateTimeOffset(2025, 1, 1, 0, 0, 0, TimeSpan.Zero));

        var policy = new SaturationAwareRetryPolicy(signal, TimeSpan.FromSeconds(2), fakeTime);
        var next = new CountingNextPolicy();
        var pipeline = new HttpPipelinePolicy[] { next };
        var message = NewMessage();

        await policy.ProcessAsync(message, pipeline);            // first attempt
        await policy.ProcessAsync(message, pipeline);            // retry 1 - Saturated
        Assert.That(next.Invocations, Is.EqualTo(1));

        fakeTime.Advance(TimeSpan.FromSeconds(3));               // beyond 2-second cooldown

        await policy.ProcessAsync(message, pipeline);            // retry 2 - Healthy, cooldown elapsed
        Assert.That(next.Invocations, Is.EqualTo(2),
            "after the cooldown elapses the policy must let retries reach the network again");
        Assert.That(message.Response.Status, Is.EqualTo(200));
    }

    [Test]
    public void Ctor_throws_on_negative_cooldown()
    {
        var signal = Substitute.For<IWalSaturationSignal>();
        Assert.That(
            () => new SaturationAwareRetryPolicy(signal, TimeSpan.FromMilliseconds(-1), TimeProvider.System),
            Throws.TypeOf<ArgumentOutOfRangeException>());
    }

    [Test]
    public void Ctor_throws_when_time_provider_is_null()
    {
        var signal = Substitute.For<IWalSaturationSignal>();
        Assert.That(
            () => new SaturationAwareRetryPolicy(signal, TimeSpan.FromSeconds(1), null!),
            Throws.ArgumentNullException);
    }

    [Test]
    public async Task Signal_state_is_re_evaluated_on_every_retry()
    {
        // Drive the signal through a sequence: first-attempt always
        // passes through; retry 1 sees Saturated and short-circuits;
        // retry 2 sees Healthy and must reach the inner pipeline,
        // proving the policy re-reads the signal on every retry rather
        // than caching the first observation.
        var states = new Queue<WalSaturationState>(new[]
        {
            // First attempt never consults the signal, so the queue
            // begins at retry 1.
            WalSaturationState.Saturated, // retry 1 - short-circuit
            WalSaturationState.Healthy,   // retry 2 - must reach inner pipeline
        });
        var signal = new SequenceSignal(states);

        // Cooldown disabled (TimeSpan.Zero) so this test exercises
        // ONLY the present-state re-read invariant - retry 2 under
        // Healthy must reach the inner pipeline regardless of what
        // retry 1 observed. The sticky-window cooldown path has its
        // own dedicated coverage in Retry_within_cooldown_after_*
        // and Retry_after_cooldown_elapsed_*.
        var policy = new SaturationAwareRetryPolicy(signal, TimeSpan.Zero, TimeProvider.System);
        var next = new CountingNextPolicy();
        var pipeline = new HttpPipelinePolicy[] { next };
        var message = NewMessage();

        await policy.ProcessAsync(message, pipeline); // first - passes through, next.Invocations = 1
        await policy.ProcessAsync(message, pipeline); // retry 1 - Saturated, short-circuit
        Assert.That(next.Invocations, Is.EqualTo(1));
        Assert.That(message.Response.Status, Is.EqualTo(503));

        await policy.ProcessAsync(message, pipeline); // retry 2 - Healthy, pass through
        Assert.That(next.Invocations, Is.EqualTo(2),
            "the policy must re-read the signal on every retry, not cache the first observation");
        Assert.That(message.Response.Status, Is.EqualTo(200),
            "the inner pipeline's successful response must replace the prior synthetic 503");
        Assert.That(signal.CallCount, Is.EqualTo(2),
            "GetAggregateState should be consulted on every retry attempt (not on the first attempt)");
    }

    /// <summary>
    /// Minimal stub <see cref="TimeProvider"/> the cooldown tests
    /// drive through a deterministic transcript of UTC instants.
    /// Hand-rolled rather than referencing
    /// Microsoft.Extensions.TimeProvider.Testing to keep the
    /// Storage.AzureTable test project's dependency surface narrow
    /// (mirrors the same approach taken in the replication test
    /// project's CachingReplicationSecretProviderTests).
    /// </summary>
    private sealed class ManualTimeProvider(DateTimeOffset start) : TimeProvider
    {
        private DateTimeOffset _now = start;
        public override DateTimeOffset GetUtcNow() => _now;
        public void Advance(TimeSpan delta) => _now = _now.Add(delta);
    }

    /// <summary>
    /// Deterministic stand-in for <see cref="IWalSaturationSignal"/>
    /// that returns the next item from a queue on every
    /// <see cref="GetAggregateState"/> call, falling back to
    /// <see cref="WalSaturationState.Healthy"/> when exhausted, and
    /// counts the calls so tests can assert exactly when the policy
    /// consulted the signal.
    /// </summary>
    private sealed class SequenceSignal(Queue<WalSaturationState> states) : IWalSaturationSignal
    {
        public int CallCount { get; private set; }
        public WalSaturationState GetAggregateState()
        {
            CallCount++;
            return states.Count > 0 ? states.Dequeue() : WalSaturationState.Healthy;
        }
        public WalSaturationState GetCurrentState(string treeId) => GetAggregateState();
        public Task WaitForHealthyAsync(string treeId, CancellationToken cancellationToken = default) =>
            Task.CompletedTask;
    }
}
