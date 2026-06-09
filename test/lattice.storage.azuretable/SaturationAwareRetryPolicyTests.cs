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

        var policy = new SaturationAwareRetryPolicy(signal);
        var next = new CountingNextPolicy();
        var pipeline = new HttpPipelinePolicy[] { next };
        var message = NewMessage();

        await policy.ProcessAsync(message, pipeline);
        await policy.ProcessAsync(message, pipeline);

        Assert.That(next.Invocations, Is.EqualTo(2),
            "Throttled is an advisory state, not a refusal: retries pass through to the inner pipeline");
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

        var policy = new SaturationAwareRetryPolicy(signal);
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
