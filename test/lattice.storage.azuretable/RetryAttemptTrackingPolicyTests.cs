using System.Diagnostics.Metrics;
using Azure;
using Azure.Core;
using Azure.Core.Pipeline;

namespace Orleans.Lattice.Storage.AzureTable.Tests;

/// <summary>
/// White-box unit tests for <see cref="RetryAttemptTrackingPolicy"/>:
/// the per-retry pipeline policy added in Phase C / step C4 of
/// <c></c> to attribute wall-time inflation to SDK retries
/// whose retries ultimately succeed (and therefore never increment
/// <see cref="LatticeMetrics.ProviderRetryExhausted"/>).
/// <para>
/// Each test attaches a <see cref="MeterListener"/> to the
/// <see cref="LatticeMetrics.Meter"/> and counts emissions of the new
/// <see cref="LatticeMetrics.ProviderRetryAttempts"/> instrument
/// (counter name <c>orleans.lattice.provider.retry.attempts</c>) for
/// the duration of the test. The policy is exercised in isolation
/// against a stub next-policy chain that stamps a synthetic
/// <see cref="Response"/> onto the message between invocations, so
/// the tests pin the policy's contract without depending on the
/// Azure SDK's full retry pipeline.
/// </para>
/// </summary>
[TestFixture]
public class RetryAttemptTrackingPolicyTests
{
    private const string CounterName = "orleans.lattice.provider.retry.attempts";

    /// <summary>
    /// Captures every emission of
    /// <see cref="LatticeMetrics.ProviderRetryAttempts"/> for the
    /// lifetime of the test. Filters to the policy's counter by
    /// instrument name so concurrent unrelated metric activity on
    /// the global <see cref="LatticeMetrics.Meter"/> is ignored.
    /// </summary>
    private sealed class RetryAttemptRecorder : IDisposable
    {
        private readonly MeterListener _listener;
        public List<(long Value, string Status)> Records { get; } = new();

        public RetryAttemptRecorder()
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
    /// Stub <see cref="HttpPipelinePolicy"/> sitting downstream of the
    /// tracker. On each invocation it advances through a queue of
    /// pre-staged <see cref="Response"/>s and attaches the next one
    /// to the message, simulating the prior-attempt response the
    /// real SDK retry policy would leave behind on a retried call.
    /// </summary>
    private sealed class StubResponsePolicy : HttpPipelinePolicy
    {
        private readonly Queue<Response> _responses;
        public int Invocations { get; private set; }

        public StubResponsePolicy(IEnumerable<Response> responses)
        {
            _responses = new Queue<Response>(responses);
        }

        public override void Process(HttpMessage message, ReadOnlyMemory<HttpPipelinePolicy> pipeline)
        {
            Invocations++;
            if (_responses.Count > 0)
            {
                message.Response = _responses.Dequeue();
            }
        }

        public override ValueTask ProcessAsync(HttpMessage message, ReadOnlyMemory<HttpPipelinePolicy> pipeline)
        {
            Process(message, pipeline);
            return ValueTask.CompletedTask;
        }
    }

    /// <summary>
    /// Minimal <see cref="Response"/> implementation so tests can
    /// stage a known HTTP status without taking a dependency on the
    /// Azure SDK's test-framework package. Only the
    /// <see cref="Response.Status"/> property is read by the
    /// policy under test.
    /// </summary>
    private sealed class StubResponse(int status) : Response
    {
        public override int Status => status;
        public override string ReasonPhrase => string.Empty;
        public override Stream? ContentStream { get => null; set { } }
        public override string ClientRequestId { get => string.Empty; set { } }
        public override void Dispose() { }
        protected override bool ContainsHeader(string name) => false;
        protected override IEnumerable<HttpHeader> EnumerateHeaders() => Array.Empty<HttpHeader>();
#pragma warning disable CS8765 // SDK declares non-nullable out; stub returns null when absent.
        protected override bool TryGetHeader(string name, out string? value) { value = null; return false; }
        protected override bool TryGetHeaderValues(string name, out IEnumerable<string>? values) { values = null; return false; }
#pragma warning restore CS8765
    }

    private static HttpMessage NewMessage()
    {
        // HttpMessage requires a Request, which is transport-specific.
        // For policy-only tests we never send the message, so a
        // minimal stub request suffices.
        return new HttpMessage(new StubRequest(), new ResponseClassifier());
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
#pragma warning disable CS8765 // SDK declares non-nullable out; stub returns null when absent.
        protected override bool TryGetHeader(string name, out string? value) { value = null; return false; }
        protected override bool TryGetHeaderValues(string name, out IEnumerable<string>? values) { values = null; return false; }
#pragma warning restore CS8765
        public override void Dispose() { }
    }

    [Test]
    public async Task First_attempt_does_not_increment_counter()
    {
        using var recorder = new RetryAttemptRecorder();
        var stub = new StubResponsePolicy(new[] { (Response)new StubResponse(200) });
        var pipeline = new HttpPipelinePolicy[] { stub };

        await RetryAttemptTrackingPolicy.Instance.ProcessAsync(NewMessage(), pipeline);

        Assert.That(stub.Invocations, Is.EqualTo(1));
        lock (recorder.Records)
        {
            Assert.That(recorder.Records, Is.Empty,
                "First attempt must not increment ProviderRetryAttempts; only retries do.");
        }
    }

    [Test]
    public async Task Second_attempt_with_503_response_records_one_attempt_tagged_503()
    {
        using var recorder = new RetryAttemptRecorder();
        var stub = new StubResponsePolicy(new[]
        {
            (Response)new StubResponse(503),
            new StubResponse(200),
        });
        var pipeline = new HttpPipelinePolicy[] { stub };
        var message = NewMessage();

        // First attempt: stub stamps a 503 onto the message; tracker
        // records nothing (first attempt, no prior response existed
        // when the tracker ran).
        await RetryAttemptTrackingPolicy.Instance.ProcessAsync(message, pipeline);
        // Second attempt on the SAME message instance: the SDK retry
        // policy would re-invoke the inner per-retry pipeline; we
        // mirror that here. The tracker now sees the prior 503 and
        // increments the counter.
        await RetryAttemptTrackingPolicy.Instance.ProcessAsync(message, pipeline);

        lock (recorder.Records)
        {
            Assert.That(recorder.Records.Count, Is.EqualTo(1));
            Assert.That(recorder.Records[0].Value, Is.EqualTo(1L));
            Assert.That(recorder.Records[0].Status, Is.EqualTo("503"));
        }
    }

    [Test]
    public async Task Three_attempts_with_503_then_503_then_200_records_two_attempts()
    {
        using var recorder = new RetryAttemptRecorder();
        var stub = new StubResponsePolicy(new[]
        {
            (Response)new StubResponse(503),
            new StubResponse(503),
            new StubResponse(200),
        });
        var pipeline = new HttpPipelinePolicy[] { stub };
        var message = NewMessage();

        await RetryAttemptTrackingPolicy.Instance.ProcessAsync(message, pipeline);
        await RetryAttemptTrackingPolicy.Instance.ProcessAsync(message, pipeline);
        await RetryAttemptTrackingPolicy.Instance.ProcessAsync(message, pipeline);

        lock (recorder.Records)
        {
            Assert.That(recorder.Records.Count, Is.EqualTo(2),
                "Two retry attempts (the 2nd and 3rd invocations) must each emit one count.");
            Assert.That(recorder.Records.Select(r => r.Status),
                Is.EquivalentTo(new[] { "503", "503" }));
        }
    }

    [Test]
    public async Task Retry_with_no_prior_response_records_status_zero()
    {
        // Simulates a transport-level retry where the previous
        // attempt never produced an HTTP response (e.g. a network
        // exception before any header bytes arrived). The SDK retry
        // policy would still re-enter the per-retry pipeline; the
        // tracker stamps its marker on the first attempt and, on the
        // second attempt with no prior response, records status 0.
        using var recorder = new RetryAttemptRecorder();
        var stub = new StubResponsePolicy(Array.Empty<Response>());
        var pipeline = new HttpPipelinePolicy[] { stub };
        var message = NewMessage();

        await RetryAttemptTrackingPolicy.Instance.ProcessAsync(message, pipeline);
        await RetryAttemptTrackingPolicy.Instance.ProcessAsync(message, pipeline);

        lock (recorder.Records)
        {
            Assert.That(recorder.Records.Count, Is.EqualTo(1));
            Assert.That(recorder.Records[0].Status, Is.EqualTo("0"));
        }
    }

    [Test]
    public void Process_synchronous_path_mirrors_async_for_retry_detection()
    {
        using var recorder = new RetryAttemptRecorder();
        var stub = new StubResponsePolicy(new[]
        {
            (Response)new StubResponse(429),
            new StubResponse(200),
        });
        var pipeline = new HttpPipelinePolicy[] { stub };
        var message = NewMessage();

        RetryAttemptTrackingPolicy.Instance.Process(message, pipeline);
        RetryAttemptTrackingPolicy.Instance.Process(message, pipeline);

        lock (recorder.Records)
        {
            Assert.That(recorder.Records.Count, Is.EqualTo(1));
            Assert.That(recorder.Records[0].Status, Is.EqualTo("429"));
        }
    }

    [Test]
    public void Instance_is_singleton_and_stateless_across_messages()
    {
        Assert.That(RetryAttemptTrackingPolicy.Instance,
            Is.SameAs(RetryAttemptTrackingPolicy.Instance));

        // Re-using the singleton with two independent messages must
        // not cross-contaminate state - each message carries its own
        // attempt marker via HttpMessage property bag.
        using var recorder = new RetryAttemptRecorder();
        var stubA = new StubResponsePolicy(new[] { (Response)new StubResponse(200) });
        var stubB = new StubResponsePolicy(new[] { (Response)new StubResponse(200) });
        RetryAttemptTrackingPolicy.Instance.Process(NewMessage(), new HttpPipelinePolicy[] { stubA });
        RetryAttemptTrackingPolicy.Instance.Process(NewMessage(), new HttpPipelinePolicy[] { stubB });

        lock (recorder.Records)
        {
            Assert.That(recorder.Records, Is.Empty,
                "Two independent first-attempt messages must not produce any retry-attempt records.");
        }
    }
}
