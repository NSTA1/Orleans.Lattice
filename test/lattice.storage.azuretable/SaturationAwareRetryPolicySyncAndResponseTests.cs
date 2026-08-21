using Azure;
using Azure.Core;
using Azure.Core.Pipeline;
using NSubstitute;

namespace Orleans.Lattice.Storage.AzureTable.Tests;

/// <summary>
/// White-box unit tests for the synchronous
/// <see cref="SaturationAwareRetryPolicy.Process(Azure.Core.HttpMessage, ReadOnlyMemory{Azure.Core.Pipeline.HttpPipelinePolicy})"/>
/// path and the full public surface of the synthetic saturated
/// <see cref="Azure.Response"/> the policy stamps onto a short-circuited
/// retry. The existing <see cref="SaturationAwareRetryPolicyTests"/>
/// fixture exercises only the asynchronous
/// <see cref="SaturationAwareRetryPolicy.ProcessAsync"/> path and the
/// <c>Retry-After</c> header hit; this fixture adds the sync twin and
/// the header/stream/reason miss paths so the pipeline policy and its
/// synthetic response are covered symmetrically.
/// </summary>
[TestFixture]
public class SaturationAwareRetryPolicySyncAndResponseTests
{
    /// <summary>
    /// Downstream policy whose synchronous
    /// <see cref="HttpPipelinePolicy.Process"/> succeeds with a 200 and
    /// tracks invocation count so tests can assert whether the inner
    /// pipeline was reached on the sync path.
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
    public void Process_first_attempt_under_saturated_signal_passes_through_to_inner_pipeline()
    {
        var signal = Substitute.For<IWalSaturationSignal>();
        signal.GetAggregateState().Returns(WalSaturationState.Saturated);

        var policy = new SaturationAwareRetryPolicy(signal);
        var next = new CountingNextPolicy();
        var pipeline = new HttpPipelinePolicy[] { next };
        var message = NewMessage();

        policy.Process(message, pipeline);

        Assert.That(next.Invocations, Is.EqualTo(1),
            "the first attempt must always reach the network, even under Saturated");
        Assert.That(message.Response.Status, Is.EqualTo(200));
    }

    [Test]
    public void Process_retry_under_saturated_signal_stamps_synthetic_503_and_skips_inner_pipeline()
    {
        var signal = Substitute.For<IWalSaturationSignal>();
        signal.GetAggregateState().Returns(WalSaturationState.Saturated);

        var policy = new SaturationAwareRetryPolicy(signal);
        var next = new CountingNextPolicy();
        var pipeline = new HttpPipelinePolicy[] { next };
        var message = NewMessage();

        policy.Process(message, pipeline); // first attempt - passes through
        policy.Process(message, pipeline); // retry - Saturated -> short-circuit

        Assert.That(next.Invocations, Is.EqualTo(1),
            "the inner pipeline must not be invoked on a saturated retry via the sync path");
        Assert.That(message.Response.Status, Is.EqualTo(503));
    }

    [Test]
    public void Process_retry_under_healthy_signal_passes_through_to_inner_pipeline()
    {
        var signal = Substitute.For<IWalSaturationSignal>();
        signal.GetAggregateState().Returns(WalSaturationState.Healthy);

        var policy = new SaturationAwareRetryPolicy(signal);
        var next = new CountingNextPolicy();
        var pipeline = new HttpPipelinePolicy[] { next };
        var message = NewMessage();

        policy.Process(message, pipeline);
        policy.Process(message, pipeline);

        Assert.That(next.Invocations, Is.EqualTo(2),
            "Healthy retries must reach the network on the sync path too");
    }

    /// <summary>
    /// Drives the policy to stamp its synthetic saturated response, then
    /// exercises every member of that response so the private
    /// <c>SaturatedResponse</c> - including the header/stream/reason miss
    /// paths that the existing hit-path test never reaches - is fully
    /// covered.
    /// </summary>
    [Test]
    public void SaturatedResponse_exposes_503_retry_after_and_miss_paths()
    {
        var signal = Substitute.For<IWalSaturationSignal>();
        signal.GetAggregateState().Returns(WalSaturationState.Saturated);

        var policy = new SaturationAwareRetryPolicy(signal);
        var next = new CountingNextPolicy();
        var pipeline = new HttpPipelinePolicy[] { next };
        var message = NewMessage();

        policy.Process(message, pipeline); // first attempt
        policy.Process(message, pipeline); // retry -> synthetic 503

        var response = message.Response;

        Assert.Multiple(() =>
        {
            Assert.That(response.Status, Is.EqualTo(503));
            Assert.That(response.ReasonPhrase, Is.Not.Empty);
            Assert.That(response.ClientRequestId, Is.Empty);
            Assert.That(response.ContentStream, Is.SameAs(Stream.Null));

            // Header enumeration surfaces the single Retry-After header.
            var enumerated = response.Headers.Select(h => (h.Name, h.Value)).ToList();
            Assert.That(enumerated, Has.Count.EqualTo(1));
            Assert.That(enumerated[0].Name, Is.EqualTo("Retry-After"));
            Assert.That(enumerated[0].Value, Is.EqualTo("0"));

            // Contains: hit (case-insensitive) and miss.
            Assert.That(response.Headers.Contains("retry-after"), Is.True);
            Assert.That(response.Headers.Contains("X-Absent"), Is.False);

            // TryGetValue: hit and miss.
            Assert.That(response.Headers.TryGetValue("Retry-After", out var single), Is.True);
            Assert.That(single, Is.EqualTo("0"));
            Assert.That(response.Headers.TryGetValue("X-Absent", out var missingSingle), Is.False);
            Assert.That(missingSingle, Is.Null);

            // TryGetValues: hit and miss.
            Assert.That(response.Headers.TryGetValues("Retry-After", out var many), Is.True);
            Assert.That(many, Is.EqualTo(new[] { "0" }));
            Assert.That(response.Headers.TryGetValues("X-Absent", out var missingMany), Is.False);
            Assert.That(missingMany, Is.Null);
        });

        // Setters are documented no-ops; assert they neither throw nor
        // mutate the observable getters.
        response.ContentStream = new MemoryStream();
        response.ClientRequestId = "ignored";
        Assert.Multiple(() =>
        {
            Assert.That(response.ContentStream, Is.SameAs(Stream.Null));
            Assert.That(response.ClientRequestId, Is.Empty);
        });

        // Dispose is a no-op and must be idempotent.
        Assert.That(() => { response.Dispose(); response.Dispose(); }, Throws.Nothing);
    }
}
