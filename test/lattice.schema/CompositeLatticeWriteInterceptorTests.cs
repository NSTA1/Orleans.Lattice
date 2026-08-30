using System.Text;
using Microsoft.Extensions.Options;
using NSubstitute;

namespace Orleans.Lattice.Schema.Tests;

/// <summary>
/// Unit tests for <see cref="CompositeLatticeWriteInterceptor"/>: single-stage
/// forwarding, ordered threading of a transformed value from one stage to the next,
/// reject / dead-letter short-circuit, and the aggregate
/// <see cref="ILatticeWriteInterceptor.InterceptsSystemOrigin"/> flag.
/// </summary>
public sealed class CompositeLatticeWriteInterceptorTests
{
    private static byte[] Utf8(string s) => Encoding.UTF8.GetBytes(s);

    private static LatticeWriteRequest Request(byte[] value) =>
        new("orders", "k1", value, LatticeOperation.Write);

    /// <summary>A hand-written stage that records what it saw and returns a fixed decision.</summary>
    private sealed class FakeStage(Func<byte[], LatticeWriteDecision> decide, bool interceptsSystemOrigin = false)
        : ILatticeWriteInterceptor
    {
        public byte[]? LastValue { get; private set; }

        public bool InterceptsSystemOrigin { get; } = interceptsSystemOrigin;

        public ValueTask<LatticeWriteDecision> OnWriteAsync(
            in LatticeWriteRequest request, CancellationToken cancellationToken = default)
        {
            LastValue = request.Value;
            return new ValueTask<LatticeWriteDecision>(decide(request.Value));
        }
    }

    [Test]
    public void Constructor_null_versioning_throws()
    {
        Assert.That(
            () => new CompositeLatticeWriteInterceptor((ILatticeWriteInterceptor)null!, null),
            Throws.ArgumentNullException);
    }

    [Test]
    public async Task OnWriteAsync_single_stage_forwards_decision()
    {
        var stage = new FakeStage(_ => LatticeWriteDecision.AcceptTransformed(Utf8("stamped")));
        var composite = new CompositeLatticeWriteInterceptor(stage, enforcement: null);

        var decision = await composite.OnWriteAsync(Request(Utf8("body")));

        Assert.That(decision.Kind, Is.EqualTo(LatticeWriteDecisionKind.AcceptTransformed));
        Assert.That(decision.TransformedValue, Is.EqualTo(Utf8("stamped")));
    }

    [Test]
    public async Task Public_constructor_uses_versioning_stage_when_enforcement_is_absent()
    {
        var provider = Substitute.For<ILatticeSchemaVersionProvider>();
        provider.GetConfigAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(new ValueTask<LatticeSchemaVersionConfig?>((LatticeSchemaVersionConfig?)null));
        var versioning = new LatticeSchemaVersionWriteInterceptor(
            provider,
            Substitute.For<ILatticeSchemaRegistry>(),
            Substitute.For<ILatticeSchemaDeadLetterStore>(),
            Options.Create(new LatticeSchemaVersioningOptions()),
            TimeProvider.System);
        var composite = new CompositeLatticeWriteInterceptor(versioning);

        var decision = await composite.OnWriteAsync(Request(Utf8("body")));

        Assert.That(decision.Kind, Is.EqualTo(LatticeWriteDecisionKind.Accept));
    }

    [Test]
    public async Task OnWriteAsync_threads_transformed_value_into_next_stage()
    {
        // enforcement (first) leaves the value plain; versioning (second) must see it.
        var enforcement = new FakeStage(_ => LatticeWriteDecision.Accept());
        var versioning = new FakeStage(_ => LatticeWriteDecision.AcceptTransformed(Utf8("stamped")));
        var composite = new CompositeLatticeWriteInterceptor(versioning, enforcement);

        var decision = await composite.OnWriteAsync(Request(Utf8("plain")));

        Assert.That(enforcement.LastValue, Is.EqualTo(Utf8("plain")));
        Assert.That(versioning.LastValue, Is.EqualTo(Utf8("plain")));
        Assert.That(decision.TransformedValue, Is.EqualTo(Utf8("stamped")));
    }

    [Test]
    public async Task OnWriteAsync_second_stage_sees_first_stages_transform()
    {
        var enforcement = new FakeStage(_ => LatticeWriteDecision.AcceptTransformed(Utf8("normalized")));
        var versioning = new FakeStage(v => LatticeWriteDecision.AcceptTransformed(v)); // echo what it saw
        var composite = new CompositeLatticeWriteInterceptor(versioning, enforcement);

        var decision = await composite.OnWriteAsync(Request(Utf8("plain")));

        Assert.That(versioning.LastValue, Is.EqualTo(Utf8("normalized")));
        Assert.That(decision.TransformedValue, Is.EqualTo(Utf8("normalized")));
    }

    [Test]
    public async Task OnWriteAsync_reject_short_circuits_remaining_stages()
    {
        var enforcement = new FakeStage(_ => LatticeWriteDecision.Reject("bad"));
        var versioning = new FakeStage(_ => LatticeWriteDecision.AcceptTransformed(Utf8("stamped")));
        var composite = new CompositeLatticeWriteInterceptor(versioning, enforcement);

        var decision = await composite.OnWriteAsync(Request(Utf8("plain")));

        Assert.That(decision.Kind, Is.EqualTo(LatticeWriteDecisionKind.Reject));
        Assert.That(versioning.LastValue, Is.Null); // never reached
    }

    [Test]
    public async Task OnWriteAsync_dead_letter_short_circuits_remaining_stages()
    {
        var enforcement = new FakeStage(_ => LatticeWriteDecision.DeadLetter("diverted"));
        var versioning = new FakeStage(_ => LatticeWriteDecision.AcceptTransformed(Utf8("stamped")));
        var composite = new CompositeLatticeWriteInterceptor(versioning, enforcement);

        var decision = await composite.OnWriteAsync(Request(Utf8("plain")));

        Assert.That(decision.Kind, Is.EqualTo(LatticeWriteDecisionKind.DeadLetter));
        Assert.That(versioning.LastValue, Is.Null);
    }

    [Test]
    public async Task OnWriteAsync_all_accept_returns_plain_accept()
    {
        var enforcement = new FakeStage(_ => LatticeWriteDecision.Accept());
        var versioning = new FakeStage(_ => LatticeWriteDecision.Accept());
        var composite = new CompositeLatticeWriteInterceptor(versioning, enforcement);

        var decision = await composite.OnWriteAsync(Request(Utf8("plain")));

        Assert.That(decision.Kind, Is.EqualTo(LatticeWriteDecisionKind.Accept));
        Assert.That(decision.TransformedValue, Is.Null);
    }

    [Test]
    public void InterceptsSystemOrigin_is_true_when_any_stage_intercepts()
    {
        var quiet = new FakeStage(_ => LatticeWriteDecision.Accept(), interceptsSystemOrigin: false);
        var loud = new FakeStage(_ => LatticeWriteDecision.Accept(), interceptsSystemOrigin: true);

        Assert.That(new CompositeLatticeWriteInterceptor(quiet, null).InterceptsSystemOrigin, Is.False);
        Assert.That(new CompositeLatticeWriteInterceptor(quiet, loud).InterceptsSystemOrigin, Is.True);
        Assert.That(new CompositeLatticeWriteInterceptor(loud, quiet).InterceptsSystemOrigin, Is.True);
    }
}
