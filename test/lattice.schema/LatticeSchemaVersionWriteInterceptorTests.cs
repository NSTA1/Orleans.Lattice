using System.Text;
using Microsoft.Extensions.Options;
using NSubstitute;

namespace Orleans.Lattice.Schema.Tests;

/// <summary>
/// Unit tests for <see cref="LatticeSchemaVersionWriteInterceptor"/>: zero-overhead
/// accept for an unversioned tree, envelope stamping on a local write, skip of
/// already-enveloped and CRDT values, and strict-ingest dead-lettering.
/// </summary>
public sealed class LatticeSchemaVersionWriteInterceptorTests
{
    private static readonly DateTimeOffset FixedNow =
        new(2026, 2, 3, 4, 5, 6, TimeSpan.Zero);

    private static byte[] Utf8(string s) => Encoding.UTF8.GetBytes(s);

    private static (LatticeSchemaVersionWriteInterceptor Interceptor, ILatticeSchemaDeadLetterStore Dlq)
        Create(
            LatticeSchemaVersionConfig? config,
            bool strictIngestEnabled = false,
            bool canUpcast = true)
    {
        var provider = Substitute.For<ILatticeSchemaVersionProvider>();
        provider.StrictIngestEnabled.Returns(strictIngestEnabled);
        provider.GetConfigAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(new ValueTask<LatticeSchemaVersionConfig?>(config));

        var registry = Substitute.For<ILatticeSchemaRegistry>();
        registry.CanUpcast(Arg.Any<uint>(), Arg.Any<uint>(), Arg.Any<uint>()).Returns(canUpcast);

        var dlq = Substitute.For<ILatticeSchemaDeadLetterStore>();
        var options = Options.Create(new LatticeSchemaVersioningOptions { DeadLetterPreviewMaxBytes = 4 });
        var interceptor = new LatticeSchemaVersionWriteInterceptor(
            provider, registry, dlq, options, new FixedTimeProvider(FixedNow));
        return (interceptor, dlq);
    }

    private static LatticeWriteRequest Request(byte[] value, LatticeOperation op = LatticeOperation.Write) =>
        new("orders", "k1", value, op);

    [Test]
    public async Task OnWriteAsync_unversioned_tree_accepts_verbatim()
    {
        var (interceptor, dlq) = Create(config: null);

        var decision = await interceptor.OnWriteAsync(Request(Utf8("anything")));

        Assert.That(decision.Kind, Is.EqualTo(LatticeWriteDecisionKind.Accept));
        Assert.That(decision.TransformedValue, Is.Null);
        await dlq.DidNotReceive().AppendAsync(Arg.Any<string>(), Arg.Any<LatticeSchemaDeadLetterEntry>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task OnWriteAsync_local_write_stamps_envelope_at_target()
    {
        var (interceptor, _) = Create(new LatticeSchemaVersionConfig(schemaId: 7, targetVersion: 3));

        var body = Utf8("{\"a\":1}");
        var decision = await interceptor.OnWriteAsync(Request(body));

        Assert.That(decision.Kind, Is.EqualTo(LatticeWriteDecisionKind.AcceptTransformed));
        Assert.That(LatticeSchemaEnvelope.IsEnveloped(decision.TransformedValue!), Is.True);
        LatticeSchemaEnvelope.TryReadHeader(decision.TransformedValue!, out var schemaId, out var version);
        Assert.That(schemaId, Is.EqualTo(7u));
        Assert.That(version, Is.EqualTo(3u));
        Assert.That(LatticeSchemaEnvelope.StripToBody(decision.TransformedValue!), Is.EqualTo(body));
    }

    [Test]
    public async Task OnWriteAsync_already_enveloped_local_write_accepts_verbatim()
    {
        var (interceptor, _) = Create(new LatticeSchemaVersionConfig(1, 2));
        var alreadyStamped = LatticeSchemaEnvelope.Encode(1, 2, Utf8("{\"a\":1}"));

        var decision = await interceptor.OnWriteAsync(Request(alreadyStamped));

        Assert.That(decision.Kind, Is.EqualTo(LatticeWriteDecisionKind.Accept));
    }

    [Test]
    public async Task OnWriteAsync_crdt_delta_is_not_stamped()
    {
        var (interceptor, _) = Create(new LatticeSchemaVersionConfig(1, 1));

        var decision = await interceptor.OnWriteAsync(Request(Utf8("delta"), LatticeOperation.CrdtApply));

        Assert.That(decision.Kind, Is.EqualTo(LatticeWriteDecisionKind.Accept));
    }

    [Test]
    public async Task OnWriteAsync_strict_ingest_non_upcastable_item_dead_letters()
    {
        var (interceptor, dlq) = Create(
            new LatticeSchemaVersionConfig(schemaId: 1, targetVersion: 3, strictIngest: true),
            strictIngestEnabled: true,
            canUpcast: false);
        LatticeSchemaDeadLetterEntry? captured = null;
        dlq.When(x => x.AppendAsync(Arg.Any<string>(), Arg.Any<LatticeSchemaDeadLetterEntry>(), Arg.Any<CancellationToken>()))
            .Do(ci => captured = ci.Arg<LatticeSchemaDeadLetterEntry>());

        // An ingested item stamped at a schema/version that cannot reach the target.
        var ingested = LatticeSchemaEnvelope.Encode(1, 1, Utf8("0123456789"));

        LatticeWriteDecision decision;
        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            decision = await interceptor.OnWriteAsync(Request(ingested));
        }

        Assert.That(decision.Kind, Is.EqualTo(LatticeWriteDecisionKind.DeadLetter));
        await dlq.Received(1).AppendAsync("orders", Arg.Any<LatticeSchemaDeadLetterEntry>(), Arg.Any<CancellationToken>());
        Assert.That(captured, Is.Not.Null);
        Assert.That(captured!.Key, Is.EqualTo("k1"));
        Assert.That(captured.TimestampUtc, Is.EqualTo(FixedNow));
        Assert.That(captured.ValuePreview.Length, Is.EqualTo(4)); // preview bounded
        Assert.That(captured.ValueByteLength, Is.EqualTo(ingested.Length));
    }

    [Test]
    public async Task OnWriteAsync_strict_ingest_upcastable_item_accepts()
    {
        var (interceptor, dlq) = Create(
            new LatticeSchemaVersionConfig(schemaId: 1, targetVersion: 3, strictIngest: true),
            strictIngestEnabled: true,
            canUpcast: true);

        var ingested = LatticeSchemaEnvelope.Encode(1, 1, Utf8("body"));

        LatticeWriteDecision decision;
        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            decision = await interceptor.OnWriteAsync(Request(ingested));
        }

        Assert.That(decision.Kind, Is.EqualTo(LatticeWriteDecisionKind.Accept));
        await dlq.DidNotReceive().AppendAsync(Arg.Any<string>(), Arg.Any<LatticeSchemaDeadLetterEntry>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task OnWriteAsync_strict_ingest_newer_than_target_dead_letters()
    {
        var (interceptor, dlq) = Create(
            new LatticeSchemaVersionConfig(schemaId: 1, targetVersion: 2, strictIngest: true),
            strictIngestEnabled: true,
            canUpcast: true);

        var ingested = LatticeSchemaEnvelope.Encode(1, 5, Utf8("body")); // v5 > target v2

        LatticeWriteDecision decision;
        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            decision = await interceptor.OnWriteAsync(Request(ingested));
        }

        Assert.That(decision.Kind, Is.EqualTo(LatticeWriteDecisionKind.DeadLetter));
        await dlq.Received(1).AppendAsync("orders", Arg.Any<LatticeSchemaDeadLetterEntry>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task OnWriteAsync_non_strict_tree_trusts_enveloped_ingest()
    {
        // Global strict on, but the tree's config leaves strict off: trust the item.
        var (interceptor, dlq) = Create(
            new LatticeSchemaVersionConfig(schemaId: 1, targetVersion: 3, strictIngest: false),
            strictIngestEnabled: true,
            canUpcast: false);

        var ingested = LatticeSchemaEnvelope.Encode(1, 1, Utf8("body"));

        LatticeWriteDecision decision;
        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            decision = await interceptor.OnWriteAsync(Request(ingested));
        }

        Assert.That(decision.Kind, Is.EqualTo(LatticeWriteDecisionKind.Accept));
        await dlq.DidNotReceive().AppendAsync(Arg.Any<string>(), Arg.Any<LatticeSchemaDeadLetterEntry>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public void InterceptsSystemOrigin_mirrors_provider_strict_flag()
    {
        Assert.That(Create(config: null, strictIngestEnabled: true).Interceptor.InterceptsSystemOrigin, Is.True);
        Assert.That(Create(config: null, strictIngestEnabled: false).Interceptor.InterceptsSystemOrigin, Is.False);
    }

    [Test]
    public void Constructor_null_arguments_throw()
    {
        var provider = Substitute.For<ILatticeSchemaVersionProvider>();
        var registry = Substitute.For<ILatticeSchemaRegistry>();
        var dlq = Substitute.For<ILatticeSchemaDeadLetterStore>();
        var options = Options.Create(new LatticeSchemaVersioningOptions());

        Assert.That(() => new LatticeSchemaVersionWriteInterceptor(null!, registry, dlq, options, TimeProvider.System), Throws.ArgumentNullException);
        Assert.That(() => new LatticeSchemaVersionWriteInterceptor(provider, null!, dlq, options, TimeProvider.System), Throws.ArgumentNullException);
        Assert.That(() => new LatticeSchemaVersionWriteInterceptor(provider, registry, null!, options, TimeProvider.System), Throws.ArgumentNullException);
        Assert.That(() => new LatticeSchemaVersionWriteInterceptor(provider, registry, dlq, null!, TimeProvider.System), Throws.ArgumentNullException);
        Assert.That(() => new LatticeSchemaVersionWriteInterceptor(provider, registry, dlq, options, null!), Throws.ArgumentNullException);
    }
}
