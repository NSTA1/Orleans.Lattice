using System.Text;
using Microsoft.Extensions.Options;
using NSubstitute;

namespace Orleans.Lattice.Schema.Tests;

/// <summary>
/// Unit tests for <see cref="LatticeSchemaWriteInterceptor"/>: zero-overhead
/// accept for an ungoverned tree, fail-closed local rejection, strict-ingest
/// dead-lettering, trusted (non-strict) ingest, and CRDT delta handling.
/// </summary>
public class LatticeSchemaWriteInterceptorTests
{
    private static readonly DateTimeOffset FixedNow =
        new(2026, 2, 3, 4, 5, 6, TimeSpan.Zero);

    private static byte[] Utf8(string s) => Encoding.UTF8.GetBytes(s);

    private static (LatticeSchemaWriteInterceptor Interceptor, ILatticeSchemaDeadLetterStore Dlq, ILatticeSchemaPolicyProvider Provider)
        Create(CompiledSchemaPolicy? compiled, bool strictIngestEnabled = false)
    {
        var provider = Substitute.For<ILatticeSchemaPolicyProvider>();
        provider.StrictIngestEnabled.Returns(strictIngestEnabled);
        provider.GetCompiledPolicyAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(new ValueTask<CompiledSchemaPolicy?>(compiled));

        var dlq = Substitute.For<ILatticeSchemaDeadLetterStore>();
        var options = Options.Create(new LatticeSchemaEnforcementOptions { DeadLetterPreviewMaxBytes = 4 });
        var interceptor = new LatticeSchemaWriteInterceptor(
            provider, dlq, options, new FixedTimeProvider(FixedNow));
        return (interceptor, dlq, provider);
    }

    private static CompiledSchemaPolicy JsonPolicy(bool strictIngest = false) =>
        CompiledSchemaPolicy.Compile(new LatticeSchemaPolicy(new[] { LatticeSchemaRule.Json() }, strictIngest));

    private static LatticeWriteRequest Request(byte[] value, LatticeOperation op = LatticeOperation.Write) =>
        new("orders", "k1", value, op);

    [Test]
    public async Task OnWriteAsync_ungoverned_tree_accepts_without_touching_dlq()
    {
        var (interceptor, dlq, _) = Create(compiled: null);

        var decision = await interceptor.OnWriteAsync(Request(Utf8("anything")));

        Assert.That(decision.Kind, Is.EqualTo(LatticeWriteDecisionKind.Accept));
        await dlq.DidNotReceive().AppendAsync(Arg.Any<string>(), Arg.Any<LatticeSchemaDeadLetterEntry>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task OnWriteAsync_valid_local_value_accepts()
    {
        var (interceptor, _, _) = Create(JsonPolicy());

        var decision = await interceptor.OnWriteAsync(Request(Utf8("{\"a\":1}")));

        Assert.That(decision.Kind, Is.EqualTo(LatticeWriteDecisionKind.Accept));
    }

    [Test]
    public void OnWriteAsync_invalid_local_value_throws_violation()
    {
        var (interceptor, _, _) = Create(JsonPolicy());

        var ex = Assert.ThrowsAsync<LatticeSchemaViolationException>(
            async () => await interceptor.OnWriteAsync(Request(Utf8("not json"))));

        Assert.That(ex!.TreeId, Is.EqualTo("orders"));
        Assert.That(ex.Key, Is.EqualTo("k1"));
    }

    [Test]
    public async Task OnWriteAsync_invalid_local_value_persists_nothing_to_dlq()
    {
        var (interceptor, dlq, _) = Create(JsonPolicy());

        try
        {
            await interceptor.OnWriteAsync(Request(Utf8("not json")));
        }
        catch (LatticeSchemaViolationException)
        {
            // expected
        }

        await dlq.DidNotReceive().AppendAsync(Arg.Any<string>(), Arg.Any<LatticeSchemaDeadLetterEntry>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task OnWriteAsync_strict_ingest_invalid_item_dead_letters()
    {
        var (interceptor, dlq, _) = Create(JsonPolicy(strictIngest: true), strictIngestEnabled: true);
        LatticeSchemaDeadLetterEntry? captured = null;
        dlq.When(x => x.AppendAsync(Arg.Any<string>(), Arg.Any<LatticeSchemaDeadLetterEntry>(), Arg.Any<CancellationToken>()))
            .Do(ci => captured = ci.Arg<LatticeSchemaDeadLetterEntry>());

        LatticeWriteDecision decision;
        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            decision = await interceptor.OnWriteAsync(Request(Utf8("not json"), LatticeOperation.Write));
        }

        Assert.That(decision.Kind, Is.EqualTo(LatticeWriteDecisionKind.DeadLetter));
        await dlq.Received(1).AppendAsync("orders", Arg.Any<LatticeSchemaDeadLetterEntry>(), Arg.Any<CancellationToken>());
        Assert.That(captured, Is.Not.Null);
        Assert.That(captured!.Key, Is.EqualTo("k1"));
        Assert.That(captured.TimestampUtc, Is.EqualTo(FixedNow));
        Assert.That(captured.Source, Is.EqualTo(LatticeSchemaDeadLetterSource.Replication));
    }

    [Test]
    public async Task OnWriteAsync_strict_ingest_bounds_preview_to_configured_max()
    {
        var (interceptor, dlq, _) = Create(JsonPolicy(strictIngest: true), strictIngestEnabled: true);
        LatticeSchemaDeadLetterEntry? captured = null;
        dlq.When(x => x.AppendAsync(Arg.Any<string>(), Arg.Any<LatticeSchemaDeadLetterEntry>(), Arg.Any<CancellationToken>()))
            .Do(ci => captured = ci.Arg<LatticeSchemaDeadLetterEntry>());

        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            await interceptor.OnWriteAsync(Request(Utf8("0123456789"))); // 10 bytes, max preview 4
        }

        Assert.That(captured!.ValuePreview.Length, Is.EqualTo(4));
        Assert.That(captured.ValueByteLength, Is.EqualTo(10));
    }

    [Test]
    public async Task OnWriteAsync_restore_source_recorded_for_restore_operation()
    {
        var (interceptor, dlq, _) = Create(JsonPolicy(strictIngest: true), strictIngestEnabled: true);
        LatticeSchemaDeadLetterEntry? captured = null;
        dlq.When(x => x.AppendAsync(Arg.Any<string>(), Arg.Any<LatticeSchemaDeadLetterEntry>(), Arg.Any<CancellationToken>()))
            .Do(ci => captured = ci.Arg<LatticeSchemaDeadLetterEntry>());

        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            await interceptor.OnWriteAsync(Request(Utf8("not json"), LatticeOperation.Restore));
        }

        Assert.That(captured!.Source, Is.EqualTo(LatticeSchemaDeadLetterSource.Restore));
    }

    [Test]
    public async Task OnWriteAsync_non_strict_policy_trusts_ingest()
    {
        // Global strict on, but the tree's policy leaves strict off: trust the item.
        var (interceptor, dlq, _) = Create(JsonPolicy(strictIngest: false), strictIngestEnabled: true);

        LatticeWriteDecision decision;
        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            decision = await interceptor.OnWriteAsync(Request(Utf8("not json")));
        }

        Assert.That(decision.Kind, Is.EqualTo(LatticeWriteDecisionKind.Accept));
        await dlq.DidNotReceive().AppendAsync(Arg.Any<string>(), Arg.Any<LatticeSchemaDeadLetterEntry>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task OnWriteAsync_crdt_opaque_delta_accepts_without_validation()
    {
        var (interceptor, _, _) = Create(JsonPolicy());

        // A non-JSON (opaque) CRDT delta is accepted here; merge-result violations
        // are the observer's responsibility.
        var decision = await interceptor.OnWriteAsync(Request(Utf8("opaque-delta"), LatticeOperation.CrdtApply));

        Assert.That(decision.Kind, Is.EqualTo(LatticeWriteDecisionKind.Accept));
    }

    [Test]
    public void OnWriteAsync_crdt_json_delta_still_validated()
    {
        var policy = CompiledSchemaPolicy.Compile(
            new LatticeSchemaPolicy(new[] { LatticeSchemaRule.MaxLength(3) }));
        var (interceptor, _, _) = Create(policy);

        // A shape-checkable (JSON) delta that violates the policy is rejected.
        Assert.ThrowsAsync<LatticeSchemaViolationException>(
            async () => await interceptor.OnWriteAsync(Request(Utf8("{\"a\":1}"), LatticeOperation.CrdtApply)));
    }

    [Test]
    public void InterceptsSystemOrigin_mirrors_provider_strict_flag()
    {
        Assert.That(Create(compiled: null, strictIngestEnabled: true).Interceptor.InterceptsSystemOrigin, Is.True);
        Assert.That(Create(compiled: null, strictIngestEnabled: false).Interceptor.InterceptsSystemOrigin, Is.False);
    }

    [Test]
    public void Constructor_null_arguments_throw()
    {
        var provider = Substitute.For<ILatticeSchemaPolicyProvider>();
        var dlq = Substitute.For<ILatticeSchemaDeadLetterStore>();
        var options = Options.Create(new LatticeSchemaEnforcementOptions());

        Assert.That(() => new LatticeSchemaWriteInterceptor(null!, dlq, options, TimeProvider.System), Throws.ArgumentNullException);
        Assert.That(() => new LatticeSchemaWriteInterceptor(provider, null!, options, TimeProvider.System), Throws.ArgumentNullException);
        Assert.That(() => new LatticeSchemaWriteInterceptor(provider, dlq, null!, TimeProvider.System), Throws.ArgumentNullException);
        Assert.That(() => new LatticeSchemaWriteInterceptor(provider, dlq, options, null!), Throws.ArgumentNullException);
    }
}
