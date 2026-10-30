using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using NSubstitute;

namespace Orleans.Lattice.Schema.Tests;

/// <summary>
/// Regression tests for #5239: over the production registration (schema
/// enforcement composed ahead of schema versioning), a value that reaches the write
/// choke point already enveloped - a replicated atomic-batch entry, a replicated
/// typed-CRDT delta, or a client re-write of stored bytes - is judged by its body
/// at the tree's target shape (the bytes a reader is served), never by its
/// version-envelope header. Without versioning registered no read strips an
/// envelope, so the whole bytes are judged and an envelope-shaped prefix cannot be
/// used to slip a value past the policy.
/// </summary>
[TestFixture]
public sealed class SchemaEnforcementEnvelopedBodyTests
{
    private const string Tree = "orders";
    private const uint SchemaId = 5;

    private static byte[] Utf8(string s) => Encoding.UTF8.GetBytes(s);

    private static LatticeSchemaPolicy StatusPolicy(bool strictIngest = true) => new(
        new[]
        {
            LatticeSchemaRule.Json(),
            LatticeSchemaRule.Regex("^(open|closed)$", memberPath: "status"),
        },
        strictIngest);

    private sealed record Harness(ILatticeWriteInterceptor Interceptor, ILatticeSchemaDeadLetterStore DeadLetters);

    private static Harness Build(
        LatticeSchemaPolicy policy,
        LatticeSchemaVersionConfig? versionConfig,
        bool registerVersioning = true,
        Action<LatticeSchemaRegistryBuilder>? configureRegistry = null)
    {
        var builder = new FakeSiloBuilder().WithCoreLattice();

        // AddLattice registers the core no-op decoder; versioning replaces it.
        builder.Services.TryAddSingleton<ILatticeValueDecoder, NullLatticeValueDecoder>();
        builder.AddLatticeSchemaEnforcement(o => o.StrictIngest = true);
        if (registerVersioning)
        {
            builder.AddLatticeSchemaVersioning(configureRegistry, o => o.StrictIngest = true);

            // The durable runtime catalog is a grain; the host registry stands in for it.
            builder.Services.RemoveAll<SchemaRuntimeRegistry>();
        }

        var policies = Substitute.For<ILatticeSchemaPolicyProvider>();
        policies.StrictIngestEnabled.Returns(true);
        policies.GetCompiledPolicyAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(new ValueTask<CompiledSchemaPolicy?>(CompiledSchemaPolicy.Compile(policy)));
        builder.Services.Replace(ServiceDescriptor.Singleton(policies));

        var versions = Substitute.For<ILatticeSchemaVersionProvider>();
        versions.StrictIngestEnabled.Returns(true);
        versions.GetConfigAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(new ValueTask<LatticeSchemaVersionConfig?>(versionConfig));
        builder.Services.Replace(ServiceDescriptor.Singleton(versions));

        var deadLetters = Substitute.For<ILatticeSchemaDeadLetterStore>();
        builder.Services.Replace(ServiceDescriptor.Singleton(deadLetters));

        var interceptor = builder.Services.BuildServiceProvider().GetRequiredService<ILatticeWriteInterceptor>();
        return new Harness(interceptor, deadLetters);
    }

    private static LatticeWriteRequest Request(byte[] value, LatticeOperation operation = LatticeOperation.Write) =>
        new(Tree, "k1", value, operation);

    private static async Task<LatticeWriteDecision> IngestAsync(Harness harness, LatticeWriteRequest request)
    {
        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            return await harness.Interceptor.OnWriteAsync(request);
        }
    }

    private static Task AssertNoDeadLetterAsync(Harness harness) =>
        harness.DeadLetters.DidNotReceive().AppendAsync(
            Arg.Any<string>(), Arg.Any<LatticeSchemaDeadLetterEntry>(), Arg.Any<CancellationToken>());

    [Test]
    public async Task Strict_ingest_of_enveloped_value_with_compliant_body_is_accepted_verbatim()
    {
        var harness = Build(StatusPolicy(), new LatticeSchemaVersionConfig(SchemaId, 1, strictIngest: true));
        var stored = LatticeSchemaEnvelope.Encode(SchemaId, 1, Utf8("{\"status\":\"open\"}"));

        var decision = await IngestAsync(harness, Request(stored));

        Assert.That(decision.Kind, Is.EqualTo(LatticeWriteDecisionKind.Accept), decision.Reason);
        await AssertNoDeadLetterAsync(harness);
    }

    [Test]
    public async Task Strict_ingest_of_enveloped_value_with_noncompliant_body_is_dead_lettered()
    {
        var harness = Build(StatusPolicy(), new LatticeSchemaVersionConfig(SchemaId, 1, strictIngest: true));
        var stored = LatticeSchemaEnvelope.Encode(SchemaId, 1, Utf8("{\"status\":\"bogus\"}"));

        var decision = await IngestAsync(harness, Request(stored));

        Assert.That(decision.Kind, Is.EqualTo(LatticeWriteDecisionKind.DeadLetter));
        Assert.That(decision.Reason, Does.Contain("status"));
        await harness.DeadLetters.Received(1).AppendAsync(
            Tree,
            Arg.Is<LatticeSchemaDeadLetterEntry>(e => e.Key == "k1" && e.ValueByteLength == stored.Length),
            Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task Strict_ingest_of_enveloped_crdt_delta_with_noncompliant_body_is_dead_lettered()
    {
        var harness = Build(StatusPolicy(), new LatticeSchemaVersionConfig(SchemaId, 1, strictIngest: true));
        var delta = LatticeSchemaEnvelope.Encode(SchemaId, 1, Utf8("{\"status\":\"bogus\"}"));

        var decision = await IngestAsync(harness, Request(delta, LatticeOperation.CrdtApply));

        Assert.That(decision.Kind, Is.EqualTo(LatticeWriteDecisionKind.DeadLetter));
    }

    [Test]
    public async Task Strict_ingest_of_enveloped_crdt_delta_with_compliant_body_is_accepted_verbatim()
    {
        var harness = Build(StatusPolicy(), new LatticeSchemaVersionConfig(SchemaId, 1, strictIngest: true));
        var delta = LatticeSchemaEnvelope.Encode(SchemaId, 1, Utf8("{\"status\":\"closed\"}"));

        var decision = await IngestAsync(harness, Request(delta, LatticeOperation.CrdtApply));

        Assert.That(decision.Kind, Is.EqualTo(LatticeWriteDecisionKind.Accept), decision.Reason);
        await AssertNoDeadLetterAsync(harness);
    }

    [Test]
    public async Task Local_rewrite_of_enveloped_value_with_compliant_body_is_accepted_verbatim()
    {
        var harness = Build(StatusPolicy(), new LatticeSchemaVersionConfig(SchemaId, 1, strictIngest: true));
        var stored = LatticeSchemaEnvelope.Encode(SchemaId, 1, Utf8("{\"status\":\"open\"}"));

        var decision = await harness.Interceptor.OnWriteAsync(Request(stored));

        Assert.That(decision.Kind, Is.EqualTo(LatticeWriteDecisionKind.Accept), decision.Reason);
    }

    [Test]
    public void Local_rewrite_of_enveloped_value_with_noncompliant_body_is_rejected()
    {
        var harness = Build(StatusPolicy(), new LatticeSchemaVersionConfig(SchemaId, 1, strictIngest: true));
        var stored = LatticeSchemaEnvelope.Encode(SchemaId, 1, Utf8("{\"status\":\"bogus\"}"));

        Assert.That(
            async () => await harness.Interceptor.OnWriteAsync(Request(stored)),
            Throws.TypeOf<LatticeSchemaViolationException>());
    }

    [Test]
    public async Task Max_length_rule_counts_the_body_not_the_envelope_header()
    {
        var body = Utf8("{\"status\":\"open\"}");
        var policy = new LatticeSchemaPolicy(new[] { LatticeSchemaRule.MaxLength(body.Length) }, strictIngest: true);
        var harness = Build(policy, new LatticeSchemaVersionConfig(SchemaId, 1, strictIngest: true));

        var decision = await IngestAsync(harness, Request(LatticeSchemaEnvelope.Encode(SchemaId, 1, body)));

        Assert.That(decision.Kind, Is.EqualTo(LatticeWriteDecisionKind.Accept), decision.Reason);
    }

    [Test]
    public async Task Enveloped_value_at_an_older_version_is_judged_at_the_target_shape()
    {
        // v1 bodies carry "state"; the v1 -> v2 upcaster renames it to "status", the
        // member the policy constrains. A reader is served the v2 shape, so that is
        // the shape enforcement judges.
        var harness = Build(
            StatusPolicy(),
            new LatticeSchemaVersionConfig(SchemaId, 2, strictIngest: true),
            configureRegistry: r => r.AddUpcaster(
                SchemaId, 1, 2,
                LatticeValueTransform.Passthrough(LatticeValueTransform.RenameMember("state", "status"))));

        var compliant = await IngestAsync(
            harness, Request(LatticeSchemaEnvelope.Encode(SchemaId, 1, Utf8("{\"state\":\"open\"}"))));
        var noncompliant = await IngestAsync(
            harness, Request(LatticeSchemaEnvelope.Encode(SchemaId, 1, Utf8("{\"state\":\"bogus\"}"))));

        Assert.That(compliant.Kind, Is.EqualTo(LatticeWriteDecisionKind.Accept), compliant.Reason);
        Assert.That(noncompliant.Kind, Is.EqualTo(LatticeWriteDecisionKind.DeadLetter));
    }

    [Test]
    public void Local_write_of_enveloped_value_newer_than_target_is_rejected()
    {
        // No reader can be served a value stamped newer than the target, so its body
        // cannot be judged at the target shape: enforcement fails closed.
        var harness = Build(StatusPolicy(), new LatticeSchemaVersionConfig(SchemaId, 1, strictIngest: true));
        var stored = LatticeSchemaEnvelope.Encode(SchemaId, 2, Utf8("{\"status\":\"open\"}"));

        Assert.That(
            async () => await harness.Interceptor.OnWriteAsync(Request(stored)),
            Throws.TypeOf<LatticeSchemaViolationException>());
    }

    [Test]
    public void Without_versioning_an_envelope_shaped_prefix_is_judged_as_part_of_the_value()
    {
        // No decoder strips the header on read, so a reader would be served the
        // prefixed bytes: they are judged whole, and the prefix is no bypass.
        var harness = Build(StatusPolicy(), versionConfig: null, registerVersioning: false);
        var forged = LatticeSchemaEnvelope.Encode(SchemaId, 1, Utf8("{\"status\":\"open\"}"));

        Assert.That(
            async () => await harness.Interceptor.OnWriteAsync(Request(forged)),
            Throws.TypeOf<LatticeSchemaViolationException>());
    }

    [Test]
    public async Task Plain_local_write_is_validated_then_stamped_at_the_target()
    {
        var harness = Build(StatusPolicy(), new LatticeSchemaVersionConfig(SchemaId, 1, strictIngest: true));
        var body = Utf8("{\"status\":\"open\"}");

        var decision = await harness.Interceptor.OnWriteAsync(Request(body));

        Assert.That(decision.Kind, Is.EqualTo(LatticeWriteDecisionKind.AcceptTransformed));
        Assert.That(decision.TransformedValue, Is.EqualTo(LatticeSchemaEnvelope.Encode(SchemaId, 1, body)));
    }
}
