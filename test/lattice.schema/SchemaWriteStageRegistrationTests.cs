using System.Text;
using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using Orleans.Hosting;

namespace Orleans.Lattice.Schema.Tests;

/// <summary>
/// Caller-level tests of the write interceptor each supported (and the one
/// unsupported) registration of the schema add-ons resolves, driven through the
/// real registration extensions and the resolved <see cref="ILatticeWriteInterceptor"/>.
/// Only the per-tree policy and version-config providers and the dead-letter store
/// are replaced (they are resolved with <c>TryAdd</c>, so a pre-registered fake
/// wins), so the stage set, its order, the system-origin opt-in and the value
/// threading are the production composition. These are the detectors for the
/// registration rows of <c>spec/schema/SchemaStageOrderingRefinement.md</c>.
/// </summary>
[TestFixture]
public sealed class SchemaWriteStageRegistrationTests
{
    private const string Tree = "orders";

    private static readonly LatticeSchemaVersionConfig Version = new(schemaId: 5, targetVersion: 2, strictIngest: true);

    private static byte[] Utf8(string s) => Encoding.UTF8.GetBytes(s);

    private static LatticeWriteRequest Request(byte[] value) => new(Tree, "k1", value, LatticeOperation.Write);

    [Test]
    public async Task Enforcement_only_rejects_an_invalid_local_write_and_accepts_a_valid_one_unchanged()
    {
        var (interceptor, _) = Resolve(enforcement: true, versioning: false, reversed: false);

        Assert.ThrowsAsync<LatticeSchemaViolationException>(
            async () => await interceptor.OnWriteAsync(Request(Utf8("not json"))));
        var decision = await interceptor.OnWriteAsync(Request(Utf8("{\"a\":1}")));

        Assert.That(decision.Kind, Is.EqualTo(LatticeWriteDecisionKind.Accept));
    }

    [Test]
    public async Task Versioning_only_stamps_a_raw_local_write_at_the_target()
    {
        var (interceptor, _) = Resolve(enforcement: false, versioning: true, reversed: false);

        var decision = await interceptor.OnWriteAsync(Request(Utf8("not json")));

        Assert.That(decision.Kind, Is.EqualTo(LatticeWriteDecisionKind.AcceptTransformed));
        Assert.That(LatticeSchemaEnvelope.TryReadHeader(decision.TransformedValue!, out var schemaId, out var version), Is.True);
        Assert.That((schemaId, version), Is.EqualTo((Version.SchemaId, Version.TargetVersion)));
    }

    [TestCase(false)]
    [TestCase(true)]
    public void Either_registration_order_resolves_one_composed_interceptor(bool reversed)
    {
        // DI level (#5241): enforcement registered after versioning re-composes
        // with the versioning stage instead of replacing it, so both orders leave
        // exactly one interceptor descriptor and it resolves to the composite.
        var builder = new FakeSiloBuilder().WithCoreLattice();
        if (reversed)
        {
            builder.AddLatticeSchemaVersioning();
            builder.AddLatticeSchemaEnforcement();
        }
        else
        {
            builder.AddLatticeSchemaEnforcement();
            builder.AddLatticeSchemaVersioning();
        }

        using var services = builder.Services.BuildServiceProvider();

        Assert.That(builder.Services.Count(d => d.ServiceType == typeof(ILatticeWriteInterceptor)), Is.EqualTo(1));
        Assert.That(services.GetRequiredService<ILatticeWriteInterceptor>(), Is.TypeOf<CompositeLatticeWriteInterceptor>());
    }

    [Test]
    public void Enforcement_only_registration_resolves_the_enforcement_interceptor()
    {
        var builder = new FakeSiloBuilder().WithCoreLattice();
        builder.AddLatticeSchemaEnforcement();
        using var services = builder.Services.BuildServiceProvider();

        Assert.That(services.GetRequiredService<ILatticeWriteInterceptor>(), Is.TypeOf<LatticeSchemaWriteInterceptor>());
    }

    [TestCase(false)]
    [TestCase(true)]
    public async Task Composed_registration_validates_the_submitted_value_then_stamps_it(bool reversed)
    {
        var (interceptor, _) = Resolve(enforcement: true, versioning: true, reversed: reversed);

        Assert.ThrowsAsync<LatticeSchemaViolationException>(
            async () => await interceptor.OnWriteAsync(Request(Utf8("not json"))));
        var body = Utf8("{\"a\":1}");
        var decision = await interceptor.OnWriteAsync(Request(body));

        Assert.That(decision.Kind, Is.EqualTo(LatticeWriteDecisionKind.AcceptTransformed));
        Assert.That(LatticeSchemaEnvelope.StripToBody(decision.TransformedValue!), Is.EqualTo(body));
    }

    [TestCase(false)]
    [TestCase(true)]
    public async Task Composed_registration_runs_enforcement_on_the_unstamped_value(bool reversed)
    {
        // A strict system-origin diversion records a preview of the bytes the
        // enforcement stage judged. Enforcement runs first in either registration
        // order, so the preview is the submitted value, never the version envelope
        // the second stage would add.
        var (interceptor, deadLetters) = Resolve(enforcement: true, versioning: true, reversed: reversed);
        var submitted = Utf8("not json");

        LatticeWriteDecision decision;
        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            decision = await interceptor.OnWriteAsync(Request(submitted));
        }

        Assert.That(decision.Kind, Is.EqualTo(LatticeWriteDecisionKind.DeadLetter));
        await deadLetters.Received(1).AppendAsync(
            Tree,
            Arg.Is<LatticeSchemaDeadLetterEntry>(entry => entry.ValuePreview.SequenceEqual(submitted)),
            Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task Reversed_registration_validates_then_stamps()
    {
        // #5241: versioning registered before enforcement used to resolve the
        // enforcement stage alone, silently dropping stamping. Enforcement now
        // re-composes with the versioning stage, so a valid write is stamped at
        // the target and an invalid one is still rejected.
        var (interceptor, _) = Resolve(enforcement: true, versioning: true, reversed: true);

        Assert.ThrowsAsync<LatticeSchemaViolationException>(
            async () => await interceptor.OnWriteAsync(Request(Utf8("not json"))));
        var decision = await interceptor.OnWriteAsync(Request(Utf8("{\"a\":1}")));

        Assert.That(decision.Kind, Is.EqualTo(LatticeWriteDecisionKind.AcceptTransformed));
        Assert.That(LatticeSchemaEnvelope.TryReadHeader(decision.TransformedValue!, out var schemaId, out var version), Is.True);
        Assert.That((schemaId, version), Is.EqualTo((Version.SchemaId, Version.TargetVersion)));
    }

    [TestCase(false, false, false, false)]
    [TestCase(true, false, true, false)]
    [TestCase(false, true, true, false)]
    [TestCase(true, true, true, false)]
    [TestCase(false, false, false, true)]
    [TestCase(true, false, true, true)]
    [TestCase(false, true, true, true)]
    [TestCase(true, true, true, true)]
    public void Composed_registration_opts_into_system_origin_when_either_stage_is_globally_strict(
        bool enforcementStrict, bool versioningStrict, bool expected, bool reversed)
    {
        var builder = new FakeSiloBuilder().WithCoreLattice();
        if (reversed)
        {
            builder.AddLatticeSchemaVersioning(configureOptions: options => options.StrictIngest = versioningStrict);
            builder.AddLatticeSchemaEnforcement(options => options.StrictIngest = enforcementStrict);
        }
        else
        {
            builder.AddLatticeSchemaEnforcement(options => options.StrictIngest = enforcementStrict);
            builder.AddLatticeSchemaVersioning(configureOptions: options => options.StrictIngest = versioningStrict);
        }

        using var services = builder.Services.BuildServiceProvider();

        var interceptor = services.GetRequiredService<ILatticeWriteInterceptor>();

        Assert.That(interceptor.InterceptsSystemOrigin, Is.EqualTo(expected));
    }

    private static (ILatticeWriteInterceptor Interceptor, ILatticeSchemaDeadLetterStore DeadLetters) Resolve(
        bool enforcement, bool versioning, bool reversed)
    {
        var builder = new FakeSiloBuilder().WithCoreLattice();

        var policies = Substitute.For<ILatticeSchemaPolicyProvider>();
        policies.StrictIngestEnabled.Returns(true);
        policies.GetCompiledPolicyAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(new ValueTask<CompiledSchemaPolicy?>(CompiledSchemaPolicy.Compile(
                new LatticeSchemaPolicy(new[] { LatticeSchemaRule.Json() }, strictIngest: true))));
        var versions = Substitute.For<ILatticeSchemaVersionProvider>();
        versions.StrictIngestEnabled.Returns(true);
        versions.GetConfigAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(new ValueTask<LatticeSchemaVersionConfig?>(Version));
        var deadLetters = Substitute.For<ILatticeSchemaDeadLetterStore>();
        builder.Services.AddSingleton(policies);
        builder.Services.AddSingleton(versions);
        builder.Services.AddSingleton(deadLetters);

        if (reversed)
        {
            AddVersioning(builder, versioning);
            AddEnforcement(builder, enforcement);
        }
        else
        {
            AddEnforcement(builder, enforcement);
            AddVersioning(builder, versioning);
        }

        var services = builder.Services.BuildServiceProvider();
        return (services.GetRequiredService<ILatticeWriteInterceptor>(), deadLetters);
    }

    private static void AddEnforcement(ISiloBuilder builder, bool add)
    {
        if (add)
        {
            builder.AddLatticeSchemaEnforcement(options => options.StrictIngest = true);
        }
    }

    private static void AddVersioning(ISiloBuilder builder, bool add)
    {
        if (add)
        {
            builder.AddLatticeSchemaVersioning(configureOptions: options => options.StrictIngest = true);
        }
    }
}
