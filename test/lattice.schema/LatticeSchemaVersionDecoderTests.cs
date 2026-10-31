using System.Text;
using System.Text.Json;
using NSubstitute;

namespace Orleans.Lattice.Schema.Tests;

/// <summary>
/// Unit tests for <see cref="LatticeSchemaVersionDecoder"/>: verbatim passthrough
/// of an un-stamped value, envelope stripping at the target version, read-time
/// upcasting of a stale value, and the newer-than-target throw.
/// </summary>
public sealed class LatticeSchemaVersionDecoderTests
{
    private static byte[] Utf8(string s) => Encoding.UTF8.GetBytes(s);

    private static LatticeValueTransform SetMember(string name, long value) =>
        LatticeValueTransform.Passthrough(
            LatticeValueTransform.SetMember(name, LatticeValueTransform.Const(LatticeConstant.Integer(value))));

    private static ILatticeSchemaVersionProvider Provider(LatticeSchemaVersionConfig? config)
    {
        var provider = Substitute.For<ILatticeSchemaVersionProvider>();
        provider.GetConfigAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(new ValueTask<LatticeSchemaVersionConfig?>(config));
        return provider;
    }

    [Test]
    public void IsActive_true_for_every_tree()
    {
        var decoder = new LatticeSchemaVersionDecoder(
            Provider(null), new LatticeSchemaRegistryBuilder().Build());

        Assert.That(decoder.IsActive("orders"), Is.True);
        Assert.That(decoder.IsActive("sys-schema-version"), Is.True);
    }

    [Test]
    public async Task DecodeAsync_un_stamped_value_returns_same_reference()
    {
        var decoder = new LatticeSchemaVersionDecoder(
            Provider(new LatticeSchemaVersionConfig(1, 1)), new LatticeSchemaRegistryBuilder().Build());
        var stored = Utf8("{\"a\":1}");

        var result = await decoder.DecodeAsync("orders", stored, CancellationToken.None);

        Assert.That(result, Is.SameAs(stored));
    }

    [Test]
    public async Task DecodeAsync_enveloped_at_target_strips_to_body()
    {
        var decoder = new LatticeSchemaVersionDecoder(
            Provider(new LatticeSchemaVersionConfig(schemaId: 1, targetVersion: 2)),
            new LatticeSchemaRegistryBuilder().Build());
        var body = Utf8("{\"a\":1}");
        var stored = LatticeSchemaEnvelope.Encode(1, 2, body);

        var result = await decoder.DecodeAsync("orders", stored, CancellationToken.None);

        Assert.That(result, Is.EqualTo(body));
    }

    [Test]
    public async Task DecodeAsync_stale_value_is_upcast_to_target()
    {
        var registry = new LatticeSchemaRegistryBuilder()
            .AddUpcaster(1, 1, 2, SetMember("b", 2))
            .Build();
        var decoder = new LatticeSchemaVersionDecoder(
            Provider(new LatticeSchemaVersionConfig(schemaId: 1, targetVersion: 2)), registry);
        var stored = LatticeSchemaEnvelope.Encode(1, 1, Utf8("{\"a\":1}"));

        var result = await decoder.DecodeAsync("orders", stored, CancellationToken.None);

        var root = JsonDocument.Parse(result).RootElement;
        Assert.That(root.GetProperty("a").GetInt32(), Is.EqualTo(1));
        Assert.That(root.GetProperty("b").GetInt32(), Is.EqualTo(2)); // upcaster ran
    }

    [Test]
    public void DecodeAsync_newer_than_target_throws_not_supported()
    {
        var decoder = new LatticeSchemaVersionDecoder(
            Provider(new LatticeSchemaVersionConfig(schemaId: 1, targetVersion: 2)),
            new LatticeSchemaRegistryBuilder().Build());
        var stored = LatticeSchemaEnvelope.Encode(1, 5, Utf8("body")); // v5 > target v2

        Assert.That(
            async () => await decoder.DecodeAsync("orders", stored, CancellationToken.None),
            Throws.InstanceOf<NotSupportedException>());
    }

    /// <summary>
    /// A stale value with no chain of hops to the target is an explicit failure,
    /// never its stored-version body passed off as the target shape.
    /// </summary>
    [Test]
    public void DecodeAsync_stale_value_with_no_upcast_path_throws_not_supported()
    {
        var registry = new LatticeSchemaRegistryBuilder()
            .AddUpcaster(1, 2, 3, SetMember("c", 3))
            .Build();
        var decoder = new LatticeSchemaVersionDecoder(
            Provider(new LatticeSchemaVersionConfig(schemaId: 1, targetVersion: 3)), registry);
        var stored = LatticeSchemaEnvelope.Encode(1, 1, Utf8("{\"a\":1}")); // no v1 -> v2 hop

        Assert.That(
            async () => await decoder.DecodeAsync("orders", stored, CancellationToken.None),
            Throws.InstanceOf<NotSupportedException>());
    }

    /// <summary>
    /// The decoder resolves the tree's runtime catalog on every stale read, so a
    /// hop registered after a failed read upcasts the same stored value on the next
    /// read without a restart or a cache to invalidate.
    /// </summary>
    [Test]
    public async Task DecodeAsync_resolves_the_runtime_catalog_on_every_read()
    {
        var host = new LatticeSchemaRegistryBuilder().AddSchema(1, 1, "v1").AddSchema(1, 2, "v2").Build();
        var withHop = new LatticeSchemaRuntimeRegistration
        {
            Upcasters = [new() { SchemaId = 1, FromVersion = 1, ToVersion = 2, Transform = SetMember("b", 2) }],
        };
        var grain = Substitute.For<ILatticeSchemaRuntimeRegistryGrain>();
        grain.GetAsync().Returns(new LatticeSchemaRuntimeRegistration(), withHop);
        var grains = Substitute.For<IGrainFactory>();
        grains.GetGrain<ILatticeSchemaRuntimeRegistryGrain>("orders", Arg.Any<string?>()).Returns(grain);
        var transforms = new LatticeValueTransformRegistry([]);
        var runtime = new SchemaRuntimeRegistry(grains, host, transforms, []);
        var decoder = new LatticeSchemaVersionDecoder(
            Provider(new LatticeSchemaVersionConfig(schemaId: 1, targetVersion: 2)), host, runtime);
        var stored = LatticeSchemaEnvelope.Encode(1, 1, Utf8("{\"a\":1}"));

        Assert.That(
            async () => await decoder.DecodeAsync("orders", stored, CancellationToken.None),
            Throws.InstanceOf<NotSupportedException>(), "no hop is registered yet");
        var result = await decoder.DecodeAsync("orders", stored, CancellationToken.None);

        Assert.That(JsonDocument.Parse(result).RootElement.GetProperty("b").GetInt32(), Is.EqualTo(2));
    }

    [Test]
    public async Task DecodeAsync_enveloped_with_no_config_strips_stored_body()
    {
        // Resolver-less replay: no config resolves, but a stamped value must never be
        // returned raw - it is stripped to its stored-version body.
        var decoder = new LatticeSchemaVersionDecoder(
            Provider(null), new LatticeSchemaRegistryBuilder().Build());
        var body = Utf8("{\"a\":1}");
        var stored = LatticeSchemaEnvelope.Encode(1, 1, body);

        var result = await decoder.DecodeAsync("orders", stored, CancellationToken.None);

        Assert.That(result, Is.EqualTo(body));
    }

    [Test]
    public async Task DecodeAsync_enveloped_with_mismatched_schema_strips_stored_body()
    {
        var decoder = new LatticeSchemaVersionDecoder(
            Provider(new LatticeSchemaVersionConfig(schemaId: 9, targetVersion: 2)),
            new LatticeSchemaRegistryBuilder().Build());
        var body = Utf8("{\"a\":1}");
        var stored = LatticeSchemaEnvelope.Encode(1, 1, body); // schema 1 != config schema 9

        var result = await decoder.DecodeAsync("orders", stored, CancellationToken.None);

        Assert.That(result, Is.EqualTo(body));
    }
}
