using System.Runtime.InteropServices;
using System.Text;
using System.Text.Json;
using NSubstitute;

namespace Orleans.Lattice.Schema.Tests;

/// <summary>
/// Unit tests for <see cref="LatticeSchemaVersionDecoder.DecodeForRestampAsync"/>,
/// the decode a transform remediation runs so it can re-envelope its rewrite at the
/// schema version the transform ran on (issue #5243): the body must match what
/// <see cref="LatticeSchemaVersionDecoder.DecodeAsync"/> returns, and the stamp must
/// be the version that body conforms to.
/// </summary>
public sealed class LatticeSchemaVersionDecoderRestampTests
{
    private const string TreeId = "orders";

    private static byte[] Utf8(string s) => Encoding.UTF8.GetBytes(s);

    private static ILatticeSchemaVersionProvider Provider(LatticeSchemaVersionConfig? config)
    {
        var provider = Substitute.For<ILatticeSchemaVersionProvider>();
        provider.GetConfigAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(new ValueTask<LatticeSchemaVersionConfig?>(config));
        return provider;
    }

    private static LatticeSchemaVersionDecoder Decoder(LatticeSchemaVersionConfig? config, ILatticeSchemaRegistry? registry = null) =>
        new(Provider(config), registry ?? new LatticeSchemaRegistryBuilder().Build());

    [Test]
    public async Task DecodeForRestampAsync_legacy_value_on_a_versioned_tree_is_stamped_at_the_target_without_a_copy()
    {
        var stored = Utf8("{\"a\":1}");

        var result = await Decoder(new LatticeSchemaVersionConfig(1, 2)).DecodeForRestampAsync(TreeId, stored, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(result.Stamped, Is.True);
            Assert.That((result.SchemaId, result.Version), Is.EqualTo((1u, 2u)));
            Assert.That(MemoryMarshal.TryGetArray(result.Body, out var segment) && ReferenceEquals(segment.Array, stored),
                Is.True, "the legacy value is the body itself");
        });
    }

    [Test]
    public async Task DecodeForRestampAsync_on_an_unversioned_tree_reports_no_stamp()
    {
        var plain = Utf8("{\"a\":1}");
        var enveloped = LatticeSchemaEnvelope.Encode(1, 1, plain);
        var decoder = Decoder(config: null);

        var fromPlain = await decoder.DecodeForRestampAsync(TreeId, plain, CancellationToken.None);
        var fromEnveloped = await decoder.DecodeForRestampAsync(TreeId, enveloped, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(fromPlain.Stamped, Is.False);
            Assert.That(fromPlain.Body.ToArray(), Is.EqualTo(plain));
            Assert.That(fromEnveloped.Stamped, Is.False);
            Assert.That(fromEnveloped.Body.ToArray(), Is.EqualTo(plain), "the body a read returns, as DecodeAsync does");
        });
    }

    [Test]
    public async Task DecodeForRestampAsync_value_at_the_target_is_a_view_past_the_header_stamped_at_the_target()
    {
        var body = Utf8("{\"a\":1}");
        var stored = LatticeSchemaEnvelope.Encode(1, 2, body);

        var result = await Decoder(new LatticeSchemaVersionConfig(1, 2)).DecodeForRestampAsync(TreeId, stored, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(result.Stamped, Is.True);
            Assert.That((result.SchemaId, result.Version), Is.EqualTo((1u, 2u)));
            Assert.That(result.Body.ToArray(), Is.EqualTo(body));
            Assert.That(MemoryMarshal.TryGetArray(result.Body, out var segment)
                && ReferenceEquals(segment.Array, stored)
                && segment.Offset == LatticeSchemaEnvelope.HeaderLength, Is.True, "no copy of the body is made");
        });
    }

    [Test]
    public async Task DecodeForRestampAsync_stale_value_is_upcast_and_stamped_at_the_target()
    {
        var registry = new LatticeSchemaRegistryBuilder()
            .AddUpcaster(1, 1, 2, LatticeValueTransform.Passthrough(
                LatticeValueTransform.SetMember("b", LatticeValueTransform.Const(LatticeConstant.Integer(2)))))
            .Build();
        var stored = LatticeSchemaEnvelope.Encode(1, 1, Utf8("{\"a\":1}"));
        var decoder = Decoder(new LatticeSchemaVersionConfig(1, 2), registry);

        var result = await decoder.DecodeForRestampAsync(TreeId, stored, CancellationToken.None);
        var read = await decoder.DecodeAsync(TreeId, stored, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That((result.Stamped, result.SchemaId, result.Version), Is.EqualTo((true, 1u, 2u)));
            Assert.That(result.Body.ToArray(), Is.EqualTo(read), "the body a client read returns");
            Assert.That(JsonDocument.Parse(result.Body).RootElement.GetProperty("b").GetInt32(), Is.EqualTo(2));
        });
    }

    [Test]
    public async Task DecodeForRestampAsync_value_of_another_schema_family_keeps_its_own_stamp()
    {
        var body = Utf8("{\"a\":1}");
        var stored = LatticeSchemaEnvelope.Encode(9, 1, body);

        var result = await Decoder(new LatticeSchemaVersionConfig(1, 2)).DecodeForRestampAsync(TreeId, stored, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That((result.Stamped, result.SchemaId, result.Version), Is.EqualTo((true, 9u, 1u)));
            Assert.That(result.Body.ToArray(), Is.EqualTo(body));
        });
    }

    [Test]
    public void DecodeForRestampAsync_value_newer_than_the_target_throws_NotSupportedException()
    {
        var stored = LatticeSchemaEnvelope.Encode(1, 3, Utf8("{\"a\":1}"));

        Assert.That(
            async () => await Decoder(new LatticeSchemaVersionConfig(1, 2)).DecodeForRestampAsync(TreeId, stored, CancellationToken.None),
            Throws.InstanceOf<NotSupportedException>());
    }

    [Test]
    public void Restamp_envelopes_only_a_stamped_input()
    {
        var rewritten = Utf8("{\"a\":2}");

        var stamped = new SchemaRestampInput(ReadOnlyMemory<byte>.Empty, Stamped: true, 4, 5).Restamp(rewritten);
        var plain = new SchemaRestampInput(ReadOnlyMemory<byte>.Empty, Stamped: false, 0, 0).Restamp(rewritten);

        Assert.Multiple(() =>
        {
            Assert.That(LatticeSchemaEnvelope.TryReadHeader(stamped, out var schemaId, out var version), Is.True);
            Assert.That((schemaId, version), Is.EqualTo((4u, 5u)));
            Assert.That(LatticeSchemaEnvelope.StripToBody(stamped), Is.EqualTo(rewritten));
            Assert.That(plain, Is.SameAs(rewritten));
        });
    }
}
