using System.Linq;
using NSubstitute;

namespace Orleans.Lattice.Schema.Tests;

/// <summary>
/// Issue #5243: a transform remediation of a versioning-enabled tree re-envelopes
/// each value it writes at the schema version the transform ran on, so the cutover
/// never produces un-enveloped values; an unversioned tree is written plain.
/// </summary>
public partial class LatticeSchemaRemediationGrainTests
{
    private static LatticeSchemaVersionDecoder VersionDecoder(LatticeSchemaVersionConfig? config)
    {
        var provider = Substitute.For<ILatticeSchemaVersionProvider>();
        provider.GetConfigAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(new ValueTask<LatticeSchemaVersionConfig?>(config));
        return new LatticeSchemaVersionDecoder(provider, new LatticeSchemaRegistryBuilder().Build());
    }

    private static LatticeValueTransform MarkRemediated() =>
        LatticeValueTransform.Passthrough(
            LatticeValueTransform.SetMember("r", LatticeValueTransform.Const(LatticeConstant.Integer(1))));

    private static bool IsAt(byte[] value, uint schemaId, uint version, string body) =>
        LatticeSchemaEnvelope.TryReadHeader(value, out var stampedSchemaId, out var stampedVersion)
        && stampedSchemaId == schemaId
        && stampedVersion == version
        && LatticeSchemaEnvelope.StripToBody(value).SequenceEqual(Utf8(body));

    [Test]
    public async Task StartAsync_over_a_versioned_tree_writes_every_value_enveloped_at_the_target_version()
    {
        var h = CreateGrainCore(
            () => EntriesBytes(("k0", Utf8("{\"a\":0}")), ("k1", Env(2, "{\"a\":1}"))),
            seedState: null,
            schemaRegistry: null,
            existingPolicy: null,
            valueDecoder: VersionDecoder(new LatticeSchemaVersionConfig(MigSchemaId, 2)));

        var report = await h.Grain.StartAsync(MarkRemediated(), JsonPolicy());

        Assert.That(report.Succeeded, Is.True, report.Reason);
        await h.Destination.Received(1).SetAsync("k0", Arg.Is<byte[]>(v => IsAt(v, MigSchemaId, 2, "{\"a\":0,\"r\":1}")));
        await h.Destination.Received(1).SetAsync("k1", Arg.Is<byte[]>(v => IsAt(v, MigSchemaId, 2, "{\"a\":1,\"r\":1}")));
    }

    [Test]
    public async Task StartAsync_over_an_unversioned_tree_writes_plain_values()
    {
        var h = CreateGrainCore(
            () => EntriesBytes(("k0", Utf8("{\"a\":0}"))),
            seedState: null,
            schemaRegistry: null,
            existingPolicy: null,
            valueDecoder: VersionDecoder(config: null));

        var report = await h.Grain.StartAsync(MarkRemediated(), JsonPolicy());

        Assert.That(report.Succeeded, Is.True, report.Reason);
        await h.Destination.Received(1).SetAsync("k0", Arg.Is<byte[]>(v => v.SequenceEqual(Utf8("{\"a\":0,\"r\":1}"))));
    }
}
