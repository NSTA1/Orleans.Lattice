using System.Text;
using NSubstitute;
using Orleans.Lattice;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Schema.Tests;

/// <summary>
/// Unit tests for how <see cref="LatticeSchemaComplianceAdmin"/> handles a value the
/// read-path <see cref="ILatticeValueDecoder"/> cannot decode (issue #5253): the
/// streaming scan stays the only read on a tree with no such value, a decode fault
/// with no active decoder still propagates, and a fault with an active decoder makes
/// the scan finish the rest of the tree from the stored form, counting the value
/// non-compliant under the shared <see cref="SchemaUndecodableValue"/> reason.
/// </summary>
[TestFixture]
public sealed class LatticeSchemaComplianceAdminUndecodableTests
{
    private const string Tree = "orders";

    private static byte[] Utf8(string s) => Encoding.UTF8.GetBytes(s);

    private static readonly CompiledSchemaPolicy Json =
        CompiledSchemaPolicy.Compile(new LatticeSchemaPolicy([LatticeSchemaRule.Json()]));

    private sealed record Harness(
        LatticeSchemaComplianceAdmin Admin,
        ILattice Grain,
        IGrainFactory GrainFactory,
        IReplicationApplyGrain Reader,
        ILatticeValueDecoder Decoder);

    private static Harness Create(bool decoderActive, bool withDecoder = true)
    {
        var grain = Substitute.For<ILattice>();
        var reader = Substitute.For<IReplicationApplyGrain>();
        var grainFactory = Substitute.For<IGrainFactory>();
        grainFactory.GetGrain<ILattice>(Tree).Returns(grain);
        grainFactory.GetGrain<IReplicationApplyGrain>(Tree).Returns(reader);
        var provider = Substitute.For<ILatticeSchemaPolicyProvider>();
        provider.GetCompiledPolicyAsync(Tree, Arg.Any<CancellationToken>())
            .Returns(new ValueTask<CompiledSchemaPolicy?>(Json));
        var decoder = Substitute.For<ILatticeValueDecoder>();
        decoder.IsActive(Tree).Returns(decoderActive);
        decoder.DecodeAsync(Tree, Arg.Any<byte[]>(), Arg.Any<CancellationToken>()).Returns(ci =>
        {
            var value = ci.ArgAt<byte[]>(1);
            return Encoding.UTF8.GetString(value) == "undecodable"
                ? throw new NotSupportedException("No upcaster registered.")
                : new ValueTask<byte[]>(value);
        });
        var admin = new LatticeSchemaComplianceAdmin(grainFactory, provider, withDecoder ? decoder : null);
        return new Harness(admin, grain, grainFactory, reader, decoder);
    }

    private static void SetEntries(ILattice grain, Func<IAsyncEnumerable<KeyValuePair<string, byte[]>>> entries) =>
        grain.EntriesAsync(
                Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<bool>(), Arg.Any<bool?>(), Arg.Any<CancellationToken>())
            .Returns(_ => entries());

    private static async IAsyncEnumerable<KeyValuePair<string, byte[]>> Faulting(params string[] decodedBefore)
    {
        foreach (var value in decodedBefore)
        {
            yield return new KeyValuePair<string, byte[]>($"k{Array.IndexOf(decodedBefore, value)}", Utf8(value));
        }

        await Task.CompletedTask;
        throw new NotSupportedException("No upcaster registered.");
    }

    private static async IAsyncEnumerable<KeyValuePair<string, byte[]>> Clean()
    {
        yield return new KeyValuePair<string, byte[]>("k0", Utf8("{}"));
        yield return new KeyValuePair<string, byte[]>("k1", Utf8("nope"));
        await Task.CompletedTask;
    }

    private static async IAsyncEnumerable<string> Keys(params string[] keys)
    {
        foreach (var key in keys)
        {
            yield return key;
        }

        await Task.CompletedTask;
    }

    [Test]
    public async Task ScanCompliance_with_an_active_decoder_and_no_undecodable_value_never_reads_the_stored_form()
    {
        var h = Create(decoderActive: true);
        SetEntries(h.Grain, Clean);

        var report = await h.Admin.ScanComplianceAsync(Tree);

        Assert.Multiple(() =>
        {
            Assert.That(report.CompliantCount, Is.EqualTo(1));
            Assert.That(report.NonCompliantCount, Is.EqualTo(1));
        });
        _ = h.Grain.DidNotReceive().KeysAsync(
            Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<bool>(), Arg.Any<bool?>(), Arg.Any<CancellationToken>());
        h.GrainFactory.DidNotReceive().GetGrain<IReplicationApplyGrain>(Arg.Any<string>(), Arg.Any<string?>());
        await h.Decoder.DidNotReceive().DecodeAsync(Arg.Any<string>(), Arg.Any<byte[]>(), Arg.Any<CancellationToken>());
    }

    [TestCase(false, true)]
    [TestCase(true, false)]
    public void ScanCompliance_without_an_active_decoder_propagates_a_decode_fault(bool decoderActive, bool withDecoder)
    {
        var h = Create(decoderActive, withDecoder);
        SetEntries(h.Grain, () => Faulting("{}"));

        Assert.ThrowsAsync<NotSupportedException>(() => h.Admin.ScanComplianceAsync(Tree));
    }

    [Test]
    public async Task ScanCompliance_after_a_decode_fault_judges_the_rest_from_the_stored_form()
    {
        var h = Create(decoderActive: true);
        SetEntries(h.Grain, () => Faulting("{}"));
        h.Grain.KeysAsync("k0\0", Arg.Any<string?>(), Arg.Any<bool>(), Arg.Any<bool?>(), Arg.Any<CancellationToken>())
            .Returns(_ => Keys("k1", "k2", "k3", "k4"));
        var stored = new Dictionary<string, byte[]?>
        {
            ["k1"] = Utf8("undecodable"),
            ["k2"] = Utf8("{}"),
            ["k3"] = null, // deleted after the key scan reached it
            ["k4"] = Utf8("nope"),
        };
        h.Reader.ReadStoredWithVersionAsync(Arg.Any<string>())
            .Returns(ci => Task.FromResult(new VersionedValue { Value = stored[ci.ArgAt<string>(0)] }));

        var report = await h.Admin.ScanComplianceAsync(Tree);

        var rows = report.RuleBreakdown.ToDictionary(r => r.Reason, r => r.Count, StringComparer.Ordinal);
        Assert.Multiple(() =>
        {
            Assert.That(report.ScannedCount, Is.EqualTo(4), "k0 streamed, k1, k2 and k4 from the stored form; k3 is gone");
            Assert.That(report.CompliantCount, Is.EqualTo(2));
            Assert.That(report.NonCompliantCount, Is.EqualTo(2));
            Assert.That(
                rows.GetValueOrDefault(SchemaUndecodableValue.ReasonPrefix + "No upcaster registered."),
                Is.EqualTo(1));
            Assert.That(rows.GetValueOrDefault(Json.Validate(Utf8("nope"))!), Is.EqualTo(1));
        });
    }
}
