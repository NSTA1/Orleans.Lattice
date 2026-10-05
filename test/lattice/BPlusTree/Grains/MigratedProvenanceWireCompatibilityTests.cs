using System.Buffers;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;
using Orleans.Serialization;
using OldWalRecord = Orleans.Lattice.Tests.BPlusTree.Grains.PrepareStampOriginalWireCompatibilityTests.PreMarkerWalRecord;
using OldLatticeMutation = Orleans.Lattice.Tests.BPlusTree.Grains.PrepareStampOriginalWireCompatibilityTests.PreMarkerLatticeMutation;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Issue #4564: the migration-provenance bit <see cref="WalRecord.IsMigrated"/>
/// (wire id 28) and its <see cref="LatticeMutation.IsMigrated"/> mirror (wire id
/// 26) are additive, like the #4522 marker beside them. A record written by new
/// code decodes on an older silo with the bit ignored, and a record written by an
/// older silo decodes on new code as not migrated, which is how replay treated
/// every record before the bit existed. The old shapes are the test-local
/// pre-#4522 mirrors <see cref="PrepareStampOriginalWireCompatibilityTests"/>
/// declares: the same ids, without either field.
/// </summary>
[TestFixture]
public sealed class MigratedProvenanceWireCompatibilityTests
{
    private ServiceProvider _services = null!;
    private Serializer<WalRecord> _walSerializer = null!;
    private Serializer<OldWalRecord> _oldWalSerializer = null!;
    private Serializer<LatticeMutation> _mutationSerializer = null!;
    private Serializer<OldLatticeMutation> _oldMutationSerializer = null!;

    [OneTimeSetUp]
    public void OneTimeSetUp()
    {
        _services = new ServiceCollection().AddSerializer().BuildServiceProvider();
        _walSerializer = _services.GetRequiredService<Serializer<WalRecord>>();
        _oldWalSerializer = _services.GetRequiredService<Serializer<OldWalRecord>>();
        _mutationSerializer = _services.GetRequiredService<Serializer<LatticeMutation>>();
        _oldMutationSerializer = _services.GetRequiredService<Serializer<OldLatticeMutation>>();
    }

    [OneTimeTearDown]
    public void OneTimeTearDown() => _services.Dispose();

    private static readonly HybridLogicalClock Stamp = new() { WallClockTicks = 638_000_000_000_000_000, Counter = 7 };

    private static WalRecord MigratedImport() => new()
    {
        TreeId = "tree",
        Op = MutationKind.Set,
        Key = "k",
        Value = [1, 2, 3],
        Timestamp = Stamp,
        IsMerge = true,
        ShardIndex = 2,
        Mode = LatticeMergeMode.LwwRegister,
        IsMigrated = true,
    };

    [Test]
    public void A_migrated_record_round_trips_the_bit_through_the_binary_encoder_and_the_converter()
    {
        var encoder = new OrleansBinaryWalRecordEncoder(_walSerializer);
        var record = MigratedImport();
        var writer = new ArrayBufferWriter<byte>();
        encoder.Encode(in record, writer);

        var decoded = encoder.Decode(writer.WrittenSpan, record.TreeId);
        var mutation = WalRecordConverter.FromWalRecord(in decoded);
        var back = WalRecordConverter.ToWalRecord(mutation, LatticeMergeMode.LwwRegister, "cluster");

        Assert.Multiple(() =>
        {
            Assert.That(decoded.IsMigrated, Is.True);
            Assert.That(mutation.IsMigrated, Is.True);
            Assert.That(back.IsMigrated, Is.True);
            Assert.That(WalRecordConverter.ToWalRecord(mutation with { IsMigrated = false }, LatticeMergeMode.LwwRegister, "cluster").IsMigrated, Is.False);
        });
    }

    [Test]
    public void Records_cross_the_upgrade_both_ways_with_the_bit_ignored_or_defaulted()
    {
        var encoder = new OrleansBinaryWalRecordEncoder(_walSerializer);
        var record = MigratedImport();
        var writer = new ArrayBufferWriter<byte>();
        encoder.Encode(in record, writer);

        var onOldSilo = _oldWalSerializer.Deserialize(new ReadOnlyMemory<byte>(writer.WrittenSpan.ToArray()));
        var fromOldSilo = encoder.Decode(
            _oldWalSerializer.SerializeToArray(new OldWalRecord { Op = MutationKind.Set, Key = "k", Timestamp = Stamp }),
            "tree");

        Assert.Multiple(() =>
        {
            Assert.That(onOldSilo.Key, Is.EqualTo("k"));
            Assert.That(onOldSilo.Timestamp, Is.EqualTo(Stamp));
            Assert.That(fromOldSilo.IsMigrated, Is.False);
            Assert.That(WalRecordConverter.FromWalRecord(in fromOldSilo).IsMigrated, Is.False);
        });
    }

    [Test]
    public void A_mutation_round_trips_the_bit_and_decodes_both_ways_across_the_upgrade()
    {
        var mutation = new LatticeMutation
        {
            TreeId = "tree",
            Kind = MutationKind.Set,
            Key = "k",
            Value = [9],
            Timestamp = Stamp,
            IsMigrated = true,
        };

        var newBytes = _mutationSerializer.SerializeToArray(mutation);
        var roundTripped = _mutationSerializer.Deserialize(newBytes);
        var onOldSilo = _oldMutationSerializer.Deserialize(newBytes);
        var fromOldSilo = _mutationSerializer.Deserialize(_oldMutationSerializer.SerializeToArray(new OldLatticeMutation
        {
            Kind = MutationKind.Set,
            Key = "k",
            Timestamp = Stamp,
        }));

        Assert.Multiple(() =>
        {
            Assert.That(roundTripped.IsMigrated, Is.True);
            Assert.That(onOldSilo.Key, Is.EqualTo("k"));
            Assert.That(fromOldSilo.IsMigrated, Is.False);
        });
    }
}
