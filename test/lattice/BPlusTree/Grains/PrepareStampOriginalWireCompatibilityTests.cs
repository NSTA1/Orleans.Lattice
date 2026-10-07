using System.Buffers;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;
using Orleans.Serialization;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Issue #4522: the <see cref="WalRecord.PrepareStampOriginal"/> marker (wire id
/// 27) and its <see cref="LatticeMutation.PrepareStampOriginal"/> mirror (wire
/// id 25) are additive, so a rolling upgrade reads both ways: a record written by
/// new code decodes on an older silo with the field ignored (so the prepare is
/// treated as unmarked, which is the pre-#4522 drain), and a record written by an
/// older silo decodes on new code as unmarked.
/// <para>
/// Every write-ahead-log codec reduces to the Orleans <c>[Id]</c> serializer for
/// <see cref="WalRecord"/>: <see cref="OrleansBinaryWalRecordEncoder"/> (the
/// default <see cref="IWalRecordEncoder"/>, which the WAL grain and the batched
/// Azure Table and file paths use) is byte-identical to
/// <c>Serializer&lt;WalRecord&gt;</c>, and the Azure Table single-entry seam
/// serializes the converted <see cref="WalRecord"/> through that same
/// <c>Serializer&lt;WalRecord&gt;</c>. The durable unresolved-replay ledger and
/// the replication envelope carry <see cref="LatticeMutation"/> through its own
/// <c>[Id]</c> serializer. The old shapes below are test-local mirrors of the
/// pre-#4522 types: the same ids, minus the marker.
/// </para>
/// </summary>
[TestFixture]
public sealed class PrepareStampOriginalWireCompatibilityTests
{
    private ServiceProvider _services = null!;
    private Serializer<WalRecord> _walSerializer = null!;
    private Serializer<PreMarkerWalRecord> _oldWalSerializer = null!;
    private Serializer<LatticeMutation> _mutationSerializer = null!;
    private Serializer<PreMarkerLatticeMutation> _oldMutationSerializer = null!;

    [OneTimeSetUp]
    public void OneTimeSetUp()
    {
        _services = new ServiceCollection().AddSerializer().BuildServiceProvider();
        _walSerializer = _services.GetRequiredService<Serializer<WalRecord>>();
        _oldWalSerializer = _services.GetRequiredService<Serializer<PreMarkerWalRecord>>();
        _mutationSerializer = _services.GetRequiredService<Serializer<LatticeMutation>>();
        _oldMutationSerializer = _services.GetRequiredService<Serializer<PreMarkerLatticeMutation>>();
    }

    [OneTimeTearDown]
    public void OneTimeTearDown() => _services.Dispose();

    private static readonly HybridLogicalClock Stamp = new() { WallClockTicks = 638_000_000_000_000_000, Counter = 4 };
    private static readonly Guid Tx = Guid.Parse("4522aaaa-0000-0000-0000-000000004522");

    private static WalRecord MarkedPrepare() => new()
    {
        TreeId = "tree",
        Op = MutationKind.Set,
        Key = "k",
        Value = [1, 2, 3],
        Timestamp = Stamp,
        TransactionId = Tx,
        IsPrepared = true,
        ShardIndex = 2,
        Mode = LatticeMergeMode.LwwRegister,
        PrepareStampOriginal = true,
    };

    [Test]
    public void A_marked_record_round_trips_the_marker_through_the_binary_encoder()
    {
        var encoder = new OrleansBinaryWalRecordEncoder(_walSerializer);
        var record = MarkedPrepare();
        var writer = new ArrayBufferWriter<byte>();
        encoder.Encode(in record, writer);

        var decoded = encoder.Decode(writer.WrittenSpan, record.TreeId);

        Assert.That(decoded.PrepareStampOriginal, Is.True);
        Assert.That(decoded.Timestamp, Is.EqualTo(Stamp));
    }

    [Test]
    public void A_record_encoded_by_new_code_decodes_on_an_older_silo_with_the_marker_ignored()
    {
        var encoder = new OrleansBinaryWalRecordEncoder(_walSerializer);
        var record = MarkedPrepare();
        var writer = new ArrayBufferWriter<byte>();
        encoder.Encode(in record, writer);

        var old = _oldWalSerializer.Deserialize(new ReadOnlyMemory<byte>(writer.WrittenSpan.ToArray()));

        Assert.Multiple(() =>
        {
            Assert.That(old.Key, Is.EqualTo("k"));
            Assert.That(old.Timestamp, Is.EqualTo(Stamp));
            Assert.That(old.TransactionId, Is.EqualTo(Tx));
            Assert.That(old.IsPrepared, Is.True);
            Assert.That(old.ShardIndex, Is.EqualTo(2));
        });
    }

    [Test]
    public void A_record_encoded_by_an_older_silo_decodes_on_new_code_as_unmarked()
    {
        var old = new PreMarkerWalRecord
        {
            Op = MutationKind.Set,
            Key = "k",
            Value = [1, 2, 3],
            Timestamp = Stamp,
            TransactionId = Tx,
            IsPrepared = true,
            ShardIndex = 2,
        };
        var bytes = _oldWalSerializer.SerializeToArray(old);

        var encoder = new OrleansBinaryWalRecordEncoder(_walSerializer);
        var decoded = encoder.Decode(bytes, "tree");

        Assert.Multiple(() =>
        {
            Assert.That(decoded.PrepareStampOriginal, Is.False);
            Assert.That(decoded.IsPrepared, Is.True);
            Assert.That(decoded.Timestamp, Is.EqualTo(Stamp));
            Assert.That(WalRecordConverter.FromWalRecord(in decoded).PrepareStampOriginal, Is.False);
        });
    }

    [Test]
    public void The_azure_table_payload_shape_round_trips_the_marker_and_reads_back_old_payloads_as_unmarked()
    {
        // The Azure Table single-entry seam stores
        // Serializer<WalRecord>.Serialize(WalRecordConverter.ToWalRecord(mutation))
        // and reads back WalRecordConverter.FromWalRecord(Deserialize(payload)).
        var mutation = WalRecordConverter.FromWalRecord(MarkedPrepare());
        var buffer = new ArrayBufferWriter<byte>();
        _walSerializer.Serialize(WalRecordConverter.ToWalRecord(mutation, mutation.Mode, string.Empty), buffer);

        var readBack = WalRecordConverter.FromWalRecord(_walSerializer.Deserialize(new ReadOnlyMemory<byte>(buffer.WrittenSpan.ToArray())));
        var oldPayload = _oldWalSerializer.SerializeToArray(new PreMarkerWalRecord { Op = MutationKind.Set, Key = "k", Timestamp = Stamp, IsPrepared = true });
        var oldReadBack = WalRecordConverter.FromWalRecord(_walSerializer.Deserialize(new ReadOnlyMemory<byte>(oldPayload)));

        Assert.That(readBack.PrepareStampOriginal, Is.True);
        Assert.That(oldReadBack.PrepareStampOriginal, Is.False);
    }

    [Test]
    public void The_converter_mirrors_the_marker_both_ways()
    {
        var mutation = WalRecordConverter.FromWalRecord(MarkedPrepare());
        Assert.That(mutation.PrepareStampOriginal, Is.True);

        var record = WalRecordConverter.ToWalRecord(mutation, LatticeMergeMode.LwwRegister, "cluster");
        Assert.That(record.PrepareStampOriginal, Is.True);

        var unmarked = WalRecordConverter.ToWalRecord(mutation with { PrepareStampOriginal = false }, LatticeMergeMode.LwwRegister, "cluster");
        Assert.That(unmarked.PrepareStampOriginal, Is.False);
    }

    [Test]
    public void A_mutation_round_trips_the_marker_and_decodes_both_ways_across_the_upgrade()
    {
        var mutation = new LatticeMutation
        {
            TreeId = "tree",
            Kind = MutationKind.Set,
            Key = "k",
            Value = [9],
            Timestamp = Stamp,
            TransactionId = Tx,
            IsPrepared = true,
            PrepareStampOriginal = true,
        };

        var newBytes = _mutationSerializer.SerializeToArray(mutation);
        var roundTripped = _mutationSerializer.Deserialize(newBytes);
        var onOldSilo = _oldMutationSerializer.Deserialize(newBytes);
        var fromOldSilo = _mutationSerializer.Deserialize(_oldMutationSerializer.SerializeToArray(new PreMarkerLatticeMutation
        {
            Kind = MutationKind.Set,
            Key = "k",
            Timestamp = Stamp,
            TransactionId = Tx,
            IsPrepared = true,
        }));

        Assert.Multiple(() =>
        {
            Assert.That(roundTripped.PrepareStampOriginal, Is.True);
            Assert.That(onOldSilo.Key, Is.EqualTo("k"));
            Assert.That(onOldSilo.IsPrepared, Is.True);
            Assert.That(fromOldSilo.PrepareStampOriginal, Is.False);
            Assert.That(fromOldSilo.IsPrepared, Is.True);
        });
    }

    /// <summary>Test-local mirror of the pre-#4522 <see cref="WalRecord"/> wire shape (no id 27).</summary>
    [GenerateSerializer]
    public readonly record struct PreMarkerWalRecord
    {
        [Id(1)] public MutationKind Op { get; init; }
        [Id(2)] public string Key { get; init; }
        [Id(4)] public byte[]? Value { get; init; }
        [Id(5)] public HybridLogicalClock Timestamp { get; init; }
        [Id(16)] public Guid TransactionId { get; init; }
        [Id(17)] public bool IsPrepared { get; init; }
        [Id(18)] public int ShardIndex { get; init; }
    }

    /// <summary>Test-local mirror of the pre-#4522 <see cref="LatticeMutation"/> wire shape (no id 25).</summary>
    [GenerateSerializer]
    public readonly record struct PreMarkerLatticeMutation
    {
        [Id(1)] public MutationKind Kind { get; init; }
        [Id(2)] public string Key { get; init; }
        [Id(5)] public HybridLogicalClock Timestamp { get; init; }
        [Id(10)] public Guid TransactionId { get; init; }
        [Id(16)] public bool IsPrepared { get; init; }
    }
}
