using System.Buffers;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Primitives;
using Orleans.Serialization;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Unit tests for <see cref="OrleansBinaryWalRecordEncoder"/>: the
/// default <see cref="IWalRecordEncoder"/> implementation that wraps
/// the canonical <c>Serializer&lt;WalRecord&gt;</c>. The encoder is
/// the seam the WAL grain consults on every append to produce the
/// exact bytes the storage provider will see, so the contract is
/// tight: <c>Encode</c> must drive an <see cref="IBufferWriter{T}"/>
/// to byte-parity with <see cref="Serializer{T}.SerializeToArray"/>,
/// and <c>Decode</c> must round-trip those bytes back to the input
/// value.
/// </summary>
[TestFixture]
public sealed class OrleansBinaryWalRecordEncoderTests
{
    private ServiceProvider _services = null!;
    private Serializer<WalRecord> _serializer = null!;

    [OneTimeSetUp]
    public void OneTimeSetUp()
    {
        _services = new ServiceCollection()
            .AddSerializer()
            .BuildServiceProvider();
        _serializer = _services.GetRequiredService<Serializer<WalRecord>>();
    }

    [OneTimeTearDown]
    public void OneTimeTearDown() => _services.Dispose();

    private static WalRecord MakeSet(string key = "k", byte[]? value = null) => new()
    {
        TreeId = "tree",
        Op = MutationKind.Set,
        Key = key,
        Value = value ?? new byte[] { 1, 2, 3 },
        Timestamp = HybridLogicalClock.Tick(HybridLogicalClock.Zero),
        OriginClusterId = "site-a",
        Mode = LatticeMergeMode.LwwRegister,
    };

    [Test]
    public void Ctor_throws_on_null_serializer()
    {
        Assert.That(
            () => new OrleansBinaryWalRecordEncoder(null!),
            Throws.InstanceOf<ArgumentNullException>());
    }

    [Test]
    public void Encode_throws_on_null_writer()
    {
        var encoder = new OrleansBinaryWalRecordEncoder(_serializer);
        var record = MakeSet();

        Assert.That(
            () => encoder.Encode(in record, null!),
            Throws.InstanceOf<ArgumentNullException>());
    }

    [Test]
    public void Encode_writes_byte_identical_payload_to_stripped_serializer_for_set()
    {
        var encoder = new OrleansBinaryWalRecordEncoder(_serializer);
        var record = MakeSet();

        var writer = new ArrayBufferWriter<byte>();
        encoder.Encode(in record, writer);

        // Encoder strips TreeId at encode time (R-116): every storage
        // and transport seam recovers the tree id from surrounding
        // context, so the slot is elided to save ~25-35 bytes per
        // entry. The expected baseline is the same record with
        // TreeId = "".
        var expected = _serializer.SerializeToArray(record with { TreeId = string.Empty });
        Assert.That(writer.WrittenSpan.ToArray(), Is.EqualTo(expected));
    }

    [Test]
    public void Encode_writes_byte_identical_payload_to_serializer_for_delete()
    {
        var encoder = new OrleansBinaryWalRecordEncoder(_serializer);
        var record = new WalRecord
        {
            TreeId = "tree",
            Op = MutationKind.Delete,
            Key = "k",
            IsTombstone = true,
            Timestamp = HybridLogicalClock.Tick(HybridLogicalClock.Zero),
            OriginClusterId = "site-a",
            Mode = LatticeMergeMode.LwwRegister,
        };

        var writer = new ArrayBufferWriter<byte>();
        encoder.Encode(in record, writer);

        var expected = _serializer.SerializeToArray(record with { TreeId = string.Empty });
        Assert.That(writer.WrittenSpan.ToArray(), Is.EqualTo(expected));
    }

    [Test]
    public void Encode_writes_byte_identical_payload_to_serializer_for_delete_range()
    {
        var encoder = new OrleansBinaryWalRecordEncoder(_serializer);
        var record = new WalRecord
        {
            TreeId = "tree",
            Op = MutationKind.DeleteRange,
            Key = "a",
            EndExclusiveKey = "z",
            IsTombstone = true,
            Timestamp = HybridLogicalClock.Tick(HybridLogicalClock.Zero),
            Mode = LatticeMergeMode.LwwRegister,
        };

        var writer = new ArrayBufferWriter<byte>();
        encoder.Encode(in record, writer);

        var expected = _serializer.SerializeToArray(record with { TreeId = string.Empty });
        Assert.That(writer.WrittenSpan.ToArray(), Is.EqualTo(expected));
    }

    [Test]
    public void Encode_writes_byte_identical_payload_for_large_value()
    {
        // 1 MiB payload exercises the buffer-writer growth path on
        // both the encoder and the parity-comparison serializer.
        var encoder = new OrleansBinaryWalRecordEncoder(_serializer);
        var value = new byte[1024 * 1024];
        for (var i = 0; i < value.Length; i++)
        {
            value[i] = (byte)(i & 0xFF);
        }
        var record = MakeSet(value: value);

        var writer = new ArrayBufferWriter<byte>();
        encoder.Encode(in record, writer);

        var expected = _serializer.SerializeToArray(record with { TreeId = string.Empty });
        Assert.That(writer.WrittenCount, Is.EqualTo(expected.Length));
        Assert.That(writer.WrittenSpan.ToArray(), Is.EqualTo(expected));
    }

    [Test]
    public void Decode_round_trips_an_encoded_set_record()
    {
        var encoder = new OrleansBinaryWalRecordEncoder(_serializer);
        var record = MakeSet();

        var writer = new ArrayBufferWriter<byte>();
        encoder.Encode(in record, writer);
        // Use the treeId-supplying overload so TreeId is restored from
        // surrounding context (R-116). The single-arg overload returns
        // a record with TreeId == "" because Encode strips that slot.
        var decoded = encoder.Decode(writer.WrittenSpan, record.TreeId);

        Assert.Multiple(() =>
        {
            Assert.That(decoded.TreeId, Is.EqualTo(record.TreeId));
            Assert.That(decoded.Op, Is.EqualTo(record.Op));
            Assert.That(decoded.Key, Is.EqualTo(record.Key));
            Assert.That(decoded.Value, Is.EqualTo(record.Value));
            Assert.That(decoded.Timestamp, Is.EqualTo(record.Timestamp));
            Assert.That(decoded.OriginClusterId, Is.EqualTo(record.OriginClusterId));
            Assert.That(decoded.Mode, Is.EqualTo(record.Mode));
        });
    }

    [Test]
    public void Decode_round_trips_an_encoded_delete_range_record()
    {
        var encoder = new OrleansBinaryWalRecordEncoder(_serializer);
        var record = new WalRecord
        {
            TreeId = "tree",
            Op = MutationKind.DeleteRange,
            Key = "a",
            EndExclusiveKey = "z",
            IsTombstone = true,
            Timestamp = HybridLogicalClock.Tick(HybridLogicalClock.Zero),
            Mode = LatticeMergeMode.LwwRegister,
        };

        var writer = new ArrayBufferWriter<byte>();
        encoder.Encode(in record, writer);
        var decoded = encoder.Decode(writer.WrittenSpan, record.TreeId);

        Assert.Multiple(() =>
        {
            Assert.That(decoded.Op, Is.EqualTo(MutationKind.DeleteRange));
            Assert.That(decoded.Key, Is.EqualTo("a"));
            Assert.That(decoded.EndExclusiveKey, Is.EqualTo("z"));
            Assert.That(decoded.IsTombstone, Is.True);
        });
    }

    [Test]
    public void Encode_is_deterministic_across_repeated_calls()
    {
        // Two back-to-back encodes of the same input must produce
        // identical bytes - the encoder must not depend on hidden
        // mutable state.
        var encoder = new OrleansBinaryWalRecordEncoder(_serializer);
        var record = MakeSet();

        var w1 = new ArrayBufferWriter<byte>();
        var w2 = new ArrayBufferWriter<byte>();
        encoder.Encode(in record, w1);
        encoder.Encode(in record, w2);

        Assert.That(w2.WrittenSpan.ToArray(), Is.EqualTo(w1.WrittenSpan.ToArray()));
    }

    [Test]
    public void Encode_strips_TreeId_slot_from_encoded_bytes()
    {
        // The encoder elides the [Id(0)] TreeId slot at serialisation
        // time because every storage and transport seam recovers it
        // from surrounding context. The byte payload of an encoded
        // record must therefore equal the byte payload of the same
        // record with TreeId already cleared, which proves the slot
        // is absent from the bytes.
        var encoder = new OrleansBinaryWalRecordEncoder(_serializer);
        var record = MakeSet() with { TreeId = "orders/eu-west-1/v3" };

        var writer = new ArrayBufferWriter<byte>();
        encoder.Encode(in record, writer);

        var stripped = _serializer.SerializeToArray(record with { TreeId = string.Empty });
        Assert.That(writer.WrittenSpan.ToArray(), Is.EqualTo(stripped));

        // Encoded bytes must be strictly shorter than the bytes that
        // would result from serialising the unstripped record (the
        // tree name is non-empty, so its absence reduces the payload).
        var unstripped = _serializer.SerializeToArray(record);
        Assert.That(writer.WrittenCount, Is.LessThan(unstripped.Length));
    }

    [Test]
    public void Decode_without_treeId_returns_record_with_empty_TreeId()
    {
        // The single-argument Decode is reserved for forensic tooling
        // that does not have surrounding context. It must surface
        // TreeId == "" so the field-stripped vs. field-bearing cases
        // are distinguishable at runtime.
        var encoder = new OrleansBinaryWalRecordEncoder(_serializer);
        var record = MakeSet() with { TreeId = "orders/eu-west-1/v3" };

        var writer = new ArrayBufferWriter<byte>();
        encoder.Encode(in record, writer);

        var decoded = encoder.Decode(writer.WrittenSpan);
        Assert.That(decoded.TreeId, Is.EqualTo(string.Empty));
        Assert.That(decoded.Key, Is.EqualTo(record.Key));
    }

    [Test]
    public void Decode_with_treeId_restores_field_from_context()
    {
        // The treeId-supplying overload re-stamps TreeId from the
        // caller-supplied context. Round-trip equivalence over every
        // [Id] slot is the production-path invariant.
        var encoder = new OrleansBinaryWalRecordEncoder(_serializer);
        var record = MakeSet() with { TreeId = "orders/eu-west-1/v3" };

        var writer = new ArrayBufferWriter<byte>();
        encoder.Encode(in record, writer);

        var decoded = encoder.Decode(writer.WrittenSpan, record.TreeId);
        Assert.That(decoded, Is.EqualTo(record));
    }

    [Test]
    public void Decode_with_null_treeId_throws()
    {
        var encoder = new OrleansBinaryWalRecordEncoder(_serializer);
        var record = MakeSet();
        var writer = new ArrayBufferWriter<byte>();
        encoder.Encode(in record, writer);
        var bytes = writer.WrittenSpan.ToArray();

        Assert.That(
            () => encoder.Decode(bytes, null!),
            Throws.InstanceOf<ArgumentNullException>());
    }

    [Test]
    public void Encode_is_safe_under_concurrent_invocation()
    {
        // The default encoder is registered as a singleton, so the
        // hot path may invoke it from many threads simultaneously
        // (one per concurrent WAL grain shard). The wrapped
        // Serializer<T> is thread-safe; pin the wrapper's behaviour.
        var encoder = new OrleansBinaryWalRecordEncoder(_serializer);
        var record = MakeSet();
        var expected = _serializer.SerializeToArray(record);

        var failures = 0;
        Parallel.For(0, 64, _ =>
        {
            var writer = new ArrayBufferWriter<byte>();
            encoder.Encode(in record, writer);
            if (!writer.WrittenSpan.SequenceEqual(expected))
            {
                Interlocked.Increment(ref failures);
            }
        });

        Assert.That(failures, Is.Zero);
    }
}
