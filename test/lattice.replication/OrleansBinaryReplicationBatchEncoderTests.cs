using System.Buffers;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication;
using Orleans.Serialization;

namespace Orleans.Lattice.Replication.Tests;

[TestFixture]
public class OrleansBinaryReplicationBatchEncoderTests
{
    private ServiceProvider _services = null!;
    private OrleansBinaryReplicationBatchEncoder _encoder = null!;

    [OneTimeSetUp]
    public void OneTimeSetUp()
    {
        _services = new ServiceCollection().AddSerializer().BuildServiceProvider();
        var serializer = _services.GetRequiredService<Serializer<ReplicationBatchEnvelope>>();
        _encoder = new OrleansBinaryReplicationBatchEncoder(serializer);
    }

    [OneTimeTearDown]
    public void OneTimeTearDown() => _services.Dispose();

    private static ReplicationBatchEnvelope MakeEnvelope(
        int wireVersion = ReplicationBatchEnvelope.CurrentVersion,
        string tree = "tree",
        string origin = "site-a",
        IReadOnlyList<ReplogEntry>? entries = null)
        => new()
        {
            WireVersion = wireVersion,
            TreeName = tree,
            OriginClusterId = origin,
            Entries = entries ?? Array.Empty<ReplogEntry>(),
        };

    private static ReplogEntry MakeEntry(string key = "k", byte b = 1) => new()
    {
        TreeId = "tree",
        Op = ReplogOp.Set,
        Key = key,
        Value = new byte[] { b },
        Timestamp = HybridLogicalClock.Tick(HybridLogicalClock.Zero),
        OriginClusterId = "site-a",
        Mode = ReplicationMode.LwwRegister,
    };

    /// <summary>
    /// Helper for tests that just want the bytes back: drives the
    /// canonical hot-path API (IBufferWriter) and exposes the writer's
    /// WrittenMemory so assertions stay terse. Tests that need to
    /// observe the writer's residual state (capacity, write count)
    /// instantiate <see cref="ArrayBufferWriter{T}"/> directly instead.
    /// </summary>
    private ReadOnlyMemory<byte> Encode(ReplicationBatchEnvelope envelope)
    {
        var writer = new ArrayBufferWriter<byte>();
        _encoder.Encode(envelope, writer);
        return writer.WrittenMemory;
    }

    [Test]
    public void Constructor_throws_when_serializer_is_null()
    {
        Assert.That(
            () => new OrleansBinaryReplicationBatchEncoder(null!),
            Throws.ArgumentNullException);
    }

    [Test]
    public void ContentType_is_canonical_binary_media_type()
    {
        Assert.That(_encoder.ContentType, Is.EqualTo("application/x-orleans-lattice-replog+binary"));
        Assert.That(_encoder.ContentType, Is.EqualTo(OrleansBinaryReplicationBatchEncoder.BinaryContentType));
    }

    [Test]
    public void CurrentWireVersion_matches_envelope_constant()
    {
        Assert.That(_encoder.CurrentWireVersion, Is.EqualTo(ReplicationBatchEnvelope.CurrentVersion));
    }

    [Test]
    public void Encode_throws_when_writer_is_null()
    {
        var envelope = MakeEnvelope();
        Assert.That(
            () => _encoder.Encode(envelope, null!),
            Throws.ArgumentNullException);
    }

    [Test]
    public void Encode_appends_bytes_to_caller_owned_writer()
    {
        // The IBufferWriter contract is "append" - the encoder must
        // not reset or overwrite bytes the caller already wrote. Pin
        // that invariant: pre-write a sentinel, encode, then verify
        // the sentinel survived at the start of the buffer and the
        // encoded payload follows.
        var writer = new ArrayBufferWriter<byte>();
        var sentinel = new byte[] { 0xAA, 0xBB, 0xCC };
        sentinel.CopyTo(writer.GetSpan(sentinel.Length));
        writer.Advance(sentinel.Length);

        var envelope = MakeEnvelope(entries: new[] { MakeEntry() });
        _encoder.Encode(envelope, writer);

        Assert.That(writer.WrittenCount, Is.GreaterThan(sentinel.Length));
        Assert.That(writer.WrittenMemory.Span[..sentinel.Length].ToArray(), Is.EqualTo(sentinel));
    }

    [Test]
    public void Encode_then_decode_preserves_every_field()
    {
        var entry = MakeEntry();
        var original = MakeEnvelope(entries: new[] { entry });

        var bytes = Encode(original);
        var copy = _encoder.Decode(bytes);

        Assert.Multiple(() =>
        {
            Assert.That(copy.WireVersion, Is.EqualTo(original.WireVersion));
            Assert.That(copy.TreeName, Is.EqualTo(original.TreeName));
            Assert.That(copy.OriginClusterId, Is.EqualTo(original.OriginClusterId));
            Assert.That(copy.Entries, Has.Count.EqualTo(1));
            Assert.That(copy.Entries[0].Key, Is.EqualTo("k"));
            Assert.That(copy.Entries[0].Value, Is.EqualTo(new byte[] { 1 }));
        });
    }

    // -- Gap (iii): end-to-end producer -> encoder -> decoder with VC --

    [Test]
    public void Encode_then_decode_preserves_vector_clock_and_dependency_summary()
    {
        // Closes the wire-format integration gap for R-080: the
        // canonical encoder must carry both causal-plus slots through
        // the IBufferWriter hot path verbatim. ReplicationBatchEnvelope
        // tests cover Serializer<T> in isolation; this test pins the
        // production seam (OrleansBinaryReplicationBatchEncoder) so a
        // future rewrite of the encoder cannot silently drop the slot.
        var vc = new VersionVector();
        vc.Entries["site-a"] = HybridLogicalClock.Tick(HybridLogicalClock.Zero);
        vc.Entries["site-b"] = HybridLogicalClock.Tick(HybridLogicalClock.Tick(HybridLogicalClock.Zero));

        var entry = new ReplogEntry
        {
            TreeId = "tree",
            Op = ReplogOp.Set,
            Key = "k",
            Value = new byte[] { 1 },
            Timestamp = HybridLogicalClock.Tick(HybridLogicalClock.Zero),
            OriginClusterId = "site-a",
            Mode = ReplicationMode.LwwRegister,
            VectorClock = vc,
            DependencySummary = vc,
        };
        var original = MakeEnvelope(entries: new[] { entry });

        var bytes = Encode(original);
        var copy = _encoder.Decode(bytes);

        var decoded = copy.Entries.Single();
        Assert.Multiple(() =>
        {
            Assert.That(decoded.VectorClock, Is.Not.Null);
            Assert.That(decoded.VectorClock!.Entries, Has.Count.EqualTo(2));
            Assert.That(decoded.VectorClock.GetClock("site-a").WallClockTicks, Is.GreaterThan(0L));
            Assert.That(decoded.VectorClock.GetClock("site-b").WallClockTicks, Is.GreaterThan(0L));
            Assert.That(decoded.DependencySummary, Is.Not.Null);
            Assert.That(decoded.DependencySummary!.Entries, Has.Count.EqualTo(2));
        });
    }

    [Test]
    public void Encode_then_decode_preserves_null_vector_clock_for_legacy_entries()
    {
        // Symmetric to the VC-present test above: an entry authored
        // without a frontier (legacy peer, non-replicated local write)
        // round-trips through the canonical encoder with both slots
        // null.
        var entry = MakeEntry();
        var original = MakeEnvelope(entries: new[] { entry });

        var bytes = Encode(original);
        var copy = _encoder.Decode(bytes);

        var decoded = copy.Entries.Single();
        Assert.Multiple(() =>
        {
            Assert.That(decoded.VectorClock, Is.Null);
            Assert.That(decoded.DependencySummary, Is.Null);
        });
    }

    [Test]
    public void Encode_stamps_current_wire_version_when_caller_supplies_zero()
    {
        var envelope = MakeEnvelope(wireVersion: 0);

        var bytes = Encode(envelope);
        var decoded = _encoder.Decode(bytes);

        Assert.That(decoded.WireVersion, Is.EqualTo(_encoder.CurrentWireVersion));
    }

    [Test]
    public void Encode_preserves_explicitly_supplied_wire_version()
    {
        // The encoder stamps only when the caller left it at the
        // default 0; an explicitly-supplied non-zero value must round
        // trip verbatim so a forward-compat producer can author
        // version-targeted payloads. CurrentVersion is supplied here
        // because Decode would reject a strictly-greater value.
        var envelope = MakeEnvelope(wireVersion: ReplicationBatchEnvelope.CurrentVersion);

        var bytes = Encode(envelope);
        var decoded = _encoder.Decode(bytes);

        Assert.That(decoded.WireVersion, Is.EqualTo(ReplicationBatchEnvelope.CurrentVersion));
    }

    [Test]
    public void Encode_normalises_null_entries_to_empty_list()
    {
        var envelope = new ReplicationBatchEnvelope
        {
            WireVersion = 1,
            TreeName = "tree",
            OriginClusterId = "site-a",
            Entries = null!,
        };

        var bytes = Encode(envelope);
        var decoded = _encoder.Decode(bytes);

        Assert.That(decoded.Entries, Is.Not.Null);
        Assert.That(decoded.Entries, Is.Empty);
    }

    [Test]
    public void Encode_throws_when_tree_name_is_null()
    {
        var envelope = new ReplicationBatchEnvelope
        {
            WireVersion = 1,
            TreeName = null!,
            OriginClusterId = "site-a",
            Entries = Array.Empty<ReplogEntry>(),
        };
        var writer = new ArrayBufferWriter<byte>();

        Assert.That(() => _encoder.Encode(envelope, writer), Throws.ArgumentException);
    }

    [Test]
    public void Encode_throws_when_tree_name_is_empty()
    {
        var envelope = MakeEnvelope(tree: string.Empty);
        var writer = new ArrayBufferWriter<byte>();
        Assert.That(() => _encoder.Encode(envelope, writer), Throws.ArgumentException);
    }

    [Test]
    public void Encode_throws_when_origin_is_null()
    {
        var envelope = new ReplicationBatchEnvelope
        {
            WireVersion = 1,
            TreeName = "tree",
            OriginClusterId = null!,
            Entries = Array.Empty<ReplogEntry>(),
        };
        var writer = new ArrayBufferWriter<byte>();

        Assert.That(() => _encoder.Encode(envelope, writer), Throws.ArgumentException);
    }

    [Test]
    public void Encode_throws_when_origin_is_empty()
    {
        var envelope = MakeEnvelope(origin: string.Empty);
        var writer = new ArrayBufferWriter<byte>();
        Assert.That(() => _encoder.Encode(envelope, writer), Throws.ArgumentException);
    }

    [Test]
    public void Encode_throws_when_wire_version_is_negative()
    {
        var envelope = MakeEnvelope(wireVersion: -1);
        var writer = new ArrayBufferWriter<byte>();
        Assert.That(() => _encoder.Encode(envelope, writer), Throws.ArgumentException);
    }

    [Test]
    public void Decode_throws_when_payload_is_empty()
    {
        Assert.That(
            () => _encoder.Decode(ReadOnlyMemory<byte>.Empty),
            Throws.ArgumentException);
    }

    [Test]
    public void Decode_throws_when_payload_is_malformed()
    {
        var bogus = new byte[] { 0xDE, 0xAD, 0xBE, 0xEF, 0x00 };
        Assert.That(
            () => _encoder.Decode(bogus),
            Throws.ArgumentException);
    }

    [Test]
    public void Decode_throws_NotSupportedException_for_newer_wire_version()
    {
        // Hand-encode an envelope with a wire version strictly greater
        // than CurrentVersion to simulate a newer producer talking to
        // this receiver. The encoder must fail fast rather than guess
        // at the layout.
        var envelope = MakeEnvelope(wireVersion: ReplicationBatchEnvelope.CurrentVersion + 1);
        var serializer = _services.GetRequiredService<Serializer<ReplicationBatchEnvelope>>();
        var bytes = serializer.SerializeToArray(envelope);

        Assert.That(
            () => _encoder.Decode(bytes),
            Throws.TypeOf<NotSupportedException>()
                .With.Message.Contains((ReplicationBatchEnvelope.CurrentVersion + 1).ToString()));
    }

    [Test]
    public void Decode_accepts_lower_or_equal_wire_version()
    {
        // A v0 (default) envelope serialised by hand must decode
        // successfully because 0 <= CurrentVersion.
        var envelope = new ReplicationBatchEnvelope
        {
            WireVersion = 0,
            TreeName = "tree",
            OriginClusterId = "site-a",
            Entries = Array.Empty<ReplogEntry>(),
        };
        var serializer = _services.GetRequiredService<Serializer<ReplicationBatchEnvelope>>();
        var bytes = serializer.SerializeToArray(envelope);

        var decoded = _encoder.Decode(bytes);

        Assert.That(decoded.WireVersion, Is.EqualTo(0));
    }

    [Test]
    public void Decode_normalises_null_entries_from_legacy_payload()
    {
        // A hand-encoded envelope with Entries left at the default
        // null may be produced by older code that pre-dated the
        // empty-list normalisation in Encode; Decode must still hand
        // back an empty list so receivers do not have to add a null
        // guard.
        var envelope = new ReplicationBatchEnvelope
        {
            WireVersion = 1,
            TreeName = "tree",
            OriginClusterId = "site-a",
            Entries = null!,
        };
        var serializer = _services.GetRequiredService<Serializer<ReplicationBatchEnvelope>>();
        var bytes = serializer.SerializeToArray(envelope);

        var decoded = _encoder.Decode(bytes);

        Assert.That(decoded.Entries, Is.Not.Null);
        Assert.That(decoded.Entries, Is.Empty);
    }

    [Test]
    public void Encode_then_decode_preserves_empty_entry_batch()
    {
        var envelope = MakeEnvelope(entries: Array.Empty<ReplogEntry>());

        var bytes = Encode(envelope);
        var decoded = _encoder.Decode(bytes);

        Assert.That(decoded.Entries, Is.Empty);
    }

    [Test]
    public void Encode_produces_smaller_payload_than_naive_json_for_byte_arrays()
    {
        // Sanity check on the bandwidth claim: binary framing of a
        // payload-heavy entry beats System.Text.Json's base64 byte[]
        // encoding by a meaningful margin. Not a strict 33% bar - the
        // envelope adds its own tags, and Orleans' wire format is
        // meaningfully more compact than JSON for small ints and
        // strings too - but a guarded floor makes the regression
        // detectable.
        var entries = Enumerable.Range(0, 50)
            .Select(i => new ReplogEntry
            {
                TreeId = "tree",
                Op = ReplogOp.Set,
                Key = $"key-{i:D4}",
                Value = new byte[256],
                Timestamp = HybridLogicalClock.Tick(HybridLogicalClock.Zero),
                OriginClusterId = "site-a",
            })
            .ToArray();
        var envelope = MakeEnvelope(entries: entries);

        var binaryBytes = Encode(envelope);
        var jsonBytes = System.Text.Json.JsonSerializer.SerializeToUtf8Bytes(entries);

        Assert.That(binaryBytes.Length, Is.LessThan(jsonBytes.Length));
    }
}
