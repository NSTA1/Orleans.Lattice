using System.Buffers;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Primitives;
using Orleans.Serialization;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Unit tests for <see cref="OrleansBinaryWalMutationEncoder"/>: the
/// default <see cref="IWalMutationEncoder"/> implementation that wraps
/// the canonical <c>Serializer&lt;LatticeMutation&gt;</c>. The encoder
/// is the seam the WAL grain consults on every append to produce the
/// exact bytes the storage provider will see, so the contract is
/// tight: <c>Encode</c> must drive an <see cref="IBufferWriter{T}"/>
/// to byte-parity with <see cref="Serializer{T}.SerializeToArray"/>,
/// and <c>Decode</c> must round-trip those bytes back to the input
/// value.
/// </summary>
[TestFixture]
public sealed class OrleansBinaryWalMutationEncoderTests
{
    private ServiceProvider _services = null!;
    private Serializer<LatticeMutation> _serializer = null!;

    [OneTimeSetUp]
    public void OneTimeSetUp()
    {
        _services = new ServiceCollection()
            .AddSerializer()
            .BuildServiceProvider();
        _serializer = _services.GetRequiredService<Serializer<LatticeMutation>>();
    }

    [OneTimeTearDown]
    public void OneTimeTearDown() => _services.Dispose();

    private static LatticeMutation MakeSet(string key = "k", byte[]? value = null) => new()
    {
        TreeId = "tree",
        Kind = MutationKind.Set,
        Key = key,
        Value = value ?? new byte[] { 1, 2, 3 },
        Timestamp = HybridLogicalClock.Tick(HybridLogicalClock.Zero),
        OriginClusterId = "site-a",
    };

    [Test]
    public void Ctor_throws_on_null_serializer()
    {
        Assert.That(
            () => new OrleansBinaryWalMutationEncoder(null!),
            Throws.InstanceOf<ArgumentNullException>());
    }

    [Test]
    public void Encode_throws_on_null_writer()
    {
        var encoder = new OrleansBinaryWalMutationEncoder(_serializer);
        var mutation = MakeSet();

        Assert.That(
            () => encoder.Encode(in mutation, null!),
            Throws.InstanceOf<ArgumentNullException>());
    }

    [Test]
    public void Encode_writes_byte_identical_payload_to_serializer_for_set()
    {
        var encoder = new OrleansBinaryWalMutationEncoder(_serializer);
        var mutation = MakeSet();

        var writer = new ArrayBufferWriter<byte>();
        encoder.Encode(in mutation, writer);

        var expected = _serializer.SerializeToArray(mutation);
        Assert.That(writer.WrittenSpan.ToArray(), Is.EqualTo(expected));
    }

    [Test]
    public void Encode_writes_byte_identical_payload_to_serializer_for_delete()
    {
        var encoder = new OrleansBinaryWalMutationEncoder(_serializer);
        var mutation = new LatticeMutation
        {
            TreeId = "tree",
            Kind = MutationKind.Delete,
            Key = "k",
            IsTombstone = true,
            Timestamp = HybridLogicalClock.Tick(HybridLogicalClock.Zero),
            OriginClusterId = "site-a",
        };

        var writer = new ArrayBufferWriter<byte>();
        encoder.Encode(in mutation, writer);

        var expected = _serializer.SerializeToArray(mutation);
        Assert.That(writer.WrittenSpan.ToArray(), Is.EqualTo(expected));
    }

    [Test]
    public void Encode_writes_byte_identical_payload_to_serializer_for_delete_range()
    {
        var encoder = new OrleansBinaryWalMutationEncoder(_serializer);
        var mutation = new LatticeMutation
        {
            TreeId = "tree",
            Kind = MutationKind.DeleteRange,
            Key = "a",
            EndExclusiveKey = "z",
            IsTombstone = true,
            Timestamp = HybridLogicalClock.Tick(HybridLogicalClock.Zero),
        };

        var writer = new ArrayBufferWriter<byte>();
        encoder.Encode(in mutation, writer);

        var expected = _serializer.SerializeToArray(mutation);
        Assert.That(writer.WrittenSpan.ToArray(), Is.EqualTo(expected));
    }

    [Test]
    public void Encode_writes_byte_identical_payload_for_large_value()
    {
        // 1 MiB payload exercises the buffer-writer growth path on
        // both the encoder and the parity-comparison serializer.
        var encoder = new OrleansBinaryWalMutationEncoder(_serializer);
        var value = new byte[1024 * 1024];
        for (var i = 0; i < value.Length; i++)
        {
            value[i] = (byte)(i & 0xFF);
        }
        var mutation = MakeSet(value: value);

        var writer = new ArrayBufferWriter<byte>();
        encoder.Encode(in mutation, writer);

        var expected = _serializer.SerializeToArray(mutation);
        Assert.That(writer.WrittenCount, Is.EqualTo(expected.Length));
        Assert.That(writer.WrittenSpan.ToArray(), Is.EqualTo(expected));
    }

    [Test]
    public void Decode_round_trips_an_encoded_set_mutation()
    {
        var encoder = new OrleansBinaryWalMutationEncoder(_serializer);
        var mutation = MakeSet();

        var writer = new ArrayBufferWriter<byte>();
        encoder.Encode(in mutation, writer);
        var decoded = encoder.Decode(writer.WrittenSpan);

        Assert.Multiple(() =>
        {
            Assert.That(decoded.TreeId, Is.EqualTo(mutation.TreeId));
            Assert.That(decoded.Kind, Is.EqualTo(mutation.Kind));
            Assert.That(decoded.Key, Is.EqualTo(mutation.Key));
            Assert.That(decoded.Value, Is.EqualTo(mutation.Value));
            Assert.That(decoded.Timestamp, Is.EqualTo(mutation.Timestamp));
            Assert.That(decoded.OriginClusterId, Is.EqualTo(mutation.OriginClusterId));
        });
    }

    [Test]
    public void Decode_round_trips_an_encoded_delete_range_mutation()
    {
        var encoder = new OrleansBinaryWalMutationEncoder(_serializer);
        var mutation = new LatticeMutation
        {
            TreeId = "tree",
            Kind = MutationKind.DeleteRange,
            Key = "a",
            EndExclusiveKey = "z",
            IsTombstone = true,
            Timestamp = HybridLogicalClock.Tick(HybridLogicalClock.Zero),
        };

        var writer = new ArrayBufferWriter<byte>();
        encoder.Encode(in mutation, writer);
        var decoded = encoder.Decode(writer.WrittenSpan);

        Assert.Multiple(() =>
        {
            Assert.That(decoded.Kind, Is.EqualTo(MutationKind.DeleteRange));
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
        var encoder = new OrleansBinaryWalMutationEncoder(_serializer);
        var mutation = MakeSet();

        var w1 = new ArrayBufferWriter<byte>();
        var w2 = new ArrayBufferWriter<byte>();
        encoder.Encode(in mutation, w1);
        encoder.Encode(in mutation, w2);

        Assert.That(w2.WrittenSpan.ToArray(), Is.EqualTo(w1.WrittenSpan.ToArray()));
    }

    [Test]
    public void Encode_is_safe_under_concurrent_invocation()
    {
        // The default encoder is registered as a singleton, so the
        // hot path may invoke it from many threads simultaneously
        // (one per concurrent WAL grain shard). The wrapped
        // Serializer<T> is thread-safe; pin the wrapper's behaviour.
        var encoder = new OrleansBinaryWalMutationEncoder(_serializer);
        var mutation = MakeSet();
        var expected = _serializer.SerializeToArray(mutation);

        var failures = 0;
        Parallel.For(0, 64, _ =>
        {
            var writer = new ArrayBufferWriter<byte>();
            encoder.Encode(in mutation, writer);
            if (!writer.WrittenSpan.SequenceEqual(expected))
            {
                Interlocked.Increment(ref failures);
            }
        });

        Assert.That(failures, Is.Zero);
    }
}
