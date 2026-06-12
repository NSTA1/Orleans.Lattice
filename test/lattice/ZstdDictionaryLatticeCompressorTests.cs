using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice;

namespace Orleans.Lattice.Tests;

[TestFixture]
public class ZstdDictionaryLatticeCompressorTests
{
    private static byte[] SelfSimilarPayload(int repeats)
    {
        var unit = "user:42|order:7|status:shipped|region:eu-west|"u8.ToArray();
        var buffer = new byte[unit.Length * repeats];
        for (var i = 0; i < repeats; i++)
        {
            unit.CopyTo(buffer.AsSpan(i * unit.Length));
        }
        return buffer;
    }

    private static ZstdDictionaryLatticeCompressor NewCompressor(uint dictionaryId = 1u, int level = 3)
    {
        var provider = new OperatorSuppliedCompressionDictionaryProvider(
            new Dictionary<uint, ReadOnlyMemory<byte>> { [dictionaryId] = SelfSimilarPayload(64) });
        return new ZstdDictionaryLatticeCompressor(level, provider);
    }

    [TestCase(0)]
    [TestCase(23)]
    [TestCase(-1)]
    public void Constructor_rejects_level_out_of_range(int level)
    {
        var provider = OperatorSuppliedCompressionDictionaryProvider.Empty;
        Assert.That(
            () => new ZstdDictionaryLatticeCompressor(level, provider),
            Throws.InstanceOf<ArgumentOutOfRangeException>());
    }

    [Test]
    public void Constructor_throws_on_null_provider()
    {
        Assert.That(
            () => new ZstdDictionaryLatticeCompressor(3, null!),
            Throws.ArgumentNullException);
    }

    [Test]
    public void Algorithm_is_zstd_dictionary()
    {
        using var c = NewCompressor();
        Assert.That(c.Algorithm, Is.EqualTo(LatticeCompression.ZstdDictionary));
    }

    [Test]
    public void HasDictionary_true_for_registered_id_false_for_unknown_and_zero()
    {
        using var c = NewCompressor(dictionaryId: 7u);
        Assert.Multiple(() =>
        {
            Assert.That(c.HasDictionary(7u), Is.True);
            Assert.That(c.HasDictionary(8u), Is.False);
            Assert.That(c.HasDictionary(0u), Is.False);
        });
    }

    [Test]
    public void Compress_then_decompress_with_dictionary_round_trips()
    {
        using var c = NewCompressor(dictionaryId: 1u);
        var payload = SelfSimilarPayload(40);

        var dest = new byte[c.GetMaxCompressedLength(payload.Length, 1u)];
        var written = c.Compress(payload, dest, 1u);

        var restored = new byte[payload.Length];
        c.Decompress(dest.AsSpan(0, written), restored, payload.Length, 1u);

        Assert.That(restored, Is.EqualTo(payload));
    }

    [Test]
    public void Compress_with_dictionary_is_not_larger_than_dictionaryless_on_self_similar_payload()
    {
        using var c = NewCompressor(dictionaryId: 1u);
        var payload = SelfSimilarPayload(8); // small, self-similar batch

        var dictDest = new byte[c.GetMaxCompressedLength(payload.Length, 1u)];
        var dictLen = c.Compress(payload, dictDest, 1u);

        var plainDest = new byte[c.GetMaxCompressedLength(payload.Length)];
        var plainLen = c.Compress(payload, plainDest); // id 0, dictionary-less

        Assert.That(dictLen, Is.LessThanOrEqualTo(plainLen));
    }

    [Test]
    public void Dictionaryless_overloads_round_trip_via_id_zero()
    {
        using var c = NewCompressor();
        var payload = SelfSimilarPayload(20);

        var dest = new byte[c.GetMaxCompressedLength(payload.Length)];
        var written = c.Compress(payload, dest);

        var restored = new byte[payload.Length];
        c.Decompress(dest.AsSpan(0, written), restored, payload.Length);

        Assert.That(restored, Is.EqualTo(payload));
    }

    [Test]
    public void Compress_throws_for_unregistered_dictionary_id()
    {
        using var c = NewCompressor(dictionaryId: 1u);
        var payload = SelfSimilarPayload(4);
        var dest = new byte[c.GetMaxCompressedLength(payload.Length, 2u)];

        Assert.That(() => c.Compress(payload, dest, 2u), Throws.ArgumentException);
    }

    [Test]
    public void Decompress_throws_when_dictionary_does_not_match()
    {
        // Compress with id 1, attempt to inflate with a compressor whose
        // id 1 dictionary differs -> invalid frame for that dictionary.
        using var producer = NewCompressor(dictionaryId: 1u);
        var payload = SelfSimilarPayload(16);
        var dest = new byte[producer.GetMaxCompressedLength(payload.Length, 1u)];
        var written = producer.Compress(payload, dest, 1u);

        var otherProvider = new OperatorSuppliedCompressionDictionaryProvider(
            new Dictionary<uint, ReadOnlyMemory<byte>> { [1u] = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8 } });
        using var consumer = new ZstdDictionaryLatticeCompressor(3, otherProvider);
        var restored = new byte[payload.Length];

        Assert.That(
            () => consumer.Decompress(dest.AsSpan(0, written), restored, payload.Length, 1u),
            Throws.ArgumentException);
    }

    [Test]
    public void Decompress_throws_on_destination_length_mismatch()
    {
        using var c = NewCompressor();
        var payload = SelfSimilarPayload(4);
        var dest = new byte[c.GetMaxCompressedLength(payload.Length, 1u)];
        var written = c.Compress(payload, dest, 1u);

        var wrong = new byte[payload.Length - 1];
        Assert.That(
            () => c.Decompress(dest.AsSpan(0, written), wrong, payload.Length, 1u),
            Throws.ArgumentException);
    }

    [Test]
    public void Operations_after_dispose_throw_object_disposed()
    {
        var c = NewCompressor();
        c.Dispose();
        var dest = new byte[16];
        Assert.That(() => c.Compress(new byte[] { 1, 2 }, dest, 1u), Throws.InstanceOf<ObjectDisposedException>());
    }

    [Test]
    public void Dispose_is_idempotent()
    {
        var c = NewCompressor();
        c.Dispose();
        Assert.That(() => c.Dispose(), Throws.Nothing);
    }

    [Test]
    public void Registration_helpers_wire_dictionary_compressor_and_provider()
    {
        var services = new ServiceCollection();
        services.AddLatticeCompressionDictionaries(
            new Dictionary<uint, ReadOnlyMemory<byte>> { [3u] = SelfSimilarPayload(8) });
        services.AddLatticeZstdDictionaryCompressor(compressionLevel: 5);
        using var sp = services.BuildServiceProvider();

        var provider = sp.GetRequiredService<ILatticeCompressionDictionaryProvider>();
        var compressor = sp.GetServices<ILatticeCompressor>()
            .OfType<ZstdDictionaryLatticeCompressor>()
            .Single();

        Assert.Multiple(() =>
        {
            Assert.That(provider.TryGetDictionary(3u, out _), Is.True);
            Assert.That(compressor.HasDictionary(3u), Is.True);
            Assert.That(compressor.Algorithm, Is.EqualTo(LatticeCompression.ZstdDictionary));
        });
    }
}
