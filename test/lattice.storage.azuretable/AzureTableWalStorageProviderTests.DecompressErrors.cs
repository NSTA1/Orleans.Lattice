using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Storage.AzureTable.Tests;

/// <summary>
/// White-box tests for the two malformed-input guards on the read-path
/// decompressor <c>DecompressPayload</c> of
/// <see cref="AzureTableWalStorageProvider"/>: a compression tag with no
/// registered <see cref="ILatticeCompressor"/> (which must surface
/// <see cref="NotSupportedException"/> so a new algorithm ships without a
/// coordinated wire-version bump) and a payload too short to carry the
/// mandatory 4-byte little-endian uncompressed-length prefix (a truncated
/// or corrupt row, which must surface <see cref="InvalidDataException"/>
/// rather than read past the buffer). The method is private, so both are
/// exercised through the reflection helper already used by the
/// length-guard tests.
/// </summary>
public partial class AzureTableWalStorageProviderTests
{
    [Test]
    public void DecompressPayload_rejects_a_tag_with_no_registered_compressor()
    {
        // The default compressing provider registers only the Zstd tag, so
        // the ZstdDictionary tag has no compressor and must be rejected as
        // unsupported before any inflate work.
        var provider = CreateCompressingProvider();
        var payload = new byte[16];

        Assert.That(
            () => InvokeDecompressPayload(provider, payload, (byte)LatticeCompression.ZstdDictionary),
            Throws.TypeOf<NotSupportedException>());
    }

    [Test]
    public void DecompressPayload_rejects_a_payload_shorter_than_the_length_prefix()
    {
        // A registered tag (Zstd) but a body of only 2 bytes: too short to
        // hold the 4-byte uncompressed-length prefix, so the guard must
        // refuse it rather than read past the buffer.
        var provider = CreateCompressingProvider();
        var payload = new byte[] { 0x01, 0x02 };

        Assert.That(
            () => InvokeDecompressPayload(provider, payload, (byte)LatticeCompression.Zstd),
            Throws.TypeOf<InvalidDataException>());
    }
}
