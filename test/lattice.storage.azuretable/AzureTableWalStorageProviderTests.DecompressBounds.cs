using System.Buffers.Binary;
using System.Reflection;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Storage.AzureTable.Tests;

/// <summary>
/// White-box tests for the read-path decompression-length guard on
/// <see cref="AzureTableWalStorageProvider"/>. The uncompressed-length
/// prefix on a compressed WAL row is read from stored bytes a compromised
/// or buggy producer could forge; <c>DecompressPayload</c> must refuse a
/// negative or over-large declared length rather than materialise an
/// arbitrary buffer (a decompression bomb). The method is private, so the
/// guard is exercised through reflection against the documented on-disk
/// layout (<c>[4-byte LE uncompressed length][compressed bytes]</c>).
/// </summary>
public partial class AzureTableWalStorageProviderTests
{
    private static object? InvokeDecompressPayload(
        AzureTableWalStorageProvider provider, byte[] payload, byte compressionTag)
    {
        var method = typeof(AzureTableWalStorageProvider).GetMethod(
            "DecompressPayload",
            BindingFlags.Instance | BindingFlags.NonPublic)!;
        try
        {
            return method.Invoke(provider, new object[] { payload, compressionTag });
        }
        catch (TargetInvocationException ex) when (ex.InnerException is not null)
        {
            throw ex.InnerException;
        }
    }

    [Test]
    public void DecompressPayload_rejects_a_length_prefix_above_the_ceiling()
    {
        var provider = CreateCompressingProvider();

        // Forge a row whose declared uncompressed length is int.MaxValue -
        // far above the 256 MiB ceiling. The guard runs before any inflate
        // work, so the compressed body can be arbitrary filler.
        var payload = new byte[16];
        BinaryPrimitives.WriteInt32LittleEndian(payload, int.MaxValue);

        Assert.That(
            () => InvokeDecompressPayload(provider, payload, (byte)LatticeCompression.Zstd),
            Throws.TypeOf<InvalidDataException>());
    }

    [Test]
    public void DecompressPayload_rejects_a_negative_length_prefix()
    {
        var provider = CreateCompressingProvider();

        var payload = new byte[16];
        BinaryPrimitives.WriteInt32LittleEndian(payload, -1);

        Assert.That(
            () => InvokeDecompressPayload(provider, payload, (byte)LatticeCompression.Zstd),
            Throws.TypeOf<InvalidDataException>());
    }
}
