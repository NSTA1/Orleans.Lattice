using Microsoft.Extensions.DependencyInjection;

namespace Orleans.Lattice.Tests.BPlusTree.PublicApiContract;

public partial class PublicApiContractTests
{
    // -- Public compression registration surface ----------------------
    //
    // These tests pin the public DI seam that lets a host plug in a
    // custom ILatticeCompressor without any core enum churn: the
    // LatticeCompressionServiceCollectionExtensions.AddLatticeCompressor
    // overloads are part of the supported public API for compression
    // configuration in Orleans.Lattice. The encoder''s byte-keyed
    // dispatch (so a host-reserved tag in [0x80, 0xFF] round-trips
    // through encode/decode) is exercised by the framing-round-trip
    // suite in the replication package; this partial pins the
    // surface that hosts see.

    [Test]
    public void PublicApiContract_AddLatticeCompressor_generic_registers_compressor()
    {
        var services = new ServiceCollection();

        var returned = services.AddLatticeCompressor<PublicApiContractCustomCompressor>();
        var sp = services.BuildServiceProvider();

        Assert.Multiple(() =>
        {
            Assert.That(returned, Is.SameAs(services));
            var resolved = sp.GetServices<ILatticeCompressor>().ToArray();
            Assert.That(resolved, Has.Length.EqualTo(1));
            Assert.That(resolved[0], Is.InstanceOf<PublicApiContractCustomCompressor>());
            Assert.That((byte)resolved[0].Algorithm, Is.EqualTo(0xA1));
        });
    }

    [Test]
    public void PublicApiContract_AddLatticeCompressor_instance_registers_compressor()
    {
        var services = new ServiceCollection();
        var instance = new PublicApiContractCustomCompressor();

        var returned = services.AddLatticeCompressor(instance);
        var sp = services.BuildServiceProvider();

        Assert.Multiple(() =>
        {
            Assert.That(returned, Is.SameAs(services));
            var resolved = sp.GetServices<ILatticeCompressor>().ToArray();
            Assert.That(resolved, Has.Length.EqualTo(1));
            Assert.That(resolved[0], Is.SameAs(instance));
        });
    }

    [Test]
    public void PublicApiContract_AddLatticeCompressor_generic_is_idempotent_for_same_type()
    {
        var services = new ServiceCollection();

        services.AddLatticeCompressor<PublicApiContractCustomCompressor>();
        services.AddLatticeCompressor<PublicApiContractCustomCompressor>();
        var sp = services.BuildServiceProvider();

        Assert.That(sp.GetServices<ILatticeCompressor>().Count(), Is.EqualTo(1));
    }

    [Test]
    public void PublicApiContract_AddLatticeCompressor_supports_side_by_side_registrations()
    {
        // Two distinct compressor types with different host-reserved
        // tags must co-exist so a host can ship multiple algorithms
        // (e.g. one for the framing tail and one for a future WAL
        // payload path) without either replacing the other.
        var services = new ServiceCollection();

        services.AddLatticeCompressor<PublicApiContractCustomCompressor>();
        services.AddLatticeCompressor<PublicApiContractAlternateCompressor>();
        var sp = services.BuildServiceProvider();

        var resolved = sp.GetServices<ILatticeCompressor>().ToArray();
        Assert.That(resolved.Select(c => (byte)c.Algorithm),
            Is.EquivalentTo(new byte[] { 0xA1, 0xA2 }));
    }

    [Test]
    public void PublicApiContract_AddLatticeCompressor_generic_throws_on_null_services()
    {
        Assert.That(
            () => LatticeCompressionServiceCollectionExtensions.AddLatticeCompressor<PublicApiContractCustomCompressor>(null!),
            Throws.ArgumentNullException);
    }

    [Test]
    public void PublicApiContract_AddLatticeCompressor_instance_throws_on_null_arguments()
    {
        Assert.Multiple(() =>
        {
            Assert.That(
                () => LatticeCompressionServiceCollectionExtensions.AddLatticeCompressor(null!, new PublicApiContractCustomCompressor()),
                Throws.ArgumentNullException);
            Assert.That(
                () => new ServiceCollection().AddLatticeCompressor((ILatticeCompressor)null!),
                Throws.ArgumentNullException);
        });
    }

    /// <summary>
    /// Test-only host-defined compressor that claims a tag in the
    /// reserved [0x80, 0xFF] range. The body is identity-copy because
    /// the only contract this fixture pins is registration / lookup,
    /// not the compression algorithm itself.
    /// </summary>
    private sealed class PublicApiContractCustomCompressor : ILatticeCompressor
    {
        public LatticeCompression Algorithm => (LatticeCompression)0xA1;
        public int GetMaxCompressedLength(int uncompressedLength) => uncompressedLength;
        public int Compress(ReadOnlySpan<byte> source, Span<byte> destination)
        {
            source.CopyTo(destination);
            return source.Length;
        }
        public void Decompress(ReadOnlySpan<byte> source, Span<byte> destination, int uncompressedLength)
            => source.CopyTo(destination);
    }

    /// <summary>Second host-defined compressor; pins side-by-side registration.</summary>
    private sealed class PublicApiContractAlternateCompressor : ILatticeCompressor
    {
        public LatticeCompression Algorithm => (LatticeCompression)0xA2;
        public int GetMaxCompressedLength(int uncompressedLength) => uncompressedLength;
        public int Compress(ReadOnlySpan<byte> source, Span<byte> destination)
        {
            source.CopyTo(destination);
            return source.Length;
        }
        public void Decompress(ReadOnlySpan<byte> source, Span<byte> destination, int uncompressedLength)
            => source.CopyTo(destination);
    }
}