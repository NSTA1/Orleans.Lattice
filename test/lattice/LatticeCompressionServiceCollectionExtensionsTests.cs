using Microsoft.Extensions.DependencyInjection;
using NSubstitute;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Unit coverage for <see cref="LatticeCompressionServiceCollectionExtensions"/>.
/// These tests pin the public DI surface that hosts use to register
/// custom <see cref="ILatticeCompressor"/> implementations without
/// any core enum churn - the behaviour is also exercised
/// end-to-end through the public API contract suite, but unit
/// coverage here is what wraps a regression to the registration
/// shape itself (null guards, idempotency, instance vs type).
/// </summary>
[TestFixture]
public class LatticeCompressionServiceCollectionExtensionsTests
{
    [Test]
    public void AddLatticeCompressor_generic_throws_on_null_services()
    {
        Assert.That(
            () => LatticeCompressionServiceCollectionExtensions.AddLatticeCompressor<FakeCompressor>(null!),
            Throws.ArgumentNullException);
    }

    [Test]
    public void AddLatticeCompressor_instance_throws_on_null_services()
    {
        var compressor = Substitute.For<ILatticeCompressor>();
        Assert.That(
            () => LatticeCompressionServiceCollectionExtensions.AddLatticeCompressor(null!, compressor),
            Throws.ArgumentNullException);
    }

    [Test]
    public void AddLatticeCompressor_instance_throws_on_null_compressor()
    {
        var services = new ServiceCollection();
        Assert.That(
            () => services.AddLatticeCompressor((ILatticeCompressor)null!),
            Throws.ArgumentNullException);
    }

    [Test]
    public void AddLatticeCompressor_generic_registers_singleton()
    {
        var services = new ServiceCollection();
        services.AddLatticeCompressor<FakeCompressor>();
        var sp = services.BuildServiceProvider();

        var resolved = sp.GetServices<ILatticeCompressor>().ToArray();
        Assert.That(resolved, Has.Length.EqualTo(1));
        Assert.That(resolved[0], Is.InstanceOf<FakeCompressor>());
    }

    [Test]
    public void AddLatticeCompressor_generic_is_idempotent_for_same_type()
    {
        var services = new ServiceCollection();
        services.AddLatticeCompressor<FakeCompressor>();
        services.AddLatticeCompressor<FakeCompressor>();
        var sp = services.BuildServiceProvider();

        var resolved = sp.GetServices<ILatticeCompressor>().ToArray();
        Assert.That(resolved, Has.Length.EqualTo(1));
    }

    [Test]
    public void AddLatticeCompressor_generic_allows_distinct_types_side_by_side()
    {
        var services = new ServiceCollection();
        services.AddLatticeCompressor<FakeCompressor>();
        services.AddLatticeCompressor<OtherFakeCompressor>();
        var sp = services.BuildServiceProvider();

        var resolved = sp.GetServices<ILatticeCompressor>().ToArray();
        Assert.That(resolved, Has.Length.EqualTo(2));
        Assert.That(resolved.Select(c => c.GetType()),
            Is.EquivalentTo(new[] { typeof(FakeCompressor), typeof(OtherFakeCompressor) }));
    }

    [Test]
    public void AddLatticeCompressor_instance_registers_singleton()
    {
        var services = new ServiceCollection();
        var instance = new FakeCompressor();
        services.AddLatticeCompressor(instance);
        var sp = services.BuildServiceProvider();

        var resolved = sp.GetServices<ILatticeCompressor>().ToArray();
        Assert.That(resolved, Has.Length.EqualTo(1));
        Assert.That(resolved[0], Is.SameAs(instance));
    }

    [Test]
    public void AddLatticeCompressor_generic_returns_same_services_for_chaining()
    {
        var services = new ServiceCollection();
        var result = services.AddLatticeCompressor<FakeCompressor>();
        Assert.That(result, Is.SameAs(services));
    }

    [Test]
    public void AddLatticeCompressor_instance_returns_same_services_for_chaining()
    {
        var services = new ServiceCollection();
        var result = services.AddLatticeCompressor(new FakeCompressor());
        Assert.That(result, Is.SameAs(services));
    }

    private sealed class FakeCompressor : ILatticeCompressor
    {
        public LatticeCompression Algorithm => (LatticeCompression)0x80;
        public int GetMaxCompressedLength(int uncompressedLength) => uncompressedLength;
        public int Compress(ReadOnlySpan<byte> source, Span<byte> destination)
        {
            source.CopyTo(destination);
            return source.Length;
        }
        public void Decompress(ReadOnlySpan<byte> source, Span<byte> destination, int uncompressedLength) => source.CopyTo(destination);
    }

    private sealed class OtherFakeCompressor : ILatticeCompressor
    {
        public LatticeCompression Algorithm => (LatticeCompression)0x81;
        public int GetMaxCompressedLength(int uncompressedLength) => uncompressedLength;
        public int Compress(ReadOnlySpan<byte> source, Span<byte> destination)
        {
            source.CopyTo(destination);
            return source.Length;
        }
        public void Decompress(ReadOnlySpan<byte> source, Span<byte> destination, int uncompressedLength) => source.CopyTo(destination);
    }
}