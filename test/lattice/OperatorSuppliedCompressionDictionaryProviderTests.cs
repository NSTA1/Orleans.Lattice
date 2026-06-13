using Orleans.Lattice;

namespace Orleans.Lattice.Tests;

[TestFixture]
public class OperatorSuppliedCompressionDictionaryProviderTests
{
    [Test]
    public void Constructor_throws_on_null_map()
    {
        Assert.That(
            () => new OperatorSuppliedCompressionDictionaryProvider(null!),
            Throws.ArgumentNullException);
    }

    [Test]
    public void Constructor_rejects_reserved_id_zero()
    {
        var map = new Dictionary<uint, ReadOnlyMemory<byte>> { [0u] = new byte[] { 1, 2, 3 } };
        Assert.That(
            () => new OperatorSuppliedCompressionDictionaryProvider(map),
            Throws.ArgumentException);
    }

    [Test]
    public void Constructor_rejects_empty_dictionary_value()
    {
        var map = new Dictionary<uint, ReadOnlyMemory<byte>> { [1u] = ReadOnlyMemory<byte>.Empty };
        Assert.That(
            () => new OperatorSuppliedCompressionDictionaryProvider(map),
            Throws.ArgumentException);
    }

    [Test]
    public void TryGetDictionary_returns_registered_bytes_for_known_id()
    {
        var bytes = new byte[] { 9, 8, 7, 6 };
        var provider = new OperatorSuppliedCompressionDictionaryProvider(
            new Dictionary<uint, ReadOnlyMemory<byte>> { [5u] = bytes });

        var found = provider.TryGetDictionary(5u, out var dictionary);

        Assert.Multiple(() =>
        {
            Assert.That(found, Is.True);
            Assert.That(dictionary.ToArray(), Is.EqualTo(bytes));
        });
    }

    [Test]
    public void TryGetDictionary_returns_false_for_unknown_id()
    {
        var provider = new OperatorSuppliedCompressionDictionaryProvider(
            new Dictionary<uint, ReadOnlyMemory<byte>> { [5u] = new byte[] { 1 } });

        var found = provider.TryGetDictionary(99u, out var dictionary);

        Assert.Multiple(() =>
        {
            Assert.That(found, Is.False);
            Assert.That(dictionary.IsEmpty, Is.True);
        });
    }

    [Test]
    public void TryGetDictionary_reports_reserved_id_zero_as_absent()
    {
        var provider = new OperatorSuppliedCompressionDictionaryProvider(
            new Dictionary<uint, ReadOnlyMemory<byte>> { [1u] = new byte[] { 1 } });

        Assert.That(provider.TryGetDictionary(0u, out var dictionary), Is.False);
        Assert.That(dictionary.IsEmpty, Is.True);
    }

    [Test]
    public void Empty_provider_resolves_nothing()
    {
        Assert.That(OperatorSuppliedCompressionDictionaryProvider.Empty.TryGetDictionary(1u, out _), Is.False);
    }

    [Test]
    public void AvailableDictionaryIds_returns_registered_ids_sorted()
    {
        var provider = new OperatorSuppliedCompressionDictionaryProvider(
            new Dictionary<uint, ReadOnlyMemory<byte>>
            {
                [7u] = new byte[] { 1 },
                [3u] = new byte[] { 2 },
                [11u] = new byte[] { 3 },
            });

        Assert.That(provider.AvailableDictionaryIds, Is.EqualTo(new uint[] { 3u, 7u, 11u }));
    }

    [Test]
    public void AvailableDictionaryIds_is_empty_for_empty_provider()
    {
        Assert.That(OperatorSuppliedCompressionDictionaryProvider.Empty.AvailableDictionaryIds, Is.Empty);
    }

    [Test]
    public void Provider_exposes_the_dictionary_catalog_interface()
    {
        ILatticeCompressionDictionaryProvider provider =
            new OperatorSuppliedCompressionDictionaryProvider(
                new Dictionary<uint, ReadOnlyMemory<byte>> { [5u] = new byte[] { 1 } });

        Assert.That(provider, Is.InstanceOf<ILatticeCompressionDictionaryCatalog>());
        Assert.That(((ILatticeCompressionDictionaryCatalog)provider).AvailableDictionaryIds,
            Is.EqualTo(new uint[] { 5u }));
    }
}
