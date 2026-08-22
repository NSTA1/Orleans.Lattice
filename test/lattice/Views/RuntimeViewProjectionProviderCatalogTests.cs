using Orleans.Lattice.Views;

namespace Orleans.Lattice.Tests.Views;

/// <summary>
/// Unit coverage for <see cref="RuntimeViewProjectionProviderCatalog"/>, the
/// immutable lookup of host-configured runtime-view projection providers.
/// </summary>
[TestFixture]
public class RuntimeViewProjectionProviderCatalogTests
{
    private static RuntimeViewProjectionProviderRegistration Registration(string key) =>
        new(key, (_, context) => new LatticeViewDefinition(
            context.ViewName,
            new FakeProjection()));

    private sealed class FakeProjection : ILatticeViewProjection
    {
        public string ProjectionVersion => "v1";

        public IEnumerable<ViewWrite> Project(LatticeMutation mutation) => Array.Empty<ViewWrite>();
    }

    [Test]
    public void TryGet_registeredKey_returnsRegistration()
    {
        var catalog = new RuntimeViewProjectionProviderCatalog(
            [Registration("provider-a"), Registration("provider-b")]);

        var result = catalog.TryGet("provider-b");

        Assert.That(result, Is.Not.Null);
        Assert.That(result!.ProviderKey, Is.EqualTo("provider-b"));
    }

    [Test]
    public void TryGet_unknownKey_returnsNull()
    {
        var catalog = new RuntimeViewProjectionProviderCatalog([Registration("provider-a")]);

        Assert.That(catalog.TryGet("missing"), Is.Null);
    }

    [Test]
    public void TryGet_isCaseSensitive()
    {
        var catalog = new RuntimeViewProjectionProviderCatalog([Registration("Provider")]);

        Assert.Multiple(() =>
        {
            Assert.That(catalog.TryGet("Provider"), Is.Not.Null);
            Assert.That(catalog.TryGet("provider"), Is.Null);
        });
    }

    [Test]
    public void Constructor_emptyRegistrations_resolvesNothing()
    {
        var catalog = new RuntimeViewProjectionProviderCatalog([]);

        Assert.That(catalog.TryGet("anything"), Is.Null);
    }

    [Test]
    public void Constructor_nullRegistrations_throws()
    {
        Assert.That(
            () => new RuntimeViewProjectionProviderCatalog(null!),
            Throws.ArgumentNullException);
    }

    [Test]
    public void Constructor_duplicateKeys_throws()
    {
        Assert.That(
            () => new RuntimeViewProjectionProviderCatalog(
                [Registration("dup"), Registration("dup")]),
            Throws.ArgumentException);
    }
}
