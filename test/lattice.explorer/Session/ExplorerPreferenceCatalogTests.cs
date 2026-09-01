using Orleans.Lattice.Explorer.Core.Session;

namespace Orleans.Lattice.Explorer.Tests.Session;

/// <summary>
/// The registry that makes the preference contract enumerable, and therefore
/// resettable and disclosable.
/// </summary>
[TestFixture]
public sealed class ExplorerPreferenceCatalogTests
{
    [Test]
    public void Constructor_SeedsTheShellsOwnKeys()
    {
        var catalog = new ExplorerPreferenceCatalog();

        Assert.That(catalog.Keys, Is.EquivalentTo(ExplorerPreferenceKeys.All));
    }

    [Test]
    public void Constructor_NullSeed_Throws()
    {
        Assert.That(() => new ExplorerPreferenceCatalog(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void Constructor_EmptySeed_YieldsAnEmptyCatalog()
    {
        Assert.That(new ExplorerPreferenceCatalog([]).Keys, Is.Empty);
    }

    [Test]
    public void Register_NewKey_AddsItAfterTheShellKeys()
    {
        var catalog = new ExplorerPreferenceCatalog();
        var key = new ExplorerPreferenceKey("feature.theme", "your theme", ExplorerPreferenceScope.User);

        var registered = catalog.Register(key);

        Assert.Multiple(() =>
        {
            Assert.That(registered, Is.SameAs(key));
            Assert.That(catalog.Keys[^1], Is.SameAs(key));
            Assert.That(catalog.Contains(key), Is.True);
        });
    }

    [Test]
    public void Register_TheSameInstanceTwice_IsIdempotent()
    {
        var catalog = new ExplorerPreferenceCatalog([]);
        var key = new ExplorerPreferenceKey("feature.thing", "the thing");

        catalog.Register(key);
        var second = catalog.Register(key);

        Assert.Multiple(() =>
        {
            Assert.That(second, Is.SameAs(key));
            Assert.That(catalog.Keys, Has.Count.EqualTo(1));
        });
    }

    [Test]
    public void Register_ADifferentKeyWithTheSameName_Throws()
    {
        var catalog = new ExplorerPreferenceCatalog([]);
        catalog.Register(new ExplorerPreferenceKey("feature.thing", "the thing"));

        Assert.That(
            () => catalog.Register(new ExplorerPreferenceKey("feature.thing", "something else")),
            Throws.InvalidOperationException.With.Message.Contains("feature.thing"));
    }

    [Test]
    public void Register_Null_Throws()
    {
        Assert.That(() => new ExplorerPreferenceCatalog([]).Register(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void TryGet_RegisteredName_ReturnsTheKey()
    {
        var catalog = new ExplorerPreferenceCatalog();

        var found = catalog.TryGet(ExplorerPreferenceKeys.ActiveArea.Name, out var key);

        Assert.Multiple(() =>
        {
            Assert.That(found, Is.True);
            Assert.That(key, Is.SameAs(ExplorerPreferenceKeys.ActiveArea));
        });
    }

    [Test]
    public void TryGet_UnknownName_ReturnsFalse()
    {
        Assert.That(new ExplorerPreferenceCatalog().TryGet("nope", out _), Is.False);
    }

    [Test]
    public void TryGet_Null_ReturnsFalse()
    {
        Assert.That(new ExplorerPreferenceCatalog().TryGet(null, out _), Is.False);
    }

    [Test]
    public void Contains_Null_IsFalse()
    {
        Assert.That(new ExplorerPreferenceCatalog().Contains(null), Is.False);
    }

    [Test]
    public void Contains_AnUnregisteredKeyWithARegisteredName_IsFalse()
    {
        var catalog = new ExplorerPreferenceCatalog();
        var impostor = new ExplorerPreferenceKey(ExplorerPreferenceKeys.ActiveArea.Name, "something else");

        Assert.That(catalog.Contains(impostor), Is.False);
    }
}
