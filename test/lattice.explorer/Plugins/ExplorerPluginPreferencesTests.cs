using NSubstitute;
using Orleans.Lattice.Explorer.Core.Session;
using Orleans.Lattice.Explorer.UI.Plugins;

namespace Orleans.Lattice.Explorer.Tests.Plugins;

/// <summary>
/// The head-supplied preference adapter. It projects the Explorer's durable
/// preference store onto the contract's narrower shape: no owner discriminator
/// and no garbage-collection sweep, so a plugin can neither tag another
/// surface's entries nor drop them.
/// </summary>
[TestFixture]
public sealed class ExplorerPluginPreferencesTests
{
    [Test]
    public void Constructor_null_store_throws()
    {
        Assert.That(() => new ExplorerPluginPreferences(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void IsLoaded_reads_through_to_the_store()
    {
        var store = Substitute.For<IUiPreferenceStore>();
        store.IsLoaded.Returns(true);

        Assert.That(new ExplorerPluginPreferences(store).IsLoaded, Is.True);
    }

    [Test]
    public async Task EnsureLoadedAsync_delegates_to_the_store()
    {
        var store = Substitute.For<IUiPreferenceStore>();
        var preferences = new ExplorerPluginPreferences(store);

        await preferences.EnsureLoadedAsync();

        await store.Received(1).EnsureLoadedAsync(Arg.Any<CancellationToken>());
    }

    [Test]
    public void TryGet_returns_the_stored_value()
    {
        var store = Substitute.For<IUiPreferenceStore>();
        store.TryGet("page-size", out Arg.Any<int>())
            .Returns(call =>
            {
                call[1] = 50;
                return true;
            });
        var preferences = new ExplorerPluginPreferences(store);

        var found = preferences.TryGet<int>("page-size", out var value);

        Assert.Multiple(() =>
        {
            Assert.That(found, Is.True);
            Assert.That(value, Is.EqualTo(50));
        });
    }

    [Test]
    public void GetOrDefault_returns_the_fallback_when_nothing_is_stored()
    {
        var store = Substitute.For<IUiPreferenceStore>();
        store.GetOrDefault("page-size", 25).Returns(25);
        var preferences = new ExplorerPluginPreferences(store);

        Assert.That(preferences.GetOrDefault("page-size", 25), Is.EqualTo(25));
    }

    [Test]
    public async Task SetAsync_persists_without_an_owner_so_it_survives_a_selection_sweep()
    {
        var store = Substitute.For<IUiPreferenceStore>();
        var preferences = new ExplorerPluginPreferences(store);

        await preferences.SetAsync("page-size", 50);

        await store.Received(1).SetAsync("page-size", 50, null, Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task RemoveAsync_delegates_to_the_store()
    {
        var store = Substitute.For<IUiPreferenceStore>();
        var preferences = new ExplorerPluginPreferences(store);

        await preferences.RemoveAsync("page-size");

        await store.Received(1).RemoveAsync("page-size", Arg.Any<CancellationToken>());
    }

    [Test]
    public void Every_member_rejects_a_null_key()
    {
        var preferences = new ExplorerPluginPreferences(Substitute.For<IUiPreferenceStore>());

        Assert.Multiple(() =>
        {
            Assert.That(() => preferences.TryGet<int>(null!, out _), Throws.ArgumentNullException);
            Assert.That(() => preferences.GetOrDefault(null!, 0), Throws.ArgumentNullException);
            Assert.That(async () => await preferences.SetAsync(null!, 0), Throws.ArgumentNullException);
            Assert.That(async () => await preferences.RemoveAsync(null!), Throws.ArgumentNullException);
        });
    }
}
