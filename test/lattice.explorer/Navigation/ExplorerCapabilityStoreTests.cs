using Orleans.Lattice.Explorer.Core.Navigation;

namespace Orleans.Lattice.Explorer.Tests.Navigation;

[TestFixture]
public class ExplorerCapabilityStoreTests
{
    [Test]
    public void Current_starts_empty()
    {
        Assert.That(new ExplorerCapabilityStore().Current, Is.SameAs(ExplorerCapabilities.Empty));
    }

    [Test]
    public void Set_replaces_map_and_raises_changed()
    {
        var store = new ExplorerCapabilityStore();
        var raised = 0;
        store.Changed += () => raised++;
        var caps = new ExplorerCapabilities { BackupListAllowed = true };

        store.Set(caps);

        Assert.Multiple(() =>
        {
            Assert.That(store.Current, Is.SameAs(caps));
            Assert.That(raised, Is.EqualTo(1));
        });
    }

    [Test]
    public void Set_null_throws()
    {
        Assert.That(() => new ExplorerCapabilityStore().Set(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void Reset_restores_empty_and_raises_changed()
    {
        var store = new ExplorerCapabilityStore();
        store.Set(new ExplorerCapabilities { BackupListAllowed = true });
        var raised = 0;
        store.Changed += () => raised++;

        store.Reset();

        Assert.Multiple(() =>
        {
            Assert.That(store.Current, Is.SameAs(ExplorerCapabilities.Empty));
            Assert.That(raised, Is.EqualTo(1));
        });
    }
}
