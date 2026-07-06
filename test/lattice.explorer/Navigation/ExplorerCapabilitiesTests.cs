using Orleans.Lattice.Explorer.Core.Navigation;

namespace Orleans.Lattice.Explorer.Tests.Navigation;

[TestFixture]
public class ExplorerCapabilitiesTests
{
    [Test]
    public void Empty_denies_backup_area_and_every_scope()
    {
        Assert.Multiple(() =>
        {
            Assert.That(ExplorerCapabilities.Empty.BackupListAllowed, Is.False);
            Assert.That(ExplorerCapabilities.Empty.BackupByScope, Is.Empty);
            Assert.That(ExplorerCapabilities.Empty.ForScope("anything"), Is.SameAs(BackupScopeCapabilitySnapshot.None));
        });
    }

    [Test]
    public void ForScope_returns_probed_snapshot_when_present()
    {
        var snapshot = new BackupScopeCapabilitySnapshot { CanList = true, CanRestore = true };
        var caps = new ExplorerCapabilities
        {
            BackupByScope = new Dictionary<string, BackupScopeCapabilitySnapshot> { ["tree-a"] = snapshot },
        };

        Assert.That(caps.ForScope("tree-a"), Is.SameAs(snapshot));
    }

    [Test]
    public void ForScope_returns_none_when_scope_absent()
    {
        Assert.That(ExplorerCapabilities.Empty.ForScope("missing"), Is.SameAs(BackupScopeCapabilitySnapshot.None));
    }

    [Test]
    public void ForScope_null_tree_throws()
    {
        Assert.That(() => ExplorerCapabilities.Empty.ForScope(null!), Throws.ArgumentNullException);
    }
}
