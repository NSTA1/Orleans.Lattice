using Orleans.Lattice.Explorer.Core.Navigation;

namespace Orleans.Lattice.Explorer.Tests.Navigation;

[TestFixture]
public class AppAreasTests
{
    [Test]
    public void Default_is_explore()
    {
        Assert.That(AppAreas.Default, Is.EqualTo(AppArea.Explore));
    }

    [Test]
    public void Ordered_lists_explore_then_backups()
    {
        var areas = AppAreas.Ordered.Select(a => a.Area).ToArray();

        Assert.That(areas, Is.EqualTo(new[] { AppArea.Explore, AppArea.Backups }));
    }

    [Test]
    public void IsEnabled_explore_always_true_even_when_empty()
    {
        Assert.That(AppAreas.IsEnabled(AppArea.Explore, ExplorerCapabilities.Empty), Is.True);
    }

    [Test]
    public void IsEnabled_backups_false_when_no_capability()
    {
        Assert.That(AppAreas.IsEnabled(AppArea.Backups, ExplorerCapabilities.Empty), Is.False);
    }

    [Test]
    public void IsEnabled_backups_true_when_coarse_allowed()
    {
        var caps = new ExplorerCapabilities { BackupListAllowed = true };

        Assert.That(AppAreas.IsEnabled(AppArea.Backups, caps), Is.True);
    }

    [Test]
    public void IsEnabled_backups_true_when_any_scope_can_list()
    {
        var caps = new ExplorerCapabilities
        {
            BackupByScope = new Dictionary<string, BackupScopeCapabilitySnapshot>
            {
                ["tree-a"] = new() { CanList = true },
            },
        };

        Assert.That(AppAreas.IsEnabled(AppArea.Backups, caps), Is.True);
    }

    [Test]
    public void IsEnabled_backups_false_when_scope_only_has_non_list_grants()
    {
        var caps = new ExplorerCapabilities
        {
            BackupByScope = new Dictionary<string, BackupScopeCapabilitySnapshot>
            {
                ["tree-a"] = new() { CanCapture = true, CanDelete = true },
            },
        };

        Assert.That(AppAreas.IsEnabled(AppArea.Backups, caps), Is.False);
    }

    [Test]
    public void IsEnabled_null_capabilities_throws()
    {
        Assert.That(() => AppAreas.IsEnabled(AppArea.Backups, null!), Throws.ArgumentNullException);
    }

    [Test]
    public void Label_returns_registered_label()
    {
        Assert.Multiple(() =>
        {
            Assert.That(AppAreas.Label(AppArea.Explore), Is.EqualTo("Explore"));
            Assert.That(AppAreas.Label(AppArea.Backups), Is.EqualTo("Backups"));
        });
    }

    [Test]
    public void Describe_unknown_area_throws()
    {
        Assert.That(() => AppAreas.Describe((AppArea)999), Throws.InstanceOf<ArgumentOutOfRangeException>());
    }
}
