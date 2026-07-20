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
    public void Ordered_lists_explore_then_backups_then_access_then_schema()
    {
        var areas = AppAreas.Ordered.Select(a => a.Area).ToArray();

        Assert.That(areas, Is.EqualTo(new[] { AppArea.Explore, AppArea.Backups, AppArea.Access, AppArea.Schema }));
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
    public void IsEnabled_access_false_when_no_capability()
    {
        Assert.That(AppAreas.IsEnabled(AppArea.Access, ExplorerCapabilities.Empty), Is.False);
    }

    [Test]
    public void IsEnabled_access_true_when_auth_admin_allowed()
    {
        var caps = new ExplorerCapabilities { AuthAdminAllowed = true };

        Assert.That(AppAreas.IsEnabled(AppArea.Access, caps), Is.True);
    }

    [Test]
    public void IsEnabled_schema_false_when_no_capability()
    {
        Assert.That(AppAreas.IsEnabled(AppArea.Schema, ExplorerCapabilities.Empty), Is.False);
    }

    [Test]
    public void IsEnabled_schema_true_when_schema_allowed()
    {
        var caps = new ExplorerCapabilities { SchemaAllowed = true };

        Assert.That(AppAreas.IsEnabled(AppArea.Schema, caps), Is.True);
    }

    [Test]
    public void Label_returns_registered_label()
    {
        Assert.Multiple(() =>
        {
            Assert.That(AppAreas.Label(AppArea.Explore), Is.EqualTo("Explore"));
            Assert.That(AppAreas.Label(AppArea.Backups), Is.EqualTo("Backups"));
            Assert.That(AppAreas.Label(AppArea.Access), Is.EqualTo("Access"));
            Assert.That(AppAreas.Label(AppArea.Schema), Is.EqualTo("Schema"));
        });
    }

    [Test]
    public void Describe_unknown_area_throws()
    {
        Assert.That(() => AppAreas.Describe((AppArea)999), Throws.InstanceOf<ArgumentOutOfRangeException>());
    }

    [Test]
    public void Visible_hides_schema_by_default()
    {
        var areas = AppAreas.Visible(new ExplorerNavigationOptions()).Select(a => a.Area).ToArray();

        Assert.That(areas, Is.EqualTo(new[] { AppArea.Explore, AppArea.Backups, AppArea.Access }));
    }

    [Test]
    public void Visible_includes_schema_when_enabled()
    {
        var options = new ExplorerNavigationOptions { EnableSchemaArea = true };

        var areas = AppAreas.Visible(options).Select(a => a.Area).ToArray();

        Assert.That(areas, Is.EqualTo(new[] { AppArea.Explore, AppArea.Backups, AppArea.Access, AppArea.Schema }));
    }

    [Test]
    public void Visible_null_options_throws()
    {
        Assert.That(() => AppAreas.Visible(null!).ToArray(), Throws.InstanceOf<ArgumentNullException>());
    }

    [Test]
    public void IsVisible_schema_follows_flag()
    {
        Assert.Multiple(() =>
        {
            Assert.That(AppAreas.IsVisible(AppArea.Schema, new ExplorerNavigationOptions()), Is.False);
            Assert.That(AppAreas.IsVisible(AppArea.Schema, new ExplorerNavigationOptions { EnableSchemaArea = true }), Is.True);
        });
    }

    [Test]
    public void IsVisible_non_schema_areas_always_visible()
    {
        var hidden = new ExplorerNavigationOptions();

        Assert.Multiple(() =>
        {
            Assert.That(AppAreas.IsVisible(AppArea.Explore, hidden), Is.True);
            Assert.That(AppAreas.IsVisible(AppArea.Backups, hidden), Is.True);
            Assert.That(AppAreas.IsVisible(AppArea.Access, hidden), Is.True);
        });
    }

    [Test]
    public void IsVisible_null_options_throws()
    {
        Assert.That(() => AppAreas.IsVisible(AppArea.Schema, null!), Throws.InstanceOf<ArgumentNullException>());
    }
}
