using Orleans.Lattice.Explorer.Plugins;

namespace Orleans.Lattice.Explorer.Tests.Plugins;

[TestFixture]
public sealed class ExplorerPluginAccessKeyTests
{
    [Test]
    public void Plugin_level_key_has_no_scope()
    {
        var key = new ExplorerPluginAccessKey("backups");

        Assert.Multiple(() =>
        {
            Assert.That(key.PluginId, Is.EqualTo("backups"));
            Assert.That(key.Scope, Is.Null);
        });
    }

    [Test]
    public void Scoped_key_carries_both_components()
    {
        var key = new ExplorerPluginAccessKey("backups", "tree-a");

        Assert.Multiple(() =>
        {
            Assert.That(key.PluginId, Is.EqualTo("backups"));
            Assert.That(key.Scope, Is.EqualTo("tree-a"));
        });
    }

    [Test]
    public void Plugin_level_constructor_rejects_a_null_id()
    {
        Assert.That(() => new ExplorerPluginAccessKey(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void Keys_with_the_same_components_are_equal()
    {
        Assert.Multiple(() =>
        {
            Assert.That(
                new ExplorerPluginAccessKey("a", "s"),
                Is.EqualTo(new ExplorerPluginAccessKey("a", "s")));
            Assert.That(
                new ExplorerPluginAccessKey("a", "s").GetHashCode(),
                Is.EqualTo(new ExplorerPluginAccessKey("a", "s").GetHashCode()));
        });
    }

    [Test]
    public void A_scoped_key_is_not_the_plugin_level_key()
    {
        Assert.That(
            new ExplorerPluginAccessKey("a", "s"),
            Is.Not.EqualTo(new ExplorerPluginAccessKey("a")));
    }

    [Test]
    public void Components_compare_ordinally_so_casing_is_significant()
    {
        Assert.Multiple(() =>
        {
            Assert.That(
                new ExplorerPluginAccessKey("a", "s"),
                Is.Not.EqualTo(new ExplorerPluginAccessKey("A", "s")));
            Assert.That(
                new ExplorerPluginAccessKey("a", "s"),
                Is.Not.EqualTo(new ExplorerPluginAccessKey("a", "S")));
        });
    }

    [Test]
    public void ToString_renders_the_plugin_level_key_as_its_id()
    {
        Assert.That(new ExplorerPluginAccessKey("backups").ToString(), Is.EqualTo("backups"));
    }

    [Test]
    public void ToString_renders_a_scoped_key_as_id_slash_scope()
    {
        Assert.That(new ExplorerPluginAccessKey("backups", "tree-a").ToString(), Is.EqualTo("backups/tree-a"));
    }

    [Test]
    public void Change_carries_the_key_and_the_new_decision()
    {
        var key = new ExplorerPluginAccessKey("a", "s");
        var change = new ExplorerPluginAccessChange(key, ExplorerPluginAccess.Allowed);

        Assert.Multiple(() =>
        {
            Assert.That(change.Key, Is.EqualTo(key));
            Assert.That(change.Access, Is.EqualTo(ExplorerPluginAccess.Allowed));
            Assert.That(
                change,
                Is.EqualTo(new ExplorerPluginAccessChange(key, ExplorerPluginAccess.Allowed)));
        });
    }
}
