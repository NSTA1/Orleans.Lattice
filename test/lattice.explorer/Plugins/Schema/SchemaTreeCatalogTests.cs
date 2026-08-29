using Orleans.Lattice.Explorer.Plugins.Schema.Domain;

namespace Orleans.Lattice.Explorer.Tests.Plugins.Schema;

/// <summary>
/// The Schema plugin's own projection of a governable tree and the result of
/// discovering them, which is what keeps the plugin's components off the
/// Explorer's shared navigation types.
/// </summary>
[TestFixture]
public sealed class SchemaTreeCatalogTests
{
    [Test]
    public void A_tree_summary_carries_the_id_label_and_the_two_badges()
    {
        var summary = new SchemaTreeSummary("orders", "Orders", "active", 4);

        Assert.Multiple(() =>
        {
            Assert.That(summary.Id, Is.EqualTo("orders"));
            Assert.That(summary.Label, Is.EqualTo("Orders"));
            Assert.That(summary.Lifecycle, Is.EqualTo("active"));
            Assert.That(summary.ShardCount, Is.EqualTo(4));
        });
    }

    [Test]
    public void A_tree_summary_allows_absent_badges()
    {
        var summary = new SchemaTreeSummary("orders", "orders", null, null);

        Assert.Multiple(() =>
        {
            Assert.That(summary.Lifecycle, Is.Null);
            Assert.That(summary.ShardCount, Is.Null);
        });
    }

    [Test]
    public void The_empty_catalog_is_successful_with_no_trees()
    {
        Assert.Multiple(() =>
        {
            Assert.That(SchemaTreeCatalog.Empty.IsSuccess, Is.True);
            Assert.That(SchemaTreeCatalog.Empty.Trees, Is.Empty);
            Assert.That(SchemaTreeCatalog.Empty.Error, Is.Null);
        });
    }

    [Test]
    public void A_successful_catalog_carries_its_trees()
    {
        var trees = new[] { new SchemaTreeSummary("orders", "orders", null, null) };

        var catalog = SchemaTreeCatalog.Succeeded(trees);

        Assert.Multiple(() =>
        {
            Assert.That(catalog.IsSuccess, Is.True);
            Assert.That(catalog.Trees, Is.EqualTo(trees));
            Assert.That(catalog.Error, Is.Null);
        });
    }

    [Test]
    public void A_failed_catalog_carries_the_message_and_no_trees()
    {
        var catalog = SchemaTreeCatalog.Failed("no endpoint");

        Assert.Multiple(() =>
        {
            Assert.That(catalog.IsSuccess, Is.False);
            Assert.That(catalog.Error, Is.EqualTo("no endpoint"));
            Assert.That(catalog.Trees, Is.Empty);
        });
    }

    [Test]
    public void A_catalog_rejects_null_inputs()
    {
        Assert.Multiple(() =>
        {
            Assert.That(() => SchemaTreeCatalog.Succeeded(null!), Throws.ArgumentNullException);
            Assert.That(() => SchemaTreeCatalog.Failed(null!), Throws.ArgumentNullException);
        });
    }
}
