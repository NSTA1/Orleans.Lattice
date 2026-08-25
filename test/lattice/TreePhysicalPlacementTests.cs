namespace Orleans.Lattice.Tests;

/// <summary>Unit tests for <see cref="TreePhysicalPlacement"/>.</summary>
public sealed class TreePhysicalPlacementTests
{
    [Test]
    public void Default_names_the_catalog_default_provider_key_and_no_filter()
    {
        var placement = TreePhysicalPlacement.Default;

        Assert.Multiple(() =>
        {
            Assert.That(placement.WalProviderKey, Is.EqualTo(IWalStorageProviderCatalog.DefaultProviderKey));
            Assert.That(placement.PlacementFilter, Is.Null);
        });
    }

    [Test]
    public void Default_is_equal_to_another_default()
    {
        // The struct is a value type: two independently produced baselines must
        // compare equal so a resolver result can be checked against Default.
        Assert.That(TreePhysicalPlacement.Default, Is.EqualTo(TreePhysicalPlacement.Default));
    }

    [Test]
    public void With_a_dedicated_key_carries_the_key_and_filter()
    {
        var placement = new TreePhysicalPlacement
        {
            WalProviderKey = "wal-acme",
            PlacementFilter = "silo-group-a",
        };

        Assert.Multiple(() =>
        {
            Assert.That(placement.WalProviderKey, Is.EqualTo("wal-acme"));
            Assert.That(placement.PlacementFilter, Is.EqualTo("silo-group-a"));
        });
    }

    [Test]
    public void A_dedicated_placement_is_not_equal_to_default()
    {
        var placement = new TreePhysicalPlacement { WalProviderKey = "wal-acme" };

        Assert.That(placement, Is.Not.EqualTo(TreePhysicalPlacement.Default));
    }
}
