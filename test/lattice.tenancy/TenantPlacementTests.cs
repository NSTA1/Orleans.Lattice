namespace Orleans.Lattice.Tenancy.Tests;

/// <summary>Unit tests for <see cref="TenantPlacement"/>.</summary>
public sealed class TenantPlacementTests
{
    [Test]
    public void Shared_names_no_provider_no_filter_and_is_not_dedicated()
    {
        var placement = TenantPlacement.Shared;

        Assert.Multiple(() =>
        {
            Assert.That(placement.WalProviderName, Is.Null);
            Assert.That(placement.PlacementFilter, Is.Null);
            Assert.That(placement.DedicatedWal, Is.False);
        });
    }

    [Test]
    public void IsShared_is_true_for_the_shared_binding()
    {
        Assert.That(TenantPlacement.Shared.IsShared, Is.True);
    }

    [Test]
    public void IsShared_is_false_when_a_wal_provider_is_bound()
    {
        var placement = new TenantPlacement { WalProviderName = "wal-a" };

        Assert.That(placement.IsShared, Is.False);
    }

    [Test]
    public void IsShared_is_false_when_a_placement_filter_is_bound()
    {
        var placement = new TenantPlacement { PlacementFilter = "silo-group-a" };

        Assert.That(placement.IsShared, Is.False);
    }

    [Test]
    public void IsShared_is_false_when_dedicated_wal_is_requested()
    {
        var placement = new TenantPlacement { DedicatedWal = true };

        Assert.That(placement.IsShared, Is.False);
    }
}
