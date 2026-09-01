namespace Orleans.Lattice.Tenancy.Tests;

/// <summary>Unit tests for <see cref="TenantBudgetApportionment"/>.</summary>
public sealed class TenantBudgetApportionmentTests
{
    [Test]
    public void StaticEvenShare_divides_the_cluster_rate_by_the_silo_count()
    {
        Assert.That(TenantBudgetApportionment.StaticEvenShare(1000, 4), Is.EqualTo(250));
    }

    [Test]
    public void StaticEvenShare_floors_the_silo_count_to_one()
    {
        Assert.That(TenantBudgetApportionment.StaticEvenShare(1000, 0), Is.EqualTo(1000));
        Assert.That(TenantBudgetApportionment.StaticEvenShare(1000, -3), Is.EqualTo(1000));
    }

    [Test]
    public void StaticEvenShare_clamps_a_negative_rate_to_zero()
    {
        Assert.That(TenantBudgetApportionment.StaticEvenShare(-100, 4), Is.EqualTo(0));
    }

    [Test]
    public void StaticEvenShare_sum_across_silos_never_exceeds_the_cluster_rate()
    {
        // 1000 / 3 = 333 (floored); 333 * 3 = 999 <= 1000.
        var share = TenantBudgetApportionment.StaticEvenShare(1000, 3);
        Assert.That(share * 3, Is.LessThanOrEqualTo(1000));
    }

    [Test]
    public void DemandProportionalShare_with_zero_total_demand_falls_back_to_static_even()
    {
        var share = TenantBudgetApportionment.DemandProportionalShare(
            clusterRate: 1000, liveSiloCount: 4, thisSiloDemand: 0, totalClusterDemand: 0, reserveFraction: 0.2);

        Assert.That(share, Is.EqualTo(250));
    }

    [Test]
    public void DemandProportionalShare_gives_a_sole_demander_the_whole_demand_pool_plus_its_even_floor()
    {
        // reserve = 200, even floor = 50, demand pool = 800 all to this silo.
        var share = TenantBudgetApportionment.DemandProportionalShare(
            clusterRate: 1000, liveSiloCount: 4, thisSiloDemand: 500, totalClusterDemand: 500, reserveFraction: 0.2);

        Assert.That(share, Is.EqualTo(850));
    }

    [Test]
    public void DemandProportionalShare_splits_the_demand_pool_in_proportion_to_demand()
    {
        // clusterRate 1000, reserve 0.2 => reserved 200, even floor 200/4=50,
        // demand pool 800. This silo drove 300 of 1200 total => 1/4 of pool = 200.
        // share = 50 + 200 = 250.
        var share = TenantBudgetApportionment.DemandProportionalShare(
            clusterRate: 1000, liveSiloCount: 4, thisSiloDemand: 300, totalClusterDemand: 1200, reserveFraction: 0.2);

        Assert.That(share, Is.EqualTo(250));
    }

    [Test]
    public void DemandProportionalShare_redistributes_idle_budget_to_a_busy_silo()
    {
        // Two silos, one busy (900 of 1000 demand) and one idle (100). The busy
        // silo's share must exceed the static-even half, and the idle silo's must
        // fall below it, while both stay within the cluster rate.
        const long clusterRate = 1000;
        var busy = TenantBudgetApportionment.DemandProportionalShare(
            clusterRate, liveSiloCount: 2, thisSiloDemand: 900, totalClusterDemand: 1000, reserveFraction: 0.2);
        var idle = TenantBudgetApportionment.DemandProportionalShare(
            clusterRate, liveSiloCount: 2, thisSiloDemand: 100, totalClusterDemand: 1000, reserveFraction: 0.2);

        Assert.Multiple(() =>
        {
            Assert.That(busy, Is.GreaterThan(500));
            Assert.That(idle, Is.LessThan(500));
            Assert.That(idle, Is.GreaterThan(0), "reserve floor keeps an idle silo non-zero");
            Assert.That(busy + idle, Is.LessThanOrEqualTo(clusterRate), "sum stays bounded by the cluster rate");
        });
    }

    [Test]
    public void DemandProportionalShare_sum_across_all_silos_stays_bounded_by_the_cluster_rate()
    {
        // Three silos with demands 600/300/100 of 1000 total.
        const long clusterRate = 1000;
        long[] demands = [600, 300, 100];
        long total = 1000;

        long sum = 0;
        foreach (var d in demands)
        {
            sum += TenantBudgetApportionment.DemandProportionalShare(
                clusterRate, liveSiloCount: 3, thisSiloDemand: d, totalClusterDemand: total, reserveFraction: 0.2);
        }

        Assert.That(sum, Is.LessThanOrEqualTo(clusterRate));
    }

    [Test]
    public void DemandProportionalShare_is_capped_at_the_cluster_rate()
    {
        // A degenerate reserve of 1.0 plus a sole demander could otherwise exceed
        // the rate; the cap holds.
        var share = TenantBudgetApportionment.DemandProportionalShare(
            clusterRate: 1000, liveSiloCount: 1, thisSiloDemand: 10, totalClusterDemand: 10, reserveFraction: 1.0);

        Assert.That(share, Is.LessThanOrEqualTo(1000));
    }

    [Test]
    public void DemandProportionalShare_clamps_the_reserve_fraction_into_range()
    {
        var low = TenantBudgetApportionment.DemandProportionalShare(
            clusterRate: 1000, liveSiloCount: 2, thisSiloDemand: 500, totalClusterDemand: 1000, reserveFraction: -1.0);
        var high = TenantBudgetApportionment.DemandProportionalShare(
            clusterRate: 1000, liveSiloCount: 2, thisSiloDemand: 500, totalClusterDemand: 1000, reserveFraction: 5.0);

        Assert.Multiple(() =>
        {
            Assert.That(low, Is.GreaterThanOrEqualTo(0));
            Assert.That(low, Is.LessThanOrEqualTo(1000));
            Assert.That(high, Is.GreaterThanOrEqualTo(0));
            Assert.That(high, Is.LessThanOrEqualTo(1000));
        });
    }

    [Test]
    public void DemandProportionalShare_treats_negative_this_silo_demand_as_bootstrap()
    {
        var share = TenantBudgetApportionment.DemandProportionalShare(
            clusterRate: 1000, liveSiloCount: 4, thisSiloDemand: -1, totalClusterDemand: 1000, reserveFraction: 0.2);

        Assert.That(share, Is.EqualTo(250), "negative local demand is treated as no signal (static-even)");
    }

    [Test]
    public void DemandProportionalShare_clamps_a_negative_cluster_rate_to_zero()
    {
        // A negative cluster rate is nonsensical; it must be clamped to zero so the
        // method returns 0 rather than propagating a negative rate to the bucket.
        var share = TenantBudgetApportionment.DemandProportionalShare(
            clusterRate: -500, liveSiloCount: 2, thisSiloDemand: 100, totalClusterDemand: 200, reserveFraction: 0.2);

        Assert.That(share, Is.EqualTo(0));
    }
}
