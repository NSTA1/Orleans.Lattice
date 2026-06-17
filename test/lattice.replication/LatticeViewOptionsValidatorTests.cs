using Orleans.Lattice.Replication.Views;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Unit tests for <see cref="LatticeViewOptionsValidator"/>, covering the Phase 4
/// atomic-staging bounds (<see cref="LatticeViewOptions.MaxStagedTransactions"/>
/// and <see cref="LatticeViewOptions.MaxStagedBytes"/>) alongside the pre-existing
/// batch-size / coalesce-window / aggregation guards.
/// </summary>
[TestFixture]
public class LatticeViewOptionsValidatorTests
{
    private static LatticeViewOptions Valid() => new()
    {
        BatchSize = 256,
        CoalesceWindow = TimeSpan.FromSeconds(1),
        AggregationFanout = 8,
        AggregationMaxGroupEntries = 0,
        MaxStagedTransactions = LatticeViewOptions.DefaultMaxStagedTransactions,
        MaxStagedBytes = LatticeViewOptions.DefaultMaxStagedBytes,
    };

    [Test]
    public void Validate_default_options_succeeds()
    {
        var result = new LatticeViewOptionsValidator().Validate(null, Valid());
        Assert.That(result.Succeeded, Is.True);
    }

    [Test]
    public void Validate_rejects_non_positive_max_staged_transactions()
    {
        var options = Valid();
        options.MaxStagedTransactions = 0;

        var result = new LatticeViewOptionsValidator().Validate(null, options);

        Assert.Multiple(() =>
        {
            Assert.That(result.Failed, Is.True);
            Assert.That(result.FailureMessage, Does.Contain(nameof(LatticeViewOptions.MaxStagedTransactions)));
        });
    }

    [Test]
    public void Validate_rejects_non_positive_max_staged_bytes()
    {
        var options = Valid();
        options.MaxStagedBytes = 0;

        var result = new LatticeViewOptionsValidator().Validate(null, options);

        Assert.Multiple(() =>
        {
            Assert.That(result.Failed, Is.True);
            Assert.That(result.FailureMessage, Does.Contain(nameof(LatticeViewOptions.MaxStagedBytes)));
        });
    }

    [Test]
    public void Validate_accepts_minimum_positive_bounds()
    {
        var options = Valid();
        options.MaxStagedTransactions = 1;
        options.MaxStagedBytes = 1;

        var result = new LatticeViewOptionsValidator().Validate(null, options);

        Assert.That(result.Succeeded, Is.True);
    }

    [Test]
    public void Validate_rejects_non_positive_read_handle_cache_ttl()
    {
        var options = Valid();
        options.ReadHandleCacheTtl = TimeSpan.Zero;

        var result = new LatticeViewOptionsValidator().Validate(null, options);

        Assert.Multiple(() =>
        {
            Assert.That(result.Failed, Is.True);
            Assert.That(result.FailureMessage, Does.Contain(nameof(LatticeViewOptions.ReadHandleCacheTtl)));
        });
    }

    [Test]
    public void Validate_rejects_reclaim_grace_not_exceeding_cache_ttl()
    {
        var options = Valid();
        options.ReadHandleCacheTtl = TimeSpan.FromSeconds(2);
        options.OldGenerationReclaimGrace = TimeSpan.FromSeconds(2);

        var result = new LatticeViewOptionsValidator().Validate(null, options);

        Assert.Multiple(() =>
        {
            Assert.That(result.Failed, Is.True);
            Assert.That(result.FailureMessage, Does.Contain(nameof(LatticeViewOptions.OldGenerationReclaimGrace)));
        });
    }

    [Test]
    public void Validate_accepts_reclaim_grace_above_cache_ttl()
    {
        var options = Valid();
        options.ReadHandleCacheTtl = TimeSpan.FromMilliseconds(50);
        options.OldGenerationReclaimGrace = TimeSpan.FromMilliseconds(200);

        var result = new LatticeViewOptionsValidator().Validate(null, options);

        Assert.That(result.Succeeded, Is.True);
    }
}
