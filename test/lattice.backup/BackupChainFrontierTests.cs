namespace Orleans.Lattice.Backup.Tests;

/// <summary>
/// Unit tests for <see cref="BackupChainFrontier"/>: the empty-origin rule
/// (#2621), the per-origin high-water, and the chain's HLC frontier (#3758).
/// </summary>
[TestFixture]
public sealed class BackupChainFrontierTests
{
    [TestCase(null)]
    [TestCase("")]
    public void NormalizeOrigin_maps_an_unstamped_row_to_no_origin(string? stamp) =>
        Assert.That(BackupChainFrontier.NormalizeOrigin(stamp), Is.Null);

    [Test]
    public void NormalizeOrigin_keeps_a_real_origin() =>
        Assert.That(BackupChainFrontier.NormalizeOrigin("eu-west"), Is.EqualTo("eu-west"));

    [Test]
    public void Observe_keeps_the_highest_tick_per_origin()
    {
        var highWater = new Dictionary<string, long>(StringComparer.Ordinal);

        BackupChainFrontier.Observe(highWater, "o1", 50);
        BackupChainFrontier.Observe(highWater, "o1", 20);
        BackupChainFrontier.Observe(highWater, "o2", 7);
        BackupChainFrontier.Observe(highWater, "o1", 90);

        Assert.That(highWater, Is.EquivalentTo(new Dictionary<string, long> { ["o1"] = 90, ["o2"] = 7 }));
    }

    [Test]
    public void Observe_clamps_a_negative_tick_to_zero()
    {
        var highWater = new Dictionary<string, long>(StringComparer.Ordinal);

        BackupChainFrontier.Observe(highWater, "o1", -5);

        Assert.That(highWater["o1"], Is.Zero);
    }

    [Test]
    public void FullCut_is_the_captured_high_water_when_the_registry_anchor_is_zero() =>
        Assert.That(BackupChainFrontier.FullCut(registryAnchorTicks: 0, capturedHighestTicks: 1234), Is.EqualTo(1234));

    [Test]
    public void FullCut_takes_the_later_of_the_anchor_and_the_captured_high_water() =>
        Assert.That(BackupChainFrontier.FullCut(registryAnchorTicks: 2000, capturedHighestTicks: 1234), Is.EqualTo(2000));

    [Test]
    public void FullCut_is_never_negative() =>
        Assert.That(BackupChainFrontier.FullCut(-10, -3), Is.Zero);

    [Test]
    public void IncrementalCut_carries_the_base_forward_when_the_delta_is_older() =>
        Assert.That(BackupChainFrontier.IncrementalCut(baseCutTicks: 500, deltaHighestTicks: 300), Is.EqualTo(500));

    [Test]
    public void IncrementalCut_advances_to_a_newer_delta() =>
        Assert.That(BackupChainFrontier.IncrementalCut(baseCutTicks: 500, deltaHighestTicks: 800), Is.EqualTo(800));

    [Test]
    public void IncrementalCut_with_an_empty_delta_keeps_the_base() =>
        Assert.That(BackupChainFrontier.IncrementalCut(baseCutTicks: 500, deltaHighestTicks: 0), Is.EqualTo(500));

    [Test]
    public void IncrementalCut_is_never_negative() =>
        Assert.That(BackupChainFrontier.IncrementalCut(-4, -9), Is.Zero);
}
