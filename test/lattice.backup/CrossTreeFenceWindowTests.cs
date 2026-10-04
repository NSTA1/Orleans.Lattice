namespace Orleans.Lattice.Backup.Tests;

/// <summary>
/// Unit tests for <see cref="CrossTreeFenceWindow"/>: the drain gate and the
/// post-capture re-observation of a cross-tree-consistent backup set.
/// </summary>
[TestFixture]
public sealed class CrossTreeFenceWindowTests
{
    [Test]
    public void IsDrained_only_when_nothing_is_in_flight()
    {
        Assert.Multiple(() =>
        {
            Assert.That(CrossTreeFenceWindow.IsDrained(0), Is.True);
            Assert.That(CrossTreeFenceWindow.IsDrained(1), Is.False);
            Assert.That(CrossTreeFenceWindow.IsDrained(5), Is.False);
        });
    }

    [Test]
    public void IsStable_when_the_epoch_did_not_move_and_nothing_is_in_flight() =>
        Assert.That(CrossTreeFenceWindow.IsStable(epochAtDrain: 7, epochNow: 7, inFlightNow: 0), Is.True);

    [Test]
    public void IsStable_refuses_a_registration_that_completed_inside_the_window() =>
        Assert.That(CrossTreeFenceWindow.IsStable(epochAtDrain: 7, epochNow: 8, inFlightNow: 0), Is.False);

    [Test]
    public void IsStable_refuses_a_saga_still_in_flight() =>
        Assert.That(CrossTreeFenceWindow.IsStable(epochAtDrain: 7, epochNow: 7, inFlightNow: 1), Is.False);
}
