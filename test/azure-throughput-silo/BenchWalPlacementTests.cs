using VehicleFleetSimulator.AzureThroughput.Engine;

namespace VehicleFleetSimulator.AzureThroughput.Silo.Tests;

/// <summary>
/// Pins the multi-account WAL spread plan shared by the single-VM silo and the
/// Layer 3 producer: partition <c>p</c> routes to <c>acct(p % accounts)</c>,
/// with index 0 left on the default provider.
/// </summary>
[TestFixture]
public class BenchWalPlacementTests
{
    [TestCase(16, 1)]
    [TestCase(16, 0)]
    public void PlanMoves_is_empty_for_a_single_account(int partitions, int accounts)
    {
        Assert.That(BenchWalPlacement.PlanMoves(partitions, accounts), Is.Empty);
    }

    [Test]
    public void PlanMoves_spreads_partitions_round_robin_and_keeps_account_zero_in_place()
    {
        var moves = BenchWalPlacement.PlanMoves(8, 3);

        Assert.That(moves, Is.EqualTo(new[]
        {
            (1, "acct1"), (2, "acct2"),
            (4, "acct1"), (5, "acct2"),
            (7, "acct1"),
        }));
    }

    [Test]
    public void PlanMoves_gives_every_account_an_equal_share_when_partitions_divide_evenly()
    {
        var moves = BenchWalPlacement.PlanMoves(16, 4);

        Assert.Multiple(() =>
        {
            Assert.That(moves, Has.Count.EqualTo(12), "a quarter of the partitions stay on the default account");
            Assert.That(moves.GroupBy(m => m.TargetProviderKey).Select(g => g.Count()), Is.All.EqualTo(4));
        });
    }
}
