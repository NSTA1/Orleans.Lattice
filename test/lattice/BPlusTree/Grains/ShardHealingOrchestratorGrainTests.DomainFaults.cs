using NSubstitute;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

public partial class ShardHealingOrchestratorGrainTests
{
    [Test]
    public void RunHealingPass_preserves_domain_refusals_instead_of_reporting_no_foldable_pair()
    {
        var h = CreateGrain(physicalShardCount: 8, baseShardCount: 2);
        var fault = new LeafProjectionStaleException("projection unavailable");
        h.ConsolidationOf(1).StartAsync(Arg.Any<int>()).Returns(Task.FromException(fault));

        var actual = Assert.ThrowsAsync<LeafProjectionStaleException>(() => h.Grain.RunHealingPassAsync());

        Assert.That(actual, Is.SameAs(fault));
        Assert.That(h.State.State.InFlightDonorShardIndices, Is.EqualTo(new[] { 1 }),
            "Keep the durable intent until reconciliation establishes the coordinator's actual outcome.");
    }
}
