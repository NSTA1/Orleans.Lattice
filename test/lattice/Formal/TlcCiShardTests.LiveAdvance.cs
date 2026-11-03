namespace Orleans.Lattice.Tests.Formal;

public sealed partial class TlcCiShardTests
{
    /// <summary>
    /// The largest state count any live-advance CI case may declare (epic #5605,
    /// member 6): each case is a scenario slice of one module, at most about
    /// five million states, so no oversized configuration reaches CI.
    /// </summary>
    private const long LiveAdvanceStateBudget = 5_000_000;

    [TestCase("WalAppendEpoch", TlcCiShard.WalAppendEpoch)]
    [TestCase("WalHandOffFence", TlcCiShard.WalHandOffFence)]
    [TestCase("WalRoutingFloor", TlcCiShard.WalRoutingFloor)]
    [TestCase("WalLiveAdvanceCore", TlcCiShard.WalLiveAdvanceCore)]
    public void Live_advance_module_routes_every_case_to_its_own_shard_with_its_base(string name, string expected)
    {
        var module = Module(name);
        var cases = new[] { TlcCiShard.OfModule(module) }
            .Concat(module.Manifest.Variants.Keys.Select(variant => TlcCiShard.OfVariant(module, variant)))
            .Concat(module.LoadMutations().Select(mutation => TlcCiShard.Of(module, mutation)))
            .ToArray();

        Assert.Multiple(() =>
        {
            Assert.That(module.LoadMutations(), Is.Not.Empty);
            Assert.That(cases, Has.Length.EqualTo(1 + module.Manifest.Variants.Count + module.Manifest.Counts.Mutations));
            Assert.That(cases, Is.All.EqualTo(expected));
            Assert.That(module.LoadMutations().All(mutation => TlcCiShard.SharesBaseShard(module, mutation)), Is.True,
                "the base must run in the mutations' process to serve their full-bound controls");
            Assert.That(LatticeShards().Single(shard => shard.Categories.Contains(expected)).Tiers,
                Is.EqualTo(new[] { "deterministic" }));
        });
    }

    [Test]
    public void Live_advance_modules_each_have_a_distinct_shard()
    {
        var shards = new[] { "WalAppendEpoch", "WalHandOffFence", "WalRoutingFloor", "WalLiveAdvanceCore" }
            .Select(name => TlcCiShard.OfModule(Module(name)))
            .ToArray();

        Assert.Multiple(() =>
        {
            Assert.That(shards, Is.Unique);
            Assert.That(shards, Has.None.EqualTo(TlcCiShard.ReplicationWal),
                "a live-advance module must not lengthen the replication-wal shard");
            Assert.That(shards, Has.None.Null, "a live-advance module must not fall into the catch-all");
        });
    }

    [TestCase("WalAppendEpoch")]
    [TestCase("WalHandOffFence")]
    [TestCase("WalRoutingFloor")]
    [TestCase("WalLiveAdvanceCore")]
    public void Live_advance_module_declares_no_case_above_the_ci_state_budget(string name)
    {
        var module = Module(name);

        Assert.Multiple(() =>
        {
            Assert.That(module.Manifest.Counts.DistinctStates, Is.LessThanOrEqualTo(LiveAdvanceStateBudget), $"{name} base");
            foreach (var (variant, states) in module.Manifest.Variants)
            {
                Assert.That(states, Is.LessThanOrEqualTo(LiveAdvanceStateBudget), $"{name}.{variant}");
            }
        });
    }
}
