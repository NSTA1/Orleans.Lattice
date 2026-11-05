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
    [TestCase("WalBulkSwitch", TlcCiShard.WalBulkSwitch)]
    [TestCase("WalLiveAdvanceComposite", TlcCiShard.WalLiveAdvanceComposite)]
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

    /// <summary>
    /// <c>WalLiveAdvanceCore</c>'s cases go to its own shard with its base, except the
    /// claimless advance's (#5837): the mutations that raise <c>WithFreeAdvance</c>, whose
    /// controls the base cannot serve, with its smallest variants in one shard, and its
    /// larger variants in another.
    /// </summary>
    [Test]
    public void Live_advance_core_routes_its_claimless_cases_apart_and_the_rest_with_its_base()
    {
        var module = Module("WalLiveAdvanceCore");
        var mutations = module.LoadMutations();
        var free = mutations.Where(mutation => mutation.Bounds.Any(bound => bound.Name == "WithFreeAdvance" && bound.Value == "TRUE")).ToArray();
        var rest = mutations.Except(free).ToArray();
        var freeVariants = module.Manifest.Variants.Keys.Where(variant => variant.StartsWith("Free", StringComparison.Ordinal)).ToArray();
        var otherVariants = module.Manifest.Variants.Keys.Except(freeVariants).ToArray();

        Assert.Multiple(() =>
        {
            Assert.That(free, Is.Not.Empty);
            Assert.That(rest, Is.Not.Empty);
            Assert.That(TlcCiShard.OfModule(module), Is.EqualTo(TlcCiShard.WalLiveAdvanceCore));
            Assert.That(otherVariants.Select(variant => TlcCiShard.OfVariant(module, variant)), Is.All.EqualTo(TlcCiShard.WalLiveAdvanceCore));
            Assert.That(rest.Select(mutation => TlcCiShard.Of(module, mutation)), Is.All.EqualTo(TlcCiShard.WalLiveAdvanceCore));
            Assert.That(rest.All(mutation => TlcCiShard.SharesBaseShard(module, mutation)), Is.True,
                "the base must run in the mutations' process to serve their full-bound controls");
            Assert.That(free.Where(mutation => mutation.Target == "ReadPositionHonest").Select(mutation => TlcCiShard.Of(module, mutation)),
                Is.Not.Empty.And.All.EqualTo(TlcCiShard.WalLiveAdvanceCoreFreeReadPosition));
            Assert.That(free.Where(mutation => mutation.Target != "ReadPositionHonest").Select(mutation => TlcCiShard.Of(module, mutation)),
                Is.Not.Empty.And.All.EqualTo(TlcCiShard.WalLiveAdvanceCoreFree));
            Assert.That(freeVariants.Select(variant => TlcCiShard.OfVariant(module, variant)),
                Is.All.AnyOf(TlcCiShard.WalLiveAdvanceCoreFree, TlcCiShard.WalLiveAdvanceCoreFreeVariants));
            foreach (var category in new[] { TlcCiShard.WalLiveAdvanceCore, TlcCiShard.WalLiveAdvanceCoreFree, TlcCiShard.WalLiveAdvanceCoreFreeReadPosition, TlcCiShard.WalLiveAdvanceCoreFreeVariants })
            {
                Assert.That(LatticeShards().Single(shard => shard.Categories.Contains(category)).Tiers, Is.EqualTo(new[] { "deterministic" }), category);
            }
        });
    }

    [Test]
    public void Live_advance_modules_each_have_a_distinct_shard()
    {
        var shards = new[] { "WalAppendEpoch", "WalHandOffFence", "WalRoutingFloor", "WalBulkSwitch", "WalLiveAdvanceCore", "WalLiveAdvanceComposite" }
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
    [TestCase("WalBulkSwitch")]
    [TestCase("WalLiveAdvanceCore")]
    [TestCase("WalLiveAdvanceComposite")]
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
