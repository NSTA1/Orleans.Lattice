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
    /// <c>WalLiveAdvanceCore</c>'s base keeps every mutation whose full-bound control it
    /// serves: each one that does not raise <c>WithFreeAdvance</c>. Its other cases, which
    /// together outran one CI leg, are spread over shards that each keep their shared
    /// controls in one process: its variants without the claimless advance by arm, the
    /// claimless advance's (#5837) mutations by target, and its claimless variants by size
    /// and family.
    /// </summary>
    [Test]
    public void Live_advance_core_routes_its_claimless_cases_and_variants_apart_and_the_rest_with_its_base()
    {
        var module = Module("WalLiveAdvanceCore");
        var mutations = module.LoadMutations();
        var free = mutations.Where(mutation => mutation.Bounds.Any(bound => bound.Name == "WithFreeAdvance" && bound.Value == "TRUE")).ToArray();
        var rest = mutations.Except(free).ToArray();
        var freeVariants = module.Manifest.Variants.Keys.Where(variant => variant.StartsWith("Free", StringComparison.Ordinal)).ToArray();
        var otherVariants = module.Manifest.Variants.Keys.Except(freeVariants).ToArray();
        string[] snapshotTargets = ["SnapshotHonest", "SnapshotForwardHonest", "SoleActivationNeverFallsOff"];
        string[] linkTargets = ["SplitEventuallyLinked", "UnlinkedNeverClaims", "RepairLatches", "PendingCloseNotLatched", "NarrowObligated"];
        string[] lazyArmVariants = ["KeyMove", "PreArmRestart", "PreArmBulk", "PreArmRecreate", "PreArmGateFlip"];

        Assert.Multiple(() =>
        {
            Assert.That(free, Is.Not.Empty);
            Assert.That(rest, Is.Not.Empty);
            Assert.That(TlcCiShard.OfModule(module), Is.EqualTo(TlcCiShard.WalLiveAdvanceCore));
            Assert.That(otherVariants.Where(lazyArmVariants.Contains).Select(variant => TlcCiShard.OfVariant(module, variant)),
                Has.Exactly(lazyArmVariants.Length).Items.And.All.EqualTo(TlcCiShard.WalLiveAdvanceCoreLazyVariants));
            Assert.That(otherVariants.Where(variant => !lazyArmVariants.Contains(variant)).Select(variant => TlcCiShard.OfVariant(module, variant)),
                Is.Not.Empty.And.All.EqualTo(TlcCiShard.WalLiveAdvanceCoreEagerVariants));
            Assert.That(rest.Select(mutation => TlcCiShard.Of(module, mutation)), Is.All.EqualTo(TlcCiShard.WalLiveAdvanceCore));
            Assert.That(rest.All(mutation => TlcCiShard.SharesBaseShard(module, mutation)), Is.True,
                "the base must run in the mutations' process to serve their full-bound controls");
            Assert.That(free.Where(mutation => mutation.Target == "ReadPositionHonest").Select(mutation => TlcCiShard.Of(module, mutation)),
                Is.Not.Empty.And.All.EqualTo(TlcCiShard.WalLiveAdvanceCoreFreeReadPosition));
            Assert.That(free.Where(mutation => snapshotTargets.Contains(mutation.Target)).Select(mutation => TlcCiShard.Of(module, mutation)),
                Is.Not.Empty.And.All.EqualTo(TlcCiShard.WalLiveAdvanceCoreFreeSnapshot));
            Assert.That(free.Where(mutation => linkTargets.Contains(mutation.Target)).Select(mutation => TlcCiShard.Of(module, mutation)),
                Is.Not.Empty.And.All.EqualTo(TlcCiShard.WalLiveAdvanceCoreFreeLink));
            Assert.That(free.Where(mutation => mutation.Target != "ReadPositionHonest" && !snapshotTargets.Contains(mutation.Target) && !linkTargets.Contains(mutation.Target))
                    .Select(mutation => TlcCiShard.Of(module, mutation)),
                Is.Not.Empty.And.All.EqualTo(TlcCiShard.WalLiveAdvanceCoreFree));
            Assert.That(freeVariants.Select(variant => TlcCiShard.OfVariant(module, variant)),
                Is.All.AnyOf(TlcCiShard.WalLiveAdvanceCoreFree, TlcCiShard.WalLiveAdvanceCoreFreeVariants, TlcCiShard.WalLiveAdvanceCoreFreeSplitVariants));
            Assert.That(freeVariants.Select(variant => TlcCiShard.OfVariant(module, variant)).Distinct().Count(), Is.EqualTo(3),
                "each claimless variant shard holds at least one variant");
            foreach (var category in new[]
            {
                TlcCiShard.WalLiveAdvanceCore, TlcCiShard.WalLiveAdvanceCoreEagerVariants, TlcCiShard.WalLiveAdvanceCoreLazyVariants,
                TlcCiShard.WalLiveAdvanceCoreFree, TlcCiShard.WalLiveAdvanceCoreFreeSnapshot, TlcCiShard.WalLiveAdvanceCoreFreeLink,
                TlcCiShard.WalLiveAdvanceCoreFreeReadPosition,
                TlcCiShard.WalLiveAdvanceCoreFreeVariants, TlcCiShard.WalLiveAdvanceCoreFreeSplitVariants,
            })
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
