namespace Orleans.Lattice.Tests.Formal;

public sealed partial class TlcCiShardTests
{
    [Test]
    public void Every_TLC_case_carries_exactly_one_module_category_matching_its_module_argument()
    {
        var cases = SpecModuleCases.TlcModules()
            .Concat(SpecModuleCases.TlcMutations())
            .Concat(SpecModuleCases.TlcVariants())
            .ToArray();
        var modules = SpecModuleCatalogue.Repository().Select(module => module.Name).ToArray();

        Assert.Multiple(() =>
        {
            Assert.That(cases, Is.Not.Empty, "a guard over no cases proves nothing");
            Assert.That(
                cases.Select(data => ((SpecModule)data.Arguments[0]!).Name).Distinct(),
                Is.EquivalentTo(modules),
                "every discovered module must contribute at least its base case");
            foreach (var data in cases)
            {
                var module = (SpecModule)data.Arguments[0]!;
                Assert.That(
                    ModuleCategories(data),
                    Is.EqualTo(new[] { TlcCiShard.ModuleCategoryPrefix + module.Name }),
                    string.Join(", ", data.Arguments));
            }
        });
    }

    [Test]
    public void Module_categories_are_a_family_no_shard_claims()
    {
        var shardCategories = LatticeShards().SelectMany(shard => shard.Categories).ToArray();

        Assert.Multiple(() =>
        {
            Assert.That(TlcCiShard.ModuleCategoryPrefix.StartsWith("TlcShard", StringComparison.Ordinal), Is.False,
                "the shard guards select the TlcShard* family by prefix, so the module family must not share it");
            Assert.That(TlcCiShard.All.Where(category => category.StartsWith(TlcCiShard.ModuleCategoryPrefix, StringComparison.Ordinal)),
                Is.Empty);
            Assert.That(shardCategories.Where(category => category.StartsWith(TlcCiShard.ModuleCategoryPrefix, StringComparison.Ordinal)),
                Is.Empty,
                "a shard claiming a module category would move that module's cases out of their measured shards");
        });
    }

    [TestCase("AtomicCommit", TlcCiShard.Atomic)]
    [TestCase("AtomicCommitCrossCluster", TlcCiShard.Atomic)]
    [TestCase("BackupCutover", TlcCiShard.Backup)]
    [TestCase("BackupIncremental", TlcCiShard.Backup)]
    [TestCase("BackupProvenance", TlcCiShard.Backup)]
    [TestCase("BackupRestore", TlcCiShard.Backup)]
    [TestCase("BPlusCascade", TlcCiShard.BPlusCascade)]
    [TestCase("ReplicationCausalDelivery", TlcCiShard.ReBootstrap)]
    [TestCase("ReplicationLowWatermark", TlcCiShard.ReBootstrap)]
    [TestCase("ReplicationReBootstrap", TlcCiShard.ReBootstrap)]
    [TestCase("ShardOwnershipCrdt", TlcCiShard.ShardOwnership)]
    [TestCase("ShardOwnershipCutover", TlcCiShard.ShardOwnership)]
    [TestCase("ShardOwnershipFold", TlcCiShard.ShardOwnership)]
    [TestCase("ShardOwnershipRetention", TlcCiShard.ShardOwnership)]
    [TestCase("WalDurability", TlcCiShard.ReplicationWal)]
    [TestCase("WalMove", TlcCiShard.ReplicationWal)]
    [TestCase("WalPinRetirement", TlcCiShard.ReplicationWal)]
    public void A_co_located_base_shares_the_shard_of_every_mutation_of_its_module(string name, string expected)
    {
        var module = Module(name);

        Assert.Multiple(() =>
        {
            Assert.That(TlcCiShard.OfModule(module), Is.EqualTo(expected));
            foreach (var mutation in module.LoadMutations())
            {
                Assert.That(TlcCiShard.Of(module, mutation), Is.EqualTo(expected), mutation.Name);
                Assert.That(TlcCiShard.SharesBaseShard(module, mutation), Is.True, mutation.Name);
            }
        });
    }

    [Test]
    public void Replication_shares_its_base_with_its_replication_wal_mutations_only()
    {
        var module = Module("Replication");
        var byShard = module.LoadMutations().ToLookup(mutation => TlcCiShard.Of(module, mutation));

        Assert.Multiple(() =>
        {
            Assert.That(TlcCiShard.OfModule(module), Is.EqualTo(TlcCiShard.ReplicationWal));
            Assert.That(byShard[TlcCiShard.ReplicationWal].All(m => TlcCiShard.SharesBaseShard(module, m)), Is.True);
            Assert.That(byShard[TlcCiShard.Convergence], Is.Not.Empty);
            Assert.That(byShard[TlcCiShard.Convergence].Any(m => TlcCiShard.SharesBaseShard(module, m)), Is.False);
        });
    }

    /// <summary>
    /// The bases kept apart from their mutations because the measured base is
    /// too long for the mutation shard's critical path (#5161): their full-bound
    /// controls keep launching rather than run the base a second time.
    /// </summary>
    [TestCase("ReplayPermits")]
    [TestCase("BackupCapture")]
    [TestCase("ShardOwnership")]
    public void An_expensive_base_does_not_share_its_mutations_shard(string name)
    {
        var module = Module(name);

        Assert.That(module.LoadMutations().Where(mutation => TlcCiShard.SharesBaseShard(module, mutation)), Is.Empty);
    }

    [Test]
    public void Module_category_is_the_prefix_followed_by_the_module_name()
    {
        Assert.That(TlcCiShard.ModuleCategory(Module("ShardOwnershipFold")), Is.EqualTo("TlcModule_ShardOwnershipFold"));
    }

    [TestCase("Bad-Name")]
    [TestCase("Has Space")]
    [TestCase("")]
    public void Module_category_refuses_a_name_a_test_filter_cannot_select(string name)
    {
        var module = Module("Replication") with { Name = name };

        Assert.Throws<InvalidOperationException>(() => TlcCiShard.ModuleCategory(module));
    }
}
