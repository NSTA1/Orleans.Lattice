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
