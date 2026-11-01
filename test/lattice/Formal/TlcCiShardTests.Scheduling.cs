namespace Orleans.Lattice.Tests.Formal;

public sealed partial class TlcCiShardTests
{
    private const string ReplayTwoPartitionsCategory = "TlcShardSchedulingReplayTwoPartitions";
    private const string SchedulingWorkloadBCategory = "TlcShardSchedulingWorkloadB";
    private const string ShippingCategory = "TlcShardSchedulingShipping";
    private const string ShippingVariantsCategory = "TlcShardSchedulingShippingVariants";

    [Test]
    public void Scheduling_replay_routes_only_its_TwoPartitions_variant_to_a_dedicated_shard()
    {
        var module = Module("ReplayPermits");
        var mutations = module.LoadMutations();

        Assert.Multiple(() =>
        {
            Assert.That(TlcCiShard.SchedulingReplayTwoPartitions, Is.EqualTo(ReplayTwoPartitionsCategory));
            Assert.That(module.Manifest.Variants.Keys, Is.EquivalentTo(new[] { "TwoPartitions", "PartialPrefix" }));
            Assert.That(TlcCiShard.OfVariant(module, "TwoPartitions"), Is.EqualTo(ReplayTwoPartitionsCategory));
            Assert.That(TlcCiShard.OfVariant(module, "PartialPrefix"), Is.EqualTo(TlcCiShard.Scheduling));
            Assert.That(TlcCiShard.OfModule(module), Is.EqualTo(TlcCiShard.Scheduling));
            Assert.That(mutations, Is.Not.Empty);
            foreach (var mutation in mutations)
                Assert.That(TlcCiShard.Of(module, mutation), Is.EqualTo(SchedulingWorkloadBCategory), mutation.Name);
            var other = Module("ReplicationShipping") with { Name = "SomeFutureScheduling" };
            Assert.That(TlcCiShard.OfVariant(other, "TwoPartitions"), Is.EqualTo(TlcCiShard.Scheduling),
                "the dedicated shard is keyed by module and variant, not by variant name alone");
        });
    }

    [Test]
    public void Scheduling_shipping_keeps_its_base_and_all_paired_mutations_together_apart_from_full_variants()
    {
        var module = Module("ReplicationShipping");
        var mutations = module.LoadMutations();

        Assert.Multiple(() =>
        {
            Assert.That(mutations, Has.Count.EqualTo(26));
            Assert.That(module.Manifest.Variants.Keys, Is.EqualTo(new[] { "FourRecords" }));
            Assert.That(TlcCiShard.OfModule(module), Is.EqualTo(ShippingCategory));
            foreach (var mutation in mutations)
                Assert.That(TlcCiShard.Of(module, mutation), Is.EqualTo(ShippingCategory), mutation.Name);
            foreach (var variant in module.Manifest.Variants.Keys.Append("FutureFullVariant"))
                Assert.That(TlcCiShard.OfVariant(module, variant), Is.EqualTo(ShippingVariantsCategory), variant);
        });
    }

    [TestCase("WalFutureScheduling")]
    [TestCase("ReplicationFutureScheduling")]
    [TestCase("SomeFutureScheduling")]
    public void Scheduling_directory_retains_a_general_fallback_before_name_prefix_routing(string name)
    {
        var module = Module("ReplicationShipping");
        var future = module with { Name = name };
        var mutation = module.LoadMutations()[0];

        Assert.Multiple(() =>
        {
            Assert.That(TlcCiShard.OfModule(future), Is.EqualTo(TlcCiShard.Scheduling));
            Assert.That(TlcCiShard.Of(future, mutation), Is.EqualTo(TlcCiShard.Scheduling));
            Assert.That(TlcCiShard.OfVariant(future, "FutureVariant"), Is.EqualTo(TlcCiShard.Scheduling));
            var ordinaryWal = Module("WalDurability");
            Assert.That(TlcCiShard.OfModule(ordinaryWal), Is.EqualTo(TlcCiShard.ReplicationWal), "its base is co-located with its mutations (#5161)");
            Assert.That(TlcCiShard.Of(ordinaryWal, mutation), Is.EqualTo(TlcCiShard.ReplicationWal));
            Assert.That(TlcCiShard.OfVariant(ordinaryWal, "FutureVariant"), Is.Null);
            var outsideScheduling = module with { Directory = ordinaryWal.Directory };
            Assert.That(TlcCiShard.OfModule(outsideScheduling), Is.Null);
            Assert.That(TlcCiShard.Of(outsideScheduling, mutation), Is.EqualTo(TlcCiShard.ReBootstrap));
            Assert.That(TlcCiShard.OfVariant(outsideScheduling, "FourRecords"), Is.EqualTo(TlcCiShard.ReBootstrap));
        });
    }

    [Test]
    public void Scheduling_categories_form_five_nonempty_disjoint_complete_groups()
    {
        var cases = SpecModuleCases.TlcModules()
            .Concat(SpecModuleCases.TlcMutations())
            .Concat(SpecModuleCases.TlcVariants())
            .Where(data => data.Arguments[0] is SpecModule module
                && Path.GetFileName(module.Directory) == "scheduling")
            .ToArray();
        var counts = cases.SelectMany(Categories).GroupBy(category => category)
            .ToDictionary(group => group.Key, group => group.Count(), StringComparer.Ordinal);
        var expectedCounts = new Dictionary<string, int>(StringComparer.Ordinal)
        {
            [TlcCiShard.Scheduling] = 106,
            [SchedulingWorkloadBCategory] = 89,
            [ReplayTwoPartitionsCategory] = 1,
            [ShippingCategory] = 27,
            [ShippingVariantsCategory] = 1,
        };

        Assert.Multiple(() =>
        {
            Assert.That(cases, Has.Length.EqualTo(224), "ten-module source boundary; not an eleven-module census");
            Assert.That(counts, Is.EquivalentTo(expectedCounts));
            foreach (var data in cases)
                Assert.That(Categories(data), Has.Exactly(1).Items, string.Join(", ", data.Arguments));
            foreach (var category in expectedCounts.Keys)
            {
                var shard = LatticeShards().SingleOrDefault(shard => shard.Categories.Contains(category));
                Assert.That(shard.Name, Is.Not.Null, $"missing shard for {category}");
                if (shard.Name is null)
                    continue;
                Assert.That(shard.Tlc, Is.True, category);
                Assert.That(shard.TlcSmoke, Is.False, category);
                Assert.That(shard.Tiers, Is.EqualTo(new[] { "deterministic" }), category);
                Assert.That(shard.Includes, Is.Empty, "a name prefix could steal other ordinary TLC cases");
            }
        });
    }

    [TestCase("base")]
    [TestCase("mutation")]
    [TestCase("variant")]
    public void Ordinary_TLC_case_population_is_unchanged_and_structural_cases_remain_untagged(string kind)
    {
        var structural = (kind switch
        {
            "base" => SpecModuleCases.Modules(),
            "mutation" => SpecModuleCases.Mutations(),
            _ => SpecModuleCases.Variants(),
        }).ToArray();
        var ordinary = (kind switch
        {
            "base" => SpecModuleCases.TlcModules(),
            "mutation" => SpecModuleCases.TlcMutations(),
            _ => SpecModuleCases.TlcVariants(),
        }).ToArray();
        static string Identity(TestCaseData data) => string.Join("|", data.Arguments);
        var identities = ordinary.Select(Identity).ToArray();

        Assert.Multiple(() =>
        {
            Assert.That(structural, Is.Not.Empty);
            Assert.That(identities, Is.EquivalentTo(structural.Select(Identity)));
            Assert.That(identities, Is.Unique, "partitioning must not duplicate a native invocation");
            Assert.That(structural.SelectMany(AllCategories), Is.Empty, "structural gates stay outside TLC shards and carry no module category");
            foreach (var data in ordinary)
            {
                var expected = data.Arguments switch
                {
                    [SpecModule module] => TlcCiShard.OfModule(module),
                    [SpecModule module, SpecMutation mutation] => TlcCiShard.Of(module, mutation),
                    [SpecModule module, string variant] => TlcCiShard.OfVariant(module, variant),
                    _ => throw new InvalidOperationException($"Unexpected {kind} arguments."),
                };
                Assert.That(Categories(data), Is.EqualTo(expected is null ? Array.Empty<string>() : [expected]),
                    Identity(data));
            }
        });
    }
}
