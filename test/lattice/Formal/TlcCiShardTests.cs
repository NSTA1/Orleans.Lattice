using System.Text.Json;
using NUnit.Framework.Internal;
using Orleans.Lattice.Testing.Hygiene;

namespace Orleans.Lattice.Tests.Formal;

/// <summary>
/// The CI routing of the TLC cases: the categories <see cref="TlcCiShard"/>
/// assigns, and the <c>test-shards.json</c> shards that select them. String and
/// reflection work over <c>spec/</c> and the workflow configuration only, so it
/// needs no JVM and runs in the <c>rest</c> shard, not a TLC one.
/// </summary>
[TestFixture]
public sealed partial class TlcCiShardTests
{
    private const string CatchAllShard = "formal-tlc";

    [Test]
    public void Every_category_is_claimed_by_exactly_one_TLC_shard_above_the_catch_all()
    {
        var shards = LatticeShards();
        var catchAll = shards.FindIndex(shard => shard.Name == CatchAllShard);
        Assert.That(catchAll, Is.GreaterThanOrEqualTo(0), "test-shards.json has no formal-tlc catch-all shard");

        var claimed = shards
            .SelectMany((shard, index) => shard.Categories.Select(category => (category, shard, index)))
            .Where(claim => claim.category.StartsWith("TlcShard", StringComparison.Ordinal))
            .ToList();

        Assert.Multiple(() =>
        {
            Assert.That(
                claimed.Select(claim => claim.category),
                Is.EquivalentTo(TlcCiShard.All),
                "every TlcCiShard category needs exactly one shard, and every TlcShard* shard category a TlcCiShard constant: an unclaimed category runs in the catch-all, and a shard with no category behind it is an empty leg");
            foreach (var (category, shard, index) in claimed)
            {
                Assert.That(shard.Tlc, Is.True, $"shard {shard.Name} selects {category} but is not marked \"tlc\"");
                Assert.That(index, Is.LessThan(catchAll), $"shard {shard.Name} must precede the catch-all, which claims every TLC case not yet claimed");
            }
        });
    }

    [Test]
    public void Every_category_routes_at_least_one_case_of_the_repository()
    {
        var routed = SpecModuleCases.TlcModules()
            .Concat(SpecModuleCases.TlcMutations())
            .Concat(SpecModuleCases.TlcVariants())
            .SelectMany(Categories)
            .Distinct()
            .ToList();

        Assert.That(
            routed,
            Is.EquivalentTo(TlcCiShard.All),
            "a category no case carries is a CI leg that runs nothing and reads as green");
    }

    [Test]
    public void Atomic_variant_groups_are_nonempty_and_balanced_by_manifest_state_count()
    {
        var variants = SpecModuleCatalogue.Repository()
            .Where(module => module.Name.StartsWith("AtomicCommit", StringComparison.Ordinal))
            .SelectMany(module => module.Manifest.Variants.Select(variant =>
                (Category: TlcCiShard.OfVariant(module, variant.Key)!, States: variant.Value)))
            .ToList();
        var loads = variants.GroupBy(variant => variant.Category)
            .ToDictionary(group => group.Key, group => group.Sum(variant => variant.States), StringComparer.Ordinal);

        Assert.Multiple(() =>
        {
            Assert.That(loads.Keys, Is.EquivalentTo(TlcCiShard.AtomicVariantShards));
            Assert.That(
                loads.Values.Max() - loads.Values.Min(),
                Is.LessThanOrEqualTo(variants.Max(variant => variant.States)),
                "state-weighted longest-processing-time assignment must keep each shard within one largest variant");
        });
    }

    [Test]
    public void The_base_model_smoke_shard_precedes_the_full_TLC_catch_all()
    {
        var shards = LatticeShards();
        var smokeIndex = shards.FindIndex(shard => shard.Name == "formal-tlc-smoke");
        var fullIndex = shards.FindIndex(shard => shard.Name == CatchAllShard);
        Assert.That(smokeIndex, Is.GreaterThanOrEqualTo(0));
        Assert.That(fullIndex, Is.GreaterThanOrEqualTo(0));
        var smoke = shards[smokeIndex];

        Assert.Multiple(() =>
        {
            Assert.That(smokeIndex, Is.LessThan(fullIndex));
            Assert.That(smoke.TlcSmoke, Is.True);
            Assert.That(smoke.Tiers, Is.EqualTo(new[] { "deterministic" }));
            Assert.That(smoke.Includes, Is.EqualTo(new[]
                { $"Formal.TlcModelCheckTests.{SpecModuleCases.SmokeTestName}" }));
            var smokeTest = typeof(TlcModelCheckTests).GetMethod(SpecModuleCases.SmokeTestName)
                ?? throw new InvalidOperationException("The smoke shard does not name a test method.");
            Assert.That(smokeTest.GetParameters(), Is.Empty);
            Assert.That(smokeTest.IsDefined(typeof(TestAttribute), inherit: false), Is.True);
        });
    }

    [TestCase("Replication", "EventualConvergence", TlcCiShard.Convergence)]
    [TestCase("Replication", "BootstrapHandoffLosesNothing", TlcCiShard.ReplicationWal)]
    [TestCase("ReplicationReBootstrap", "", TlcCiShard.ReBootstrap)]
    [TestCase("WalDurability", "", TlcCiShard.ReplicationWal)]
    [TestCase("ShardOwnershipRetention", "", TlcCiShard.ShardOwnership)]
    [TestCase("AtomicCommitCrossCluster", "", TlcCiShard.Atomic)]
    [TestCase("BackupCapture", "", TlcCiShard.Backup)]
    [TestCase("ReplicationCausalDelivery", "", TlcCiShard.ReBootstrap)]
    [TestCase("ReplicationLowWatermark", "", TlcCiShard.ReBootstrap)]
    public void A_mutation_routes_to_its_modules_shard(string moduleName, string mutationPrefix, string? expected)
    {
        var module = Module(moduleName);
        var mutation = module.LoadMutations().First(m => m.Name.StartsWith(mutationPrefix, StringComparison.Ordinal));

        Assert.That(TlcCiShard.Of(module, mutation), Is.EqualTo(expected), $"{moduleName}/{mutation.Name}");
    }

    [Test]
    public void Bplus_cascade_base_and_mutations_share_one_shard()
    {
        var module = Module("BPlusCascade");
        var mutations = module.LoadMutations();

        Assert.Multiple(() =>
        {
            Assert.That(mutations, Is.Not.Empty);
            Assert.That(TlcCiShard.OfModule(module), Is.EqualTo(TlcCiShard.BPlusCascade));
            Assert.That(mutations.Select(mutation => TlcCiShard.Of(module, mutation)).Distinct(),
                Is.EqualTo(new[] { TlcCiShard.BPlusCascade }));
            Assert.That(mutations.All(mutation => TlcCiShard.SharesBaseShard(module, mutation)), Is.True,
                "the base must run in the mutations' process to serve their full-bound controls");
        });
    }

    [Test]
    public void Every_category_routed_TLC_shard_runs_only_the_deterministic_tier()
    {
        var categoryShards = LatticeShards()
            .Where(shard => shard.Categories.Any(category => category.StartsWith("TlcShard", StringComparison.Ordinal)))
            .ToList();

        Assert.Multiple(() =>
        {
            Assert.That(categoryShards, Has.Count.EqualTo(TlcCiShard.All.Count));
            foreach (var shard in categoryShards)
            {
                Assert.That(shard.Tiers, Is.EqualTo(new[] { "deterministic" }),
                    $"{shard.Name}: a TlcShard* category is carried only by TLC cases, which are never Chaos or Coyote, so any other tier is an empty dotnet test start");
            }
        });
    }

    [Test]
    public void A_module_no_group_names_stays_in_the_catch_all()
    {
        var replication = Module("Replication");
        var unknown = replication with { Name = "SomeFutureModule" };

        Assert.Multiple(() =>
        {
            Assert.That(TlcCiShard.Of(unknown, replication.LoadMutations()[0]), Is.Null);
            Assert.That(TlcCiShard.OfVariant(unknown, "Any"), Is.Null);
        });
    }

    [Test]
    public void Variants_leave_the_catch_all_only_for_explicitly_sharded_areas()
    {
        var variants = SpecModuleCatalogue.Repository()
            .SelectMany(module => module.Manifest.Variants.Keys.Select(variant => (module, variant)))
            .ToList();
        Assert.That(variants.Where(v => v.module.Name.StartsWith("AtomicCommit", StringComparison.Ordinal)), Is.Not.Empty);
        Assert.That(variants.Where(v => v.module.Name == "ReplicationLowWatermark"), Is.Not.Empty);
        Assert.That(variants.Where(v => !v.module.Name.StartsWith("AtomicCommit", StringComparison.Ordinal) && !v.module.Name.StartsWith("Replication", StringComparison.Ordinal)), Is.Not.Empty);

        Assert.Multiple(() =>
        {
            foreach (var (module, variant) in variants)
            {
                var category = TlcCiShard.OfVariant(module, variant);
                if (module.Name.StartsWith("AtomicCommit", StringComparison.Ordinal))
                {
                    Assert.That(
                        TlcCiShard.AtomicVariantShards.Contains(category!),
                        Is.True,
                        $"{module.Name}.{variant} should route to one of the atomic variant shards");
                }
                else
                {
                    var expected = Path.GetFileName(module.Directory) == "scheduling"
                        ? module.Name == "ReplicationShipping" ? "TlcShardSchedulingShippingVariants"
                            : module.Name == "ReplayPermits" && variant == "TwoPartitions"
                            ? "TlcShardSchedulingReplayTwoPartitions"
                            : module.Name is "WalGcStarvationDrive" or "SagaReadProgress" or "TagIndexReconcile"
                            ? TlcCiShard.SchedulingWorkloadB : TlcCiShard.Scheduling
                        : module.Name.StartsWith("Replication", StringComparison.Ordinal)
                        ? TlcCiShard.ReBootstrap
                        : module.Name.StartsWith("Schema", StringComparison.Ordinal)
                        ? TlcCiShard.Schema
                        : module.Name == "MaterialisedViewFence"
                        ? TlcCiShard.MaterialisedViewFence
                        : module.Name == "MaterialisedViewHorizon"
                        ? TlcCiShard.MaterialisedViewHorizon
                        : module.Name == "MaterialisedViewAggregation"
                        ? TlcCiShard.MaterialisedViewAggregation
                        : module.Name == "MaterialisedViewRebind"
                        ? TlcCiShard.MaterialisedViewRebind
                        : module.Name == "MaterialisedViewReadyClosure"
                        ? TlcCiShard.MaterialisedViewReadyClosure
                        : module.Name == "MaterialisedViewSourceAdmission"
                        ? TlcCiShard.MaterialisedViewSourceAdmission
                        : module.Name == "MaterialisedViewResolvedIdentity"
                        ? TlcCiShard.MaterialisedViewResolvedIdentity
                        : module.Name == "MaterialisedViewJointRepair"
                        ? TlcCiShard.MaterialisedViewJointRepair
                        : module.Name.StartsWith("MaterialisedView", StringComparison.Ordinal)
                        ? TlcCiShard.MaterialisedViews
                        : module.Name.StartsWith("AntiEntropy", StringComparison.Ordinal)
                        ? module.Name == "AntiEntropy" && variant == "Alias"
                            ? TlcCiShard.AntiEntropy2 : TlcCiShard.AntiEntropy
                        : module.Name.StartsWith("TreeLifecycle", StringComparison.Ordinal)
                        ? TlcCiShard.TreeLifecycle
                        : module.Name.StartsWith("Alias", StringComparison.Ordinal)
                        ? TlcCiShard.Aliasing
                        : null;
                    Assert.That(category, Is.EqualTo(expected), $"{module.Name}.{variant}");
                }

            }
        });
    }

    [Test]
    public void Tag_sets_the_category_of_a_mutation_and_a_variant_case_and_leaves_any_other_case_alone()
    {
        var rebootstrap = Module("ReplicationReBootstrap");
        var atomic = Module("AtomicCommitCrossCluster");
        var mutation = TlcCiShard.Tag(new TestCaseData(rebootstrap, rebootstrap.LoadMutations()[0]));
        var variant = TlcCiShard.Tag(new TestCaseData(atomic, atomic.Manifest.Variants.Keys.First()));
        var baseCase = TlcCiShard.Tag(new TestCaseData(atomic));
        var other = TlcCiShard.Tag(new TestCaseData("not a module"));

        Assert.Multiple(() =>
        {
            Assert.That(Categories(mutation), Is.EqualTo(new[] { TlcCiShard.ReBootstrap }));
            Assert.That(TlcCiShard.AtomicVariantShards.Contains(Categories(variant).Single()), Is.True);
            Assert.That(Categories(baseCase), Is.EqualTo(new[] { TlcCiShard.Atomic }), "its base is co-located with its mutations (#5161)");
            Assert.That(ModuleCategories(mutation), Is.EqualTo(new[] { "TlcModule_ReplicationReBootstrap" }));
            Assert.That(ModuleCategories(variant), Is.EqualTo(new[] { "TlcModule_AtomicCommitCrossCluster" }));
            Assert.That(ModuleCategories(baseCase), Is.EqualTo(new[] { "TlcModule_AtomicCommitCrossCluster" }));
            Assert.That(AllCategories(other), Is.Empty);
        });
    }

    [TestCase("view-joint-repair", TlcCiShard.MaterialisedViewJointRepair)]
    [TestCase("tree-lifecycle", TlcCiShard.TreeLifecycle)]
    [TestCase("aliasing", TlcCiShard.Aliasing)]
    [TestCase("schema", TlcCiShard.Schema)]
    public void Lifecycle_areas_route_every_base_variant_and_mutation_together(string area, string expected)
    {
        var modules = SpecModuleCatalogue.Repository().Where(module =>
            Path.GetFileName(module.Directory) == area).ToList();
        Assert.That(modules, Is.Not.Empty, $"the {area} shard must contain real models");
        Assert.That(LatticeShards().Single(shard => shard.Categories.Contains(expected)).Tiers,
            Is.EqualTo(new[] { "deterministic" }), "JVM work must not create empty Coyote and Chaos items");
        foreach (var module in modules)
        {
            Assert.That(TlcCiShard.OfModule(module), Is.EqualTo(expected));
            foreach (var mutation in module.LoadMutations())
                Assert.That(TlcCiShard.Of(module, mutation), Is.EqualTo(expected));
            foreach (var variant in module.Manifest.Variants.Keys)
                Assert.That(TlcCiShard.OfVariant(module, variant), Is.EqualTo(expected));
        }
    }

    [TestCase("MaterialisedViewTail", TlcCiShard.MaterialisedViews, 24)]
    [TestCase("MaterialisedViewFence", TlcCiShard.MaterialisedViewFence, 11)]
    [TestCase("MaterialisedViewHorizon", TlcCiShard.MaterialisedViewHorizon, 25)]
    [TestCase("MaterialisedViewAggregation", TlcCiShard.MaterialisedViewAggregation, 20)]
    [TestCase("MaterialisedViewRebind", TlcCiShard.MaterialisedViewRebind, 22)]
    [TestCase("MaterialisedViewReadyClosure", TlcCiShard.MaterialisedViewReadyClosure, 15)]
    [TestCase("MaterialisedViewSourceAdmission", TlcCiShard.MaterialisedViewSourceAdmission, 26)]
    [TestCase("MaterialisedViewResolvedIdentity", TlcCiShard.MaterialisedViewResolvedIdentity, 20)]
    public void Measured_view_modules_route_every_case_to_a_nonempty_deterministic_shard(
        string name, string category, int expectedCases)
    {
        var module = Module(name);
        var cases = new[] { TlcCiShard.OfModule(module) }
            .Concat(module.Manifest.Variants.Keys.Select(variant => TlcCiShard.OfVariant(module, variant)))
            .Concat(module.LoadMutations().Select(mutation => TlcCiShard.Of(module, mutation)))
            .ToArray();
        Assert.Multiple(() =>
        {
            Assert.That(cases, Has.Length.EqualTo(expectedCases));
            Assert.That(cases, Is.All.EqualTo(category));
            Assert.That(LatticeShards().Single(shard => shard.Categories.Contains(category)).Tiers,
                Is.EqualTo(new[] { "deterministic" }));
        });
    }

    [Test]
    public void Split_gate_cycle_runs_route_every_case_to_one_measured_deterministic_shard()
    {
        var module = Module("SplitGateCycles");
        var cases = new[] { TlcCiShard.OfModule(module) }
            .Concat(module.Manifest.Variants.Keys.Select(variant => TlcCiShard.OfVariant(module, variant)))
            .Concat(module.LoadMutations().Select(mutation => TlcCiShard.Of(module, mutation)))
            .ToArray();
        Assert.Multiple(() =>
        {
            Assert.That(cases, Has.Length.EqualTo(39));
            Assert.That(cases, Is.All.EqualTo(TlcCiShard.SplitGate));
            Assert.That(LatticeShards().Single(shard => shard.Categories.Contains(TlcCiShard.SplitGate)).Tiers,
                Is.EqualTo(new[] { "deterministic" }));
            Assert.That(TlcCiShard.OfModule(Module("SplitLinkTurns")), Is.Null,
                "the companion model is small enough to stay in the catch-all");
        });
    }

    [Test]
    public void Anti_entropy_runs_are_partitioned_into_three_measured_nonempty_deterministic_groups()
    {
        var module = Module("AntiEntropy");
        var cases = new[] { TlcCiShard.OfModule(module)! }
            .Concat(module.Manifest.Variants.Keys.Select(variant => TlcCiShard.OfVariant(module, variant)!))
            .Concat(module.LoadMutations().Select(mutation => TlcCiShard.Of(module, mutation)!))
            .GroupBy(category => category, StringComparer.Ordinal)
            .ToDictionary(group => group.Key, group => group.Count(), StringComparer.Ordinal);
        Assert.Multiple(() =>
        {
            Assert.That(cases, Has.Count.EqualTo(3));
            Assert.That(cases[TlcCiShard.AntiEntropy], Is.EqualTo(18));
            Assert.That(cases[TlcCiShard.AntiEntropy2], Is.EqualTo(9));
            Assert.That(cases[TlcCiShard.AntiEntropy3], Is.EqualTo(7));
            Assert.That(TlcCiShard.OfVariant(module, "Alias"), Is.EqualTo(TlcCiShard.AntiEntropy2));
            foreach (var category in cases.Keys)
                Assert.That(LatticeShards().Single(shard => shard.Categories.Contains(category)).Tiers,
                    Is.EqualTo(new[] { "deterministic" }));
        });
    }

    [Test]
    public void Schema_modules_route_every_case_to_the_dedicated_schema_shard_with_their_base()
    {
        var modules = SpecModuleCatalogue.Repository()
            .Where(module => module.Name.StartsWith("Schema", StringComparison.Ordinal))
            .ToList();

        Assert.Multiple(() =>
        {
            Assert.That(modules.Select(module => module.Name),
                Is.SupersetOf(new[] { "SchemaPolicyConfig", "SchemaStageOrdering" }));
            Assert.That(modules.Select(module => Path.GetFileName(module.Directory)).Distinct(),
                Is.EqualTo(new[] { "schema" }), "every Schema* module lives in spec/schema/");
            foreach (var module in modules)
            {
                Assert.That(module.Manifest.Variants, Is.Not.Empty, $"{module.Name} needs a larger-bound variant");
                foreach (var mutation in module.LoadMutations())
                {
                    Assert.That(TlcCiShard.Of(module, mutation), Is.EqualTo(TlcCiShard.Schema), $"{module.Name}/{mutation.Name}");
                    Assert.That(TlcCiShard.SharesBaseShard(module, mutation), Is.True,
                        "the base must run in the mutations' process to serve their full-bound controls");
                }
            }
        });
    }

    [Test]
    public void Arguments_are_validated()
    {
        var module = Module("Replication");
        Assert.Multiple(() =>
        {
            Assert.Throws<ArgumentNullException>(() => TlcCiShard.Of(null!, module.LoadMutations()[0]));
            Assert.Throws<ArgumentNullException>(() => TlcCiShard.Of(module, null!));
            Assert.Throws<ArgumentNullException>(() => TlcCiShard.OfVariant(null!, "v"));
            Assert.Throws<ArgumentNullException>(() => TlcCiShard.OfVariant(module, null!));
            Assert.Throws<ArgumentNullException>(() => TlcCiShard.Tag(null!));
            Assert.Throws<ArgumentNullException>(() => TlcCiShard.ModuleCategory(null!));
        });
    }

    private static SpecModule Module(string name) =>
        SpecModuleCatalogue.Repository().Single(module => module.Name == name);

    /// <summary>
    /// The <c>TlcShard*</c> routing categories of a case. The per-module
    /// <c>TlcModule_*</c> family is deliberately left out: it routes nothing, so
    /// every shard guard must ignore it.
    /// </summary>
    private static IEnumerable<string> Categories(TestCaseData data) =>
        AllCategories(data).Where(category => category.StartsWith("TlcShard", StringComparison.Ordinal));

    private static IEnumerable<string> ModuleCategories(TestCaseData data) =>
        AllCategories(data).Where(category => category.StartsWith(TlcCiShard.ModuleCategoryPrefix, StringComparison.Ordinal));

    private static IEnumerable<string> AllCategories(TestCaseData data) =>
        data.Properties.ContainsKey(PropertyNames.Category)
            ? data.Properties[PropertyNames.Category].Cast<string>()
            : [];

    private static List<(string Name, bool Tlc, bool TlcSmoke, List<string> Categories, List<string> Includes, List<string> Tiers)> LatticeShards()
    {
        var path = Path.Combine(HygieneRepository.FindRepoRoot(), ".github", "workflows", "test-shards.json");
        using var document = JsonDocument.Parse(File.ReadAllText(path));
        return document.RootElement.GetProperty("lattice").GetProperty("shards").EnumerateArray()
            .Select(shard => (
                shard.GetProperty("shard").GetString()!,
                shard.TryGetProperty("tlc", out var tlc) && tlc.GetBoolean(),
                shard.TryGetProperty("tlcSmoke", out var smoke) && smoke.GetBoolean(),
                shard.TryGetProperty("includeCategories", out var categories)
                    ? categories.EnumerateArray().Select(c => c.GetString()!).ToList()
                    : [],
                shard.TryGetProperty("include", out var includes)
                    ? includes.EnumerateArray().Select(c => c.GetString()!).ToList()
                    : [],
                shard.TryGetProperty("tiers", out var tiers)
                    ? tiers.EnumerateArray().Select(c => c.GetString()!).ToList()
                    : []))
            .ToList();
    }
}
