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
public sealed class TlcCiShardTests
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
        var routed = SpecModuleCases.Mutations()
            .Concat(SpecModuleCases.Variants())
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
    public void Only_an_AtomicCommit_or_a_replication_companion_variant_leaves_the_catch_all()
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
                    var expected = module.Name.StartsWith("Replication", StringComparison.Ordinal)
                        ? TlcCiShard.ReBootstrap
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

        Assert.Multiple(() =>
        {
            Assert.That(Categories(mutation), Is.EqualTo(new[] { TlcCiShard.ReBootstrap }));
            Assert.That(TlcCiShard.AtomicVariantShards.Contains(Categories(variant).Single()), Is.True);
            Assert.That(Categories(baseCase), Is.Empty);
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
        });
    }

    private static SpecModule Module(string name) =>
        SpecModuleCatalogue.Repository().Single(module => module.Name == name);

    private static IEnumerable<string> Categories(TestCaseData data) =>
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
