using NUnit.Framework;

namespace Orleans.Lattice.Tests.Formal;

/// <summary>
/// The negative controls and building-block checks of the runnability gate:
/// a synthetic <c>test/</c> tree whose fixtures each carry one disqualifying
/// attribute must be reported, and its clean fixtures must not be.
/// </summary>
internal sealed partial class RefinementDetectorRunnabilityTests
{
    private const string ProbePackage = "probe";

    private const string OrphanPackage = "orphan";

    private const string ExcludedCategory = "Excluded";

    private static readonly Dictionary<string, string> SyntheticFiles = new(StringComparer.Ordinal)
    {
        [$"{ProbePackage}/Probes.cs"] =
            """
            namespace Synthetic.Probes;

            public class CleanFixture
            {
                private sealed class Helper { }

                [Test]
                public void Runs() { }
            }

            public class MethodIgnored
            {
                [Test, Ignore("flaky")]
                public void Skipped() { }
            }

            public class MethodExplicit
            {
                [Test]
                [NUnit.Framework.ExplicitAttribute("local only")]
                public void Skipped() { }
            }

            [Explicit("local only")]
            public class FixtureExplicit
            {
                [Test]
                public void Skipped() { }
            }

            [TestFixture(Ignore = "broken")]
            public class FixtureNamedIgnore
            {
                [Test]
                public void Skipped() { }
            }

            public class MethodCategory
            {
                [Test, Category("Excluded")]
                public void Skipped() { }
            }

            [TestFixture(Category = "Fine,Excluded")]
            public class FixtureNamedCategory
            {
                [Test]
                public void Skipped() { }
            }

            public class Rows
            {
                [TestCase(1, Explicit = true)]
                [TestCase(2, Ignore = "broken")]
                public void All_rows_out(int value) { }

                [TestCase(1, IgnoreReason = "broken")]
                [TestCase(2)]
                public void One_row_runs(int value) { }

                [TestCase(1, Category = "Excluded")]
                public void Row_excluded(int value) { }
            }

            public class HelperOnly
            {
                public void Not_a_test() { }
            }
            """,
        [$"{ProbePackage}/Bases.cs"] =
            """
            namespace Synthetic.Probes;

            [Category("Excluded")]
            public abstract class ExcludedBase { }

            public sealed class InheritsCategory : ExcludedBase
            {
                [Test]
                public void Skipped() { }
            }

            [Explicit]
            public abstract class ExplicitBase { }

            public sealed class InheritsExplicit : ExplicitBase
            {
                [Test]
                public void Maybe() { }
            }

            [Ignore("broken")]
            public abstract class IgnoredBase { }

            public sealed class InheritsIgnore : IgnoredBase
            {
                [Test]
                public void Maybe() { }
            }
            """,
        [$"{ProbePackage}/PartialA.cs"] =
            """
            namespace Synthetic.Probes
            {
                [Ignore("broken")]
                public partial class PartialIgnored { }
            }
            """,
        [$"{ProbePackage}/PartialB.cs"] =
            """
            namespace Synthetic.Probes
            {
                public partial class PartialIgnored
                {
                    [Test]
                    public void Skipped() { }
                }
            }
            """,
        [$"{OrphanPackage}/Orphan.cs"] =
            """
            namespace Synthetic.Orphans;

            public class OrphanFixture
            {
                [Test]
                public void Runs() { }
            }
            """,
    };

    private static readonly Lazy<(string Root, RefinementDetectorRunnabilityIndex Index)> Synthetic = new(BuildSynthetic);

    /// <summary>The probe package's only lane drops the excluded category.</summary>
    private static RefinementDetectorRunnabilityLanes SyntheticLanes() =>
        new([new RefinementDetectorRunnabilityLanes.Lane(ProbePackage, "probe", "all", $"TestCategory!={ExcludedCategory}")]);

    private static (string Root, RefinementDetectorRunnabilityIndex Index) BuildSynthetic()
    {
        var root = Path.Combine(Path.GetTempPath(), "detector-runnability-" + Guid.NewGuid().ToString("N"), "test");
        foreach (var (relative, text) in SyntheticFiles)
        {
            var path = Path.Combine(root, relative.Replace('/', Path.DirectorySeparatorChar));
            Directory.CreateDirectory(Path.GetDirectoryName(path)!);
            File.WriteAllText(path, text);
        }

        return (root, new RefinementDetectorRunnabilityIndex(root));
    }

    [OneTimeTearDown]
    public void Delete_the_synthetic_tree()
    {
        if (Synthetic.IsValueCreated)
        {
            var parent = Path.GetDirectoryName(Synthetic.Value.Root)!;
            if (Directory.Exists(parent))
            {
                Directory.Delete(parent, recursive: true);
            }
        }
    }

    private static List<string> SyntheticProblems(string fixture, string method) =>
        Problems([new CitedDetector(fixture, method, "synthetic note")], Synthetic.Value.Index, SyntheticLanes(), (_, _) => true, out _);

    /// <summary>One disqualifying construct, and a fragment its report must contain.</summary>
    public static IEnumerable<TestCaseData> Disqualified() =>
    [
        new TestCaseData("MethodIgnored", "Skipped", "[Ignore] on the method").SetName("Method_level_Ignore_is_reported"),
        new TestCaseData("MethodExplicit", "Skipped", "[Explicit] on the method").SetName("Method_level_Explicit_is_reported"),
        new TestCaseData("FixtureExplicit", "Skipped", "[Explicit] on fixture FixtureExplicit").SetName("Fixture_level_Explicit_is_reported"),
        new TestCaseData("FixtureNamedIgnore", "Skipped", "Ignore = \"broken\" in [TestFixture]").SetName("TestFixture_Ignore_argument_is_reported"),
        new TestCaseData("PartialIgnored", "Skipped", "[Ignore] on fixture PartialIgnored (test/probe/PartialA.cs)").SetName("Ignore_on_another_partial_declaration_is_reported"),
        new TestCaseData("MethodCategory", "Skipped", "no CI lane selects").SetName("Method_category_no_lane_selects_is_reported"),
        new TestCaseData("FixtureNamedCategory", "Skipped", "no CI lane selects").SetName("TestFixture_category_no_lane_selects_is_reported"),
        new TestCaseData("InheritsCategory", "Skipped", "no CI lane selects").SetName("Category_inherited_from_a_base_fixture_is_reported"),
        new TestCaseData("Rows", "All_rows_out", "every one of its 2 [TestCase] row(s)").SetName("Every_TestCase_row_ignored_or_explicit_is_reported"),
        new TestCaseData("Rows", "Row_excluded", "no CI lane selects").SetName("TestCase_row_category_no_lane_selects_is_reported"),
        new TestCaseData("OrphanFixture", "Runs", "not a package CI selects").SetName("A_test_in_a_package_no_lane_plans_is_reported"),
        new TestCaseData("HelperOnly", "Not_a_test", "finds no NUnit-attributed declaration").SetName("A_citation_the_index_cannot_find_is_reported"),
    ];

    [TestCaseSource(nameof(Disqualified))]
    public void Negative_control(string fixture, string method, string expected)
    {
        Assert.That(SyntheticProblems(fixture, method), Has.Some.Contains(expected));
    }

    [TestCase("CleanFixture", "Runs")]
    [TestCase("Rows", "One_row_runs")]
    public void Control_a_runnable_test_is_not_reported(string fixture, string method)
    {
        Assert.That(SyntheticProblems(fixture, method), Is.Empty);
    }

    [Test]
    public void Base_fixture_Ignore_and_Explicit_follow_NUnit_attribute_inheritance()
    {
        // NUnit applies a base fixture's attributes only where the attribute
        // type declares itself inherited, so the expectation is read from
        // NUnit's own types rather than assumed.
        static bool Inherited(Type type) =>
            type.GetCustomAttributes(typeof(AttributeUsageAttribute), inherit: true).OfType<AttributeUsageAttribute>().Single().Inherited;

        Assert.Multiple(() =>
        {
            Assert.That(
                SyntheticProblems("InheritsExplicit", "Maybe").Any(p => p.Contains("[Explicit] on base fixture ExplicitBase", StringComparison.Ordinal)),
                Is.EqualTo(Inherited(typeof(ExplicitAttribute))));
            Assert.That(
                SyntheticProblems("InheritsIgnore", "Maybe").Any(p => p.Contains("[Ignore] on base fixture IgnoredBase", StringComparison.Ordinal)),
                Is.EqualTo(Inherited(typeof(IgnoreAttribute))));
        });
    }

    [Test]
    public void The_index_records_the_full_name_package_and_merged_categories()
    {
        var state = Synthetic.Value.Index.Find("FixtureNamedCategory", "Skipped").Single();

        Assert.Multiple(() =>
        {
            Assert.That(state.Package, Is.EqualTo(ProbePackage));
            Assert.That(state.FullName, Is.EqualTo("Synthetic.Probes.FixtureNamedCategory.Skipped"));
            Assert.That(state.Categories, Is.EquivalentTo(new[] { "Fine", ExcludedCategory }));
            Assert.That(state.Sources, Is.EqualTo(new[] { "test/probe/Probes.cs" }));
        });
    }

    [Test]
    public void A_runnable_and_an_explicit_test_in_the_repository_are_told_apart()
    {
        // Real fixtures, across partial files: BPlusLeafGrainTests is declared in
        // many files, and only its ReadCost partial carries the explicit test.
        var index = RefinementDetectorRunnabilityIndex.ForRepository();
        var self = index.Find(nameof(RefinementDetectorRunnabilityTests), nameof(A_runnable_and_an_explicit_test_in_the_repository_are_told_apart)).Single();
        var explicitTest = index.Find("BPlusLeafGrainTests", "Measure_raw_and_versioned_read_costs").Single();

        Assert.Multiple(() =>
        {
            Assert.That(self.Disqualifiers, Is.Empty);
            Assert.That(self.Package, Is.EqualTo("lattice"));
            Assert.That(self.FullName, Is.EqualTo(typeof(RefinementDetectorRunnabilityTests).FullName + "." + nameof(A_runnable_and_an_explicit_test_in_the_repository_are_told_apart)));
            Assert.That(explicitTest.Disqualifiers, Has.Some.Contains("[Explicit] on the method"));
            Assert.That(explicitTest.Categories, Does.Contain("Performance"));
        });
    }

    [Test]
    public void The_planner_lanes_cover_every_tier_of_the_core_package()
    {
        var lanes = RefinementDetectorRunnabilityLanes.ForRepository();
        if (lanes is null)
        {
            Assert.Ignore("no Python interpreter is available on this host");
        }

        const string FullName = "Orleans.Lattice.Tests.Formal.Probe.Runs";
        var none = new HashSet<string>(StringComparer.Ordinal);

        Assert.Multiple(() =>
        {
            Assert.That(lanes!.Packages, Does.Contain("lattice").And.Contain("lattice.integration"));
            Assert.That(lanes.Packages, Does.Not.Contain("lattice.explorer.uitests"));
            Assert.That(lanes.Selecting("lattice", FullName, none).Select(l => l.Tier), Does.Contain("deterministic"));
            Assert.That(lanes.Selecting("lattice", FullName, new HashSet<string>(["Coyote"])).Select(l => l.Tier), Does.Contain("coyote"));
            Assert.That(lanes.Selecting("lattice", FullName, new HashSet<string>(["Chaos"])).Select(l => l.Tier), Does.Contain("chaos"));
            Assert.That(lanes.Selecting("no-such-package", FullName, none), Is.Empty);
        });
    }

    [Test]
    public void A_matrix_with_a_skipped_item_is_not_a_full_plan()
    {
        const string Matrix =
            """
            [ { "items": [
              { "package": "p", "label": "p / s (deterministic)", "tier": "deterministic", "filter": "TestCategory!=Chaos" },
              { "package": "p", "label": "p / s (chaos)", "tier": "chaos", "filter": "TestCategory=Chaos", "skip": "policy" }
            ] } ]
            """;

        Assert.That(() => RefinementDetectorRunnabilityLanes.ParseMatrix(Matrix), Throws.InvalidOperationException.With.Message.Contains("p / s (chaos)"));
        Assert.That(
            RefinementDetectorRunnabilityLanes.ParseMatrix(Matrix.Replace(", \"skip\": \"policy\"", string.Empty, StringComparison.Ordinal)).Select(l => l.Tier),
            Is.EqualTo(new[] { "deterministic", "chaos" }));
    }

    [TestCase("TestCategory=A", "X.Y.M", "A", true)]
    [TestCase("TestCategory!=A", "X.Y.M", "A", false)]
    [TestCase("TestCategory!=A", "X.Y.M", "A,B", false)]
    [TestCase("FullyQualifiedName~X.Y", "X.Y.M(a)", "", true)]
    [TestCase("FullyQualifiedName!~X.Y", "X.Y.M(a)", "", false)]
    [TestCase("(TestCategory=A|TestCategory=B)&FullyQualifiedName!~Q", "X.Y.M", "B", true)]
    [TestCase("(FullyQualifiedName~X&TestCategory!=A)&(TestCategory!=Chaos&TestCategory!=Coyote)", "X.Y.M", "Coyote", false)]
    [TestCase("TestCategory=A|TestCategory=B&TestCategory=C", "X", "A", true)]
    [TestCase("TestCategory=A|TestCategory=B&TestCategory=C", "X", "B", false)]
    public void The_filter_evaluator_reads_vstest_filters(string filter, string fullName, string categories, bool expected)
    {
        var set = categories.Split(',', StringSplitOptions.RemoveEmptyEntries).ToHashSet(StringComparer.Ordinal);

        Assert.That(RefinementDetectorRunnabilityFilter.Compile(filter)(fullName, set), Is.EqualTo(expected));
    }

    [TestCase("TestCategory")]
    [TestCase("Priority=1")]
    [TestCase("(TestCategory=A")]
    [TestCase("TestCategory=A&")]
    public void The_filter_evaluator_refuses_what_it_does_not_model(string filter)
    {
        Assert.That(() => RefinementDetectorRunnabilityFilter.Compile(filter), Throws.TypeOf<FormatException>());
    }

    [TestCase("Chaos,AzureStorageEmulator", "Chaos")]
    [TestCase("Coyote", "Coyote")]
    [TestCase("AzureStorageEmulator,Integration", "AzureStorageEmulator")]
    [TestCase("Integration", "deterministic")]
    [TestCase("", "deterministic")]
    public void The_census_tier_is_read_from_the_categories(string categories, string expected)
    {
        Assert.That(
            TierOf(categories.Split(',', StringSplitOptions.RemoveEmptyEntries).ToHashSet(StringComparer.Ordinal)),
            Is.EqualTo(expected));
    }
}
