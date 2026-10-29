using System.Text;
using NUnit.Framework;

namespace Orleans.Lattice.Tests.Formal;

/// <summary>
/// Gates that every detector a refinement note cites is a test CI executes
/// (#5154), and reports a per-module census of those detectors by tier.
/// <para>
/// WHAT THIS ADDS TO <see cref="RefinementDetectorMappingTests"/>. That fixture
/// proves each cited <c>Fixture.Method</c> exists and carries an NUnit test
/// attribute. Existing is not running: an <c>[Ignore]</c> or <c>[Explicit]</c> on
/// the method, on any partial declaration of its fixture or inherited from a
/// base fixture, or a category set no CI lane selects, leaves the row reading
/// as detected while nothing ever executes the detector. This fixture closes
/// that gap and stays out of the existence question, which the other fixture
/// owns: a citation that does not resolve is skipped here, not double-reported.
/// </para>
/// <para>
/// "A CI LANE" MEANS THE PLANNER'S FULL PLAN. The lanes are derived by running
/// <c>plan-test-matrix.py</c> over the packages CI selects, with no tier
/// skipped, and evaluating its filters (see
/// <see cref="RefinementDetectorRunnabilityLanes"/>). A member pull request
/// skips the Coyote and chaos tiers by policy, so a detector those tiers run
/// is runnable but not run on every pull request; the census reports such rows
/// and does not fail them.
/// </para>
/// </summary>
[TestFixture]
internal sealed partial class RefinementDetectorRunnabilityTests
{
    /// <summary>A cited test, and how to describe the citation in a failure.</summary>
    /// <param name="Fixture">The fixture's simple name.</param>
    /// <param name="Method">The test method's name.</param>
    /// <param name="Citation">Where the citation is, for messages.</param>
    internal sealed record CitedDetector(string Fixture, string Method, string Citation);

    /// <summary>The census tiers, in report order.</summary>
    internal static readonly string[] Tiers = ["deterministic", "Coyote", "Chaos", "AzureStorageEmulator"];

    [TestCaseSource(typeof(SpecModuleCases), nameof(SpecModuleCases.Modules))]
    public void Every_cited_detector_is_a_test_CI_executes(SpecModule module)
    {
        ArgumentNullException.ThrowIfNull(module);

        var cited = CitedDetectors(module);
        Assert.That(
            cited,
            Is.Not.Empty,
            "No detector references were extracted at all, which means this gate is checking nothing.");

        var resolver = RefinementDetectorResolver.ForRepository();
        var lanes = RefinementDetectorRunnabilityLanes.ForRepository();
        var problems = Problems(cited, RefinementDetectorRunnabilityIndex.ForRepository(), lanes, resolver.TestExists, out var examined);

        if (lanes is null)
        {
            Assert.Warn("No Python interpreter is available on this host, so the planner's lanes were not derived and "
                + "only [Ignore] and [Explicit] were checked. CI checks categories as well.");
        }

        Assert.Multiple(() =>
        {
            Assert.That(
                examined,
                Is.GreaterThan(0),
                "No cited detector resolved, so no run state was examined. Every_test_named_as_a_detector_exists "
                + "reports why; this gate proves nothing until it does.");

            Assert.That(
                problems,
                Is.Empty,
                module.Describe(module.RefinementNotePath) + Environment.NewLine
                + string.Join(Environment.NewLine + Environment.NewLine, problems));
        });
    }

    [Test]
    public void The_census_of_cited_detectors_by_tier_is_reported()
    {
        // Reports, and fails only when it has nothing to report. Rows detected
        // only by Chaos or Coyote tests are allowed (those tiers run on fully
        // gated runs), but they are listed so the reader can see them.
        var index = RefinementDetectorRunnabilityIndex.ForRepository();
        var resolver = RefinementDetectorResolver.ForRepository();
        var report = new StringBuilder();
        var repository = new Dictionary<string, string>(StringComparer.Ordinal);

        report.AppendLine("Refinement detector census (#5154): cited detectors by tier, per module.");
        report.AppendLine("A detector cited by several rows of one module counts once for that module.");
        report.AppendLine(Line("Module", "Detectors", Tiers, "Rows detected only by Chaos or Coyote"));

        foreach (var module in SpecModuleCatalogue.Repository())
        {
            var tiers = new Dictionary<string, string>(StringComparer.Ordinal);
            var rowTiers = new Dictionary<string, List<string>>(StringComparer.Ordinal);

            foreach (var detector in RefinementCodeSymbols.ExtractDetectors(BehaviourTables(module)))
            {
                if (!resolver.TestExists(detector.TypeName, detector.MemberName)
                    || index.Find(detector.TypeName, detector.MemberName) is not [var state])
                {
                    continue;
                }

                var tier = TierOf(state.Categories);
                tiers[detector.Text] = tier;
                repository[detector.Text] = tier;
                if (!rowTiers.TryGetValue(detector.Row, out var list))
                {
                    rowTiers[detector.Row] = list = [];
                }

                list.Add(tier);
            }

            var faultOnly = rowTiers
                .Where(row => row.Value.All(tier => tier is "Chaos" or "Coyote"))
                .Select(row => row.Key)
                .ToArray();

            Assert.That(tiers, Is.Not.Empty, $"{module.Name}: no cited detector resolved, so the census for it is empty.");
            report.AppendLine(Line(
                module.Name,
                tiers.Count.ToString(System.Globalization.CultureInfo.InvariantCulture),
                Tiers.Select(t => tiers.Values.Count(v => v == t).ToString(System.Globalization.CultureInfo.InvariantCulture)).ToArray(),
                faultOnly.Length == 0 ? "-" : string.Join(", ", faultOnly)));
        }

        report.AppendLine(Line(
            "Repository (distinct)",
            repository.Count.ToString(System.Globalization.CultureInfo.InvariantCulture),
            Tiers.Select(t => repository.Values.Count(v => v == t).ToString(System.Globalization.CultureInfo.InvariantCulture)).ToArray(),
            string.Empty));

        TestContext.Out.WriteLine(report.ToString());

        Assert.That(repository, Is.Not.Empty, "the census found no cited detector at all");
        Assert.That(repository.Values.Distinct(), Is.SubsetOf(Tiers));
    }

    /// <summary>
    /// The census tier of a test with these categories. Chaos and Coyote are
    /// the planner's fault-injection tiers; AzureStorageEmulator marks a test
    /// that needs Azurite, which every CI leg provisions.
    /// </summary>
    internal static string TierOf(IReadOnlySet<string> categories)
    {
        ArgumentNullException.ThrowIfNull(categories);

        foreach (var tier in new[] { "Chaos", "Coyote", "AzureStorageEmulator" })
        {
            if (categories.Contains(tier))
            {
                return tier;
            }
        }

        return "deterministic";
    }

    /// <summary>
    /// The run-state problems of <paramref name="cited"/>. A citation the
    /// resolver cannot find is skipped (the mapping gate owns it); every other
    /// one is counted in <paramref name="examined"/> and must be declared exactly
    /// once, carry no disqualifier, live in a package CI selects, and have a
    /// case some lane of that package selects. With no lanes, the last two are
    /// not checked.
    /// </summary>
    internal static List<string> Problems(
        IEnumerable<CitedDetector> cited,
        RefinementDetectorRunnabilityIndex index,
        RefinementDetectorRunnabilityLanes? lanes,
        Func<string, string, bool> resolves,
        out int examined)
    {
        ArgumentNullException.ThrowIfNull(cited);
        ArgumentNullException.ThrowIfNull(index);
        ArgumentNullException.ThrowIfNull(resolves);

        var problems = new List<string>();
        var seen = new HashSet<string>(StringComparer.Ordinal);
        examined = 0;

        foreach (var detector in cited)
        {
            var name = $"{detector.Fixture}.{detector.Method}";
            if (!seen.Add(name) || !resolves(detector.Fixture, detector.Method))
            {
                continue;
            }

            examined++;
            var states = index.Find(detector.Fixture, detector.Method);
            if (states.Count == 0)
            {
                problems.Add($"{detector.Citation}: '{name}' resolves as a test, but the runnability index finds no "
                    + "NUnit-attributed declaration of it, so whether it runs cannot be checked.");
                continue;
            }

            if (states.Count > 1)
            {
                problems.Add($"{detector.Citation}: '{name}' is declared by {states.Count} fixtures "
                    + $"({string.Join(", ", states.Select(s => s.FullName))}), so the citation does not name one test.");
                continue;
            }

            var state = states[0];
            if (state.Disqualifiers.Count > 0)
            {
                problems.Add($"{detector.Citation}: '{name}' never runs in CI: {string.Join("; ", state.Disqualifiers)}.");
                continue;
            }

            if (lanes is null)
            {
                continue;
            }

            if (!lanes.Packages.Contains(state.Package))
            {
                problems.Add($"{detector.Citation}: '{name}' lives in test/{state.Package}, which is not a package CI "
                    + "selects (a src/ directory or a TEST_ONLY_PACKAGES entry of select-test-packages.sh), so no "
                    + "planned lane runs it.");
                continue;
            }

            if (!state.CaseCategories.Any(categories => lanes.Selecting(state.Package, state.FullName, categories).Count > 0))
            {
                var categories = state.Categories.Count == 0 ? "no categories" : "categories " + string.Join(", ", state.Categories.Order(StringComparer.Ordinal));
                problems.Add($"{detector.Citation}: '{name}' carries {categories}, which no CI lane selects: every lane "
                    + $"plan-test-matrix.py plans for test/{state.Package} filters it out.");
            }
        }

        return problems;
    }

    private static IReadOnlyList<RefinementTable> BehaviourTables(SpecModule module)
    {
        var tables = RefinementNote.ReadTables(module);
        return [tables[RefinementNote.ActionSection], tables[RefinementNote.PropertySection]];
    }

    private static List<CitedDetector> CitedDetectors(SpecModule module) =>
        RefinementCodeSymbols.ExtractDetectors(BehaviourTables(module))
            .Select(d => new CitedDetector(
                d.TypeName,
                d.MemberName,
                $"{module.Describe(module.RefinementNotePath)} line {d.LineNumber} (row {d.Row}) cites '{d.Text}'"))
            .ToList();

    private static string Line(string module, string detectors, IReadOnlyList<string> tiers, string rows) =>
        $"{module,-40} {detectors,9} {tiers[0],13} {tiers[1],6} {tiers[2],5} {tiers[3],20}  {rows}".TrimEnd();
}
