using System.Reflection;
using NUnit.Framework;

namespace Orleans.Lattice.Testing.Hygiene;

/// <summary>
/// Regression: every <c>[TestFixture]</c> that exercises an out-of-process
/// dependency - an Orleans <c>Orleans.TestingHost.TestCluster</c>, an
/// ASP.NET Core <c>TestServer</c>, a <c>Microsoft.Extensions.Hosting.IHost</c>,
/// a <c>Grpc.Net.Client.GrpcChannel</c>, or any user-defined
/// <c>*ClusterFixture</c> helper - must carry one of the slow-category tags
/// (<c>Integration</c>, <c>Chaos</c>, or <c>AzureStorageEmulator</c>) at the
/// fixture level so it is excluded from the Tier 2 fast dev loop.
/// <para>
/// Without the tag, a single such fixture re-introduces silo startup latency
/// into the inner dev loop and silently inflates Tier 2 from seconds to
/// minutes. The detection signals are deliberately narrow (field / property
/// types only, no name-based heuristics) so this gate flags real cluster
/// fixtures without false positives on unit tests that happen to be
/// <c>*IntegrationTests</c> by name.
/// </para>
/// <para>
/// The scan targets <see cref="object.GetType"/>'s assembly, so a concrete
/// subclass in each test project runs the gate against that project's own
/// assembly. See <c>.github/instructions/testing.instructions.md</c> section
/// "Categorization conventions".
/// </para>
/// </summary>
public abstract class IntegrationCategoryHygieneTestsBase
{
    // NUnit categories that exclude a fixture from the Tier 2 fast dev loop.
    // A cluster-based fixture must carry at least one of these so it stays
    // out of the fast loop.
    private static readonly HashSet<string> SlowCategories = new(StringComparer.Ordinal)
    {
        "Integration",
        "Chaos",
        "AzureStorageEmulator",
    };

    // Full type names whose presence as an instance field or property on a
    // fixture indicates the fixture spins up an out-of-process or host-level
    // dependency. Matched by FullName so this test does not require a
    // compile-time reference to every assembly listed here - the type only
    // needs to be loaded in the test AppDomain, which it will be if some
    // fixture in the assembly uses it.
    private static readonly HashSet<string> IntegrationBearingTypeNames = new(StringComparer.Ordinal)
    {
        "Orleans.TestingHost.TestCluster",
        "Microsoft.AspNetCore.TestHost.TestServer",
        "Microsoft.Extensions.Hosting.IHost",
        "Grpc.Net.Client.GrpcChannel",
    };

    /// <summary>
    /// Walks every loaded <c>[TestFixture]</c> in the consuming test
    /// assembly, decides whether it is a cluster-based fixture using only
    /// field / property type signals, and fails if a detected fixture lacks
    /// one of the slow-category tags. The failure message lists every
    /// offending fixture's <see cref="Type.FullName"/> and the categories it
    /// already carries so the fix is mechanical.
    /// </summary>
    [Test]
    public void Every_cluster_based_fixture_carries_a_slow_category()
    {
        var assembly = GetType().Assembly;

        var allTypes = SafeGetTypes(assembly).ToList();

        var fixtures = allTypes
            .Where(t => t.IsClass && !t.IsAbstract)
            .Where(HasTestFixtureAttribute)
            .OrderBy(t => t.FullName, StringComparer.Ordinal)
            .ToList();

        // Anti-vacuity control (issue #2275), in two parts, because the
        // obvious one is worthless here.
        //
        // NOT ASSERTED: fixtures.Count > 0. This fixture is itself a
        // [TestFixture] in the assembly it reflects over, so that predicate is
        // TRUE BY CONSTRUCTION and could not have come out differently. It
        // would read exactly like a control while proving nothing - the defect
        // this whole control exists to prevent.
        //
        // (1) The real vacuity path is SafeGetTypes degrading: it swallows
        // ReflectionTypeLoadException and returns only the types that did
        // load, so a load failure shrinks the population silently and the gate
        // reports clean over whatever survived.
        HygieneDenominator.RequireExamined(
            allTypes.Count, nameof(IntegrationCategoryHygieneTestsBase), "loaded types", assembly.FullName ?? assembly.ToString());

        // (2) End-to-end self-detection: the running fixture must find ITSELF
        // through the same reflection and attribute-reading path it uses to
        // find everything else. Unlike a count, this can genuinely fail - if
        // HasTestFixtureAttribute or the type walk breaks, the gate stops
        // detecting fixtures and this is what says so.
        Assert.That(fixtures, Does.Contain(GetType()),
            $"HYGIENE GATE VACUOUS: '{nameof(IntegrationCategoryHygieneTestsBase)}' did not detect its own "
            + $"fixture type '{GetType().FullName}' while reflecting over {assembly.GetName().Name}, so its "
            + "fixture-discovery path is broken and its clean report means nothing. "
            + $"It found {fixtures.Count} fixture(s) among {allTypes.Count} loaded type(s).");

        var violations = new List<string>();
        foreach (var fixture in fixtures)
        {
            if (!IsClusterBasedFixture(fixture)) continue;

            var categories = GetCategoryNames(fixture);
            if (!categories.Overlaps(SlowCategories))
            {
                var existing = categories.Count == 0
                    ? "<none>"
                    : string.Join(", ", categories.OrderBy(c => c, StringComparer.Ordinal));
                violations.Add($"{fixture.FullName}: cluster-based fixture (detected via field/property type) "
                    + $"is missing a slow-category tag. Existing categories: [{existing}].");
            }
        }

        Assert.That(violations, Is.Empty,
            "Cluster-based test fixtures must declare a slow-category tag at the fixture level "
            + "so they are excluded from the Tier 2 dev-loop filter. Add "
            + "[Category(\"Integration\")] (or [Category(\"Chaos\")] for stress suites, or "
            + "[Category(\"AzureStorageEmulator\")] for emulator-dependent suites) above the "
            + "[TestFixture] declaration. See .github/instructions/testing.instructions.md "
            + "section 'Categorization conventions'."
            + Environment.NewLine
            + string.Join(Environment.NewLine, violations));
    }

    private static bool IsClusterBasedFixture(Type fixture)
    {
        var flags = BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.DeclaredOnly;
        for (var t = fixture; t is not null && t != typeof(object); t = t.BaseType)
        {
            foreach (var f in t.GetFields(flags))
                if (IsIntegrationBearingType(f.FieldType)) return true;
            foreach (var p in t.GetProperties(flags))
                if (IsIntegrationBearingType(p.PropertyType)) return true;
        }
        return false;
    }

    private static bool IsIntegrationBearingType(Type type)
    {
        if (type.FullName is { } name && IntegrationBearingTypeNames.Contains(name))
            return true;

        // Any user-defined helper whose simple name ends in "ClusterFixture" -
        // covers the EventStream / FaultInjection / FourShard / SmallLeaf /
        // MultiPageFourShard / MutationObserver / PublishEventsOverride /
        // TwoSite / PublicApiContract / PublicReplicationApi cluster fixtures
        // and any future siblings.
        return type.Name.EndsWith("ClusterFixture", StringComparison.Ordinal);
    }

    private static bool HasTestFixtureAttribute(Type type) =>
        type.GetCustomAttributes<TestFixtureAttribute>(inherit: true).Any();

    private static HashSet<string> GetCategoryNames(Type type) =>
        type.GetCustomAttributes<CategoryAttribute>(inherit: true)
            .Select(a => a.Name)
            .ToHashSet(StringComparer.Ordinal);

    private static IEnumerable<Type> SafeGetTypes(Assembly assembly)
    {
        try { return assembly.GetTypes(); }
        catch (ReflectionTypeLoadException ex) { return ex.Types.Where(t => t is not null)!; }
    }
}
