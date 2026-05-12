using System.Reflection;

namespace Orleans.Lattice.Replication.Grpc.Tests;

/// <summary>
/// Regression: every <c>[TestFixture]</c> that exercises an out-of-process
/// dependency - an Orleans <see cref="Orleans.TestingHost.TestCluster"/>, an
/// ASP.NET Core <c>TestServer</c>, a <c>Microsoft.Extensions.Hosting.IHost</c>,
/// a <c>Grpc.Net.Client.GrpcChannel</c>, or any user-defined
/// <c>*ClusterFixture</c> helper - must carry one of the slow-category tags
/// (<c>Integration</c>, <c>Chaos</c>, or <c>AzureTableEmulator</c>) at the
/// fixture level so it is excluded from the Tier 2 fast dev loop.
/// <para>
/// See <c>.github/instructions/testing.instructions.md</c> section
/// "Categorization conventions". Sibling copies of this fixture live in
/// every test project that contains cluster-based fixtures so the gate
/// runs against each assembly independently.
/// </para>
/// </summary>
[TestFixture]
public class IntegrationCategoryHygieneTests
{
    private static readonly HashSet<string> SlowCategories = new(StringComparer.Ordinal)
    {
        "Integration",
        "Chaos",
        "AzureTableEmulator",
    };

    private static readonly HashSet<string> IntegrationBearingTypeNames = new(StringComparer.Ordinal)
    {
        "Orleans.TestingHost.TestCluster",
        "Microsoft.AspNetCore.TestHost.TestServer",
        "Microsoft.Extensions.Hosting.IHost",
        "Grpc.Net.Client.GrpcChannel",
    };

    /// <summary>
    /// Walks every loaded <c>[TestFixture]</c> in this assembly, decides
    /// whether it is a cluster-based fixture using only field / property
    /// type signals, and fails if a detected fixture lacks one of the
    /// slow-category tags.
    /// </summary>
    [Test]
    public void Every_cluster_based_fixture_carries_a_slow_category()
    {
        var assembly = Assembly.GetExecutingAssembly();

        var fixtures = SafeGetTypes(assembly)
            .Where(t => t.IsClass && !t.IsAbstract)
            .Where(HasTestFixtureAttribute)
            .OrderBy(t => t.FullName, StringComparer.Ordinal)
            .ToList();

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
            + "[Category(\"AzureTableEmulator\")] for emulator-dependent suites) above the "
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
