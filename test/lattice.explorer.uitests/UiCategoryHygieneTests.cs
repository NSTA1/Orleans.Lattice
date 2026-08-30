using System.Reflection;

namespace Orleans.Lattice.Explorer.UiTests;

/// <summary>
/// Hygiene gate: every <c>[TestFixture]</c> in this assembly must carry
/// <c>[Category("UI")]</c>.
/// <para>
/// The UI category is load-bearing. Browser tests are excluded from every default
/// test filter by category alone (mirroring the <c>Chaos</c> / <c>Coyote</c> /
/// <c>AzureStorageEmulator</c> opt-in tiers), and this project has no <c>src/</c>
/// counterpart, so the core CI matrix never runs it - only the dedicated Explorer
/// UI workflow does. An untagged fixture would therefore leak into lanes that have no
/// browser installed and fail there rather than in the UI workflow. This gate is the
/// last line of defence against that.
/// </para>
/// <para>
/// A vacuity guard fails if the scan finds no fixtures at all, so the gate can never
/// pass by accidentally scanning an empty assembly.
/// </para>
/// </summary>
[TestFixture]
[Category("UI")]
public sealed class UiCategoryHygieneTests
{
    private const string UiCategory = "UI";

    /// <summary>
    /// Walks every concrete <c>[TestFixture]</c> in this assembly and fails if any
    /// lacks the <c>UI</c> category. The failure message lists each offender's full
    /// type name and the categories it already carries, so the fix is mechanical.
    /// </summary>
    [Test]
    public void Every_fixture_carries_the_UI_category()
    {
        var fixtures = FixtureTypes().ToList();

        // Vacuity guard: prove the scan actually found fixtures, so a future refactor
        // that empties or renames the assembly cannot let this gate pass silently.
        Assert.That(fixtures, Is.Not.Empty,
            "The UI-category hygiene scan found no [TestFixture] types in this assembly. "
            + "Either the assembly is empty (a build or reference regression) or the scan is "
            + "no longer targeting the UI-test assembly.");

        var violations = new List<string>();
        foreach (var fixture in fixtures)
        {
            var categories = GetCategoryNames(fixture);
            if (!categories.Contains(UiCategory))
            {
                var existing = categories.Count == 0
                    ? "<none>"
                    : string.Join(", ", categories.OrderBy(c => c, StringComparer.Ordinal));
                violations.Add($"{fixture.FullName}: missing [Category(\"{UiCategory}\")]. "
                    + $"Existing categories: [{existing}].");
            }
        }

        Assert.That(violations, Is.Empty,
            "Every [TestFixture] in the UI-test project must declare [Category(\"UI\")] at the "
            + "fixture level so browser tests stay out of every browser-free default filter. Add "
            + "[Category(\"UI\")] above the [TestFixture] declaration."
            + Environment.NewLine
            + string.Join(Environment.NewLine, violations));
    }

    private IEnumerable<Type> FixtureTypes() =>
        SafeGetTypes(GetType().Assembly)
            .Where(t => t.IsClass && !t.IsAbstract)
            .Where(HasTestFixtureAttribute)
            .OrderBy(t => t.FullName, StringComparer.Ordinal);

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
