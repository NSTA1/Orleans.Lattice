using System.Reflection;

namespace Orleans.Lattice.Explorer.UiTests.Journeys;

/// <summary>
/// Hygiene gate: every fixture that drives the journey web head must live in this
/// namespace.
/// <para>
/// <b>Why this is load-bearing.</b> NUnit scopes a <c>[SetUpFixture]</c> to its own
/// namespace and the namespaces beneath it, so
/// <see cref="JourneyAppHostSetup"/> starts the journey head only for fixtures declared
/// here. A fixture that derives from <see cref="JourneyTestBase"/> but is declared in
/// the parent namespace compiles, is discovered, and then fails every one of its tests
/// in set-up with "the journey web head is not running" - a failure that reads like a
/// broken host rather than a misplaced file, and which cost real time to diagnose once
/// already. This gate turns that into one clear failure naming the offending type.
/// </para>
/// <para>
/// A vacuity guard fails if the scan finds no journey fixtures at all, so the gate can
/// never pass by scanning an empty set.
/// </para>
/// </summary>
[TestFixture]
[Category("UI")]
[Category("Integration")]
public sealed class JourneyNamespaceHygieneTests
{
    private static readonly string RequiredNamespace = typeof(JourneyTestBase).Namespace!;

    /// <summary>
    /// Fails any concrete <see cref="JourneyTestBase"/> fixture declared outside the
    /// namespace whose set-up fixture starts the head it depends on.
    /// </summary>
    [Test]
    public void Every_journey_fixture_lives_in_the_namespace_that_starts_its_host()
    {
        var fixtures = JourneyFixtures().ToList();

        Assert.That(fixtures, Is.Not.Empty,
            "The journey-fixture scan found no concrete subclasses of "
            + nameof(JourneyTestBase)
            + ", so this gate would pass without checking anything. Either the journey suite was "
            + "removed or the scan is no longer targeting the UI-test assembly.");

        var misplaced = fixtures
            .Where(t => !string.Equals(t.Namespace, RequiredNamespace, StringComparison.Ordinal))
            .Select(t => $"{t.FullName}: declared in '{t.Namespace ?? "<global>"}'")
            .ToList();

        Assert.That(misplaced, Is.Empty,
            $"Every journey fixture must be declared in '{RequiredNamespace}', because that is the "
            + "namespace " + nameof(JourneyAppHostSetup) + " scopes its one-time set-up to. A "
            + "fixture declared elsewhere runs with no journey head and fails in set-up."
            + Environment.NewLine
            + string.Join(Environment.NewLine, misplaced));
    }

    /// <summary>
    /// Every journey fixture must also carry <c>Integration</c> alongside the <c>UI</c>
    /// category the sibling gate enforces, so it stays out of the strict-delta lanes
    /// that run no browser.
    /// </summary>
    [Test]
    public void Every_journey_fixture_carries_the_integration_category()
    {
        var fixtures = JourneyFixtures().ToList();

        Assert.That(fixtures, Is.Not.Empty,
            "The journey-fixture scan found nothing, so this gate would pass vacuously.");

        var missing = fixtures
            .Where(t => !Categories(t).Contains("Integration"))
            .Select(t => $"{t.FullName}: [{string.Join(", ", Categories(t).OrderBy(c => c, StringComparer.Ordinal))}]")
            .ToList();

        Assert.That(missing, Is.Empty,
            "Every journey fixture must declare [Category(\"Integration\")] as well as "
            + "[Category(\"UI\")]."
            + Environment.NewLine
            + string.Join(Environment.NewLine, missing));
    }

    private static IEnumerable<Type> JourneyFixtures() =>
        SafeGetTypes(typeof(JourneyNamespaceHygieneTests).Assembly)
            .Where(t => t.IsClass && !t.IsAbstract && typeof(JourneyTestBase).IsAssignableFrom(t))
            .OrderBy(t => t.FullName, StringComparer.Ordinal);

    private static HashSet<string> Categories(Type type) =>
        type.GetCustomAttributes<CategoryAttribute>(inherit: true)
            .Select(a => a.Name)
            .ToHashSet(StringComparer.Ordinal);

    private static IEnumerable<Type> SafeGetTypes(Assembly assembly)
    {
        try { return assembly.GetTypes(); }
        catch (ReflectionTypeLoadException ex) { return ex.Types.Where(t => t is not null)!; }
    }
}
