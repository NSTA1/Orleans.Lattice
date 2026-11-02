using System.Reflection;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests;

/// <summary>
/// Hygiene gate: every <c>[TestFixture]</c> in this assembly carries <c>[Category("EndToEnd")]</c>,
/// which is how the journeys lane selects it; and a fixture that drives a browser
/// (<c>UI</c>) also carries <c>AzureStorageEmulator</c> and the reverse, because every browser
/// journey runs on an estate and every estate fixture is a journey. A vacuity guard fails if
/// the scan finds no fixture.
/// </summary>
[TestFixture]
[Category("EndToEnd")]
public sealed class EndToEndCategoryHygieneTests
{
    /// <summary>Walks every concrete fixture and reports each one that breaks the rule.</summary>
    [Test]
    public void Every_fixture_carries_the_end_to_end_categories()
    {
        var fixtures = GetType().Assembly.GetTypes()
            .Where(type => type is { IsClass: true, IsAbstract: false } && type.GetCustomAttributes<TestFixtureAttribute>(inherit: true).Any())
            .OrderBy(type => type.FullName, StringComparer.Ordinal)
            .ToList();
        Assert.That(fixtures, Has.Count.GreaterThanOrEqualTo(5), "the scan found too few fixtures; it is not reading this assembly");

        var violations = new List<string>();
        foreach (var fixture in fixtures)
        {
            var categories = fixture.GetCustomAttributes<CategoryAttribute>(inherit: true).Select(category => category.Name).ToHashSet(StringComparer.Ordinal);
            if (!categories.Contains("EndToEnd"))
            {
                violations.Add($"{fixture.FullName}: missing [Category(\"EndToEnd\")].");
            }

            if (categories.Contains("UI") != categories.Contains("AzureStorageEmulator"))
            {
                violations.Add($"{fixture.FullName}: carries one of UI and AzureStorageEmulator without the other.");
            }
        }

        Assert.That(violations, Is.Empty, string.Join(Environment.NewLine, violations));
    }
}
