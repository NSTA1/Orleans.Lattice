using System.IO;
using System.Text.RegularExpressions;
using Orleans.Lattice.Explorer.Core.Navigation;
using Orleans.Lattice.Explorer.Core.Session;
using Orleans.Lattice.Testing.Hygiene;

namespace Orleans.Lattice.Explorer.Tests;

/// <summary>
/// The epic-wide lower-case decision, made unbreakable: every route the Explorer
/// declares, every segment and query key it emits, and every preference key it
/// stores is canonical lower case.
/// </summary>
/// <remarks>
/// <para>
/// A URL is shared, bookmarked, typed and logged. A shell that answers
/// <c>/explore/trees</c> but not <c>/Explore/Trees</c> is a shell whose links
/// break depending on who typed them, and the failure is invisible to the author
/// who introduced it - their own link works. Convention alone does not survive
/// that, so it is asserted here instead.
/// </para>
/// <para>
/// Three things are checked, plus the scanner's own detection:
/// </para>
/// <list type="number">
/// <item>No <c>@page</c> directive in the Explorer declares an upper-case literal
/// segment.</item>
/// <item>Every constant on <see cref="ExplorerRouteSegments"/> - the reserved
/// route vocabulary - is canonical.</item>
/// <item>Every declared preference key name is canonical, so stored state obeys
/// the same one spelling rule as the URL.</item>
/// <item>The scanner detects what it claims to, so a change that neuters the
/// pattern fails here rather than silently passing the gates above.</item>
/// </list>
/// </remarks>
[TestFixture]
public sealed class RouteCaseHygieneTests
{
    private const string ExplorerSourceRoot = "src/lattice.explorer";

    private static readonly Regex PageDirective = new(
        @"^\s*@page\s+""(?<template>[^""]*)""",
        RegexOptions.Compiled | RegexOptions.Multiline);

    [Test]
    public void Every_declared_page_route_is_lower_case()
    {
        var repoRoot = HygieneRepository.FindRepoRoot();

        var violations = new List<string>();
        var scanned = 0;
        var templates = 0;

        foreach (var file in EnumerateRazorFiles(repoRoot))
        {
            scanned++;
            var text = File.ReadAllText(file);
            foreach (Match match in PageDirective.Matches(text))
            {
                templates++;
                var template = match.Groups["template"].Value;
                foreach (var segment in NonCanonicalSegments(template))
                {
                    violations.Add($"{Relative(repoRoot, file)}: @page \"{template}\" has segment '{segment}'");
                }
            }
        }

        Assert.Multiple(() =>
        {
            Assert.That(scanned, Is.GreaterThan(0), "the scan found no Razor files, so it proved nothing");
            Assert.That(templates, Is.GreaterThan(0), "the scan found no @page routes, so it proved nothing");
            Assert.That(
                violations,
                Is.Empty,
                "Every Explorer route segment is lower case (epic #1845): /explore/trees/orders/data, "
                + "never /Explore/Trees. A link that works only for the person who typed it is not a link.\n"
                + string.Join('\n', violations));
        });
    }

    [Test]
    public void Every_reserved_route_segment_constant_is_canonical()
    {
        var constants = typeof(ExplorerRouteSegments)
            .GetFields(System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Static)
            .Where(static field => field.IsLiteral && field.FieldType == typeof(string))
            .ToArray();

        Assert.Multiple(() =>
        {
            Assert.That(constants, Is.Not.Empty, "the reserved vocabulary must not be empty");

            foreach (var field in constants)
            {
                var value = (string?)field.GetRawConstantValue();
                Assert.That(
                    ExplorerRouteSlug.IsCanonical(value),
                    Is.True,
                    $"ExplorerRouteSegments.{field.Name} is '{value}', which is not canonical lower case");
            }
        });
    }

    [Test]
    public void Every_declared_preference_key_is_canonical()
    {
        // Stored state and the URL share one spelling rule, so a key added to the
        // contract cannot drift into a spelling the route grammar would reject.
        Assert.Multiple(() =>
        {
            Assert.That(ExplorerPreferenceKeys.All, Is.Not.Empty);

            foreach (var key in new ExplorerPreferenceCatalog().Keys)
            {
                Assert.That(
                    ExplorerRouteSlug.IsCanonical(key.Name),
                    Is.True,
                    $"preference key '{key.Name}' is not canonical lower case");
            }
        });
    }

    [Test]
    public void Scanner_detects_an_upper_case_segment_it_is_shown()
    {
        // The smoke detector's own battery test: if this stops failing on a
        // planted violation, the gates above are passing vacuously.
        Assert.Multiple(() =>
        {
            Assert.That(NonCanonicalSegments("/Explore/trees"), Is.EqualTo(new[] { "Explore" }));
            Assert.That(NonCanonicalSegments("/explore/TagIndexes"), Is.EqualTo(new[] { "TagIndexes" }));
            Assert.That(NonCanonicalSegments("/explore/trees"), Is.Empty);
            Assert.That(NonCanonicalSegments("/"), Is.Empty);

            // Route parameters and catch-alls are declarations, not spellings a
            // user ever types, so they are out of scope by construction.
            Assert.That(NonCanonicalSegments("/{*shellPath}"), Is.Empty);
            Assert.That(NonCanonicalSegments("/explore/{Kind}"), Is.Empty);
        });
    }

    [Test]
    public void Scanner_finds_the_pages_this_issue_declared()
    {
        var repoRoot = HygieneRepository.FindRepoRoot();

        var templates = new List<string>();
        foreach (var file in EnumerateRazorFiles(repoRoot))
        {
            foreach (Match match in PageDirective.Matches(File.ReadAllText(file)))
            {
                templates.Add(match.Groups["template"].Value);
            }
        }

        Assert.Multiple(() =>
        {
            Assert.That(templates, Does.Contain("/"), "the bare address must stay routable");
            Assert.That(
                templates,
                Does.Contain("/{*shellPath}"),
                "the shell's catch-all is what makes every view addressable");
            Assert.That(
                templates,
                Does.Contain("/reset-view"),
                "the reset-view escape must stay reachable by address");
        });
    }

    private static IEnumerable<string> NonCanonicalSegments(string template)
    {
        foreach (var segment in template.Split('/', StringSplitOptions.RemoveEmptyEntries))
        {
            if (segment.StartsWith('{'))
            {
                continue;
            }

            if (!ExplorerRouteSlug.IsCanonical(segment))
            {
                yield return segment;
            }
        }
    }

    private static IEnumerable<string> EnumerateRazorFiles(string repoRoot)
    {
        var root = Path.Combine(repoRoot, ExplorerSourceRoot.Replace('/', Path.DirectorySeparatorChar));
        return Directory.Exists(root)
            ? Directory.EnumerateFiles(root, "*.razor", SearchOption.AllDirectories)
            : [];
    }

    private static string Relative(string repoRoot, string file) =>
        Path.GetRelativePath(repoRoot, file).Replace('\\', '/');
}
