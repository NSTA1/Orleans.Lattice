using System.IO;
using System.Reflection;
using System.Text.RegularExpressions;
using Orleans.Lattice.Testing.Docs;
using Orleans.Lattice.Testing.Hygiene;

namespace Orleans.Lattice.Tests.Hygiene;

/// <summary>
/// The content-hygiene lane in <c>ci.yml</c> must actually reach the gates it
/// claims to run, and the required check must actually observe it.
/// <para>
/// The em-dash, mojibake, deletion-mandate and docs-snippet gates are ordinary
/// NUnit fixtures, so they run only when something runs their project. A change
/// that touches no compiled source skips the test matrix entirely, which is why
/// <c>build-and-test</c> could report green having executed no content gate
/// whatsoever (#2443). The <c>hygiene</c> job closes that hole by running those
/// fixtures for the projects that own the changed files.
/// </para>
/// <para>
/// That job selects tests with a single <c>--filter</c> expression written in
/// YAML, which no compiler checks and no test exercises. A gate whose name the
/// filter does not match would not fail anything - it would simply never run on
/// that path, reinstating the bug one level up. This fixture evaluates the
/// filter the way <c>dotnet test</c> would, against every scoped gate that
/// actually exists in the repository, so the two cannot drift apart unnoticed.
/// </para>
/// <para>
/// The scan is deliberately repository-wide rather than reflective over this
/// assembly. Reflection would prove the filter reaches the three gates the core
/// project happens to declare, when the filter's real job is to reach the same
/// gates in the other twenty-odd test projects - where, as it turns out, they
/// are declared in different namespaces.
/// </para>
/// </summary>
[TestFixture]
public sealed class CiContentHygieneLaneTests
{
    private const string WorkflowPath = ".github/workflows/ci.yml";

    /// <summary>The lane's <c>--filter</c> expression, as assigned in the job.</summary>
    private static readonly Regex FilterAssignment = new(
        @"^\s*filter='(?<filter>[^']*)'\s*$",
        RegexOptions.Compiled | RegexOptions.Multiline);

    /// <summary>One <c>FullyQualifiedName~</c> clause of a vstest filter.</summary>
    private static readonly Regex FullyQualifiedNameClause = new(
        @"FullyQualifiedName~(?<needle>[^|&]+)",
        RegexOptions.Compiled);

    private static readonly Regex FileNamespace = new(
        @"(?m)^namespace\s+(?<ns>[\w\.]+)\s*;",
        RegexOptions.Compiled);

    [Test]
    public void The_workflow_declares_a_content_hygiene_job()
    {
        var workflow = ReadWorkflow();

        Assert.Multiple(() =>
        {
            Assert.That(
                workflow,
                Does.Match(@"(?m)^  hygiene:\s*$"),
                $"{WorkflowPath} must declare the `hygiene` job that runs the content gates "
                + "for changes which skip the test matrix (#2443).");

            Assert.That(
                workflow,
                Does.Match(@"(?m)^      hygiene_projects:"),
                $"{WorkflowPath} must export the `hygiene_projects` plan output; without it the "
                + "lane has nothing to run and its `if:` condition is never satisfied.");
        });
    }

    /// <summary>
    /// The lane is only a gate if the required check fails when it fails. The
    /// verdict reads every upstream result explicitly rather than relying on job
    /// ordering, so the lane must appear in both halves: the <c>needs</c> list
    /// that makes the result available, and the loop that inspects it.
    /// </summary>
    [Test]
    public void The_required_check_observes_the_content_hygiene_job()
    {
        var workflow = ReadWorkflow();

        Assert.Multiple(() =>
        {
            Assert.That(
                workflow,
                Does.Match(@"(?m)^    needs: \[[^\]]*\bhygiene\b[^\]]*\]"),
                "`build-and-test` must list `hygiene` in its `needs`, or the lane's result is "
                + "invisible to the required check and a failing gate merges.");

            Assert.That(
                workflow,
                Does.Contain("\"hygiene:$HYGIENE\""),
                "the verdict step must inspect the hygiene job's result. Adding it to `needs` "
                + "alone gates nothing: `if: always()` means the verdict runs whatever the "
                + "upstream jobs did, and only this loop turns a failure into a red check.");
        });
    }

    /// <summary>
    /// Every scoped content gate in the repository must be selectable by the
    /// lane's filter. This is the anti-drift half: the filter is YAML, the
    /// fixtures are C#, and nothing else compares them.
    /// </summary>
    [Test]
    public void The_lane_filter_selects_every_scoped_content_gate()
    {
        var needles = FilterNeedles();
        var fixtures = ScopedFixtures();

        // Anti-vacuity control, in the same spirit as the HygieneDenominator the
        // gates themselves carry: a scan that matched nothing would pass this
        // test while asserting nothing whatsoever about the filter.
        Assert.That(
            fixtures,
            Has.Count.GreaterThanOrEqualTo(40),
            "expected to find the scoped content-gate fixtures across the test projects. "
            + "Finding far fewer means the source scan no longer recognises them and this "
            + "test is checking nothing.");

        var unreachable = fixtures
            .Where(fixture => !needles.Any(needle =>
                fixture.Contains(needle, StringComparison.Ordinal)))
            .OrderBy(name => name, StringComparer.Ordinal)
            .ToArray();

        Assert.That(
            unreachable,
            Is.Empty,
            "these content-gate fixtures are not selected by the `hygiene` lane's --filter in "
            + $"{WorkflowPath}, so they never run on a change that skips the test matrix - "
            + "which is the exact hole that job exists to close. Either rename the fixture to "
            + "match the filter, or widen the filter."
            + Environment.NewLine
            + $"Filter clauses: {string.Join(", ", needles)}"
            + Environment.NewLine
            + string.Join(Environment.NewLine, unreachable));
    }

    /// <summary>
    /// The filter matches on class name, so that convention is load-bearing:
    /// every concrete gate is its abstract base with the <c>Base</c> suffix
    /// dropped. State it directly, rather than leaving it as an unwritten
    /// assumption the filter silently depends on.
    /// </summary>
    [Test]
    public void Every_scoped_gate_is_named_after_its_base_and_declared_widely()
    {
        var byGate = ScopedFixtures()
            .GroupBy(name => name[(name.LastIndexOf('.') + 1)..], StringComparer.Ordinal)
            .ToDictionary(group => group.Key, group => group.Count(), StringComparer.Ordinal);

        Assert.Multiple(() =>
        {
            Assert.That(
                byGate.Keys.Order(StringComparer.Ordinal),
                Is.EquivalentTo(ScopedBaseNames()
                    .Select(baseName => baseName[..^"Base".Length])
                    .Order(StringComparer.Ordinal)),
                "every abstract scoped gate in the shared testing library should have concrete "
                + "fixtures named after it, and nothing else should be found. A mismatch means "
                + "either a gate nothing implements, or a naming convention this scan has lost "
                + "track of - and the filter's correctness rests on that convention.");

            Assert.That(
                byGate.Where(entry => entry.Value < 2).Select(entry => entry.Key),
                Is.Empty,
                "expected each gate to be declared in several test projects; the whole point "
                + "of the filter is that it selects them in projects other than this one.");
        });
    }

    /// <summary>
    /// The filter's <c>FullyQualifiedName~</c> needles, read from the workflow.
    /// </summary>
    private static string[] FilterNeedles()
    {
        var assignment = FilterAssignment.Match(ReadWorkflow());

        Assert.That(
            assignment.Success,
            Is.True,
            $"expected the hygiene lane in {WorkflowPath} to assign its test filter as "
            + "`filter='...'` on its own line. This fixture reads that assignment; spelling it "
            + "another way makes the filter unverifiable rather than wrong.");

        var needles = FullyQualifiedNameClause
            .Matches(assignment.Groups["filter"].Value)
            .Select(match => match.Groups["needle"].Value.Trim())
            .ToArray();

        Assert.That(needles, Is.Not.Empty, "expected at least one FullyQualifiedName clause");
        return needles;
    }

    /// <summary>
    /// The abstract gates in the shared testing library that bind a scan scope -
    /// that is, every gate whose reach is partitioned across test projects and
    /// which therefore has to be run somewhere for the files it owns. Derived by
    /// reflection rather than listed, so a gate added to the library is covered
    /// the day it is added.
    /// </summary>
    private static string[] ScopedBaseNames()
    {
        var names = typeof(HygieneScanScope).Assembly
            .GetTypes()
            .Where(type => type is { IsAbstract: true, IsClass: true })
            .Where(type => type.Name.EndsWith("Base", StringComparison.Ordinal))
            .Where(DeclaresAScanScope)
            .Select(type => type.Name)
            .ToArray();

        Assert.That(
            names,
            Has.Length.GreaterThanOrEqualTo(4),
            "expected the shared testing library to declare the scoped content gates "
            + "(em-dash, mojibake, deletion mandate, docs snippets).");

        return names;
    }

    private static bool DeclaresAScanScope(Type type)
    {
        var scope = type.GetProperty(
            "Scope",
            BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.Public
            | BindingFlags.DeclaredOnly);

        return scope is not null
            && scope.GetMethod is { IsAbstract: true }
            && (scope.PropertyType == typeof(HygieneScanScope)
                || scope.PropertyType == typeof(DocsSnippetScope));
    }

    /// <summary>
    /// The fully-qualified names of every concrete scoped gate across all test
    /// projects, read from source. The lane runs <c>dotnet test</c> against
    /// other projects' assemblies, which this one does not reference, so source
    /// is the only place the whole set is visible.
    /// </summary>
    private static List<string> ScopedFixtures()
    {
        var declaration = new Regex(
            @"class\s+(?<name>\w+)\s*:\s*(?<base>" + string.Join('|', ScopedBaseNames()) + @")\b",
            RegexOptions.Compiled);

        var testRoot = Path.Combine(HygieneRepository.FindRepoRoot(), "test");
        var fixtures = new List<string>();

        foreach (var file in HygieneRepository.EnumerateFiles(testRoot, "*.cs"))
        {
            var text = File.ReadAllText(file);
            var match = declaration.Match(text);
            if (!match.Success) continue;

            var ns = FileNamespace.Match(text);
            Assert.That(
                ns.Success,
                Is.True,
                $"expected a file-scoped namespace in {file}, which declares a scoped gate");

            fixtures.Add($"{ns.Groups["ns"].Value}.{match.Groups["name"].Value}");
        }

        return fixtures;
    }

    private static string ReadWorkflow()
    {
        var path = Path.Combine(
            HygieneRepository.FindRepoRoot(),
            WorkflowPath.Replace('/', Path.DirectorySeparatorChar));

        Assert.That(File.Exists(path), Is.True, $"expected {WorkflowPath}");
        return File.ReadAllText(path);
    }
}
