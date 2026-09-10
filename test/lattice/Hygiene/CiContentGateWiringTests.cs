using System.Text.RegularExpressions;
using Orleans.Lattice.Testing.Hygiene;

namespace Orleans.Lattice.Tests.Hygiene;

/// <summary>
/// The content-gate wiring gate: the job that runs the repository's text gates
/// must be unskippable, must be required, and must actually select every gate
/// fixture that exists.
/// <para>
/// Three families of gate police TEXT rather than code - the formal refinement
/// fixtures under <c>test/lattice/Formal/</c> (which read <c>spec/*.md</c>),
/// the hygiene fixtures under <c>test/&lt;pkg&gt;/Hygiene/</c> (em-dash,
/// mojibake, and siblings, which scan every tracked text file), and the
/// documentation snippet compilations under <c>test/&lt;pkg&gt;/Docs/</c>.
/// Every one of them is an ordinary NUnit test, and nothing in
/// <c>ci.yml</c> ran any of them on a markdown-only pull request: the paths
/// filter excluded <c>**/*.md</c>, so the whole test matrix was skipped and the
/// required check still reported success. The em-dash gate exists to catch
/// prose pasted from a word processor, which lands in markdown, so its miss
/// rate on its own target population was total.
/// </para>
/// <para>
/// The fix is a <c>content-gates</c> job carrying no condition at all. This
/// fixture guards the three ways that fix can quietly rot:
/// </para>
/// <list type="number">
/// <item>A condition (<c>if:</c> or <c>needs:</c>) gets added to the job, so it
/// can be SKIPPED. Actions treats a skipped dependency as non-blocking, so the
/// required check would go green having run nothing - the original defect,
/// reintroduced one level up.</item>
/// <item>The required check stops depending on it, or starts accepting
/// <c>skipped</c> from it, which has the same effect by a different route.</item>
/// <item>A gate fixture is added, renamed, or moved so that the job's
/// <c>--filter</c> no longer selects it. That failure is silent: the job stays
/// green while enforcing less than it claims.</item>
/// </list>
/// <para>
/// The third assertion is the one that needs the machinery. It derives the
/// population from the filesystem - every <c>[TestFixture]</c> under a gate
/// directory - rather than from a hand-maintained list, so a new fixture joins
/// the denominator automatically instead of having to be remembered. That is
/// the same reasoning as <c>HygieneDenominator.RequireExamined</c>, applied one
/// level up: a vacuity control inside a test cannot defend against the test not
/// being selected.
/// </para>
/// <para>
/// The runtime half of the guard lives in <c>run-text-gates.py</c>, which fails
/// the job when the run executed no tests, or no tests for one of the three
/// families. Between them, a gate can neither fall outside the filter nor be
/// selected but never run.
/// </para>
/// </summary>
[TestFixture]
public sealed class CiContentGateWiringTests
{
    private const string WorkflowPath = ".github/workflows/ci.yml";
    private const string ContentGateJob = "content-gates";
    private const string RequiredCheckJob = "build-and-test";

    /// <summary>The directories whose fixtures the content gate must select.</summary>
    private static readonly string[] GateDirectories = ["Formal", "Hygiene", "Docs"];

    /// <summary>
    /// A class-level attribute run immediately followed by a concrete class
    /// declaration. Abstract bases (which live in the shared testing library and
    /// are never selected directly) do not match.
    /// </summary>
    private static readonly Regex FixtureDeclaration = new(
        @"(?<attributes>(?:^[ \t]*\[[^\]]*\][ \t]*\r?\n)+)[ \t]*(?:public|internal)[ \t]+"
        + @"(?:sealed[ \t]+)?(?:partial[ \t]+)?class[ \t]+(?<name>\w+)",
        RegexOptions.Compiled | RegexOptions.Multiline);

    private static readonly Regex FileNamespace = new(
        @"^namespace\s+(?<name>[^;\s]+)\s*;",
        RegexOptions.Compiled | RegexOptions.Multiline);

    private static readonly Regex CategoryAttribute = new(
        @"\[Category\(""(?<name>[^""]+)""\)\]",
        RegexOptions.Compiled);

    [Test]
    public void The_content_gate_job_carries_no_condition_of_any_kind()
    {
        var job = JobBlock(ContentGateJob);

        // Job-level keys sit at four spaces. Step-level `if:` (eight spaces) is
        // fine and is used by the artifact upload, so the indent is part of the
        // assertion rather than an accident of it.
        var conditions = Regex
            .Matches(job, @"^    (?<key>if|needs):.*$", RegexOptions.Multiline)
            .Select(match => match.Value.Trim())
            .ToArray();

        Assert.That(
            conditions,
            Is.Empty,
            $"the '{ContentGateJob}' job in {WorkflowPath} must run on every pull request without "
            + "exception. A job with an 'if:' or a 'needs:' can be SKIPPED - by its own condition, or "
            + "because an upstream job failed or was skipped - and Actions does not treat a skipped "
            + "dependency as blocking. That is precisely the defect this job exists to close: a required "
            + "check reporting green because the gates that would have failed never ran."
            + Environment.NewLine
            + string.Join(Environment.NewLine, conditions));
    }

    [Test]
    public void The_required_check_depends_on_the_content_gate()
    {
        var job = JobBlock(RequiredCheckJob);
        var needs = Regex.Match(job, @"^    needs:\s*(?<value>.+)$", RegexOptions.Multiline);

        Assert.That(needs.Success, Is.True, $"expected a 'needs:' on '{RequiredCheckJob}' in {WorkflowPath}");
        Assert.That(
            needs.Groups["value"].Value,
            Does.Contain(ContentGateJob),
            $"'{RequiredCheckJob}' is the required status check, so it must depend on '{ContentGateJob}'. "
            + "Without the dependency the gate job's result is not read at all and a failing gate cannot "
            + "turn the required check red.");
    }

    [Test]
    public void The_required_check_refuses_a_skipped_content_gate()
    {
        var job = JobBlock(RequiredCheckJob);

        var arm = Regex.Match(
            job,
            @"case\s+""\$CONTENT_GATES""\s+in(?<body>[\s\S]*?)esac",
            RegexOptions.Multiline);

        Assert.That(
            arm.Success,
            Is.True,
            $"'{RequiredCheckJob}' must inspect the '{ContentGateJob}' result explicitly. Folding it into "
            + "the loop that accepts 'success|skipped' for the other jobs would let a skipped gate pass.");

        var body = arm.Groups["body"].Value;

        Assert.That(
            Regex.IsMatch(body, @"^\s*success\)\s*;;\s*$", RegexOptions.Multiline),
            Is.True,
            "expected a bare 'success)' arm: success is the only acceptable result from the content gate.");

        Assert.That(
            Regex.IsMatch(body, @"^\s*[^)\r\n]*\bskipped\b[^)\r\n]*\)\s*;;\s*$", RegexOptions.Multiline),
            Is.False,
            $"'{RequiredCheckJob}' must not accept 'skipped' from '{ContentGateJob}'. A skipped gate job "
            + "verified nothing, and a required check that goes green on it is the defect being fixed.");

        Assert.That(
            Regex.IsMatch(body, @"skipped\)[\s\S]*?ok=false"),
            Is.True,
            "expected the 'skipped' result to fail the verdict, named explicitly so the error says what "
            + "actually went wrong.");
    }

    [Test]
    public void The_declared_filter_is_the_one_the_job_runs()
    {
        var job = JobBlock(ContentGateJob);

        Assert.That(
            Filter(job),
            Is.Not.Empty,
            $"expected a CONTENT_GATE_FILTER on the '{ContentGateJob}' job in {WorkflowPath}");

        Assert.That(
            job,
            Does.Contain("$CONTENT_GATE_FILTER"),
            "the filter this fixture reads must be the filter the job actually passes to the test runner; "
            + "a second, divergent copy would make every assertion here decorative.");
    }

    [Test]
    public void Every_declared_content_gate_fixture_is_selected_by_the_filter()
    {
        var filter = Filter(JobBlock(ContentGateJob));

        var selectors = Regex
            .Matches(filter, @"FullyQualifiedName~(?<term>[\w.]+)")
            .Select(match => match.Groups["term"].Value)
            .ToArray();

        var excludedCategories = Regex
            .Matches(filter, @"Category!=(?<name>\w+)")
            .Select(match => match.Groups["name"].Value)
            .ToArray();

        Assert.That(selectors, Is.Not.Empty, $"could not parse any selector out of '{filter}'");

        var fixtures = DiscoverFixtures();

        // Non-vacuity, in two directions. An empty population would make the
        // assertion below pass while proving nothing, and so would a population
        // that had silently lost a whole family (a renamed directory, a project
        // dropped from the solution).
        Assert.That(fixtures, Is.Not.Empty, "the fixture scan reached no gate directories at all");

        foreach (var directory in GateDirectories)
        {
            Assert.That(
                fixtures.Any(fixture => fixture.Directory == directory),
                Is.True,
                $"found no [TestFixture] under any test/*/{directory}/ directory. Either the family moved "
                + "or the scan is broken; either way this fixture would be asserting less than it claims.");
        }

        var unselected = fixtures
            .Where(fixture => !fixture.Categories.Intersect(excludedCategories, StringComparer.Ordinal).Any())
            .Where(fixture => !selectors.Any(selector =>
                fixture.FullName.Contains(selector, StringComparison.Ordinal)))
            .Select(fixture => $"{fixture.Path}: {fixture.FullName}")
            .OrderBy(entry => entry, StringComparer.Ordinal)
            .ToArray();

        Assert.That(
            unselected,
            Is.Empty,
            $"these fixtures live in a content-gate directory but no selector in '{filter}' matches them, "
            + "so CI builds them and never runs them. That is a silent hole: the gate stays green while "
            + "enforcing less than it claims. Either put the fixture in a namespace naming its family "
            + "(for example '...Tests.Hygiene'), or widen the filter."
            + Environment.NewLine
            + string.Join(Environment.NewLine, unselected));
    }

    /// <summary>
    /// Every concrete <c>[TestFixture]</c> declared under a gate directory of a
    /// test project, excluding the shared testing library whose types are
    /// abstract bases rather than runnable fixtures.
    /// </summary>
    private static IReadOnlyList<GateFixture> DiscoverFixtures()
    {
        var testRoot = Path.Combine(HygieneRepository.FindRepoRoot(), "test");
        Assert.That(Directory.Exists(testRoot), Is.True, "expected a test/ directory");

        var fixtures = new List<GateFixture>();

        foreach (var project in Directory.EnumerateDirectories(testRoot))
        {
            if (Path.GetFileName(project).Equals("shared", StringComparison.OrdinalIgnoreCase)) continue;

            foreach (var directory in GateDirectories)
            {
                var gateDirectory = Path.Combine(project, directory);
                foreach (var file in HygieneRepository.EnumerateFiles(gateDirectory, "*.cs"))
                {
                    var text = File.ReadAllText(file);
                    var namespaceName = FileNamespace.Match(text).Groups["name"].Value;

                    foreach (Match declaration in FixtureDeclaration.Matches(text))
                    {
                        var attributes = declaration.Groups["attributes"].Value;
                        if (!attributes.Contains("[TestFixture", StringComparison.Ordinal)) continue;

                        fixtures.Add(new GateFixture(
                            Directory: directory,
                            Path: Relative(file),
                            FullName: $"{namespaceName}.{declaration.Groups["name"].Value}",
                            Categories: CategoryAttribute
                                .Matches(attributes)
                                .Select(match => match.Groups["name"].Value)
                                .ToArray()));
                    }
                }
            }
        }

        return fixtures;
    }

    private static string Filter(string job) =>
        Regex.Match(job, @"^      CONTENT_GATE_FILTER:\s*(?<value>.+?)\s*$", RegexOptions.Multiline)
            .Groups["value"].Value;

    /// <summary>
    /// The body of one top-level job, from its two-space key to the next one.
    /// </summary>
    private static string JobBlock(string jobId)
    {
        var yaml = File.ReadAllText(Path.Combine(
            HygieneRepository.FindRepoRoot(),
            WorkflowPath.Replace('/', Path.DirectorySeparatorChar)));

        var start = Regex.Match(yaml, $@"^  {Regex.Escape(jobId)}:[ \t]*\r?$", RegexOptions.Multiline);
        Assert.That(start.Success, Is.True, $"expected a '{jobId}:' job in {WorkflowPath}");

        var rest = yaml[(start.Index + start.Length)..];
        var next = Regex.Match(rest, @"^  [A-Za-z0-9_.-]+:[ \t]*\r?$", RegexOptions.Multiline);

        return next.Success ? rest[..next.Index] : rest;
    }

    private static string Relative(string path) =>
        Path.GetRelativePath(HygieneRepository.FindRepoRoot(), path).Replace('\\', '/');

    private sealed record GateFixture(
        string Directory,
        string Path,
        string FullName,
        IReadOnlyList<string> Categories);
}
