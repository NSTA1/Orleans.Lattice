using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using NUnit.Framework;
using Orleans.Lattice.Testing.Hygiene;

namespace Orleans.Lattice.Tests.Hygiene;

/// <summary>
/// Gates <c>tools/Invoke-RepositoryWideGates.ps1</c>, the single command that runs every
/// repository-wide metric gate.
/// <para>
/// The repository-wide gate population was a document. #2991 made that document correct and
/// enforced it against a source scan, and the correction held. A worker on this epic then ran
/// the gates as a deliberate verification step, having read the corrected document, and ran
/// six of eleven - missing every gate outside <c>test/lattice/</c>, which is verbatim the
/// blindness the document itself warns about. So the defect was never the document's
/// contents. The gap is between a correct document and a correct run, and no further
/// document correction closes it, because every worker re-derives the command line by hand
/// from prose and the failure is silent in both directions. See #3020.
/// </para>
/// <para>
/// Silent in both directions is literal. A <c>dotnet test --filter</c> that matches nothing
/// prints "No test matches the given testcase filter" and exits 0, and under
/// <c>--verbosity quiet</c> prints nothing at all (#3017), so a gate that does not exist is
/// indistinguishable from a gate that passed. And <c>FullyQualifiedName~Hygiene</c> matches
/// the namespace, returning a healthy non-vacuous count for the wrong population, so
/// demanding a count does not catch it either.
/// </para>
/// <para>
/// The load-bearing property this fixture defends is that the run list and the enforced list
/// are <b>one source, not two that agree</b>. Two lists that must agree will drift; one
/// cannot.
/// </para>
/// </summary>
[TestFixture]
public sealed class RepositoryWideGateRunnerTests
{
    private const string RunnerRelativePath = "tools/Invoke-RepositoryWideGates.ps1";

    private const string InstructionsPath = ".github/instructions/testing.instructions.md";

    /// <summary>
    /// The core test project, named here only as the directory a run list must reach
    /// <i>beyond</i>. It is not a gate name and never enters the run list.
    /// </summary>
    private const string CoreTestProject = "test/lattice";

    /// <summary>
    /// A fixture name chosen so that it matches nothing, used as the runner's known-answer
    /// control. It is asserted absent from the tree by the control's own test, so it cannot
    /// quietly acquire a match and turn the control green for the wrong reason.
    /// </summary>
    private const string BogusFixtureName = "ThisFixtureDoesNotExistControl";

    /// <summary>
    /// Fixtures a worker plausibly runs while believing they are running repository-wide
    /// gates, which must not appear in the run list, each with the reason it is excluded.
    /// <para>
    /// Both are real, and that is the point: they were observed in an actual enumeration of
    /// "the repository-wide gates" by a careful worker. A near miss recorded with its reason
    /// reddens by name if a future runner widens to admit it, where silent absence would
    /// not.
    /// </para>
    /// </summary>
    private static readonly IReadOnlyDictionary<string, string> RecordedNearMissFixtures =
        new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["MetricsDocCoverageTests"] =
                "the per-package src/lattice doc-coverage fixture, not the repository-wide "
                    + "enrolment gate. The instructions file says so explicitly, and the "
                    + "enrolment gate exists precisely because this one is per-package. A "
                    + "runner built on a '~MetricsDocCoverage' substring filter admits it.",
            ["MetricEmissionScannerTests"] =
                "a unit test of the scanner helper. It builds a synthetic src/ directory "
                    + "under a temporary root and never calls FindRepoRoot, so it scans no "
                    + "part of the real tree. Running it tells you the helper works, not "
                    + "that any package was scanned.",
        };

    private static string RepoRoot => HygieneRepository.FindRepoRoot();

    private static readonly Regex NamespaceDeclaration =
        new(@"^\s*namespace\s+([\w\.]+)", RegexOptions.Multiline | RegexOptions.Compiled);

    private static string RunnerPath =>
        Path.Combine(RepoRoot, RunnerRelativePath.Replace('/', Path.DirectorySeparatorChar));

    private static IReadOnlyList<string> TestSourceText() =>
        HygieneRepository
            .EnumerateFiles(Path.Combine(RepoRoot, "test"), "*.cs")
            .Where(static p => !HygieneRepository.HasExcludedSegment(p))
            .Select(File.ReadAllText)
            .ToList();

    private sealed record RunListEntry(string Fixture, string Project, string ProjectFile, string Filter);

    /// <summary>
    /// The directory holding the agent playbooks whose gate invocations are checked against
    /// the tree.
    /// </summary>
    private const string AgentsRelativePath = ".github/agents";

    /// <summary>
    /// Matches a documented runner invocation. <c>-Project</c> is optional because the runner
    /// defaults it, so an invocation that omits it is still a real invocation and must still
    /// be checked rather than silently skipped.
    /// </summary>
    private static readonly Regex DocumentedInvocation =
        new(
            @"Invoke-RepositoryWideGates\.ps1\s+-Fixture\s+(?<fixture>[A-Za-z0-9_]+)"
                + @"(?:\s+-Project\s+(?<project>[A-Za-z0-9_./-]+))?",
            RegexOptions.Compiled);

    /// <summary>
    /// Matches a raw <c>dotnet test --filter</c> that selects a fixture by name. Anchored on
    /// the <c>Tests</c> suffix so an ordinary category filter such as
    /// <c>--filter "TestCategory!=Chaos"</c> is not swept up: that one cannot be vacuous in
    /// the way a mistyped fixture name is.
    /// </summary>
    private static readonly Regex BareGateFilter =
        new(@"dotnet test .*--filter\s+""FullyQualifiedName~[A-Za-z0-9_.]*Tests""", RegexOptions.Compiled);

    private static IReadOnlyList<string> AgentPlaybooks()
    {
        var directory = Path.Combine(RepoRoot, AgentsRelativePath.Replace('/', Path.DirectorySeparatorChar));
        return Directory.Exists(directory)
            ? Directory.GetFiles(directory, "*.agent.md", SearchOption.TopDirectoryOnly)
            : Array.Empty<string>();
    }

    private static IReadOnlyList<(string File, string Fixture, string Project)> DocumentedAgentInvocations()
    {
        var found = new List<(string, string, string)>();
        var seen = new HashSet<string>(StringComparer.Ordinal);

        foreach (var path in AgentPlaybooks())
        {
            foreach (Match match in DocumentedInvocation.Matches(File.ReadAllText(path)))
            {
                var fixture = match.Groups["fixture"].Value;
                var project = match.Groups["project"].Success
                    ? match.Groups["project"].Value
                    : CoreTestProject;

                // The same pair is documented in more than one playbook, and resolving it
                // costs a process launch, so each distinct pair is resolved once.
                if (seen.Add($"{fixture}|{project}"))
                {
                    found.Add((Path.GetFileName(path), fixture, project));
                }
            }
        }

        return found;
    }

    /// <summary>
    /// Asks the runner what filter it would use for an explicitly named fixture, without
    /// running it. This drives the real resolver rather than a reimplementation of it, for the
    /// same reason <see cref="EmitRunList"/> does.
    /// </summary>
    private static string EmitNamedFixtureFilter(string fixture, string project)
    {
        var shell = FindShell();
        if (shell is null)
        {
            Assert.Ignore("Neither pwsh nor powershell is available on this host.");
        }

        var psi = new ProcessStartInfo(shell!)
        {
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
            WorkingDirectory = RepoRoot,
        };
        psi.ArgumentList.Add("-NoProfile");
        psi.ArgumentList.Add("-File");
        psi.ArgumentList.Add(RunnerPath);
        psi.ArgumentList.Add("-Emit");
        psi.ArgumentList.Add("-Fixture");
        psi.ArgumentList.Add(fixture);
        psi.ArgumentList.Add("-Project");
        psi.ArgumentList.Add(project);

        using var process = Process.Start(psi)!;
        var stdout = process.StandardOutput.ReadToEnd();
        var stderr = process.StandardError.ReadToEnd();
        process.WaitForExit();

        Assert.That(
            process.ExitCode,
            Is.Zero,
            $"{RunnerRelativePath} -Emit -Fixture {fixture} -Project {project} exited "
                + $"{process.ExitCode}.{Environment.NewLine}stdout:{Environment.NewLine}{stdout}"
                + $"{Environment.NewLine}stderr:{Environment.NewLine}{stderr}");

        var cells = stdout.Trim().Split('\t');
        Assert.That(
            cells,
            Has.Length.EqualTo(4),
            $"{RunnerRelativePath} -Emit did not produce four tab-separated cells for "
                + $"-Fixture {fixture} -Project {project}: '{stdout.Trim()}'");

        return cells[3].Trim();
    }

    private static string? FindShell()
    {
        var names = OperatingSystem.IsWindows()
            ? new[] { "pwsh.exe", "powershell.exe" }
            : new[] { "pwsh", "powershell" };

        var path = Environment.GetEnvironmentVariable("PATH") ?? string.Empty;
        foreach (var directory in path.Split(Path.PathSeparator, StringSplitOptions.RemoveEmptyEntries))
        {
            foreach (var name in names)
            {
                string candidate;
                try
                {
                    candidate = Path.Combine(directory.Trim('"'), name);
                }
                catch (ArgumentException)
                {
                    continue;
                }

                if (File.Exists(candidate))
                {
                    return candidate;
                }
            }
        }

        return null;
    }

    /// <summary>
    /// Drives the runner's real derivation path and parses what it emits. There is
    /// deliberately no synthetic entry point that would let a test inject a run list: the
    /// adjacent <c>Assert-DeployManifest.ps1</c> defect (#2983) was a refusal path that had
    /// been perturbation-tested only through a synthetic entry point, so the tests proved a
    /// code path operations never reached. This reads what the real command really derives.
    /// </summary>
    private static IReadOnlyList<RunListEntry> EmitRunList()
    {
        var shell = FindShell();
        if (shell is null)
        {
            Assert.Ignore("Neither pwsh nor powershell is available on this host.");
        }

        var psi = new ProcessStartInfo(shell!)
        {
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
            WorkingDirectory = RepoRoot,
        };
        psi.ArgumentList.Add("-NoProfile");
        psi.ArgumentList.Add("-File");
        psi.ArgumentList.Add(RunnerPath);
        psi.ArgumentList.Add("-Emit");

        using var process = Process.Start(psi)!;
        var stdout = process.StandardOutput.ReadToEnd();
        var stderr = process.StandardError.ReadToEnd();
        process.WaitForExit();

        Assert.That(
            process.ExitCode,
            Is.Zero,
            $"{RunnerRelativePath} -Emit exited {process.ExitCode}.{Environment.NewLine}"
                + $"stdout:{Environment.NewLine}{stdout}{Environment.NewLine}"
                + $"stderr:{Environment.NewLine}{stderr}");

        var entries = new List<RunListEntry>();
        foreach (var line in stdout.Split('\n'))
        {
            var trimmed = line.Trim('\r', ' ');
            if (trimmed.Length == 0)
            {
                continue;
            }

            var cells = trimmed.Split('\t');
            Assert.That(
                cells,
                Has.Length.EqualTo(4),
                $"{RunnerRelativePath} -Emit produced a line that is not four "
                    + $"tab-separated cells: '{trimmed}'");

            entries.Add(new RunListEntry(cells[0], cells[1], cells[2], cells[3]));
        }

        Assert.That(
            entries,
            Is.Not.Empty,
            $"{RunnerRelativePath} -Emit produced an empty run list. A runner that derives "
                + "no gates runs nothing and reports success, which is the exact defect it "
                + "exists to remove.");

        return entries;
    }

    [Test]
    public void Runner_exists_and_names_no_gate_of_its_own()
    {
        Assert.That(
            File.Exists(RunnerPath),
            Is.True,
            $"expected the repository-wide gate runner at {RunnerRelativePath}");

        var expected = RepositoryWideGateEnrolmentTests.ExpectedDocumentedFixtures();

        Assert.That(
            expected,
            Is.Not.Empty,
            "the enforced gate population is empty, so this fixture would assert nothing "
                + "about the runner's independence from it");

        var script = File.ReadAllText(RunnerPath);
        var hardcoded = expected
            .Where(name => script.Contains(name, StringComparison.Ordinal))
            .OrderBy(static n => n, StringComparer.Ordinal)
            .ToList();

        Assert.That(
            hardcoded,
            Is.Empty,
            $"{RunnerRelativePath} names these gates literally: {string.Join(", ", hardcoded)}. "
                + "The runner must derive its run list from the gate table, not carry a copy "
                + "of it. A copy is a second place for the population to be stated, and a "
                + "second place is what drifts - which is the defect this whole item exists "
                + "to remove.");
    }

    [Test]
    public void Runner_run_list_matches_the_population_enforced_from_source()
    {
        var runList = EmitRunList();
        var actual = runList.Select(static e => e.Fixture).ToHashSet(StringComparer.Ordinal);

        Assert.That(
            actual,
            Has.Count.EqualTo(runList.Count),
            "the runner emitted the same gate more than once, which would double-run it and "
                + "inflate the apparent gate count");

        var expected = RepositoryWideGateEnrolmentTests
            .ExpectedDocumentedFixtures()
            .ToHashSet(StringComparer.Ordinal);

        Assert.That(
            expected,
            Is.Not.Empty,
            "the enforced gate population is empty, so this comparison would be vacuous");

        var missingFromRun = expected.Except(actual).OrderBy(static n => n, StringComparer.Ordinal).ToList();
        var extraInRun = actual.Except(expected).OrderBy(static n => n, StringComparer.Ordinal).ToList();

        Assert.Multiple(() =>
        {
            Assert.That(
                missingFromRun,
                Is.Empty,
                "These gates are enforced as repository-wide but the runner does not run "
                    + $"them: {string.Join(", ", missingFromRun)}. A worker invoking the "
                    + "command would believe they had run every gate.");
            Assert.That(
                extraInRun,
                Is.Empty,
                "The runner runs these, but they are not part of the enforced repository-wide "
                    + $"population: {string.Join(", ", extraInRun)}. A run list wider than the "
                    + "enforced population overstates what the command proves.");
        });
    }

    [Test]
    public void Runner_reaches_every_project_the_gates_live_in()
    {
        var runList = EmitRunList();

        var outsideCore = runList
            .Where(static e => !string.Equals(e.Project, CoreTestProject, StringComparison.Ordinal))
            .ToList();

        Assert.That(
            outsideCore,
            Is.Not.Empty,
            "The run list contains no gate outside "
                + $"{CoreTestProject}, so this assertion would pass while the command "
                + "reproduced exactly the blindness it exists to correct: running everything "
                + "under the core test project and nothing beyond it. If a gate genuinely "
                + "moved, this must be re-derived deliberately rather than relaxed.");

        var projects = runList
            .Select(static e => e.Project)
            .Distinct(StringComparer.Ordinal)
            .OrderBy(static p => p, StringComparer.Ordinal)
            .ToList();

        Assert.That(
            projects,
            Has.Count.GreaterThan(1),
            $"the run list spans only {string.Join(", ", projects)}; the whole point of the "
                + "command is that the gates do not all live in one test project");

        foreach (var entry in runList)
        {
            var projectFile = Path.Combine(
                RepoRoot,
                entry.ProjectFile.Replace('/', Path.DirectorySeparatorChar));

            Assert.That(
                File.Exists(projectFile),
                Is.True,
                $"the runner maps {entry.Fixture} to {entry.ProjectFile}, which does not "
                    + "exist, so that gate would fail to run at all");
        }
    }

    [Test]
    public void Substring_filter_selects_exactly_its_own_gate()
    {
        var expected = RepositoryWideGateEnrolmentTests.ExpectedDocumentedFixtures();
        var sources = TestSourceText();

        Assert.That(
            sources,
            Has.Count.GreaterThan(100),
            $"only {sources.Count} test sources were read, which is too few for this scan to "
                + "be meaningful; the enumeration is probably broken rather than the tree "
                + "genuinely this small");

        var neverSeen = new List<string>();
        var overMatched = new List<string>();

        foreach (var gate in expected.OrderBy(static n => n, StringComparer.Ordinal))
        {
            // The runner filters on FullyQualifiedName~<gate>, so any longer identifier
            // containing the gate name is also selected. Dots are not word characters, so a
            // namespace or method boundary does not over-match; a longer identifier does.
            var standalone = new Regex($@"(?<!\w){Regex.Escape(gate)}(?!\w)", RegexOptions.None);
            var extended = new Regex($@"(?<=\w){Regex.Escape(gate)}|{Regex.Escape(gate)}(?=\w)", RegexOptions.None);

            var seen = false;
            var widened = false;
            foreach (var text in sources)
            {
                seen |= standalone.IsMatch(text);
                widened |= extended.IsMatch(text);
            }

            if (!seen)
            {
                neverSeen.Add(gate);
            }

            if (widened)
            {
                overMatched.Add(gate);
            }
        }

        Assert.Multiple(() =>
        {
            // The known-positive half. Without it a scan that read nothing useful would
            // report no over-matches and read as a clean pass.
            Assert.That(
                neverSeen,
                Is.Empty,
                "These gates were not found anywhere under test/ as a standalone identifier, "
                    + $"so this scan is not seeing them at all: {string.Join(", ", neverSeen)}. "
                    + "Treat this as the scan being broken, not as the gates being absent.");
            Assert.That(
                overMatched,
                Is.Empty,
                "A longer identifier under test/ contains these gate names, so the runner's "
                    + $"FullyQualifiedName~ filter selects more than the gate: "
                    + $"{string.Join(", ", overMatched)}. The command would report an "
                    + "executed count covering a population wider than the gate, which reads "
                    + "as a healthy pass.");
        });
    }

    [Test]
    public void Recorded_near_miss_fixtures_stay_out_of_the_run_list()
    {
        Assert.That(
            RecordedNearMissFixtures,
            Is.Not.Empty,
            "the near-miss record is empty, so this fixture asserts nothing");

        var runList = EmitRunList();
        var actual = runList.Select(static e => e.Fixture).ToHashSet(StringComparer.Ordinal);
        var sources = TestSourceText();

        var admitted = new List<string>();
        var vanished = new List<string>();

        foreach (var (fixture, _) in RecordedNearMissFixtures.OrderBy(static p => p.Key, StringComparer.Ordinal))
        {
            if (actual.Contains(fixture))
            {
                admitted.Add(fixture);
            }

            var declaration = new Regex($@"class\s+{Regex.Escape(fixture)}\b", RegexOptions.None);
            if (!sources.Any(text => declaration.IsMatch(text)))
            {
                vanished.Add(fixture);
            }
        }

        Assert.Multiple(() =>
        {
            Assert.That(
                admitted,
                Is.Empty,
                "The runner admits these into the run list, but each is recorded as a near "
                    + $"miss rather than a repository-wide gate: {string.Join(", ", admitted)}. "
                    + "Reasons: "
                    + string.Join(
                        "; ",
                        admitted.Select(name => $"{name} - {RecordedNearMissFixtures[name]}")));

            // An exclusion whose subject no longer exists is trivially satisfied, so the
            // assertion above would keep passing after it had stopped meaning anything.
            Assert.That(
                vanished,
                Is.Empty,
                "These fixtures are recorded as near misses that must stay out of the run "
                    + $"list, but no longer exist: {string.Join(", ", vanished)}. The "
                    + "exclusion is now vacuous - remove the record, or update it to name "
                    + "whatever replaced them.");
        });
    }

    [Test]
    public void Instructions_route_the_reader_to_the_runner()
    {
        var path = Path.Combine(RepoRoot, InstructionsPath.Replace('/', Path.DirectorySeparatorChar));
        Assert.That(File.Exists(path), Is.True, $"{InstructionsPath} is missing.");

        var text = File.ReadAllText(path);

        Assert.That(
            text.Contains(RunnerRelativePath, StringComparison.Ordinal),
            Is.True,
            $"{InstructionsPath} no longer names {RunnerRelativePath}. The table is the run "
                + "list's source, so the instructions must send the reader to the command "
                + "rather than invite them to compose filters by hand - which is what "
                + "produced a six-of-eleven run by a worker who had read the table.");
    }

    /// <summary>
    /// Every gate invocation documented in an agent playbook must name a fixture that really
    /// exists in the project the invocation names.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Routing the playbooks through the runner (#3017) replaced a set of hand-composed
    /// <c>dotnet test --filter</c> lines with a set of hand-written fixture/project pairs. That
    /// removes the silent-success failure - the runner refuses a zero-executed gate - but it
    /// does not remove the possibility that a pair names a fixture which no longer exists, or
    /// one that was never spelled correctly. A rename would leave the playbook naming a ghost,
    /// and the only symptom would be a red gate at the moment somebody followed the playbook.
    /// </para>
    /// <para>
    /// So the pairs are checked here against the tree, through the runner's own resolver
    /// rather than a second one. The discriminator is the resolver's documented fallback: a
    /// name it can locate in source resolves to a namespace-qualified filter ending in a dot,
    /// and a name it cannot locate falls back to the bare name. That fallback is deliberate -
    /// it keeps a bogus name runnable so the runner can report zero executed - which is
    /// exactly why the bare shape is the signal that a documented pair is wrong.
    /// </para>
    /// </remarks>
    [Test]
    public void Agent_documented_gate_invocations_name_fixtures_that_exist()
    {
        var invocations = DocumentedAgentInvocations();

        Assert.That(
            invocations,
            Is.Not.Empty,
            $"No '{RunnerRelativePath} -Fixture ...' invocation was found under "
                + $"{AgentsRelativePath}. This test exists to check those invocations against "
                + "the tree, so finding none means it is passing vacuously rather than that "
                + "the playbooks are clean.");

        var unresolved = new List<string>();
        foreach (var (file, fixture, project) in invocations)
        {
            var filter = EmitNamedFixtureFilter(fixture, project);
            if (!filter.EndsWith('.'))
            {
                unresolved.Add($"{file}: -Fixture {fixture} -Project {project} -> '{filter}'");
            }
        }

        Assert.That(
            unresolved,
            Is.Empty,
            "An agent playbook documents a gate invocation whose fixture could not be located "
                + "in the project it names, so the runner fell back to a bare-name filter. "
                + "Following that playbook would run zero tests and fail the gate:"
                + Environment.NewLine
                + string.Join(Environment.NewLine, unresolved));
    }

    /// <summary>
    /// The agent playbooks must not reach a gate fixture through a hand-composed
    /// <c>dotnet test --filter</c>, which exits 0 when it matches nothing.
    /// </summary>
    [Test]
    public void Agent_playbooks_do_not_invoke_gate_fixtures_through_a_bare_filter()
    {
        var offenders = new List<string>();
        foreach (var path in AgentPlaybooks())
        {
            foreach (var line in File.ReadAllLines(path))
            {
                var match = BareGateFilter.Match(line);
                if (match.Success)
                {
                    offenders.Add($"{Path.GetFileName(path)}: {line.Trim()}");
                }
            }
        }

        Assert.That(
            offenders,
            Is.Empty,
            "An agent playbook invokes a gate fixture through a raw 'dotnet test --filter'. "
                + "That exits 0 when the filter matches nothing (#3017), so a mistyped fixture "
                + $"name reads as a passing gate. Route it through {RunnerRelativePath}, which "
                + "reports the executed count and refuses zero:"
                + Environment.NewLine
                + string.Join(Environment.NewLine, offenders));
    }

    /// <summary>
    /// The runner's name-to-source resolution must agree, gate for gate, with the resolution
    /// the enrolment gate already uses.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The runner is a shell script and the enrolment gate is C#, so the two cannot literally
    /// share a function. What they must not do is <i>disagree</i>. Two independent resolvers
    /// are two things that can each drift from the table without the other noticing, and the
    /// entire premise of the gate list is that the documented set and the executed set cannot
    /// diverge. So the second implementation is permitted only while it is checked against
    /// the first, which is what this does: the script's answer is read out of its real emitted
    /// run list, and compared against <c>SourceFilesForType</c>.
    /// </para>
    /// <para>
    /// This is also a known-answer test for the script's resolver, in the direction that
    /// matters. A resolver that silently fails to find a source file still produces a
    /// runnable filter - the bare-name fallback - so its failure mode is a filter that works
    /// and is merely loose, not an error. Comparing against an independently-derived answer
    /// is the only thing that distinguishes those two.
    /// </para>
    /// </remarks>
    [Test]
    public void Runner_resolves_each_gate_to_the_same_source_the_enrolment_gate_does()
    {
        var entries = EmitRunList();

        Assert.Multiple(() =>
        {
            foreach (var entry in entries)
            {
                var sources = RepositoryWideGateEnrolmentTests.SourceFilesForType(entry.Fixture);

                Assert.That(
                    sources,
                    Is.Not.Empty,
                    $"The enrolment gate resolves no source file for {entry.Fixture}, so the "
                        + "runner is running a gate the gate list cannot see.");

                var declared = sources
                    .Select(File.ReadAllText)
                    .Select(static text => NamespaceDeclaration.Match(text))
                    .Where(static match => match.Success)
                    .Select(static match => match.Groups[1].Value)
                    .Distinct(StringComparer.Ordinal)
                    .ToArray();

                Assert.That(
                    declared,
                    Has.Length.EqualTo(1),
                    $"{entry.Fixture} resolves to {declared.Length} distinct namespaces "
                        + $"({string.Join(", ", declared)}), so there is no single answer for "
                        + "the runner to agree with.");

                Assert.That(
                    entry.Filter,
                    Is.EqualTo($"FullyQualifiedName~{declared[0]}.{entry.Fixture}."),
                    $"The runner resolved {entry.Fixture} to a filter that does not match the "
                        + "namespace the enrolment gate's own resolver finds for it. The two "
                        + "name-resolution paths have diverged, which is exactly the failure "
                        + "the single-source rule exists to prevent.");
            }
        });
    }

    /// <summary>
    /// Every gate's filter must be anchored to its declaring namespace and terminated with a
    /// dot, and the gate set must span more than one namespace.
    /// </summary>
    /// <remarks>
    /// The second half is the load-bearing one, and it is a measurement rather than an
    /// opinion. A filter shaped like a namespace reads as the more precise option, so the
    /// standing temptation is to collapse the run list to one <c>~Namespace</c> filter. That
    /// is only safe if the namespace and the gate set are the same population, and here they
    /// are not: the eleven gates are declared across three namespaces, so any single
    /// namespace filter silently drops the gates outside it while looking tighter than the
    /// run list it replaced. This test fails the moment that stops being measurable, so the
    /// claim cannot outlive the evidence for it.
    /// </remarks>
    [Test]
    public void Every_gate_filter_is_anchored_to_its_own_declaring_namespace()
    {
        var entries = EmitRunList();

        Assert.Multiple(() =>
        {
            foreach (var entry in entries)
            {
                Assert.That(
                    entry.Filter,
                    Does.StartWith("FullyQualifiedName~"),
                    $"{entry.Fixture} is not filtered by fully-qualified name.");

                var value = entry.Filter["FullyQualifiedName~".Length..];

                Assert.That(
                    value,
                    Does.EndWith($".{entry.Fixture}."),
                    $"{entry.Fixture} resolves to the filter value '{value}', which is not "
                        + "its namespace followed by its own name and a terminating dot. "
                        + "Without the trailing dot the filter is a bare substring and also "
                        + "selects any longer identifier that contains this one.");
            }

            var namespaces = entries
                .Select(entry => entry.Filter["FullyQualifiedName~".Length..]
                    .TrimEnd('.')
                    .Replace($".{entry.Fixture}", string.Empty, StringComparison.Ordinal))
                .Distinct(StringComparer.Ordinal)
                .ToArray();

            Assert.That(
                namespaces,
                Has.Length.GreaterThan(1),
                "Every repository-wide gate now resolves to a single namespace "
                    + $"({string.Join(", ", namespaces)}). That would make a namespace-shaped "
                    + "filter equivalent to the run list today, but the equivalence is a "
                    + "property of the current tree rather than of the gate set, and nothing "
                    + "stops the next gate being declared elsewhere. Enumerate by exact name.");
        });
    }

    /// <summary>
    /// A name that matches nothing must report an executed count of zero and fail the run.
    /// </summary>
    /// <remarks>
    /// This is the instrument's own known-answer test, and it is run through the real reader
    /// rather than a synthetic seam, because the value it checks is the one a reader is most
    /// likely to get wrong. A reader that counts result nodes is correct at every non-zero
    /// count and wrong precisely at zero, since an empty node set wraps into a one-element
    /// array and an absent gate reports as a small passing one. The failure is invisible
    /// until the day it matters, which is the day a gate stops being discovered.
    /// </remarks>
    [Test]
    public void A_name_that_matches_nothing_reports_zero_executed_and_fails()
    {
        var shell = FindShell();
        if (shell is null)
        {
            Assert.Ignore("Neither pwsh nor powershell is available on this host.");
        }

        var psi = new ProcessStartInfo(shell!)
        {
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
            WorkingDirectory = RepoRoot,
        };
        psi.ArgumentList.Add("-NoProfile");
        psi.ArgumentList.Add("-File");
        psi.ArgumentList.Add(RunnerPath);
        psi.ArgumentList.Add("-Fixture");
        psi.ArgumentList.Add(BogusFixtureName);

        using var process = Process.Start(psi)!;
        var stdout = process.StandardOutput.ReadToEnd();
        var stderr = process.StandardError.ReadToEnd();
        process.WaitForExit();

        var detail = $"{Environment.NewLine}stdout:{Environment.NewLine}{stdout}"
            + $"{Environment.NewLine}stderr:{Environment.NewLine}{stderr}";

        // The control is only a known answer while the answer is actually known. If a real
        // fixture ever takes this name the run would execute something, EXECUTED would be
        // non-zero, and the control would redden for a reason that has nothing to do with
        // the reader - so measure the premise rather than assume it.
        var declaringSources = HygieneRepository
            .EnumerateFiles(Path.Combine(RepoRoot, "test"), "*.cs")
            .Where(static p => !HygieneRepository.HasExcludedSegment(p))
            .Where(p => !Path.GetFileName(p).StartsWith(nameof(RepositoryWideGateRunnerTests), StringComparison.Ordinal))
            .Where(p => File.ReadAllText(p).Contains(BogusFixtureName, StringComparison.Ordinal))
            .ToArray();

        Assert.That(
            declaringSources,
            Is.Empty,
            $"'{BogusFixtureName}' now appears in {string.Join(", ", declaringSources)}. The "
                + "control's premise - that this name matches nothing - no longer holds, so "
                + "the control no longer measures the reader. Choose another name.");

        Assert.Multiple(() =>
        {
            Assert.That(
                stdout,
                Does.Contain($"{BogusFixtureName}"),
                $"The runner did not report the requested fixture at all.{detail}");

            Assert.That(
                stdout,
                Does.Contain("EXECUTED=0"),
                $"A fixture name that matches nothing did not report an executed count of "
                    + $"zero. The reader cannot represent the value it exists to read.{detail}");

            Assert.That(
                process.ExitCode,
                Is.Not.Zero,
                $"A run in which nothing executed exited zero, which reads as a pass.{detail}");

            Assert.That(
                stdout,
                Does.Not.Contain("REPOSITORY-WIDE GATES OK"),
                $"A run of an explicitly named fixture set claimed to be the repository-wide "
                    + $"run. The two must never be confusable in a transcript.{detail}");
        });
    }
}
