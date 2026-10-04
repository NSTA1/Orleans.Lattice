using System.Diagnostics;
using System.Text.Json;
using System.Text.RegularExpressions;
using Orleans.Lattice.Testing.Hygiene;

namespace Orleans.Lattice.Tests.Hygiene;

/// <summary>
/// Tiered gating for member pull requests (#4430): a pull request whose base is
/// an integration branch skips the Coyote and chaos tiers, and every other run
/// skips nothing.
/// <para>
/// The rule lives in <c>.github/workflows/tier-scope.py</c> and its effect in
/// <c>plan-test-matrix.py --skip-tiers</c>. Both are driven here UNMODIFIED,
/// as the workflow drives them, rather than re-implemented: a copy of the rule
/// in this fixture would agree with itself forever. The workflow wiring that
/// connects them, and the <c>build-and-test</c> verdict that refuses a skip
/// outside a member pull request, are pinned textually.
/// </para>
/// <para>
/// The direction that matters is the fully gated one. A member pull request
/// that ran too much costs runner minutes; a pull request into <c>main</c> that
/// ran too little merges a Coyote or chaos regression nobody executed. So the
/// <c>main</c> and <c>release/**</c> cases, and every base the rule does not
/// recognise, are asserted to keep every tier.
/// </para>
/// </summary>
[TestFixture]
public sealed class CiMemberPullRequestTieringTests
{
    private const string WorkflowDirectory = ".github/workflows";
    private const string TierScopeScript = WorkflowDirectory + "/tier-scope.py";
    private const string PlannerScript = WorkflowDirectory + "/plan-test-matrix.py";
    private const string CiWorkflow = WorkflowDirectory + "/ci.yml";
    private const string PublishWorkflow = WorkflowDirectory + "/publish.yml";

    /// <summary>The tiers a member pull request skips, in planner order.</summary>
    private const string MemberSkippedTiers = "coyote,chaos";

    /// <summary>Bounds every child interpreter; tripping it means a hang.</summary>
    private static readonly TimeSpan ScriptTimeout = TimeSpan.FromMinutes(2);

    private static string RepoRoot => HygieneRepository.FindRepoRoot();

    [TestCase("main")]
    [TestCase("release/1.4")]
    [TestCase("release/2.0.x")]
    [TestCase("release/epic/not-an-integration-branch")]
    public void Main_and_release_based_pull_requests_keep_every_tier(string baseRef)
    {
        var decision = DecideTierScope("pull_request", baseRef);

        Assert.That(decision["scope"], Is.EqualTo("full"), Describe(baseRef, decision));
        Assert.That(
            decision["skipped_tiers"],
            Is.Empty,
            "a pull request into '" + baseRef + "' must run every tier; skipping one here lets a Coyote or chaos "
                + "regression merge without ever being executed. " + Describe(baseRef, decision));
        Assert.That(decision["max_legs"], Is.EqualTo("10"), Describe(baseRef, decision));
    }

    [TestCase("push", "")]
    [TestCase("push", "feat/epic/formal-coverage")]
    [TestCase("workflow_dispatch", "")]
    [TestCase("pull_request", "")]
    [TestCase("pull_request", "develop")]
    [TestCase("pull_request", "feat/epic/")]
    public void Every_run_that_is_not_a_member_pull_request_keeps_every_tier(string eventName, string baseRef)
    {
        var decision = DecideTierScope(eventName, baseRef);

        Assert.That(
            decision["skipped_tiers"],
            Is.Empty,
            "only a member pull request into an integration branch may skip tiers. The integration-branch push "
                + "lane in particular is where a member's skipped tiers run, so it must skip nothing. "
                + Describe(eventName + " " + baseRef, decision));
        Assert.That(decision["scope"], Is.EqualTo("full"));
    }

    [TestCase("feat/epic/formal-coverage")]
    [TestCase("fix/epic/atomicity-audit-residuals")]
    [TestCase("docs/epic/x")]
    public void Member_pull_requests_skip_exactly_the_coyote_and_chaos_tiers(string baseRef)
    {
        var decision = DecideTierScope("pull_request", baseRef);

        Assert.That(decision["scope"], Is.EqualTo("member"), Describe(baseRef, decision));
        Assert.That(
            decision["skipped_tiers"],
            Is.EqualTo(MemberSkippedTiers),
            "a member pull request skips the two exploration-dominated tiers and nothing else; the "
                + "deterministic tier (which carries the TLC fixtures) always runs. " + Describe(baseRef, decision));
        Assert.That(decision["reason"], Does.Contain(baseRef), "the stated reason must name the integration branch");
    }

    [Test]
    public void The_planner_skips_exactly_the_requested_tiers_and_drops_no_shard()
    {
        // One package of each shape: crossed with the tiers (lattice), sharded
        // but untiered (repocontext), and unsharded (replication, which carries
        // chaos fixtures).
        string[] packages = ["lattice", "lattice.api.mcp.repocontext", "lattice.replication"];

        var full = Plan(packages, skipTiers: null);
        var member = Plan(packages, skipTiers: MemberSkippedTiers);

        var fullRun = full.Where(item => item.Skip is null).ToList();
        var memberRun = member.Where(item => item.Skip is null).ToList();
        var memberSkipped = member.Where(item => item.Skip is not null).ToList();

        Assert.That(full.Any(item => item.Skip is not null), Is.False, "a full plan must skip nothing");
        Assert.That(
            fullRun.Select(item => item.Tier).Distinct(),
            Is.SupersetOf(new[] { "deterministic", "coyote", "chaos" }),
            "the full plan no longer crosses lattice with every tier; this comparison would prove nothing");

        Assert.That(
            memberRun.Where(item => item.Tier is "coyote" or "chaos").Select(item => item.Label),
            Is.Empty,
            "a member plan must not run a Coyote or chaos item");

        Assert.That(
            memberSkipped.Select(item => item.Label),
            Is.EquivalentTo(fullRun.Where(item => item.Tier is "coyote" or "chaos").Select(item => item.Label)),
            "every Coyote and chaos item of the full plan must be RECORDED as skipped on the member plan - not "
                + "silently dropped, and nothing else skipped");

        Assert.That(
            memberRun.Select(item => (item.Package, item.Shard)).Distinct(),
            Is.EquivalentTo(fullRun.Select(item => (item.Package, item.Shard)).Distinct()),
            "every shard of the full plan must still run its deterministic surface on the member plan");

        foreach (var item in memberRun.Where(item => item.Tier != "deterministic" || item.Package != "lattice"))
        {
            Assert.That(
                item.Filter,
                Does.Contain("TestCategory!=Coyote").And.Contain("TestCategory!=Chaos"),
                "untiered item '" + item.Label + "' must exclude the skipped tiers' categories");
            Assert.That(
                item.ExcludedTiers,
                Is.EqualTo(new[] { "coyote", "chaos" }),
                "untiered item '" + item.Label + "' must record which tiers it excluded");
        }

        foreach (var item in fullRun.Where(item => item.Package != "lattice"))
        {
            Assert.That(
                item.Filter ?? string.Empty,
                Does.Not.Contain("TestCategory"),
                "a full plan's untiered item '" + item.Label + "' must not be narrowed by category");
        }
    }

    [Test]
    public void The_planner_refuses_to_skip_the_deterministic_tier()
    {
        var (exitCode, _, stderr) = RunPlanner(["lattice"], "deterministic", out _);

        Assert.That(exitCode, Is.Not.Zero, "skipping the deterministic tier must be refused, not obeyed");
        Assert.That(stderr, Does.Contain("deterministic tier is never skipped"));
    }

    [Test]
    public void The_workflow_feeds_the_tier_decision_to_the_planner()
    {
        var ci = Read(CiWorkflow);

        Assert.That(
            Regex.Matches(ci, Regex.Escape("python3 .github/workflows/tier-scope.py")).Count,
            Is.EqualTo(1),
            "ci.yml must decide the tier scope exactly once, through tier-scope.py");
        Assert.That(ci, Does.Contain("--event \"$EVENT\"").And.Contain("--base-ref \"$BASE_REF\""));
        Assert.That(ci, Does.Contain("BASE_REF: ${{ github.base_ref }}"));

        Assert.That(
            Regex.Matches(ci, Regex.Escape("--output-matrix ")).Count,
            Is.EqualTo(1),
            "ci.yml must plan the test matrix in exactly one place, so the tier decision cannot be bypassed "
                + "(the apps lane's --emit-shard-filters call plans nothing and is not counted)");
        Assert.That(
            ci,
            Does.Contain("SKIPPED_TIERS: ${{ steps.tierscope.outputs.skipped_tiers }}")
                .And.Contain("MAX_LEGS: ${{ steps.tierscope.outputs.max_legs }}")
                .And.Contain("--skip-tiers \"$SKIPPED_TIERS\"")
                .And.Contain("--max-legs \"$MAX_LEGS\""),
            "the matrix step must take its skipped tiers and leg cap from the tier-scope decision");

        // The release gate runs before a package reaches NuGet, and is never a
        // member pull request.
        Assert.That(
            Read(PublishWorkflow),
            Does.Not.Contain("--skip-tiers"),
            "publish.yml must never skip a tier");
    }

    [Test]
    public void The_required_check_refuses_skipped_tiers_outside_a_member_pull_request()
    {
        var verdict = VerdictStep();

        Assert.That(
            verdict,
            Does.Contain("SKIPPED_TIERS: ${{ needs.plan.outputs.skipped_tiers }}"),
            "the verdict must read what the plan actually skipped");

        Assert.That(
            Regex.IsMatch(
                verdict,
                @"pull_request:main\|pull_request:release/\*\)\s*tiers_may_skip=false"),
            Is.True,
            "the verdict must forbid a skip on a pull request into main or a release line");

        Assert.That(
            Regex.IsMatch(verdict, @"\*\)\s*tiers_may_skip=false"),
            Is.True,
            "the verdict's default arm must forbid a skip, so an unrecognised event or base fails closed");

        Assert.That(
            Regex.IsMatch(
                verdict,
                @"if \[ -n ""\$SKIPPED_TIERS"" \] && \[ ""\$tiers_may_skip"" != ""true"" \]; then[\s\S]*?ok=false"),
            Is.True,
            "a forbidden skip must fail the verdict, not merely be reported");

        Assert.That(
            verdict,
            Does.Contain("Test tiers NOT run"),
            "a skip must be announced on the run summary so it cannot read as a pass");
    }

    [Test]
    public void The_required_check_accepts_only_success_from_the_job_that_carries_the_guards()
    {
        var verdict = VerdictStep();

        var arm = Regex.Match(verdict, @"case\s+""\$PLAN""\s+in(?<body>[\s\S]*?)esac");

        Assert.That(arm.Success, Is.True, "the verdict must inspect the plan job's result explicitly");
        Assert.That(
            Regex.IsMatch(arm.Groups["body"].Value, @"^\s*success\)\s*;;\s*$", RegexOptions.Multiline),
            Is.True,
            "'success' must be the only result accepted from plan: it carries every guard step, and a skipped "
                + "plan is a run whose guards never ran");
        Assert.That(
            Regex.IsMatch(verdict, @"for pair in [^\n]*""plan:"),
            Is.False,
            "plan must not be folded into the loop that accepts 'skipped'");
    }

    private static string VerdictStep()
    {
        var ci = Read(CiWorkflow).Replace("\r\n", "\n", StringComparison.Ordinal);
        var start = ci.IndexOf("      - name: Verdict\n", StringComparison.Ordinal);

        Assert.That(start, Is.GreaterThanOrEqualTo(0), "expected a 'Verdict' step in ci.yml");

        var next = Regex.Match(ci[(start + 1)..], @"^(      - |  [A-Za-z0-9_.-]+:)", RegexOptions.Multiline);
        return next.Success ? ci.Substring(start, next.Index + 1) : ci[start..];
    }

    private static string Read(string relative) =>
        File.ReadAllText(Path.Combine(RepoRoot, relative.Replace('/', Path.DirectorySeparatorChar)));

    private static string Describe(string input, IReadOnlyDictionary<string, string> decision) =>
        "(" + input + " -> " + string.Join(", ", decision.Select(pair => pair.Key + "=" + pair.Value)) + ")";

    private static Dictionary<string, string> DecideTierScope(string eventName, string baseRef)
    {
        var (exitCode, stdout, stderr) = RunPython(
            [Path.Combine(RepoRoot, TierScopeScript), "--event", eventName, "--base-ref", baseRef]);

        Assert.That(exitCode, Is.Zero, "tier-scope.py failed: " + stderr);

        var decision = stdout.Replace("\r\n", "\n", StringComparison.Ordinal)
            .Split('\n', StringSplitOptions.RemoveEmptyEntries)
            .Select(line => line.Split('=', 2))
            .Where(parts => parts.Length == 2)
            .ToDictionary(parts => parts[0], parts => parts[1], StringComparer.Ordinal);

        Assert.That(
            decision.Keys,
            Is.SupersetOf(new[] { "scope", "skipped_tiers", "max_legs", "reason" }),
            "tier-scope.py did not print its decision: " + stdout);

        return decision;
    }

    private sealed record PlannedItem(
        string Package,
        string Shard,
        string Tier,
        string Label,
        string? Filter,
        string? Skip,
        IReadOnlyList<string> ExcludedTiers);

    private static IReadOnlyList<PlannedItem> Plan(string[] packages, string? skipTiers)
    {
        var (exitCode, _, stderr) = RunPlanner(packages, skipTiers, out var matrix);

        Assert.That(exitCode, Is.Zero, "plan-test-matrix.py failed: " + stderr);

        using var document = JsonDocument.Parse(matrix);
        var items = new List<PlannedItem>();

        foreach (var leg in document.RootElement.EnumerateArray())
        {
            foreach (var item in leg.GetProperty("items").EnumerateArray())
            {
                items.Add(new PlannedItem(
                    item.GetProperty("package").GetString()!,
                    item.GetProperty("shard").GetString()!,
                    item.GetProperty("tier").GetString()!,
                    item.GetProperty("label").GetString()!,
                    item.GetProperty("filter").ValueKind == JsonValueKind.String
                        ? item.GetProperty("filter").GetString()
                        : null,
                    item.TryGetProperty("skip", out var skip) ? skip.GetString() : null,
                    item.TryGetProperty("excluded_tiers", out var excluded)
                        ? excluded.EnumerateArray().Select(entry => entry.GetString()!).ToArray()
                        : []));
            }
        }

        Assert.That(items, Is.Not.Empty, "the planner produced no items");
        return items;
    }

    private static (int ExitCode, string StandardOutput, string StandardError) RunPlanner(
        string[] packages,
        string? skipTiers,
        out string matrix)
    {
        var work = Directory.CreateTempSubdirectory("tiering-");

        try
        {
            var packageFile = Path.Combine(work.FullName, "packages.txt");
            var matrixFile = Path.Combine(work.FullName, "matrix.json");
            File.WriteAllLines(packageFile, packages);

            List<string> arguments =
            [
                Path.Combine(RepoRoot, PlannerScript),
                "--packages", packageFile,
                "--shards", Path.Combine(RepoRoot, WorkflowDirectory, "test-shards.json"),
                "--durations", Path.Combine(RepoRoot, WorkflowDirectory, "test-durations.tsv"),
                "--output-matrix", matrixFile,
                "--report-file", Path.Combine(work.FullName, "report.md"),
            ];

            if (skipTiers is not null)
            {
                arguments.AddRange(["--skip-tiers", skipTiers, "--skip-reason", "member pull request (test)"]);
            }

            var result = RunPython(arguments);
            matrix = File.Exists(matrixFile) ? File.ReadAllText(matrixFile) : string.Empty;
            return result;
        }
        finally
        {
            work.Delete(recursive: true);
        }
    }

    /// <summary>
    /// Runs a script under the first working Python interpreter. Missing Python
    /// is a broken pipeline under CI and a visible skip locally - never an
    /// inconclusive, which NUnit would not count.
    /// </summary>
    private static (int ExitCode, string StandardOutput, string StandardError) RunPython(IEnumerable<string> arguments)
    {
        var interpreter = FindPython();

        if (interpreter is null)
        {
            if (string.Equals(Environment.GetEnvironmentVariable("GITHUB_ACTIONS"), "true", StringComparison.OrdinalIgnoreCase))
            {
                Assert.Fail("no Python interpreter was found on the CI runner, so the tiering rule cannot be checked");
            }

            Assert.Ignore("no Python interpreter is available on this host");
        }

        var start = new ProcessStartInfo(interpreter!)
        {
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
            WorkingDirectory = RepoRoot,
        };

        foreach (var argument in arguments)
        {
            start.ArgumentList.Add(argument);
        }

        return Run(start);
    }

    private static string? FindPython()
    {
        // `python3` on Windows is often a Store alias stub that prints an error,
        // so each candidate is probed rather than assumed.
        string[] candidates = OperatingSystem.IsWindows() ? ["python", "py", "python3"] : ["python3", "python"];

        foreach (var candidate in candidates)
        {
            try
            {
                var probe = new ProcessStartInfo(candidate)
                {
                    RedirectStandardOutput = true,
                    RedirectStandardError = true,
                    UseShellExecute = false,
                };
                probe.ArgumentList.Add("--version");

                var (exitCode, stdout, stderr) = Run(probe);

                if (exitCode == 0 && (stdout + stderr).Contains("Python 3", StringComparison.Ordinal))
                {
                    return candidate;
                }
            }
            catch (System.ComponentModel.Win32Exception)
            {
                // Not on PATH.
            }
        }

        return null;
    }

    /// <summary>
    /// Drains both pipes before waiting, and bounds the wait, so a stuck child
    /// fails with a diagnosis instead of a blame-hang abort.
    /// </summary>
    private static (int ExitCode, string StandardOutput, string StandardError) Run(ProcessStartInfo start)
    {
        using var process = Process.Start(start)!;
        var stdoutTask = process.StandardOutput.ReadToEndAsync();
        var stderrTask = process.StandardError.ReadToEndAsync();

        if (!process.WaitForExit((int)ScriptTimeout.TotalMilliseconds))
        {
            try
            {
                process.Kill(entireProcessTree: true);
            }
            catch (InvalidOperationException)
            {
                // Already exited.
            }

            Assert.Fail(
                $"'{start.FileName} {string.Join(' ', start.ArgumentList)}' did not exit within "
                + $"{ScriptTimeout.TotalSeconds:0}s.");
        }

        return (process.ExitCode, stdoutTask.GetAwaiter().GetResult(), stderrTask.GetAwaiter().GetResult());
    }
}
