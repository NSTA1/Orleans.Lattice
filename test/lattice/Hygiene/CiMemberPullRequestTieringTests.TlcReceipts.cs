using System.Text.Json;
using System.Text.RegularExpressions;
using Orleans.Lattice.Tests.Formal;

namespace Orleans.Lattice.Tests.Hygiene;

/// <summary>
/// TLC verdict receipts (#5164, contract C5). The planner removes modules a
/// trusted receipt proves from the exhaustive TLC shards, using the real case
/// census and categories (<see cref="TlcCaseCensus"/>) and its own filter
/// evaluator in <c>tlc-receipts.py</c>. These checks re-evaluate the filters it
/// emits with this fixture's independent evaluator: a cached module's cases
/// must leave the plan entirely, and every other TLC case must still be
/// selected by exactly one running item. A receipt defect therefore fails here,
/// toolchain-free, instead of skipping a verdict nobody notices. That the
/// integration-branch push lane always plans exhaustive TLC with receipts is
/// pinned beside them.
/// </summary>
public sealed partial class CiMemberPullRequestTieringTests
{
    private const string IntegrationBranch = "ci/epic/formal-pipeline";

    private sealed record ReceiptItem(string Label, string Shard, string? Filter, bool Tlc, bool Smoke, string? Skip);

    [Test]
    public void With_every_module_cached_no_running_item_selects_a_census_case_and_every_other_TLC_case_runs_once()
    {
        var census = TlcCaseCensus.Build();
        var items = PlanWithReceipts(census.Keys);

        Assert.Multiple(() =>
        {
            Assert.That(ReceiptPartitionProblems(census.Keys.ToHashSet(StringComparer.Ordinal), items), Is.Empty);
            Assert.That(items.Where(item => item.Skip is not null && item.Tlc), Is.Not.Empty,
                "with every module cached, category-only TLC shards must be recorded as verified by receipt");
            Assert.That(items.Where(item => item.Skip is not null && item.Tlc).Select(item => item.Skip),
                Is.All.StartsWith("verified by receipt"));
            Assert.That(items.Single(item => item.Label == "lattice / formal-tlc (deterministic)").Filter,
                Does.Contain("TestCategory!=TlcModule_"),
                "the name-prefix catch-all is only narrowed: it also holds TLC tests outside the census");
            Assert.That(items.Where(item => item.Smoke && item.Skip is null), Is.Not.Empty,
                "receipts never touch the base-model smoke shard");
        });
    }

    [Test]
    public void With_one_module_cached_only_its_cases_leave_the_plan()
    {
        var census = TlcCaseCensus.Build();
        var cached = census.Keys.Where(module => module.StartsWith("AntiEntropy", StringComparison.Ordinal)).Take(1).ToHashSet(StringComparer.Ordinal);
        Assert.That(cached, Is.Not.Empty, "the census has no AntiEntropy module; pick another module for this check");

        var items = PlanWithReceipts(cached);

        Assert.That(ReceiptPartitionProblems(cached, items), Is.Empty);
    }

    [Test]
    public void With_no_module_cached_the_plan_is_unchanged()
    {
        var plain = Plan(["lattice"], skipTiers: null);
        var receipts = PlanWithReceipts([]);

        Assert.That(receipts.Select(item => (item.Label, item.Filter, item.Skip)),
            Is.EquivalentTo(plain.Select(item => (item.Label, item.Filter, item.Skip))));
    }

    [Test]
    public void The_receipt_tools_self_test_passes()
    {
        var (exitCode, stdout, stderr) = RunPython([Path.Combine(RepoRoot, WorkflowDirectory, "tlc-receipts.py"), "--self-test"]);

        Assert.That(exitCode, Is.Zero, stdout + stderr);
        Assert.That(stderr, Does.Match(@"Ran [1-9]\d* tests").And.Contain("OK"), "the self-test must run tests, not pass vacuously");
    }

    /// <summary>
    /// An integration-branch push is never skipped by its own diff: that would
    /// assume the previous push's run passed exhaustive TLC, which may have
    /// failed or been cancelled by this push's concurrency group. It plans
    /// exhaustive TLC with receipts on, which exist only for fingerprints that
    /// actually passed.
    /// </summary>
    [Test]
    public void An_integration_push_always_plans_exhaustive_TLC_with_receipts_on()
    {
        var decision = DecideTierScope("push", "", [], IntegrationBranch);

        Assert.Multiple(() =>
        {
            Assert.That(decision["skip_tlc_reason"], Is.Empty, "a push never skips exhaustive TLC");
            Assert.That(decision["skipped_tiers"], Is.Empty, "the push lane runs every test tier");
            Assert.That(decision["tlc_receipts"], Is.EqualTo("on"));
        });
    }

    [Test]
    public void Release_lines_and_a_dispatch_with_receipts_off_ignore_receipts()
    {
        Assert.Multiple(() =>
        {
            Assert.That(DecideTierScope("pull_request", "release/1.4", ["spec/a/A.tla"])["tlc_receipts"], Is.EqualTo("off"));
            var push = DecideTierScope("push", "", [], "release/1.4");
            Assert.That(push["tlc_receipts"], Is.EqualTo("off"));
            Assert.That(push["skip_tlc_reason"], Is.Empty, "a release-line push never skips exhaustive TLC");
            Assert.That(DecideTierScope("workflow_dispatch", "", null, "feat/x", tlcReceipts: "off")["tlc_receipts"], Is.EqualTo("off"));
            Assert.That(DecideTierScope("workflow_dispatch", "", null, "feat/x")["tlc_receipts"], Is.EqualTo("on"));
            Assert.That(DecideTierScope("pull_request", "main", ["spec/a/A.tla"])["tlc_receipts"], Is.EqualTo("on"));
        });
    }

    [Test]
    public void The_workflow_wires_receipts_through_plan_legs_and_aggregate()
    {
        var ci = Read(CiWorkflow);
        var verdict = VerdictStep();

        Assert.Multiple(() =>
        {
            Assert.That(ci, Does.Not.Contain("github.event.before"),
                "the push lane must not decide TLC from its own diff (#5187 review)");
            Assert.That(ci, Does.Contain("--tlc-receipts \"$TLC_RECEIPTS_INPUT\"")
                .And.Contain("TLC_RECEIPTS_INPUT: ${{ inputs.tlc_receipts || 'on' }}"));
            Assert.That(Regex.IsMatch(ci, @"(?m)^  workflow_dispatch:\s*\n    inputs:\s*\n      tlc_receipts:"), Is.True,
                "workflow_dispatch must carry the tlc_receipts switch");
            Assert.That(Regex.Matches(ci, Regex.Escape("uses: ./.github/actions/tlc-receipts")).Count, Is.EqualTo(3),
                "one lookup in the plan, one partial save per leg, one receipt save in the aggregate");
            Assert.That(ci, Does.Contain("--tlc-cached-modules \"$RUNNER_TEMP/tlc-plan/cached-modules.txt\""));
            Assert.That(ci, Does.Contain("tlc-receipts.py assemble").And.Contain("--require-complete"),
                "an incomplete module must fail the aggregate");
            Assert.That(Regex.IsMatch(verdict, @"push:[^)]*\)\s*[^;]*;\s*tlc_may_skip=true"), Is.False,
                "the verdict must not accept a TLC skip on any push");
        });
    }

    private static List<ReceiptItem> PlanWithReceipts(IEnumerable<string> cachedModules)
    {
        var work = Directory.CreateTempSubdirectory("receipts-");
        try
        {
            var cachedFile = Path.Combine(work.FullName, "cached-modules.txt");
            var censusFile = Path.Combine(work.FullName, "census.json");
            var categoriesFile = Path.Combine(work.FullName, "categories.json");
            File.WriteAllLines(cachedFile, cachedModules);
            File.WriteAllText(censusFile, TlcCaseCensus.Serialize(TlcCaseCensus.Build()));
            File.WriteAllText(categoriesFile, TlcCaseCensus.Serialize(TlcCaseCensus.BuildCategories()));

            var (exitCode, _, stderr) = RunPlanner(["lattice"], skipTiers: null, out var matrix,
                extraArguments: ["--tlc-cached-modules", cachedFile, "--tlc-census", censusFile, "--tlc-case-categories", categoriesFile]);
            Assert.That(exitCode, Is.Zero, "plan-test-matrix.py failed: " + stderr);

            using var document = JsonDocument.Parse(matrix);
            return document.RootElement.EnumerateArray()
                .SelectMany(leg => leg.GetProperty("items").EnumerateArray())
                .Select(item => new ReceiptItem(
                    item.GetProperty("label").GetString()!,
                    item.GetProperty("shard").GetString()!,
                    item.GetProperty("filter").ValueKind == JsonValueKind.String ? item.GetProperty("filter").GetString() : null,
                    item.TryGetProperty("tlc", out var tlc) && tlc.GetBoolean(),
                    item.TryGetProperty("tlcSmoke", out var smoke) && smoke.GetBoolean(),
                    item.TryGetProperty("skip", out var skip) ? skip.GetString() : null))
                .ToList();
        }
        finally
        {
            work.Delete(recursive: true);
        }
    }

    private static List<string> ReceiptPartitionProblems(IReadOnlySet<string> cached, IReadOnlyList<ReceiptItem> items)
    {
        var problems = new List<string>();
        var running = items.Where(item => item.Skip is null)
            .Select(item => (Item: item, Predicate: VsTestFilter.Compile(item.Filter)))
            .ToArray();

        foreach (var testCase in TlcCases())
        {
            var module = testCase.Categories
                .Where(category => category.StartsWith(TlcCiShard.ModuleCategoryPrefix, StringComparison.Ordinal))
                .Select(category => category[TlcCiShard.ModuleCategoryPrefix.Length..])
                .SingleOrDefault();
            var selecting = running.Where(entry => entry.Predicate(testCase)).Select(entry => entry.Item).ToArray();

            if (module is not null && cached.Contains(module))
            {
                if (selecting.Length > 0)
                {
                    problems.Add($"cached module {module}'s case {testCase.FullName} still runs in {selecting[0].Label}");
                }
            }
            else if (selecting.Length != 1)
            {
                problems.Add($"TLC case {testCase.FullName} is selected by {selecting.Length} running item(s), not exactly one");
            }
            else if (!selecting[0].Tlc && !selecting[0].Smoke)
            {
                problems.Add($"TLC case {testCase.FullName} is selected by non-TLC item {selecting[0].Label}");
            }
        }

        foreach (var item in items.Where(item => item.Skip is not null && item.Tlc))
        {
            if (!item.Skip!.StartsWith("verified by receipt", StringComparison.Ordinal))
            {
                problems.Add($"TLC item {item.Label} is skipped for '{item.Skip}', not by a receipt");
            }
        }

        return problems;
    }
}
