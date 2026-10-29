using System.Text.Json;
using System.Text.RegularExpressions;
using Orleans.Lattice.Testing.Hygiene;

namespace Orleans.Lattice.Tests.Hygiene;

/// <summary>
/// The receipt warmer's contract (#5165, epic #5148): <c>formal-receipts.yml</c> is the
/// writer of TLC verdict receipts in <c>main</c>'s cache scope. It runs on every push to
/// <c>main</c> and on demand, never on a pull request; it queues and never cancels; its
/// token is read-only; and it is classified as routing in <c>tlc-inputs.json</c>.
/// <para>
/// <b>Why it exists.</b> An Actions cache written by a pull request is visible only to that
/// pull request, and <c>ci.yml</c> does not run on pushes to <c>main</c>. Without a writer
/// in <c>main</c>'s scope, every pull request opened after a spec merge would rerun the
/// merged module until some <c>main</c> run saved its receipt.
/// </para>
/// <para>
/// <b>Why a guard.</b> Every property pinned here fails silently. A pull-request trigger
/// would write receipts into a scope nobody else reads and pay TLC twice. A cancelled run
/// loses the receipts it was about to save and shows a red cross on a <c>main</c> commit
/// although nothing failed, and <c>cancel-in-progress: true</c> is the idiom of most other
/// lanes, so it is exactly what a tidying edit would introduce. A write grant on the token
/// would let a lane that runs repository code on every merge write to the repository.
/// </para>
/// </summary>
[TestFixture]
public sealed class CiFormalReceiptsLaneTests
{
    private const string Workflow = ".github/workflows/formal-receipts.yml";

    private const string TlcInputs = ".github/workflows/tlc-inputs.json";

    /// <summary>
    /// The lane runs on pushes to <c>main</c> and by hand, and on nothing else.
    /// </summary>
    [Test]
    public void The_receipt_lane_runs_on_pushes_to_main_and_by_hand_only()
    {
        var triggers = TopLevelBlock(Read(), "on");

        Assert.Multiple(() =>
        {
            Assert.That(triggers, Does.Match(@"(?m)^  push:\s*\r?\n    branches:\s*\[\s*main\s*\]\s*\r?$"),
                $"{Workflow} must run on every push to main, and only main: receipts it saves are "
                + "read by every pull request only because they are written in main's cache scope");

            Assert.That(triggers, Does.Match(@"(?m)^  workflow_dispatch:"),
                $"{Workflow} must keep 'workflow_dispatch:' to re-warm main after cache eviction");

            Assert.That(triggers, Does.Not.Match(@"(?m)^  (pull_request|pull_request_target|schedule|workflow_run):"),
                $"{Workflow} must not run on pull requests, whose caches no other run can read, nor "
                + "on a schedule: the nightly Coverage TLC legs are the scheduled writer");

            Assert.That(Regex.Matches(triggers, @"(?m)^  [a-z_]+:").Count, Is.EqualTo(2),
                $"{Workflow} must declare exactly two triggers, push and workflow_dispatch");
        });
    }

    /// <summary>
    /// The token can read the repository and list caches, and nothing more: no job widens it.
    /// </summary>
    [Test]
    public void The_receipt_lane_holds_a_read_only_token()
    {
        var yaml = Read();
        var permissions = TopLevelBlock(yaml, "permissions");
        var grants = Regex.Matches(permissions, @"(?m)^  (?<scope>[a-z-]+):\s*(?<level>\S+)\s*$")
            .Select(match => $"{match.Groups["scope"].Value}: {match.Groups["level"].Value}")
            .Order(StringComparer.Ordinal)
            .ToArray();

        Assert.Multiple(() =>
        {
            Assert.That(grants, Is.EqualTo(new[] { "actions: read", "contents: read" }),
                $"{Workflow} must grant exactly 'contents: read' and 'actions: read' (to list the "
                + "receipt cache entries). The cache actions authenticate with the runner's own "
                + "token and need no GITHUB_TOKEN scope.");

            Assert.That(Regex.Matches(yaml, @"(?m)^[ \t]+permissions:").Count, Is.Zero,
                $"no job in {Workflow} may declare its own permissions: it runs repository code on "
                + "every merge to main, so the workflow-level read-only grant is the whole grant");

            Assert.That(yaml, Does.Not.Match(@"(?m)^\s*[a-z-]+:\s*write\s*$"),
                $"nothing in {Workflow} may hold a write grant");
        });
    }

    /// <summary>
    /// The lane queues every run in one group and never cancels one.
    /// </summary>
    [Test]
    public void The_receipt_lane_queues_and_never_cancels_a_run()
    {
        var yaml = Read();
        var concurrency = TopLevelBlock(yaml, "concurrency");

        Assert.Multiple(() =>
        {
            Assert.That(concurrency, Does.Match(@"(?m)^  group:\s*formal-receipts-\$\{\{\s*github\.ref\s*\}\}\s*$"),
                $"{Workflow} must serialise its runs in one group per ref, so two pushes do not run "
                + "the same modules side by side");

            Assert.That(concurrency, Does.Match(@"(?m)^  cancel-in-progress:\s*false\s*$"),
                $"{Workflow} must not cancel an in-progress run: it would lose the receipts that run "
                + "was about to save, and a cancelled run rolls up as a red cross on its main commit");

            Assert.That(concurrency, Does.Match(@"(?m)^  queue:\s*max\s*$"),
                $"{Workflow} must set 'queue: max'. Without it a concurrency group holds one pending "
                + "run, and a newer push replaces it, which cancels it.");

            Assert.That(Regex.IsMatch(yaml, @"^\s*cancel-in-progress:(?!\s*false\s*$)", RegexOptions.Multiline),
                Is.False,
                $"{Workflow} must not cancel in-progress runs anywhere, at the workflow or a job level");
        });
    }

    /// <summary>
    /// The lane fingerprints at the pushed commit, reads only main's receipts (plus the
    /// dispatched ref), runs only the planned legs with the shared toolchain, and writes and
    /// saves receipts after them from this run's evidence alone.
    /// </summary>
    [Test]
    public void The_receipt_lane_runs_tlc_only_for_modules_without_a_receipt_and_saves_theirs()
    {
        var yaml = Read();
        var plan = Job(yaml, "plan");
        var tlc = Job(yaml, "tlc");
        var receipts = Job(yaml, "receipts");

        Assert.Multiple(() =>
        {
            Assert.That(plan, Does.Contain("python3 .github/workflows/tlc-fingerprint.py --output"),
                "the plan job must fingerprint every module (contract C4) at the pushed commit");

            Assert.That(plan, Does.Contain("tlc-receipts.py candidates")
                .And.Contain("refs=\"refs/heads/${DEFAULT_BRANCH}\""),
                "the receipt listing must read main's scope; a receipt from any other scope could "
                + "have been planted by a pull request");

            Assert.That(plan, Does.Contain("uses: ./.github/actions/tlc-receipts")
                    .And.Contain("operation: lookup").And.Not.Contain("ignore-receipts"),
                "the plan must restore and validate main's receipts through the shared action, and "
                + "run only the modules without one");

            Assert.That(plan, Does.Contain("plan-coverage-matrix.py").And.Contain("--tlc-modules"),
                "the TLC legs must be narrowed to the modules without a receipt");

            Assert.That(plan, Does.Match(@"(?m)^      leg_count:\s*\$\{\{\s*steps\.matrix\.outputs\.leg_count\s*\|\|\s*'0'\s*\}\}\s*$"),
                "a push whose modules all hold receipts must plan zero legs, not an unset count");

            Assert.That(tlc, Does.Contain("fromJson(needs.plan.outputs.legs)")
                .And.Contain("needs.plan.outputs.leg_count != '0'"),
                "the TLC job must fan out over the planned legs and skip when every module has a receipt");

            Assert.That(tlc, Does.Match(@"(?m)^    timeout-minutes:\s*\$\{\{\s*matrix\.leg\.timeout\s*\}\}\s*$"),
                "a TLC-only leg has no inactivity collector, so its job timeout must come from the planner");

            Assert.That(tlc, Does.Contain("uses: ./.github/actions/tla-toolchain"),
                "the TLC legs must provision the toolchain through the one shared action");

            Assert.That(tlc, Does.Contain("python3 .github/workflows/run-test-leg.py"),
                "the TLC legs must run through run-test-leg.py, which writes the evidence receipts "
                + "are assembled from");

            Assert.That(tlc, Does.Not.Contain("tlc-receipt").And.Not.Contain("actions/cache"),
                "a TLC leg must never read a receipt or a cache itself; the plan decides what runs");

            Assert.That(receipts, Does.Match(@"(?m)^    needs:\s*\[\s*plan,\s*tlc\s*\]\s*$"),
                "receipts must be written only after the TLC legs");

            Assert.That(receipts, Does.Match(@"(?m)^    if:.*!cancelled\(\)"),
                "a partly red run must still save the receipts of the modules that passed");

            Assert.That(receipts, Does.Contain("pattern: tlc-results-${{ github.run_attempt }}-*"),
                "a receipt must come from one attempt's evidence, never a mix of attempts");

            Assert.That(receipts, Does.Contain("tlc-receipts.py assemble").And.Contain("--require-complete")
                .And.Contain("uses: ./.github/actions/tlc-receipts").And.Contain("operation: save"),
                "the receipts job must write and save receipts through the shared C5 tooling");

            Assert.That(receipts, Does.Contain("summarise-test-legs.py").And.Contain("--expect-legs"),
                "the receipts job must judge every planned leg, so a leg that died reads as a failure");
        });
    }

    /// <summary>
    /// The workflow is classified as routing: receipts decide which modules later runs skip,
    /// but cannot change a verdict.
    /// </summary>
    [Test]
    public void The_receipt_lane_is_classified_as_routing()
    {
        using var inputs = JsonDocument.Parse(Read(TlcInputs));
        var routing = inputs.RootElement.GetProperty("routing").EnumerateArray()
            .Select(element => element.GetString())
            .ToArray();

        Assert.That(routing, Does.Contain(Workflow),
            $"{Workflow} must be listed in 'routing' in {TlcInputs}: it decides which modules run, "
            + "not what a module's verdict is");
    }

    /// <summary>
    /// Reads a top-level block up to the next line that starts in the first column. Fails
    /// rather than returning an empty string, because every assertion above would pass
    /// against one.
    /// </summary>
    private static string TopLevelBlock(string yaml, string key)
    {
        var start = Regex.Match(yaml, $@"^{Regex.Escape(key)}:[ \t]*\r?$", RegexOptions.Multiline);

        Assert.That(start.Success, Is.True,
            $"expected a top-level '{key}:' block in {Workflow}; if it moved, this fixture "
            + "stopped guarding it and must be updated with it");

        var rest = yaml[(start.Index + start.Length)..];
        var end = Regex.Match(rest, @"^\S", RegexOptions.Multiline);
        var block = end.Success ? rest[..end.Index] : rest;

        Assert.That(block.Trim(), Is.Not.Empty,
            $"the '{key}:' block in {Workflow} is empty, so nothing asserted about it means anything");

        return block;
    }

    /// <summary>
    /// Reads one job's block by its id, up to the next job or the end of the file. Fails
    /// rather than returning an empty string, for the same reason.
    /// </summary>
    private static string Job(string yaml, string jobId)
    {
        var start = Regex.Match(yaml, $@"^  {Regex.Escape(jobId)}:[ \t]*\r?$", RegexOptions.Multiline);

        Assert.That(start.Success, Is.True,
            $"expected a job with id '{jobId}' in {Workflow}. If it was renamed, this fixture "
            + "stopped guarding it and must be updated with it.");

        var rest = yaml[(start.Index + start.Length)..];
        var next = Regex.Match(rest, @"^  [A-Za-z0-9_-]+:[ \t]*\r?$|^\S", RegexOptions.Multiline);
        var block = next.Success ? rest[..next.Index] : rest;

        Assert.That(block.Trim(), Is.Not.Empty,
            $"the job '{jobId}' in {Workflow} has an empty body, so nothing asserted about it "
            + "means anything");

        return block;
    }

    private static string Read(string file = Workflow) =>
        File.ReadAllText(Path.Combine(
            HygieneRepository.FindRepoRoot(),
            file.Replace('/', Path.DirectorySeparatorChar)));
}
