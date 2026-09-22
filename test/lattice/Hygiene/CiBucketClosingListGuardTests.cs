using System.Text.RegularExpressions;
using Orleans.Lattice.Testing.Hygiene;

namespace Orleans.Lattice.Tests.Hygiene;

/// <summary>
/// The bucket closing-list gate: an integration ("bucket") pull request into
/// the default branch must carry every closing reference its member pull
/// requests deliberately gave up, and the gate that checks it must itself be
/// executed.
/// <para>
/// GitHub honours a closing keyword only when the pull request targets the
/// default branch, so a member pull request based on a bucket is forbidden one
/// and records the deferral as <c>Refs #N</c> instead. That makes the bucket's
/// own pull request the single place the closure can happen. CI enforced the
/// member half (<c>Guard - inert closing keywords</c>) and nothing enforced the
/// bucket half, which is the half that actually closes the issues: on the live
/// bucket pull request #2482 the computed closing list named 324 issues and 22
/// completed ones were missing. The list did not break; it stopped being
/// extended, and a list that has stopped being extended renders identically to
/// a maintained one (issue #3320).
/// </para>
/// <para>
/// Scope, stated because overstating it is the failure mode this area keeps
/// producing: the gate is decidable only on artefacts that exist while the pull
/// request is OPEN - the merged member pull requests, their bodies, the closing
/// set GitHub computes for the bucket, and each referenced number's kind and
/// state. It deliberately does NOT assert that the issues are closed. That is
/// true only after the merge, so a predicate asking for it could never pass on
/// an open pull request and would block every bucket forever.
/// </para>
/// <para>
/// The last assertion here is the one that matters most and looks the least
/// like a test. The gate's behavioural coverage lives in
/// <c>bucket-closing-list-guard-selftest.py</c>, which drives the guard file CI
/// runs against a local fake GitHub. That fixture checks its own wiring - but
/// it is itself wired in the same workflow, so deleting both steps together
/// would leave nothing to report the loss. The wiring has to be held in place
/// by something outside the workflow it is wiring, which is what this fixture
/// is for.
/// </para>
/// </summary>
[TestFixture]
public sealed class CiBucketClosingListGuardTests
{
    private const string WorkflowPath = ".github/workflows/ci.yml";
    private const string GuardPath = ".github/workflows/bucket-closing-list-guard.py";
    private const string SelfTestPath = ".github/workflows/bucket-closing-list-guard-selftest.py";
    private const string GuardStep = "Guard - bucket closing list";

    /// <summary>
    /// The gate must fire on the bucket's shape - base is the default branch,
    /// head is an epic/bucket branch - and on no other.
    /// </summary>
    [Test]
    public void The_bucket_gate_fires_on_a_bucket_pull_request_and_no_other()
    {
        var step = Step(GuardStep);

        Assert.That(step, Does.Contain("base.ref == github.event.repository.default_branch"),
            $"the '{GuardStep}' step in {WorkflowPath} must require the base to be the "
            + "default branch. A closing keyword is honoured nowhere else, so demanding a "
            + "complete closing list at any other base would fail member pull requests for "
            + "omitting references that could not have worked there.");

        Assert.That(step, Does.Contain("'/epic/'"),
            $"the '{GuardStep}' step in {WorkflowPath} must require the head to be an "
            + "epic/bucket branch. Without it the gate would demand a closing list from "
            + "every ordinary pull request into the default branch, which carries no "
            + "members and would fail its own non-vacuity check on all of them.");
    }

    /// <summary>
    /// The verdict must be reconciled against the closing set GitHub computes,
    /// never against the body text the two disagree on.
    /// </summary>
    [Test]
    public void The_bucket_gate_reads_the_computed_closing_set_not_the_body_text()
    {
        var guard = Guard();

        Assert.That(guard, Does.Contain("closingIssuesReferences"),
            $"{GuardPath} must read the closing set GitHub itself computes. An oversized "
            + "pull-request body silently truncates GitHub's own parse to a clean prefix - "
            + "measured on this repository, a 136,402-character body had 229 of 234 "
            + "references honoured and a 24,570-character one had all 234, with no error "
            + "and no warning - so a body-reading check and a computed-set check disagree "
            + "on exactly the cases that matter.");

        Assert.That(guard, Does.Contain("hasNextPage"),
            $"{GuardPath} must page the computed closing set to exhaustion. A bucket of "
            + "this repository's size exceeds one page, and a gate that read only the "
            + "first would report every reference past it as missing.");
    }

    /// <summary>
    /// A population the gate could not discover must fail, not pass. The two
    /// are otherwise indistinguishable in the result.
    /// </summary>
    [Test]
    public void The_bucket_gate_fails_when_its_discovery_step_finds_nothing()
    {
        var guard = Guard();

        Assert.That(guard, Does.Contain("if not members:"),
            $"{GuardPath} must fail when it discovers no merged member pull request. "
            + "Reconciling a closing list against an empty population certifies any list "
            + "whatsoever, and silently certifying an empty population is precisely the "
            + "defect class this gate exists to catch - so a gate that did it would refute "
            + "itself.");

        Assert.That(guard, Does.Contain("if not claims:"),
            $"{GuardPath} must fail when no merged member records a 'Refs #N' claim. The "
            + "claims are the entire left-hand side of the reconciliation, so an empty "
            + "claim set means the comparison ran against nothing while still reporting a "
            + "member count that looks like work.");

        Assert.That(guard, Does.Contain("self_test("),
            $"{GuardPath} must drive its own predicate over planted controls before it "
            + "reaches the network. A clean bucket exercises the reference parser and the "
            + "region stripper on almost nothing, so their only guaranteed exercise on a "
            + "real run is the planted one.");
    }

    /// <summary>
    /// The gate's behavioural self-test must be invoked by the workflow. An
    /// uninvoked detector and a clean repository produce the same silence.
    /// </summary>
    [Test]
    public void The_bucket_gates_self_test_is_invoked_by_the_workflow()
    {
        var root = HygieneRepository.FindRepoRoot();

        Assert.That(File.Exists(Path.Combine(root, GuardPath.Replace('/', Path.DirectorySeparatorChar))),
            Is.True,
            $"expected {GuardPath} to exist; the '{GuardStep}' step runs this file, so "
            + "without it the step fails on every bucket pull request");

        Assert.That(File.Exists(Path.Combine(root, SelfTestPath.Replace('/', Path.DirectorySeparatorChar))),
            Is.True,
            $"expected {SelfTestPath} to exist; it is the only executable coverage of the "
            + $"'{GuardStep}' gate, which otherwise runs for the first time on the pull "
            + "request whose verdict is the thing in question");

        var yaml = File.ReadAllText(Path.Combine(
            root, WorkflowPath.Replace('/', Path.DirectorySeparatorChar)));

        Assert.That(yaml, Does.Contain(SelfTestPath),
            $"{WorkflowPath} must invoke {SelfTestPath}. That fixture checks its own wiring, "
            + "but it is wired in this same workflow, so removing both steps together would "
            + "report nothing. This assertion is outside the workflow for that reason.");
    }

    /// <summary>
    /// The gate needs read scopes the workflow-level block does not grant.
    /// </summary>
    [Test]
    public void The_bucket_gates_job_grants_the_read_scopes_it_needs()
    {
        var yaml = File.ReadAllText(Path.Combine(
            HygieneRepository.FindRepoRoot(),
            WorkflowPath.Replace('/', Path.DirectorySeparatorChar)));

        Assert.That(yaml, Does.Contain("pull-requests: read").And.Contain("issues: read"),
            $"{WorkflowPath} must grant the '{GuardStep}' step read access to pull requests "
            + "and issues. The workflow-level block grants only 'contents: read', and a "
            + "job-level block REPLACES it rather than adding to it, so both scopes have to "
            + "be stated explicitly or every API read the gate makes returns 404 - which it "
            + "reports as a failure, blocking every bucket pull request.");
    }

    /// <summary>
    /// Reads the guard script, failing rather than returning an empty string.
    /// </summary>
    private static string Guard()
    {
        var path = Path.Combine(
            HygieneRepository.FindRepoRoot(),
            GuardPath.Replace('/', Path.DirectorySeparatorChar));

        Assert.That(File.Exists(path), Is.True,
            $"expected {GuardPath}; without it every assertion here would be asserted "
            + "against nothing");

        var text = File.ReadAllText(path);

        Assert.That(text.Trim(), Is.Not.Empty, $"{GuardPath} is empty");

        return text;
    }

    /// <summary>
    /// Reads one workflow step by its <c>name:</c>, up to the next step at the
    /// same indentation. Fails rather than returning an empty string, because
    /// every assertion above would pass against one.
    /// </summary>
    private static string Step(string stepName)
    {
        var yaml = File.ReadAllText(Path.Combine(
            HygieneRepository.FindRepoRoot(),
            WorkflowPath.Replace('/', Path.DirectorySeparatorChar)));

        var start = Regex.Match(yaml, $@"^      - name: {Regex.Escape(stepName)}[ \t]*\r?$",
            RegexOptions.Multiline);

        Assert.That(start.Success, Is.True,
            $"expected a step named '{stepName}' in {WorkflowPath}. If it was renamed, this "
            + "fixture stopped guarding it and must be updated with it.");

        var rest = yaml[(start.Index + start.Length)..];
        var next = Regex.Match(rest, @"^      - (name|uses):", RegexOptions.Multiline);
        var block = next.Success ? rest[..next.Index] : rest;

        Assert.That(block.Trim(), Is.Not.Empty,
            $"the step named '{stepName}' in {WorkflowPath} has an empty body, so nothing "
            + "asserted about it means anything");

        return block;
    }
}
