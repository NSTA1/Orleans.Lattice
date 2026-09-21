using System.Text.RegularExpressions;
using Orleans.Lattice.Testing.Hygiene;

namespace Orleans.Lattice.Tests.Hygiene;

/// <summary>
/// The inert-closing-keyword gate: a closing keyword that GitHub will not
/// honour must be reported wherever it is written, and the gate that reports it
/// must itself be executed.
/// <para>
/// GitHub acts on a closing keyword only when the pull request targets the
/// default branch, so on a member pull request based on an epic or bucket
/// branch a <c>Closes #N</c> is inert: the pull request merges, reads as
/// correct, and leaves the issue open. The gate in <c>ci.yml</c> caught that in
/// the pull-request BODY and nowhere else, so the same keyword written in a
/// COMMIT MESSAGE was doubly inert - GitHub ignored it, and reviewing the body
/// would not reveal it (issue #3024).
/// </para>
/// <para>
/// Scope, stated because overstating it would be the defect this epic keeps
/// finding: across the 200 merged member pull requests on this integration
/// branch, zero carried a closing keyword in their own commits and three
/// carried one in the squash message composed at merge time. A squash message
/// does not exist until the merge is performed, so no pull-request-time check
/// can read it. The commit arm closes a real and previously unguarded surface;
/// it is not the surface those three came from.
/// </para>
/// <para>
/// The last assertion here is the one that matters most and looks the least
/// like a test. The gate's behavioural coverage lives in
/// <c>closing-keyword-guard-selftest.py</c>, which extracts the gate from
/// <c>ci.yml</c> unmodified and drives both arms against real git repositories.
/// A self-test that is not invoked is indistinguishable from a clean gate - it
/// reports nothing either way - so the wiring that invokes it has to be held in
/// place by something outside the workflow it is wiring.
/// </para>
/// </summary>
[TestFixture]
public sealed class CiClosingKeywordGuardTests
{
    private const string WorkflowPath = ".github/workflows/ci.yml";
    private const string SelfTestPath = ".github/workflows/closing-keyword-guard-selftest.py";
    private const string GuardStep = "Guard - inert closing keywords";

    /// <summary>
    /// The gate must read the pull request's own commit messages, addressed by
    /// an explicit range rather than a fixed count back from HEAD.
    /// </summary>
    [Test]
    public void The_closing_keyword_gate_scans_the_pull_requests_own_commit_messages()
    {
        var step = Step(GuardStep);

        Assert.That(step, Does.Contain("honoured("),
            $"expected the '{GuardStep}' step in {WorkflowPath} to define its predicate; "
            + "this fixture found the step but not the predicate, so every assertion "
            + "below would have been asserted against the wrong text");

        Assert.That(step, Does.Contain("rev-list"),
            $"the '{GuardStep}' step in {WorkflowPath} must enumerate the pull request's "
            + "own commits. Without it the gate reads the body alone, and a closing "
            + "keyword in a commit message stays doubly inert: GitHub will not honour it "
            + "at this base, and reviewing the body will not reveal it.");

        Assert.That(step, Does.Contain("BASE_SHA").And.Contain("HEAD_SHA"),
            $"the '{GuardStep}' step in {WorkflowPath} must address its commit range with "
            + "the event's base and head shas. On a pull_request event HEAD is the "
            + "synthetic merge commit, so a range anchored on HEAD would scan the base's "
            + "settled history or miss the commits this pull request adds.");

        Assert.That(step, Does.Contain("--format=%B"),
            $"the '{GuardStep}' step in {WorkflowPath} must read the FULL commit message. "
            + "Every closing keyword in this repository's history sat in a commit BODY, so "
            + "a subject-only read would scan past all of them while still reporting a "
            + "commit count that looks like work.");
    }

    /// <summary>
    /// One predicate, shared. Two matchers that must agree will drift, and the
    /// drift would appear as one arm accepting what the other rejects.
    /// </summary>
    [Test]
    public void Both_arms_of_the_closing_keyword_gate_share_one_predicate()
    {
        var step = Step(GuardStep);

        var definitions = Regex.Matches(step, @"^\s*def honoured\(", RegexOptions.Multiline);

        Assert.That(definitions.Count, Is.EqualTo(1),
            $"the '{GuardStep}' step in {WorkflowPath} must define honoured() exactly once "
            + $"and call it from both arms, but it defines it {definitions.Count} times. "
            + "Two parsers that must agree with GitHub, and with each other, will drift.");
    }

    /// <summary>
    /// An unread commit population must fail, not pass. It is otherwise
    /// indistinguishable in the result from a clean one.
    /// </summary>
    [Test]
    public void The_closing_keyword_gate_fails_when_it_reads_no_commits()
    {
        var step = Step(GuardStep);

        Assert.That(step, Does.Contain("if not messages:"),
            $"the '{GuardStep}' step in {WorkflowPath} must fail when its commit range "
            + "resolves to zero commits. Every pull request adds at least one, so an empty "
            + "range is a broken range - and reporting it as clean is exactly the defect "
            + "this gate exists to prevent.");

        Assert.That(step, Does.Contain("<control-honoured>"),
            $"the '{GuardStep}' step in {WorkflowPath} must drive its real commit scanner "
            + "over planted control messages on every run. The predicate's own planted "
            + "probes prove honoured() still discriminates; they cannot prove that anything "
            + "still calls it with a commit message, and a clean pull request exercises the "
            + "scanner on nothing.");
    }

    /// <summary>
    /// The gate's behavioural self-test must be invoked by the workflow. An
    /// uninvoked detector and a clean repository produce the same silence.
    /// </summary>
    [Test]
    public void The_closing_keyword_gates_self_test_is_invoked_by_the_workflow()
    {
        var root = HygieneRepository.FindRepoRoot();

        Assert.That(File.Exists(Path.Combine(root, SelfTestPath.Replace('/', Path.DirectorySeparatorChar))),
            Is.True,
            $"expected {SelfTestPath} to exist; it is the only executable coverage of the "
            + $"'{GuardStep}' gate, which is an inline heredoc with no importable surface");

        var yaml = File.ReadAllText(Path.Combine(
            root, WorkflowPath.Replace('/', Path.DirectorySeparatorChar)));

        Assert.That(yaml, Does.Contain(SelfTestPath),
            $"{WorkflowPath} must invoke {SelfTestPath}. A self-test that is never run "
            + $"reports nothing whether the gate is healthy or blind, so leaving it "
            + "unwired would remove the gate's only coverage without removing any green.");
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
