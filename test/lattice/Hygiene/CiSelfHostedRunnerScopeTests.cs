using System.Text.RegularExpressions;
using Orleans.Lattice.Testing.Hygiene;

namespace Orleans.Lattice.Tests.Hygiene;

/// <summary>
/// The elastic self-hosted runner pool (<c>tools/ci-runners</c>) must only ever run trusted
/// code. The repository is public and belongs to a user account, so runners cannot be
/// restricted to chosen workflows the way an organisation runner group can: any workflow
/// that names the pool's labels can reach it. A <c>pull_request</c> run executes the
/// workflow file from the pull request itself, so a fork could otherwise send its own code
/// to the pool. This fixture holds the rule at the narrowest seam: a workflow that can
/// target the pool, through the <c>TLC_RUNNER</c> variable or a <c>self-hosted</c> label,
/// must not trigger on <c>pull_request</c> or <c>pull_request_target</c>.
/// </summary>
[TestFixture]
public sealed class CiSelfHostedRunnerScopeTests
{
    private static readonly Regex PoolReference = new(@"vars\.TLC_RUNNER|self-hosted", RegexOptions.Compiled);

    private static readonly Regex PullRequestTrigger = new(
        @"^on:\s*\[?[^\r\n]*\bpull_request(_target)?\b|^\s{2}pull_request(_target)?\s*:",
        RegexOptions.Multiline | RegexOptions.Compiled);

    /// <summary>
    /// No workflow that can target the self-hosted pool may run on a pull request trigger.
    /// </summary>
    [Test]
    public void Only_workflows_without_pull_request_triggers_may_target_the_self_hosted_pool()
    {
        var workflows = Directory.GetFiles(
            Path.Combine(HygieneRepository.FindRepoRoot(), ".github", "workflows"), "*.yml");

        var targeting = workflows
            .Select(path => (Name: Path.GetFileName(path), Text: File.ReadAllText(path)))
            .Where(w => PoolReference.IsMatch(StripComments(w.Text)))
            .ToList();

        Assert.That(targeting.Select(w => w.Name), Is.SupersetOf(new[] { "coverage.yml", "formal-receipts.yml" }),
            "expected the two trusted TLC workflows to opt in to the pool; if they no longer do, "
            + "this fixture is guarding nothing and must be revisited");

        Assert.Multiple(() =>
        {
            foreach (var (name, text) in targeting)
            {
                Assert.That(PullRequestTrigger.IsMatch(text), Is.False,
                    $"{name} can target the self-hosted runner pool and triggers on a pull request. "
                    + "A pull request run executes the workflow file from the pull request, so a fork "
                    + "could send its own code to the pool. Keep pool jobs in push-to-main, schedule "
                    + "and dispatch workflows only (tools/ci-runners/README.md).");
            }
        });
    }

    /// <summary>
    /// The trigger regex must recognise every form a pull request trigger can take, and
    /// must not match a workflow without one.
    /// </summary>
    [TestCase("on:\n  pull_request:\n    branches: [main]\n", true)]
    [TestCase("on:\n  pull_request_target:\n", true)]
    [TestCase("on: [push, pull_request]\n", true)]
    [TestCase("on: pull_request\n", true)]
    [TestCase("on:\n  push:\n    branches: [main]\n  schedule:\n    - cron: '0 3 * * *'\n", false)]
    [TestCase("on:\n  workflow_dispatch:\n", false)]
    public void The_pull_request_trigger_pattern_matches_every_trigger_form(string yaml, bool matches)
    {
        Assert.That(PullRequestTrigger.IsMatch(yaml), Is.EqualTo(matches));
    }

    private static string StripComments(string yaml) =>
        Regex.Replace(yaml, @"(?m)^\s*#.*$", string.Empty);
}
