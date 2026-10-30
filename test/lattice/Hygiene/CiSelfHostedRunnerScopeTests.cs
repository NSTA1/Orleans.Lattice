using System.Text.RegularExpressions;
using Orleans.Lattice.Testing.Hygiene;

namespace Orleans.Lattice.Tests.Hygiene;

/// <summary>
/// Which workflows may send work to the elastic self-hosted runner pool
/// (<c>tools/ci-runners</c>). Pull-request legs overflow to the pool when the hosted
/// runners are busy (<c>runner-overflow.py</c>), which is safe because the pool's
/// runners are single-use containers holding no secrets and fork runs need the owner's
/// approval. <c>pull_request_target</c> is another matter: it runs with the base
/// repository's secrets and a write-capable token, and a workflow that checks out the
/// pull request there would hand that context to fork code. So a workflow that can
/// target the pool must never trigger on <c>pull_request_target</c>.
/// </summary>
[TestFixture]
public sealed class CiSelfHostedRunnerScopeTests
{
    private static readonly Regex PoolReference = new(@"vars\.RUNNER_POOL|matrix\.leg\.runsOn|self-hosted", RegexOptions.Compiled);

    private static readonly Regex PullRequestTargetTrigger = new(
        @"^on:\s*\[?[^\r\n]*\bpull_request_target\b|^\s{2}pull_request_target\s*:",
        RegexOptions.Multiline | RegexOptions.Compiled);

    /// <summary>
    /// No workflow that can target the self-hosted pool may run on <c>pull_request_target</c>.
    /// </summary>
    [Test]
    public void Workflows_that_can_target_the_self_hosted_pool_never_trigger_on_pull_request_target()
    {
        var workflows = Directory.GetFiles(
            Path.Combine(HygieneRepository.FindRepoRoot(), ".github", "workflows"), "*.yml");

        var targeting = workflows
            .Select(path => (Name: Path.GetFileName(path), Text: File.ReadAllText(path)))
            .Where(w => PoolReference.IsMatch(StripComments(w.Text)))
            .ToList();

        Assert.That(targeting.Select(w => w.Name), Is.SupersetOf(new[] { "ci.yml", "coverage.yml", "formal-receipts.yml" }),
            "expected the three matrix lanes to route legs through runner-overflow.py; if they no longer do, "
            + "this fixture is guarding nothing and must be revisited");

        Assert.Multiple(() =>
        {
            foreach (var (name, text) in targeting)
            {
                Assert.That(PullRequestTargetTrigger.IsMatch(text), Is.False,
                    $"{name} can target the self-hosted runner pool and triggers on pull_request_target, "
                    + "which runs with the base repository's secrets and a write-capable token. "
                    + "See tools/ci-runners/README.md.");
            }
        });
    }

    /// <summary>
    /// Every leg job that routes through the plan must also skip the Azurite service on
    /// the pool: Container Apps runs no Docker, so a service container would fail the job.
    /// </summary>
    [TestCase("ci.yml")]
    [TestCase("coverage.yml")]
    public void A_routed_leg_with_an_Azurite_service_skips_it_on_the_pool(string workflow)
    {
        var text = File.ReadAllText(Path.Combine(HygieneRepository.FindRepoRoot(), ".github", "workflows", workflow));

        Assert.Multiple(() =>
        {
            Assert.That(text, Does.Contain("image: ${{ matrix.leg.pool && '' || 'mcr.microsoft.com/azure-storage/azurite:"),
                $"{workflow}: the Azurite service image must be empty on a pool leg");
            Assert.That(text, Does.Contain("- name: Start Azurite on the self-hosted pool"),
                $"{workflow}: a pool leg must start the Azurite baked into the pool image");
            Assert.That(text, Does.Not.Contain("vars.TLC_RUNNER"),
                $"{workflow}: TLC_RUNNER was replaced by runner-overflow.py routing");
        });
    }

    /// <summary>
    /// The trigger regex must recognise every form a <c>pull_request_target</c> trigger can
    /// take, and must not match plain <c>pull_request</c>.
    /// </summary>
    [TestCase("on:\n  pull_request_target:\n", true)]
    [TestCase("on: [push, pull_request_target]\n", true)]
    [TestCase("on: pull_request_target\n", true)]
    [TestCase("on:\n  pull_request:\n    branches: [main]\n", false)]
    [TestCase("on: [push, pull_request]\n", false)]
    [TestCase("on:\n  workflow_dispatch:\n", false)]
    public void The_pull_request_target_pattern_matches_every_trigger_form(string yaml, bool matches)
    {
        Assert.That(PullRequestTargetTrigger.IsMatch(yaml), Is.EqualTo(matches));
    }

    private static string StripComments(string yaml) =>
        Regex.Replace(yaml, @"(?m)^\s*#.*$", string.Empty);
}
