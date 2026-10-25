namespace Orleans.Lattice.Tests.Hygiene;

public sealed partial class CiMemberPullRequestTieringTests
{
    [Test]
    public void Declared_TLC_items_preserve_the_job_timeout_without_disabling_ordinary_hang_detection()
    {
        var result = RunPython([Path.Combine(RepoRoot, WorkflowDirectory, "tlc-watchdog-selftest.py")]);

        Assert.That(result.ExitCode, Is.Zero, result.StandardOutput + result.StandardError);
        Assert.That(
            result.StandardError,
            Does.Match(@"Ran [1-9][0-9]* tests"),
            "the watchdog regression driver must execute a non-empty suite");
    }
}
