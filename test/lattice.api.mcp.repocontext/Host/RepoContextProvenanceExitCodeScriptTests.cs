using System.Diagnostics;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Host;

/// <summary>
/// Runs the exit-status coverage for the provenance acquisition script
/// (<c>samples/RepoContextContainer/scripts/Test-ProvenanceExitCode.ps1</c>) as part of the
/// ordinary non-chaos suite, so its assertions are gated rather than merely present.
/// <para>
/// This is the third leg of the provenance coverage. <see cref="RepoContextContainerProvenanceScriptTests"/>
/// adjudicates the pure verdict and <see cref="RepoContextArchiveGitReadingScriptTests"/>
/// adjudicates the impure git reader; neither ever RUNS <c>Assert-ContainerProvenance.ps1</c>,
/// so for a whole release neither could see that the script printed a perfect verdict and
/// reported the opposite of it to any caller gating on <c>$LASTEXITCODE -eq 0</c> (issue
/// #2718). The exit code is a property of the script as a whole, so only an end-to-end run
/// can observe it.
/// </para>
/// <para>
/// The suite needs git (its sandbox checkout is a real repository, and the archive probe it
/// depends on is a real <c>fatal: not a git repository</c>), so its execution leg skips where
/// git or a PowerShell host is absent. That skip is an NUnit <c>Ignore</c>, never a pass. The
/// structural assertion NEVER skips.
/// </para>
/// <para>
/// It does NOT need docker: the suite places a shim first on PATH and serves canned readings
/// from files. That is deliberate. Requiring a specific live deployment is what left this
/// script's exit code untested in the first place, and a guard that can only run where the
/// thing it guards is already deployed is not a guard.
/// </para>
/// </summary>
[TestFixture]
public sealed class RepoContextProvenanceExitCodeScriptTests
{
    /// <summary>
    /// A floor, not a count. It sits below the suite's real size so ordinary additions and
    /// removals do not touch it, while a suite emptied - or cut to a token assertion to turn a
    /// red leg green - still fails.
    /// </summary>
    private const int MinimumAssertions = 20;

    /// <summary>
    /// The distinct exit code the suite uses to announce "git absent". Kept separate from a
    /// failing-assertion count (which is the exit code otherwise) so a skip can never be read
    /// as a pass or as a single failure.
    /// </summary>
    private const int GitAbsentExitCode = 3;

    private static string RepoRoot => Path.GetFullPath(
        Path.Combine(TestContext.CurrentContext.TestDirectory, "..", "..", "..", "..", ".."));

    private static string ScriptsDirectory => Path.Combine(
        RepoRoot, "samples", "RepoContextContainer", "scripts");

    private static string SuitePath => Path.Combine(ScriptsDirectory, "Test-ProvenanceExitCode.ps1");

    private static int CountAssertions() =>
        File.ReadAllLines(SuitePath).Count(l => l.TrimStart().StartsWith("_Assert -Name", StringComparison.Ordinal));

    [Test]
    public void The_provenance_exit_code_suite_still_carries_its_assertions()
    {
        // Runs on every host, including one with no PowerShell or git, so the fixture cannot
        // go quiet at the same moment the suite does.
        Assert.That(File.Exists(SuitePath), Is.True, $"expected the provenance exit code suite at {SuitePath}");
        Assert.That(
            File.Exists(Path.Combine(ScriptsDirectory, "Assert-ContainerProvenance.ps1")),
            Is.True,
            "expected the acquisition script whose exit status the suite runs end to end");

        Assert.That(
            CountAssertions(),
            Is.GreaterThanOrEqualTo(MinimumAssertions),
            $"the provenance exit code suite carries fewer than {MinimumAssertions} assertions, which means "
                + "it has been gutted rather than trimmed");
    }

    [Test]
    public void The_provenance_exit_code_suite_passes()
    {
        var shell = FindExecutable("pwsh") ?? FindExecutable("powershell");
        if (shell is null)
        {
            Assert.Ignore("Neither pwsh nor powershell is available on this host.");
        }

        // git absence is a LOUD, reported skip. The suite cannot build its sandbox without it,
        // and a guard that ran with nothing to examine while reporting green is the exact
        // false-green this bucket exists to catch.
        if (FindExecutable("git") is null)
        {
            Assert.Ignore("git is not available on this host, so the exit-code suite cannot build its sandbox.");
        }

        var psi = new ProcessStartInfo(shell!, $"-NoProfile -File \"{SuitePath}\"")
        {
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
            WorkingDirectory = ScriptsDirectory,
        };

        using var process = Process.Start(psi)!;
        var stdout = process.StandardOutput.ReadToEnd();
        var stderr = process.StandardError.ReadToEnd();
        process.WaitForExit(180_000);

        if (process.ExitCode == GitAbsentExitCode && stdout.Contains("SKIPPED: git", StringComparison.Ordinal))
        {
            Assert.Ignore("The suite reported git absent at run time." + Environment.NewLine + stdout + stderr);
        }

        // The tally is asserted BEFORE the exit code because a terminating PowerShell error
        // also exits 1, and read naively that says "one assertion failed" - a precise,
        // plausible, wrong reading of a run that never reached its first check. A fixture whose
        // whole subject is an exit code that lied must not emit one itself.
        var tally = stdout
            .Split('\n')
            .Select(l => l.Trim())
            .LastOrDefault(l => l.StartsWith("Total ", StringComparison.Ordinal));

        Assert.That(
            tally,
            Is.Not.Null,
            "the provenance exit code suite printed no tally, so it did not reach the end of its run. "
                + "Its exit code is NOT a failure count in this state - suspect a terminating error, most "
                + "likely the docker shim failing to serve a fixture or the sandbox checkout failing to "
                + "initialise."
                + Environment.NewLine
                + stdout
                + stderr);

        Assert.That(
            process.ExitCode,
            Is.Zero,
            $"the provenance exit code suite reported {process.ExitCode} failing assertion(s)."
                + Environment.NewLine
                + stdout
                + stderr);

        Assert.That(tally, Does.Contain("Failed 0"));

        // A run that stopped early exits zero and prints a clean-looking tally with a smaller
        // Total, which is precisely the plausible-but-unreliable artefact this fixture refuses.
        var total = int.Parse(
            tally!.Split(' ', StringSplitOptions.RemoveEmptyEntries)[1],
            System.Globalization.CultureInfo.InvariantCulture);

        Assert.That(
            total,
            Is.GreaterThanOrEqualTo(CountAssertions()),
            $"the suite ran {total} assertions but the file declares {CountAssertions()}, so the run ended "
                + "before the last one");
    }

    private static string? FindExecutable(string name)
    {
        var extensions = OperatingSystem.IsWindows()
            ? new[] { ".exe", ".cmd", ".bat" }
            : new[] { string.Empty };

        foreach (var directory in (Environment.GetEnvironmentVariable("PATH") ?? string.Empty)
            .Split(Path.PathSeparator, StringSplitOptions.RemoveEmptyEntries))
        {
            foreach (var extension in extensions)
            {
                try
                {
                    var candidate = Path.Combine(directory.Trim('"'), name + extension);
                    if (File.Exists(candidate))
                    {
                        return candidate;
                    }
                }
                catch (ArgumentException)
                {
                    // A malformed PATH entry is not this fixture's problem.
                }
            }
        }

        return null;
    }
}
