using System.Diagnostics;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Host;

/// <summary>
/// Runs the git-dependent half of the archive durability coverage
/// (<c>samples/RepoContextContainer/scripts/Test-ArchiveGitReading.ps1</c>) as part of the
/// ordinary non-chaos suite, so its assertions are gated rather than merely present.
/// <para>
/// This is the companion to <see cref="RepoContextContainerProvenanceScriptTests"/>. That
/// suite is deliberately git-free: it drives the pure predicate
/// <c>Test-GitReadingIsExaminable</c> against stderr strings captured once by hand. This
/// suite closes the two gaps that leaves - whether those pinned strings still describe the
/// installed git, and whether the impure <c>Get-ArchiveGitReading</c> wires real git output
/// through to the predicate faithfully - and to do that it SHELLS OUT to a real git binary
/// and builds real filesystem states.
/// </para>
/// <para>
/// Because it needs git, its execution leg skips where git or a PowerShell host is absent.
/// That skip is reported as an NUnit <c>Ignore</c>, never as a pass: a conformance guard
/// that goes quietly green when it examined nothing is the exact false-green this bucket
/// exists to catch. The structural assertion NEVER skips - it runs on every host and fails
/// if the suite has lost its assertions, whether or not it could be executed.
/// </para>
/// </summary>
[TestFixture]
public sealed class RepoContextArchiveGitReadingScriptTests
{
    /// <summary>
    /// A floor, not a count. It sits well below the suite's real size so ordinary additions
    /// and removals do not touch it, while a suite emptied - or cut to a token assertion to
    /// turn a red leg green - still fails.
    /// </summary>
    private const int MinimumAssertions = 12;

    /// <summary>
    /// The distinct exit code the script uses to announce "git absent". Kept separate from a
    /// failing-assertion count (which is the exit code otherwise) so a skip can never be read
    /// as a pass or as a single failure.
    /// </summary>
    private const int GitAbsentExitCode = 3;

    private static string RepoRoot => Path.GetFullPath(
        Path.Combine(TestContext.CurrentContext.TestDirectory, "..", "..", "..", "..", ".."));

    private static string ScriptsDirectory => Path.Combine(
        RepoRoot, "samples", "RepoContextContainer", "scripts");

    private static string SuitePath => Path.Combine(ScriptsDirectory, "Test-ArchiveGitReading.ps1");

    private static int CountAssertions() =>
        File.ReadAllLines(SuitePath).Count(l => l.TrimStart().StartsWith("_Assert -Name", StringComparison.Ordinal));

    [Test]
    public void The_archive_git_reading_suite_still_carries_its_assertions()
    {
        // Runs on every host, including one with no PowerShell or git, so the fixture cannot
        // go quiet at the same moment the suite does.
        Assert.That(File.Exists(SuitePath), Is.True, $"expected the archive git reading suite at {SuitePath}");
        Assert.That(
            File.Exists(Path.Combine(ScriptsDirectory, "_provenance.ps1")),
            Is.True,
            "expected the pure library the suite's predicate lives in");
        Assert.That(
            File.Exists(Path.Combine(ScriptsDirectory, "Assert-ContainerProvenance.ps1")),
            Is.True,
            "expected the acquisition script the suite extracts Get-ArchiveGitReading from");

        Assert.That(
            CountAssertions(),
            Is.GreaterThanOrEqualTo(MinimumAssertions),
            $"the archive git reading suite carries fewer than {MinimumAssertions} assertions, which means "
                + "it has been gutted rather than trimmed");
    }

    [Test]
    public void The_archive_git_reading_suite_passes()
    {
        var shell = FindExecutable("pwsh") ?? FindExecutable("powershell");
        if (shell is null)
        {
            Assert.Ignore("Neither pwsh nor powershell is available on this host.");
        }

        // git absence is a LOUD, reported skip. A conformance guard that ran with no git to
        // compare against would examine nothing while reporting green, so it is ignored here
        // rather than passed - and the script itself refuses to run in that state too.
        if (FindExecutable("git") is null)
        {
            Assert.Ignore("git is not available on this host, so the git-message conformance guard cannot run.");
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
        process.WaitForExit(120_000);

        // A git that vanished between the check above and the run, or a hand-run on a host
        // without git, exits with the distinct skip code. Honour it as an ignore, not a
        // failure, so the reason survives instead of surfacing as a mysterious red.
        if (process.ExitCode == GitAbsentExitCode && stdout.Contains("SKIPPED: git", StringComparison.Ordinal))
        {
            Assert.Ignore("The suite reported git absent at run time." + Environment.NewLine + stdout + stderr);
        }

        // The suite exits with its failure count, so a non-zero exit and the printed tally are
        // two independent readings of the same run. The tally is asserted BEFORE the exit code
        // because a terminating PowerShell error also exits 1, and read naively that says "one
        // assertion failed" - a precise, plausible, wrong reading of a run that never reached
        // its first check. This fixture refuses artefacts that cannot signal their own
        // unreliability, so it must not emit one itself.
        var tally = stdout
            .Split('\n')
            .Select(l => l.Trim())
            .LastOrDefault(l => l.StartsWith("Total ", StringComparison.Ordinal));

        Assert.That(
            tally,
            Is.Not.Null,
            "the archive git reading suite printed no tally, so it did not reach the end of its run. "
                + "Its exit code is NOT a failure count in this state - suspect a terminating error, most "
                + "likely the function-extraction anchor drifting or a pinned constant being renamed."
                + Environment.NewLine
                + stdout
                + stderr);

        Assert.That(
            process.ExitCode,
            Is.Zero,
            $"the archive git reading suite reported {process.ExitCode} failing assertion(s)."
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
