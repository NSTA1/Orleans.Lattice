using System.Diagnostics;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Host;

/// <summary>
/// Runs the acceptance rig's tuning integrity conformance suite
/// (<c>samples/RepoContextContainer/scripts/Test-TuningIntegrity.ps1</c>) as part of the
/// ordinary non-chaos suite, so its assertions are GATED rather than merely present.
/// <para>
/// The suite covers one defect class at several points in the rig: a deployment that can be
/// misconfigured, or built from the wrong source, without saying so. Issues #2887, #2928,
/// #2929, #2930 and #2931 are each an instance of it. Every one of those defects is silent
/// by construction - the stack starts, reports healthy, and produces a measurement nobody
/// can tell is void - so the guards against them are worth exactly as much as the evidence
/// that they still fire. A guard nobody runs is indistinguishable from a clean config.
/// </para>
/// <para>
/// This follows <see cref="RepoContextArchiveGitReadingScriptTests"/> deliberately, down to
/// the order of its readings: the structural assertion runs on every host and never skips,
/// the execution leg reports an absent PowerShell host as an NUnit <c>Ignore</c> rather than
/// a pass, and the printed tally is asserted BEFORE the exit code.
/// </para>
/// </summary>
[TestFixture]
public sealed class RepoContextTuningIntegrityScriptTests
{
    /// <summary>
    /// A floor, not a count. It sits below the suite's real size so ordinary additions and
    /// removals do not touch it, while a suite emptied - or cut to a token assertion to turn
    /// a red leg green - still fails. The suite enforces the same floor internally; this is
    /// the second, independent reading of it, from outside the script that could have been
    /// edited to lower its own.
    /// </summary>
    private const int MinimumAssertions = 32;

    /// <summary>
    /// The distinct exit code the suite uses to announce that it could not run at all -
    /// its knob table no longer carries the knobs it adjudicates. That is a code regression
    /// rather than an environment limitation, so it is reported as a FAILURE here and never
    /// as an ignore: the suite examined nothing, and a conformance guard that goes quiet at
    /// the moment its subject disappears is the defect class it exists to catch.
    /// <para>
    /// It is also why the failing-assertion exit is a fixed 1 rather than the failure count.
    /// Exiting with the count reads as self-documenting and collides: a run with exactly
    /// three failures would arrive here indistinguishable from "could not run".
    /// </para>
    /// </summary>
    private const int CouldNotRunExitCode = 3;

    private static string RepoRoot => Path.GetFullPath(
        Path.Combine(TestContext.CurrentContext.TestDirectory, "..", "..", "..", "..", ".."));

    private static string ScriptsDirectory => Path.Combine(
        RepoRoot, "samples", "RepoContextContainer", "scripts");

    private static string SuitePath => Path.Combine(ScriptsDirectory, "Test-TuningIntegrity.ps1");

    private static int CountAssertions() =>
        File.ReadAllLines(SuitePath).Count(l => l.TrimStart().StartsWith("_Assert -Name", StringComparison.Ordinal));

    [Test]
    public void The_tuning_integrity_suite_still_carries_its_assertions()
    {
        // Runs on every host, including one with no PowerShell, so the fixture cannot go
        // quiet at the same moment the suite does.
        Assert.That(File.Exists(SuitePath), Is.True, $"expected the tuning integrity suite at {SuitePath}");

        Assert.Multiple(() =>
        {
            Assert.That(
                File.Exists(Path.Combine(ScriptsDirectory, "_tuningKnobs.ps1")),
                Is.True,
                "expected the pure knob-adjudication library the suite drives for the radix and "
                    + "ancestry guards (issues #2928 and #2931)");
            Assert.That(
                File.Exists(Path.Combine(ScriptsDirectory, "_deployManifest.ps1")),
                Is.True,
                "expected the pure deploy-manifest library the suite drives for the attribution "
                    + "and divergence guards (issue #2931)");
            Assert.That(
                File.Exists(Path.Combine(ScriptsDirectory, "New-TuningEnv.ps1")),
                Is.True,
                "expected the generator the suite lifts its corpus and merge functions out of "
                    + "(issues #2930 and #2929)");
        });

        Assert.That(
            CountAssertions(),
            Is.GreaterThanOrEqualTo(MinimumAssertions),
            $"the tuning integrity suite carries fewer than {MinimumAssertions} assertions, which means "
                + "it has been gutted rather than trimmed");
    }

    [Test]
    public void The_tuning_integrity_suite_passes()
    {
        var shell = FindExecutable("pwsh") ?? FindExecutable("powershell");
        if (shell is null)
        {
            Assert.Ignore("Neither pwsh nor powershell is available on this host.");
        }

        // The corpus leg measures a real checkout through `git ls-files`, which is the whole
        // point of the #2930 fix: a count taken from the filesystem is a property of the
        // CHECKOUT, so two checkouts of one commit derive two different grants and run-to-run
        // comparison breaks silently. With no git there is nothing to compare against, so the
        // run is ignored rather than passed.
        if (FindExecutable("git") is null)
        {
            Assert.Ignore("git is not available on this host, so the tracked-corpus assertions cannot run.");
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

        Assert.That(
            process.ExitCode,
            Is.Not.EqualTo(CouldNotRunExitCode),
            "the tuning integrity suite reported that it COULD NOT RUN - its knob table no longer "
                + "carries the knobs it adjudicates, so it examined nothing. This is a regression in "
                + "the knob table, not an environment limitation, and is failed rather than ignored."
                + Environment.NewLine
                + stdout
                + stderr);

        // The tally is asserted BEFORE the exit code because a terminating PowerShell error
        // also exits 1, and read naively that says "one assertion failed" - a precise,
        // plausible, wrong reading of a run that never reached its first check. A suite whose
        // subject is artefacts that cannot signal their own unreliability must not emit one.
        var tally = stdout
            .Split('\n')
            .Select(l => l.Trim())
            .LastOrDefault(l => l.StartsWith("Total ", StringComparison.Ordinal));

        Assert.That(
            tally,
            Is.Not.Null,
            "the tuning integrity suite printed no tally, so it did not reach the end of its run. "
                + "Its exit code is NOT a failure count in this state - suspect a terminating error, "
                + "most likely an AST extraction anchor drifting or a pinned constant being renamed."
                + Environment.NewLine
                + stdout
                + stderr);

        Assert.That(
            process.ExitCode,
            Is.Zero,
            $"the tuning integrity suite reported {process.ExitCode} failing assertion(s)."
                + Environment.NewLine
                + stdout
                + stderr);

        Assert.That(tally, Does.Contain("Failed 0"));

        // A run that stopped early exits zero and prints a clean-looking tally with a smaller
        // Total, which is precisely the plausible-but-unreliable artefact this fixture refuses.
        var total = int.Parse(
            tally!.Split(' ', StringSplitOptions.RemoveEmptyEntries)[1],
            System.Globalization.CultureInfo.InvariantCulture);

        Assert.Multiple(() =>
        {
            Assert.That(
                total,
                Is.GreaterThanOrEqualTo(CountAssertions()),
                $"the suite ran {total} assertions but the file declares {CountAssertions()}, so the run "
                    + "ended before the last one");

            Assert.That(
                total,
                Is.GreaterThanOrEqualTo(MinimumAssertions),
                $"the suite ran {total} assertions, below this fixture's floor of {MinimumAssertions}. "
                    + "The suite enforces the same floor itself; this reading exists because that one "
                    + "lives in the file that would have been edited to lower it.");
        });
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
