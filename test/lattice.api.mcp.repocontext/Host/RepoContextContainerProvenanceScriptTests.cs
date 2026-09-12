using System.Diagnostics;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Host;

/// <summary>
/// Runs the sample's own provenance test suite
/// (<c>samples/RepoContextContainer/scripts/Test-ContainerProvenance.ps1</c>) as part of
/// the ordinary non-chaos suite, so its assertions are gated rather than merely present.
/// <para>
/// The suite is pure PowerShell over the pure adjudication functions in
/// <c>_provenance.ps1</c>: it needs no Docker, no git, and no running container, which is
/// what makes it runnable here at all. It is the only place the archive durability check
/// added for issue #2627 is exercised in both directions - a refusal fixture alone cannot
/// distinguish a check keyed on the archive path from one keyed on the compose working
/// directory, because the second passes every refusal case.
/// </para>
/// <para>
/// The execution leg skips when the host has no PowerShell. A skip that hides a gutted
/// suite would be exactly the failure this bucket exists to catch, so the structural
/// assertions below NEVER skip: they run on every host and fail if the suite has lost its
/// assertions, whether or not it could be executed.
/// </para>
/// </summary>
[TestFixture]
public sealed class RepoContextContainerProvenanceScriptTests
{
    /// <summary>
    /// A floor, not a count. It is far below the suite's real size so ordinary additions
    /// and removals do not touch it, while a suite that had been emptied - or reduced to a
    /// token assertion to make a red leg green - still fails.
    /// </summary>
    private const int MinimumAssertions = 100;

    private static string RepoRoot => Path.GetFullPath(
        Path.Combine(TestContext.CurrentContext.TestDirectory, "..", "..", "..", "..", ".."));

    private static string ScriptsDirectory => Path.Combine(
        RepoRoot, "samples", "RepoContextContainer", "scripts");

    private static string SuitePath => Path.Combine(ScriptsDirectory, "Test-ContainerProvenance.ps1");

    private static int CountAssertions() =>
        File.ReadAllLines(SuitePath).Count(l => l.TrimStart().StartsWith("_Assert -Name", StringComparison.Ordinal));

    [Test]
    public void The_provenance_suite_still_carries_its_assertions()
    {
        // Runs on every host, including one with no PowerShell, so the fixture cannot go
        // quiet at the same moment the suite does.
        Assert.That(File.Exists(SuitePath), Is.True, $"expected the provenance test suite at {SuitePath}");
        Assert.That(
            File.Exists(Path.Combine(ScriptsDirectory, "_provenance.ps1")),
            Is.True,
            "expected the pure adjudication library the suite exercises");

        Assert.That(
            CountAssertions(),
            Is.GreaterThanOrEqualTo(MinimumAssertions),
            $"the provenance suite carries fewer than {MinimumAssertions} assertions, which means it "
                + "has been gutted rather than trimmed");
    }

    [Test]
    public void The_provenance_suite_passes()
    {
        var shell = FindExecutable("pwsh") ?? FindExecutable("powershell");
        if (shell is null)
        {
            Assert.Ignore("Neither pwsh nor powershell is available on this host.");
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

        // The suite exits with its failure count, so a non-zero exit and the printed
        // tally are two independent readings of the same run. Both are asserted, because
        // a suite that crashed before printing would otherwise be indistinguishable from
        // one that passed silently.
        var tally = stdout
            .Split('\n')
            .Select(l => l.Trim())
            .LastOrDefault(l => l.StartsWith("Total ", StringComparison.Ordinal));

        // The tally is asserted BEFORE the exit code, and the exit-code message is
        // conditioned on it, because a terminating PowerShell error also exits 1. Read
        // naively, that exit code says "one assertion failed" - a precise, plausible,
        // entirely wrong reading of a run that never reached its first check. This
        // fixture is here to refuse artefacts that cannot signal their own
        // unreliability, so it must not emit one itself.
        Assert.That(
            tally,
            Is.Not.Null,
            "the provenance suite printed no tally, so it did not reach the end of its run. "
                + "Its exit code is NOT a failure count in this state - suspect a terminating "
                + "error, which on a cross-platform run most often means a path cmdlet that "
                + "resolves a Windows drive qualifier."
                + Environment.NewLine
                + stdout
                + stderr);

        Assert.That(
            process.ExitCode,
            Is.Zero,
            $"the provenance suite reported {process.ExitCode} failing assertion(s)."
                + Environment.NewLine
                + stdout
                + stderr);

        Assert.That(tally, Does.Contain("Failed 0"));

        // A run that stopped early exits zero and prints a clean-looking tally with a
        // smaller Total, which is precisely the plausible-but-unreliable artefact this
        // fixture is here to refuse.
        var total = int.Parse(
            tally!.Split(' ', StringSplitOptions.RemoveEmptyEntries)[1],
            System.Globalization.CultureInfo.InvariantCulture);

        Assert.That(
            total,
            Is.GreaterThanOrEqualTo(CountAssertions()),
            $"the suite ran {total} assertions but the file declares {CountAssertions()}, so the run "
                + "ended before the last one");
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
