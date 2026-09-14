using System.Diagnostics;
using System.Globalization;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Host;

/// <summary>
/// Runs the sample's deploy-manifest suite
/// (<c>samples/RepoContextContainer/scripts/Test-DeployManifest.ps1</c>) as part of the
/// ordinary non-chaos suite, so its assertions are gated rather than merely present.
/// <para>
/// That suite exists because of issue #2983, in which
/// <c>Assert-DeployManifest.ps1</c> never called <c>docker compose config</c> at all. Its
/// declared half was empty on every real invocation, the declared/effective divergence
/// check short-circuited, and the script printed <c>DEPLOY MANIFEST OK</c> across thirteen
/// deployments. The refusal path HAD been perturbation-tested - but only through the
/// synthetic <c>-DeclaredReading</c> entry point, so the tests proved a code path
/// operations never reached. A guard proven against synthetic input and deployed against
/// real input it cannot read.
/// </para>
/// <para>
/// Hence this fixture gates a suite that drives the REAL acquisition path, not only the
/// pure library. Where <c>docker compose</c> is unavailable that section is skipped by the
/// suite itself, counted as a skip, and never as a pass.
/// </para>
/// </summary>
[TestFixture]
public sealed class RepoContextDeployManifestScriptTests
{
    /// <summary>
    /// A floor, not a count. Far below the suite's real size so ordinary additions and
    /// removals do not touch it, while a suite reduced to a token assertion still fails.
    /// </summary>
    private const int MinimumAssertions = 35;

    /// <summary>
    /// The floor on assertions actually EXECUTED. Deliberately below the daemon-free
    /// total, because the acquisition section legitimately does not run on a host without
    /// <c>docker compose</c>. It is the denominator guard: a run that stopped early exits
    /// zero and prints a clean-looking tally with a smaller Total, which is the
    /// plausible-but-unreliable artefact this whole family of fixtures refuses.
    /// </summary>
    private const int MinimumExecutedAssertions = 25;

    private static string RepoRoot => Path.GetFullPath(
        Path.Combine(TestContext.CurrentContext.TestDirectory, "..", "..", "..", "..", ".."));

    private static string ScriptsDirectory => Path.Combine(
        RepoRoot, "samples", "RepoContextContainer", "scripts");

    private static string SuitePath => Path.Combine(ScriptsDirectory, "Test-DeployManifest.ps1");

    private static int CountAssertions() =>
        File.ReadAllLines(SuitePath).Count(l => l.TrimStart().StartsWith("_Assert -Name", StringComparison.Ordinal));

    [Test]
    public void The_deploy_manifest_suite_still_carries_its_assertions()
    {
        // Runs on every host, including one with no PowerShell, so the fixture cannot go
        // quiet at the same moment the suite does.
        Assert.That(File.Exists(SuitePath), Is.True, $"expected the deploy manifest test suite at {SuitePath}");
        Assert.That(
            File.Exists(Path.Combine(ScriptsDirectory, "_deployManifest.ps1")),
            Is.True,
            "expected the pure library the suite exercises");
        Assert.That(
            File.Exists(Path.Combine(ScriptsDirectory, "Assert-DeployManifest.ps1")),
            Is.True,
            "expected the acquiring script the suite drives");

        Assert.That(
            CountAssertions(),
            Is.GreaterThanOrEqualTo(MinimumAssertions),
            $"the deploy manifest suite carries fewer than {MinimumAssertions} assertions, which "
                + "means it has been gutted rather than trimmed");
    }

    /// <summary>
    /// The acquisition must be driven by the script, not described by a comment. #2983 was
    /// precisely a comment that described tolerating a resolution failure sitting above code
    /// that never attempted a resolution, so the presence of the call is asserted here
    /// rather than inferred from the prose beside it.
    /// </summary>
    [Test]
    public void The_assert_script_actually_resolves_the_declared_half()
    {
        var script = File.ReadAllText(Path.Combine(ScriptsDirectory, "Assert-DeployManifest.ps1"));

        Assert.That(
            script,
            Does.Contain("docker compose"),
            "Assert-DeployManifest.ps1 no longer invokes docker compose, so its declared half "
                + "cannot be resolved - this is the #2983 regression exactly");

        // The overlay is the second half of #2983: a bare resolution reads only
        // docker-compose.yml and docker-compose.override.yml, and every knob the manifest
        // tracks is set by the tuning overlay. Resolving without it reports every knob
        // absent while now claiming to have looked, which is worse than not looking.
        var library = File.ReadAllText(Path.Combine(ScriptsDirectory, "_deployManifest.ps1"));

        Assert.That(
            library,
            Does.Contain("docker-compose.tuning.yml"),
            "the compose file list no longer names the tuning overlay, so a resolution would "
                + "silently omit every attribution-relevant knob");

        // <absent> and <unreadable> licence opposite conclusions and must not share a token.
        Assert.That(
            library,
            Does.Contain("<unreadable>"),
            "the manifest can no longer distinguish 'not declared' from 'could not be read'");
    }

    [Test]
    public void The_deploy_manifest_suite_passes()
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
        process.WaitForExit(180_000);

        var tally = stdout
            .Split('\n')
            .Select(l => l.Trim())
            .LastOrDefault(l => l.StartsWith("Total ", StringComparison.Ordinal));

        // The tally is asserted BEFORE the exit code, and the exit-code message is
        // conditioned on it, because a terminating PowerShell error also exits 1. Read
        // naively that says "one assertion failed" - a precise, plausible, entirely wrong
        // reading of a run that never reached its first check.
        Assert.That(
            tally,
            Is.Not.Null,
            "the deploy manifest suite printed no tally, so it did not reach the end of its run. "
                + "Its exit code is NOT a failure count in this state - suspect a terminating error."
                + Environment.NewLine
                + stdout
                + stderr);

        Assert.That(
            process.ExitCode,
            Is.Zero,
            $"the deploy manifest suite reported {process.ExitCode} failing assertion(s)."
                + Environment.NewLine
                + stdout
                + stderr);

        Assert.That(tally, Does.Contain("Failed 0"));

        var fields = tally!.Split(' ', StringSplitOptions.RemoveEmptyEntries);
        var total = int.Parse(fields[1], CultureInfo.InvariantCulture);

        Assert.That(
            total,
            Is.GreaterThanOrEqualTo(MinimumExecutedAssertions),
            $"the suite executed only {total} assertions, below the floor of {MinimumExecutedAssertions}, "
                + "so the run ended early or the suite was hollowed out"
                + Environment.NewLine
                + stdout);

        // A skipped check must be visible as a skip. A harness that renders "could not
        // check" the same way it renders "checked and fine" is the #2983 shape, and this
        // fixture would be pointless if the suite it gates could do it.
        var skipped = int.Parse(fields[7], CultureInfo.InvariantCulture);

        if (skipped > 0)
        {
            Assert.That(
                stdout,
                Does.Contain("NOT counted as passes"),
                $"the suite skipped {skipped} check(s) without stating that the skips are excluded "
                    + "from the population, so its green summary overstates what it covered"
                    + Environment.NewLine
                    + stdout);
        }
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
