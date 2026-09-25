using System.Diagnostics;
using System.IO;
using System.Text.RegularExpressions;
using Orleans.Lattice.Testing.Hygiene;

namespace Orleans.Lattice.Tests.Hygiene;

/// <summary>
/// Drives the derivation half of <c>New-TuningEnv.ps1</c> - the host clamp and its two
/// refusals - with injected host figures, so the contract the script states in its own
/// <c>.DESCRIPTION</c> is executed on a runner rather than asserted in prose (issue #2832).
/// </summary>
/// <remarks>
/// <para>
/// The only other arm that runs the script uses <c>-CorpusOnly</c>, which returns above
/// the whole derivation block, so before this file every refusal below that exit was
/// unreachable in CI. <c>-HostMemoryBytes</c> and <c>-HostAvailableMemoryBytes</c> make
/// the host readings injectable, which is what lets a runner of any size reach each
/// branch deterministically. <c>-DryRun</c> and an <c>-OutFile</c> inside the sandbox
/// keep every run from touching the tracked sample directory.
/// </para>
/// <para>
/// Each refusal is pinned as a PAIR straddling its threshold. A refusal asserted alone
/// passes just as well for a script that refuses everything; the granting half is what
/// distinguishes the contract from its negation. The floor pair in particular is the
/// assertion the issue asked for: at a ceiling a sliver below the floor the script must
/// REFUSE rather than grant the ceiling, and a sliver above it must grant exactly the floor.
/// </para>
/// </remarks>
public sealed partial class LocalDeploymentRunbookHygieneTests
{
    private const long GiB = 1L << 30;

    /// <summary>
    /// The line <c>-DryRun</c> prints for the repocontext grant. Its absence is how a run
    /// proves it emitted no <c>.env</c> content at all.
    /// </summary>
    private static readonly Regex GrantedMemLimit = new(
        @"^REPOCONTEXT_MEM_LIMIT=(?<mib>\d+)m\s*$",
        RegexOptions.Compiled | RegexOptions.Multiline);

    /// <summary>
    /// A host ceiling that binds a sliver below the floor must be REFUSED, and one a sliver
    /// above it must grant exactly the floor.
    /// </summary>
    /// <remarks>
    /// One indexed file requests the floor (6 GiB). The ceiling is
    /// <c>0.45 * host - 5 GiB</c>, which crosses 6 GiB at 24.44 GiB of host memory, so
    /// 24.4 GiB leaves 5.98 GiB (refuse) and 24.5 GiB leaves 6.03 GiB (grant 6144 MiB).
    /// Granting the ceiling below the floor, dropping the refusal, or moving the floor
    /// reddens one half of the pair.
    /// </remarks>
    [Test]
    public void The_derivation_refuses_a_ceiling_below_the_floor_rather_than_granting_less()
    {
        WithSandbox(1, sandbox =>
        {
            var below = RunTuningEnv(
                sandbox,
                hostMemoryBytes: 26_199_187_046, // 24.4 GiB
                hostAvailableBytes: 64 * GiB);

            var above = RunTuningEnv(
                sandbox,
                hostMemoryBytes: 26_306_561_024, // 24.5 GiB
                hostAvailableBytes: 64 * GiB);

            Assert.Multiple(() =>
            {
                Assert.That(
                    below.ExitCode,
                    Is.Not.EqualTo(0),
                    "a 5.98 GiB repocontext ceiling is below the 6 GiB floor, so the script must "
                    + $"refuse. Output:\n{below.Output}");
                Assert.That(
                    below.Output,
                    Does.Contain("REFUSING: this host cannot run this stack"),
                    "the floor refusal must name itself, so an operator is not left to infer it.");
                Assert.That(
                    GrantedMemLimit.IsMatch(below.Output),
                    Is.False,
                    "a refused derivation must emit no grant at all. A REPOCONTEXT_MEM_LIMIT line "
                    + "here means the script granted the sub-floor ceiling - the silent "
                    + "under-grant that presents as OutOfMemoryException in grain-state reads "
                    + "while the container reports healthy.");

                Assert.That(
                    above.ExitCode,
                    Is.EqualTo(0),
                    $"a 6.03 GiB ceiling clears the 6 GiB floor and must derive. Output:\n{above.Output}");
                Assert.That(
                    GrantedMiB(above.Output),
                    Is.EqualTo(6144),
                    "one indexed file requests the floor, and a ceiling above the floor must "
                    + "grant it exactly.");
            });
        });
    }

    /// <summary>
    /// A ceiling between the floor and the corpus requirement grants the ceiling and WARNS,
    /// naming the requirement, the ceiling and the shortfall; it never under-grants silently.
    /// </summary>
    /// <remarks>
    /// 3,000 files request <c>1.2 * (3 GiB + 3000 MiB)</c> = 7.12 GiB; a 25 GiB host offers
    /// a 6.25 GiB (6400 MiB) ceiling. Also pins that an injected reading is LABELLED in both
    /// the console and the generated header, so a file derived from a hypothetical host
    /// cannot pass for one measured on a real one.
    /// </remarks>
    [Test]
    public void A_ceiling_between_floor_and_requirement_grants_the_ceiling_and_names_the_shortfall()
    {
        WithSandbox(3000, sandbox =>
        {
            var run = RunTuningEnv(
                sandbox,
                hostMemoryBytes: 25 * GiB,
                hostAvailableBytes: 64 * GiB);

            Assert.Multiple(() =>
            {
                Assert.That(run.ExitCode, Is.EqualTo(0), $"the ceiling is above the floor, so the stack may start. Output:\n{run.Output}");
                Assert.That(
                    GrantedMiB(run.Output),
                    Is.EqualTo(6400),
                    "the grant must be clamped to the 6.25 GiB host ceiling, not the 7.12 GiB "
                    + "requirement the host cannot honour.");
                Assert.That(
                    run.Output,
                    Does.Contain("The host ceiling binds BELOW the corpus requirement"),
                    "a grant below the derived requirement must be announced. Without the "
                    + "warning this is the silent under-grant the script exists to prevent.");
                Assert.That(run.Output, Does.Match(@"corpus needs\s*:\s*7[.,]12 GiB"), "the warning must name the requirement.");
                Assert.That(run.Output, Does.Match(@"host offers\s*:\s*6[.,]25 GiB"), "the warning must name the ceiling.");
                Assert.That(run.Output, Does.Match(@"shortfall\s*:\s*0[.,]87 GiB"), "the warning must name the shortfall.");
                Assert.That(
                    run.Output,
                    Does.Contain("[OVERRIDDEN via -HostMemoryBytes, not measured]"),
                    "an injected host figure must be labelled in the console output.");
                Assert.That(
                    run.Output,
                    Does.Match(@"#\s+host memory\s*:\s*25 GiB\s+\(OVERRIDDEN via -HostMemoryBytes, not measured\)"),
                    "an injected host figure must be labelled in the generated .env header.");
            });
        });
    }

    /// <summary>
    /// A derivation that fits host TOTAL but not what is FREE right now must be refused,
    /// unless the operator passes <c>-IgnoreHostLoad</c>.
    /// </summary>
    /// <remarks>
    /// One file on a 64 GiB host grants 6 GiB, so the stack commits 11 GiB with the
    /// embedder. Exactly 11 GiB free derives; one byte less refuses; and the same
    /// one-byte-short reading derives again under <c>-IgnoreHostLoad</c>. The last arm is
    /// what shows the refusal is caused by the free-memory reading and not by anything
    /// else about the host.
    /// </remarks>
    [Test]
    public void The_derivation_refuses_when_free_memory_cannot_hold_the_commitment()
    {
        WithSandbox(1, sandbox =>
        {
            var exact = RunTuningEnv(sandbox, hostMemoryBytes: 64 * GiB, hostAvailableBytes: 11 * GiB);
            var oneByteShort = RunTuningEnv(sandbox, hostMemoryBytes: 64 * GiB, hostAvailableBytes: (11 * GiB) - 1);
            var ignored = RunTuningEnv(
                sandbox,
                hostMemoryBytes: 64 * GiB,
                hostAvailableBytes: (11 * GiB) - 1,
                "-IgnoreHostLoad");

            Assert.Multiple(() =>
            {
                Assert.That(
                    exact.ExitCode,
                    Is.EqualTo(0),
                    $"11 GiB free exactly holds the 11 GiB commitment and must derive. Output:\n{exact.Output}");
                Assert.That(GrantedMiB(exact.Output), Is.EqualTo(6144));

                Assert.That(
                    oneByteShort.ExitCode,
                    Is.Not.EqualTo(0),
                    $"one byte less than the commitment is free, so the script must refuse. Output:\n{oneByteShort.Output}");
                Assert.That(
                    oneByteShort.Output,
                    Does.Contain("REFUSING: the host does not have enough FREE memory"),
                    "the concurrent-load refusal must name itself.");
                Assert.That(
                    oneByteShort.Output,
                    Does.Contain("[OVERRIDDEN via -HostAvailableMemoryBytes, not measured]"),
                    "an injected free-memory figure must be labelled.");
                Assert.That(
                    GrantedMemLimit.IsMatch(oneByteShort.Output),
                    Is.False,
                    "a refused derivation must emit no grant.");

                Assert.That(
                    ignored.ExitCode,
                    Is.EqualTo(0),
                    $"-IgnoreHostLoad must proceed past the free-memory refusal. Output:\n{ignored.Output}");
                Assert.That(
                    GrantedMiB(ignored.Output),
                    Is.EqualTo(6144),
                    "-IgnoreHostLoad skips the refusal; it must not change the derived grant.");
            });
        });
    }

    /// <summary>
    /// Undoes the console wrapping pwsh applies to a terminating error. Under redirected
    /// output the ConciseView error formatter folds the thrown message onto
    /// <c>"     | "</c>-prefixed continuation lines at the host width, which can split a
    /// refusal phrase across two lines; rejoining them keeps a phrase assertion from
    /// depending on where the wrap happened to fall.
    /// </summary>
    private static string Normalise(string output) =>
        Regex.Replace(Regex.Replace(output, @"\r?\n[ \t]*\|[ \t]?", " "), "[ \t]{2,}", " ");

    private static int GrantedMiB(string output)
    {
        var match = GrantedMemLimit.Match(output);
        Assert.That(match.Success, Is.True, $"expected a REPOCONTEXT_MEM_LIMIT line in the -DryRun output:\n{output}");
        return int.Parse(match.Groups["mib"].Value, System.Globalization.CultureInfo.InvariantCulture);
    }

    private static void WithSandbox(int fileCount, Action<string> body)
    {
        var sandbox = Path.Combine(Path.GetTempPath(), "lattice-tuning-" + Guid.NewGuid().ToString("N"));

        try
        {
            Directory.CreateDirectory(sandbox);

            for (var i = 0; i < fileCount; i++)
            {
                File.WriteAllText(Path.Combine(sandbox, $"source{i}.cs"), "//");
            }

            body(sandbox);
        }
        finally
        {
            try { Directory.Delete(sandbox, recursive: true); } catch { /* best effort */ }
        }
    }

    private static (int ExitCode, string Output) RunTuningEnv(
        string sandbox,
        long hostMemoryBytes,
        long hostAvailableBytes,
        params string[] extra)
    {
        var script = Path.Combine(
            HygieneRepository.FindRepoRoot(),
            ComposeDirectory.Replace('/', Path.DirectorySeparatorChar),
            "scripts",
            "New-TuningEnv.ps1");

        Assert.That(File.Exists(script), Is.True, $"expected the derivation script at {script}.");

        foreach (var shell in new[] { "pwsh", "powershell" })
        {
            var start = new ProcessStartInfo(shell)
            {
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false,
            };

            foreach (var argument in new[]
            {
                "-NoProfile", "-File", script,
                "-WorkspacePath", sandbox,
                "-OutFile", Path.Combine(sandbox, ".env"),
                "-DryRun",
                "-HostMemoryBytes", hostMemoryBytes.ToString(System.Globalization.CultureInfo.InvariantCulture),
                "-HostAvailableMemoryBytes", hostAvailableBytes.ToString(System.Globalization.CultureInfo.InvariantCulture),
            })
            {
                start.ArgumentList.Add(argument);
            }

            foreach (var argument in extra)
            {
                start.ArgumentList.Add(argument);
            }

            Process? process;

            try
            {
                process = Process.Start(start);
            }
            catch
            {
                continue;
            }

            if (process is null)
            {
                continue;
            }

            using (process)
            {
                var stderr = process.StandardError.ReadToEndAsync();
                var stdout = process.StandardOutput.ReadToEnd();

                if (!process.WaitForExit(milliseconds: 120_000))
                {
                    try { process.Kill(entireProcessTree: true); } catch { /* best effort */ }
                    Assert.Fail($"`{shell} New-TuningEnv.ps1` did not exit within two minutes.");
                }

                return (process.ExitCode, Normalise(stdout + "\n" + stderr.GetAwaiter().GetResult()));
            }
        }

        RequireToolchain("neither `pwsh` nor `powershell` could be started.");
        return (-1, string.Empty);
    }
}
