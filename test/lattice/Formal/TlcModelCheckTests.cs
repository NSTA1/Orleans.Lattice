using System.Diagnostics;
using Orleans.Lattice.Testing.Hygiene;

namespace Orleans.Lattice.Tests.Formal;

/// <summary>
/// Runs TLC over the TLA+ specification in <c>spec/</c> and over its checked-in
/// mutants, so that a property is required to demonstrate it can go red.
/// <para>
/// WHY THIS EXISTS. Until now nothing in CI ran TLC at all, and no property or
/// refinement row was required to show that it can fail. The atomicity audit
/// (epic #2299) found four verification artefacts that were green because they
/// could not reach the state they were named for, and identified this as the
/// single recommendation that would have caught the most of what it found:
/// <em>every property ships with a named mutation under which it fires,
/// checked in as a cfg</em>. See issue #2323.
/// </para>
/// <para>
/// PROOF OF CONCEPT SCOPE. One property is paired here
/// (<c>MonotonicVisibility</c>, with the mutant from issue #2320). The other
/// eleven in <c>spec/AtomicCommit.cfg</c> are not yet paired; this fixture is
/// the harness they will hang off, and the
/// <see cref="Every_checked_in_mutant_is_covered_by_this_fixture"/> guard
/// fails the moment a mutant is added to <c>spec/mutations/</c> without a test
/// beside it, so the pairing cannot silently fall behind the directory.
/// </para>
/// <para>
/// TIER. Tagged <c>[Category("Tlc")]</c> because it needs an external
/// toolchain (a JVM and <c>tla2tools.jar</c>), which is the same reason
/// <c>AzureStorageEmulator</c> exists as a category. The documented Tier 1 dev
/// loop excludes it so a contributor without a JVM is not blocked; the CI
/// matrix's <c>deterministic</c> tier is the complement of Chaos and Coyote, so
/// it runs there with no change to the matrix planner.
/// </para>
/// <para>
/// FAIL CLOSED IN CI. When the toolchain is missing this fixture
/// <see cref="Assert.Ignore(string)"/>s locally but FAILS under GitHub Actions.
/// The repository has already been bitten by the opposite choice: the
/// emulator-gated suites call <c>Assert.Inconclusive</c>, which NUnit counts as
/// neither passed, failed, nor skipped, so a run with the emulator down still
/// prints <c>Passed!</c> with <c>Skipped: 0</c> and only the total drops. A
/// verification gate that quietly evaporates when its tool is absent is worth
/// less than no gate at all, because it still reads as coverage.
/// </para>
/// </summary>
[TestFixture]
[Category("Tlc")]
public sealed class TlcModelCheckTests
{
    /// <summary>The base specification, which must hold.</summary>
    private const string BaseSpecDirectory = "spec";

    /// <summary>The mutants, each of which must fail in a named way.</summary>
    private const string MutationDirectory = "spec/mutations";

    /// <summary>
    /// TLC is fast on these bounded instances - the base model is 7,649 states
    /// at depth 17 and finishes in about three seconds, and the mutant trips at
    /// depth 4 in about one. This ceiling is therefore a hang guard rather than
    /// a budget, and a run that approaches it means something is wrong with the
    /// model rather than that the timeout is too tight.
    /// </summary>
    private static readonly TimeSpan RunTimeout = TimeSpan.FromMinutes(5);

    private string _java = string.Empty;
    private string _jar = string.Empty;

    [OneTimeSetUp]
    public void ResolveToolchain()
    {
        var jar = Environment.GetEnvironmentVariable("TLA_TOOLS_JAR");
        if (string.IsNullOrWhiteSpace(jar))
        {
            jar = Path.Combine(HygieneRepository.FindRepoRoot(), "tools", "tla2tools.jar");
        }

        var java = ResolveJava();

        if (java is null || !File.Exists(jar))
        {
            var missing = java is null ? "a Java runtime" : $"tla2tools.jar (looked in '{jar}')";
            var message =
                $"the TLA+ toolchain is unavailable: could not find {missing}. "
                + "Set TLA_TOOLS_JAR to an absolute path, or place the jar at tools/tla2tools.jar, "
                + "and make sure 'java' is on PATH or JAVA_HOME is set. "
                + "See spec/README.md.";

            // Under CI the toolchain is provisioned by the workflow, so absence
            // is a broken pipeline and must be loud. Skipping here would report
            // a green model-checking gate that checked no model.
            if (IsContinuousIntegration)
            {
                Assert.Fail(message);
            }

            Assert.Ignore(message);
        }

        _java = java!;
        _jar = jar;
    }

    /// <summary>
    /// The positive control. If the base specification stopped holding, every
    /// mutant assertion below would still pass (a mutant only has to go red,
    /// and a broken base goes red too), so the suite would report that
    /// verification is working while checking a specification that does not
    /// hold. This test is what stops that reading.
    /// </summary>
    [Test]
    public void The_base_specification_holds()
    {
        var result = RunTlc(BaseSpecDirectory, "AtomicCommit.tla", "AtomicCommit.cfg");

        Assert.That(
            result.Output,
            Does.Contain("Model checking completed. No error has been found."),
            "spec/AtomicCommit.cfg must hold against spec/AtomicCommit.tla."
            + Environment.NewLine
            + result.Output);

        Assert.That(result.ExitCode, Is.Zero, "TLC reported a failure exit code for the base specification.");
    }

    /// <summary>
    /// The paired mutation for <c>MonotonicVisibility</c> (issue #2320).
    /// <para>
    /// The base specification cannot express the production hazard of issue
    /// #2318, because no variable stands between the recorded decision and the
    /// reader. The mutant interposes one: a reported status that diverges from
    /// the stored decision. Nothing about the property changes - it was already
    /// live, correctly worded and load-bearing, and it fires the instant the
    /// state becomes expressible.
    /// </para>
    /// </summary>
    [Test]
    public void The_decision_expiry_mutant_violates_monotonic_visibility()
    {
        var result = RunTlc(
            MutationDirectory,
            "AtomicCommitDecisionExpiry.tla",
            "AtomicCommitDecisionExpiry.cfg");

        Assert.That(
            result.ExitCode,
            Is.Not.Zero,
            "the mutant must not model-check clean; a mutation that does not fire proves nothing."
            + Environment.NewLine
            + result.Output);

        Assert.That(
            result.Output,
            Does.Contain("Action property MonotonicVisibility is violated."),
            "TLC must name MonotonicVisibility specifically. Asserting on the property name rather than "
            + "merely on a non-zero exit is what makes a MISATTRIBUTED violation fail here: during the audit "
            + "a cfg that prepended a property instead of replacing it produced two violations attributed to "
            + "the wrong properties, and a bare exit-code check would have accepted it."
            + Environment.NewLine
            + result.Output);

        // One property per mutant cfg, asserted rather than assumed. A cfg that
        // accumulates properties reports several violations at once, which is
        // exactly how that audit misattribution read as a confident result.
        Assert.That(
            CountOccurrences(result.Output, " is violated."),
            Is.EqualTo(1),
            "the mutant cfg must name exactly one property, so the violation it reports is unambiguous."
            + Environment.NewLine
            + result.Output);
    }

    /// <summary>
    /// A non-vacuity guard over the mutation directory itself. The pairing rule
    /// is only worth something if adding a mutant without a test beside it
    /// fails; otherwise the directory grows and the fixture silently keeps
    /// checking the one pair it started with.
    /// </summary>
    [Test]
    public void Every_checked_in_mutant_is_covered_by_this_fixture()
    {
        var directory = RepoPath(MutationDirectory);
        Assert.That(Directory.Exists(directory), Is.True, $"expected {MutationDirectory}");

        var mutants = Directory
            .EnumerateFiles(directory, "*.tla")
            .Select(Path.GetFileNameWithoutExtension)
            .OrderBy(name => name, StringComparer.Ordinal)
            .ToArray();

        Assert.That(mutants, Is.Not.Empty, $"expected at least one mutant in {MutationDirectory}");

        // Update this list, and add the matching test above, when a mutant is
        // added. The remaining eleven properties of spec/AtomicCommit.cfg are
        // tracked by issue #2323.
        var covered = new[] { "AtomicCommitDecisionExpiry" };

        Assert.That(
            mutants,
            Is.EquivalentTo(covered),
            $"every mutant in {MutationDirectory} needs a test in this fixture asserting the specific "
            + "property it makes fire, and every cfg needs its mutant. A mutant nobody runs is a file, "
            + "not a gate.");

        foreach (var mutant in mutants)
        {
            Assert.That(
                File.Exists(Path.Combine(directory, $"{mutant}.cfg")),
                Is.True,
                $"{mutant}.tla has no {mutant}.cfg, so nothing selects the property it is paired with.");
        }
    }

    private static bool IsContinuousIntegration =>
        string.Equals(
            Environment.GetEnvironmentVariable("GITHUB_ACTIONS"),
            "true",
            StringComparison.OrdinalIgnoreCase);

    private static string RepoPath(string relative) =>
        Path.Combine(HygieneRepository.FindRepoRoot(), relative.Replace('/', Path.DirectorySeparatorChar));

    private static string? ResolveJava()
    {
        var executable = OperatingSystem.IsWindows() ? "java.exe" : "java";

        var home = Environment.GetEnvironmentVariable("JAVA_HOME");
        if (!string.IsNullOrWhiteSpace(home))
        {
            var candidate = Path.Combine(home, "bin", executable);
            if (File.Exists(candidate))
            {
                return candidate;
            }
        }

        var path = Environment.GetEnvironmentVariable("PATH") ?? string.Empty;
        return path
            .Split(Path.PathSeparator, StringSplitOptions.RemoveEmptyEntries)
            .Select(directory => Path.Combine(directory.Trim(), executable))
            .FirstOrDefault(File.Exists);
    }

    private static int CountOccurrences(string text, string needle)
    {
        var count = 0;
        var index = text.IndexOf(needle, StringComparison.Ordinal);
        while (index >= 0)
        {
            count++;
            index = text.IndexOf(needle, index + needle.Length, StringComparison.Ordinal);
        }

        return count;
    }

    /// <summary>
    /// Runs TLC on a copy of the module and its cfg in a scratch directory.
    /// TLC writes its states and metadata beside the module it is checking, so
    /// running it in place would leave build output inside <c>spec/</c>.
    /// </summary>
    private TlcResult RunTlc(string directory, string module, string config)
    {
        var source = RepoPath(directory);
        var scratch = Path.Combine(Path.GetTempPath(), $"lattice-tlc-{Guid.NewGuid():N}");
        Directory.CreateDirectory(scratch);

        try
        {
            File.Copy(Path.Combine(source, module), Path.Combine(scratch, module));
            File.Copy(Path.Combine(source, config), Path.Combine(scratch, config));

            var info = new ProcessStartInfo(_java)
            {
                WorkingDirectory = scratch,
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false,
            };

            info.ArgumentList.Add("-cp");
            info.ArgumentList.Add(_jar);
            info.ArgumentList.Add("tlc2.TLC");
            info.ArgumentList.Add("-config");
            info.ArgumentList.Add(config);
            info.ArgumentList.Add("-workers");
            info.ArgumentList.Add("auto");
            info.ArgumentList.Add("-cleanup");
            info.ArgumentList.Add(module);

            using var process = Process.Start(info)
                ?? throw new InvalidOperationException($"could not start '{_java}'");

            // Both streams are drained concurrently: TLC is chatty enough on
            // stdout to fill a pipe buffer, and reading them in sequence would
            // deadlock against a process blocked writing to the other one.
            var stdout = process.StandardOutput.ReadToEndAsync();
            var stderr = process.StandardError.ReadToEndAsync();

            if (!process.WaitForExit((int)RunTimeout.TotalMilliseconds))
            {
                process.Kill(entireProcessTree: true);
                Assert.Fail($"TLC did not finish within {RunTimeout.TotalMinutes} minutes for {module}.");
            }

            var output = string.Concat(stdout.GetAwaiter().GetResult(), stderr.GetAwaiter().GetResult());
            TestContext.Out.WriteLine(output);

            return new TlcResult(process.ExitCode, output);
        }
        finally
        {
            try
            {
                Directory.Delete(scratch, recursive: true);
            }
            catch (IOException)
            {
                // A scratch directory the OS is still holding open is not worth
                // failing a verification run over; the temp path is disposable.
            }
        }
    }

    private sealed record TlcResult(int ExitCode, string Output);
}
