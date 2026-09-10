using System.Diagnostics;
using System.Text.RegularExpressions;
using Orleans.Lattice.Testing.Hygiene;

namespace Orleans.Lattice.Tests.Formal;

/// <summary>
/// Runs TLC over the TLA+ specification in <c>spec/</c> and over a mutation of
/// it per checked property, so that every property is required to demonstrate
/// it can go red.
/// <para>
/// WHY THIS EXISTS. The atomicity audit (epic #2299) found verification
/// artefacts that were green because they could not reach the state they were
/// named for. None failed; all read exactly like verification that works. That
/// is the problem in one line: an artefact that asserts a property it is
/// structurally unable to check is indistinguishable, from the outside, from
/// one that checks it. Passing tells you nothing, because passing is what both
/// cases do. Issue #2323 is the remedy - every property ships with a named
/// mutation under which it fires - and this fixture is it.
/// </para>
/// <para>
/// EACH TEST IS A CONTROLLED EXPERIMENT, NOT AN ASSERTION THAT SOMETHING FAILS.
/// Every mutation runs TWICE against the same generated single-property cfg:
/// once on the unmutated base (the control, which must be clean) and once on
/// the mutant (which must report that property). Asserting only the second half
/// would be the very mistake this fixture exists to catch - a mutant can go red
/// for reasons having nothing to do with the property, and a one-armed test
/// cannot tell the two apart. The control arm is what makes a red mutant
/// evidence rather than merely a red mutant, and it is also the standing proof
/// that the harness is not vacuous: the identical property, the identical cfg
/// and the identical machinery produce green on one input and red on the other.
/// </para>
/// <para>
/// MUTANTS ARE GENERATED, NOT CHECKED IN. See <see cref="SpecMutation"/> for
/// why: a checked-in mutant copy silently stops being evidence about a base it
/// has drifted from, which is the audit's own failure shape reintroduced by the
/// fix for it. Deriving each mutant from the current base at run time makes
/// drift unexpressible instead of merely detectable.
/// </para>
/// <para>
/// TIER. Tagged <c>[Category("Tlc")]</c> because it needs an external toolchain
/// (a JVM and <c>tla2tools.jar</c>) - the same reason
/// <c>AzureStorageEmulator</c> exists as a category. The documented Tier 1 dev
/// loop excludes it so a contributor without a JVM is not blocked; CI's
/// <c>deterministic</c> tier is the complement of <c>Chaos</c> and
/// <c>Coyote</c>, so it runs there with no matrix-planner change.
/// </para>
/// <para>
/// FAIL CLOSED IN CI. When the toolchain is missing this fixture
/// <c>Assert.Ignore</c>s locally but FAILS under GitHub Actions. The repository
/// has been bitten by the opposite choice: emulator-gated suites call
/// <c>Assert.Inconclusive</c>, which NUnit counts as neither passed, failed nor
/// skipped, so a run with the emulator down still prints <c>Passed!</c> with
/// <c>Skipped: 0</c> and only the total drops. A verification gate that quietly
/// evaporates when its tool is absent is worth less than no gate, because it
/// still reads as coverage.
/// </para>
/// </summary>
[TestFixture]
[Category("Tlc")]
public sealed class TlcModelCheckTests
{
    private const string CleanBanner = "Model checking completed. No error has been found.";

    /// <summary>
    /// TLC is fast on these bounded instances: the base model is 7,649 states
    /// at depth 17 in about three seconds, and a mutant usually trips in about
    /// one. This ceiling is a hang guard rather than a budget, and a run
    /// approaching it means the model is wrong, not that the timeout is tight.
    /// </summary>
    private static readonly TimeSpan RunTimeout = TimeSpan.FromMinutes(5);

    private static string SpecDirectory => Path.Combine(HygieneRepository.FindRepoRoot(), "spec");

    private static string MutationDirectory => Path.Combine(SpecDirectory, "mutations");

    private static string BaseSpecification => File.ReadAllText(Path.Combine(SpecDirectory, "AtomicCommit.tla"));

    private static string BaseConfig => File.ReadAllText(Path.Combine(SpecDirectory, "AtomicCommit.cfg"));

    /// <summary>
    /// Exposed as a test-case source so each mutation is an individually named
    /// test. A single test looping over the catalogue would stop at the first
    /// failure and hide how many of the pairings are broken.
    /// </summary>
    public static IEnumerable<SpecMutation> Mutations() => SpecMutationCatalogue.Load(MutationDirectory);

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
                + "and make sure 'java' is on PATH or JAVA_HOME is set. See spec/README.md.";

            // In CI the toolchain is provisioned by the workflow, so absence is
            // a broken pipeline and must be loud. Skipping here would report a
            // green model-checking gate that checked no model.
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
    /// The base specification holds against its own full model: all seven
    /// invariants and all five temporal properties at once.
    /// <para>
    /// This is separate from the per-mutation control arms, and both are needed.
    /// A control arm checks one property in isolation; this checks that they
    /// hold together, which is the claim <c>spec/README.md</c> makes and the one
    /// a reader of the repository relies on.
    /// </para>
    /// </summary>
    [Test]
    public void The_base_specification_holds()
    {
        var result = RunTlc(BaseSpecification, BaseConfig, "AtomicCommit");

        Assert.That(
            result.Output,
            Does.Contain(CleanBanner),
            "spec/AtomicCommit.cfg must hold against spec/AtomicCommit.tla."
            + Environment.NewLine + result.Output);
        Assert.That(result.ExitCode, Is.Zero, "TLC reported a failure exit code for the base specification.");
    }

    /// <summary>
    /// The paired mutation for one property, run as a two-arm experiment.
    /// <para>
    /// Arm 1 (control) runs the generated single-property cfg against the
    /// unmutated base and requires it to be clean. This is what stops the test
    /// passing for the wrong reason. Without it, a mutation that broke the
    /// module outright, or a property already violated on the base, would
    /// produce a red mutant and read as a successful pairing.
    /// </para>
    /// <para>
    /// Arm 2 runs the same cfg against the mutant and requires TLC to report
    /// that property. For invariants and action properties the assertion is on
    /// the property NAME, so a violation misattributed to a different property
    /// fails here - during the audit a cfg that prepended a property instead of
    /// replacing it reported violations against the wrong properties, and a
    /// bare exit-code check would have accepted it. TLC does not name the
    /// property for a true liveness violation, so for those the guard is the
    /// single-property cfg plus the violation count asserted below.
    /// </para>
    /// </summary>
    [TestCaseSource(nameof(Mutations))]
    public void Each_property_fires_under_its_mutation_and_not_on_the_base(SpecMutation mutation)
    {
        var baseSpec = BaseSpecification;
        var config = mutation.BuildConfig(BaseConfig);

        var control = RunTlc(baseSpec, config, "AtomicCommit");
        Assert.That(
            control.Output,
            Does.Contain(CleanBanner),
            $"CONTROL ARM FAILED for '{mutation.Name}'. Property '{mutation.Target}' must HOLD on the "
            + "unmutated base specification, otherwise the mutant going red proves nothing about the "
            + "mutation. Either the base specification is broken or the generated cfg is wrong."
            + Environment.NewLine + control.Output);

        var mutant = RunTlc(mutation.Apply(baseSpec), config, mutation.Module);

        Assert.That(
            mutant.ExitCode,
            Is.Not.Zero,
            $"'{mutation.Name}' ({mutation.Summary}) model-checked CLEAN. A mutation that does not fire "
            + $"proves nothing about '{mutation.Target}', which is exactly the vacuity epic #2299 found."
            + Environment.NewLine + mutant.Output);

        Assert.That(
            mutant.Output,
            Does.Contain(mutation.ExpectedBanner),
            $"'{mutation.Name}' went red, but not in the expected way. Expected TLC to report "
            + $"'{mutation.ExpectedBanner}'. A red run whose cause is not the named property is not "
            + "evidence for that property."
            + Environment.NewLine + mutant.Output);

        Assert.That(
            CountViolationLines(mutant.Output),
            Is.EqualTo(1),
            $"'{mutation.Name}' reported more than one violation. The generated cfg names exactly one "
            + "property so that the violation is unambiguous; more than one means the cfg or the "
            + "mutation is doing something unintended."
            + Environment.NewLine + mutant.Output);
    }

    /// <summary>
    /// Completeness, driven by the model rather than by a hand-maintained list.
    /// Every name in the base cfg's INVARIANTS and PROPERTIES blocks must have
    /// a mutation, so adding a property without pairing it fails here.
    /// <para>
    /// This is the gate that keeps #2323 closed rather than merely satisfied
    /// once. A pairing rule that covers today's properties but not tomorrow's
    /// decays into exactly the state the audit found.
    /// </para>
    /// </summary>
    [Test]
    public void Every_property_the_base_model_checks_has_a_mutation()
    {
        var checkedProperties = SpecMutationCatalogue.ReadCheckedProperties(BaseConfig);
        var paired = Mutations().Select(m => m.Target).ToArray();

        Assert.That(
            checkedProperties,
            Is.Not.Empty,
            "parsed no properties out of spec/AtomicCommit.cfg, so this gate would be vacuous.");

        Assert.That(
            paired,
            Is.EquivalentTo(checkedProperties),
            "every property spec/AtomicCommit.cfg checks needs a mutation in spec/mutations/ that makes "
            + "it fire, and every mutation needs to target a property the model actually checks. "
            + $"Model checks: [{string.Join(", ", checkedProperties.Order(StringComparer.Ordinal))}]. "
            + $"Mutations target: [{string.Join(", ", paired.Order(StringComparer.Ordinal))}].");

        Assert.That(paired, Is.Unique, "two mutations target the same property; each needs its own.");
    }

    /// <summary>
    /// The drift gate, and deliberately cheap: it applies every mutation to the
    /// current base without running TLC, so an edit to
    /// <c>spec/AtomicCommit.tla</c> that invalidates an anchor fails in
    /// milliseconds with a message naming the mutation and the exact anchor
    /// text, instead of surfacing as a confusing TLC parse error minutes later.
    /// </summary>
    [Test]
    public void Every_mutation_applies_cleanly_to_the_current_base_specification()
    {
        var baseSpec = BaseSpecification;
        var mutations = Mutations().ToArray();

        Assert.That(mutations, Is.Not.Empty, "expected at least one mutation in spec/mutations/.");

        Assert.Multiple(() =>
        {
            foreach (var mutation in mutations)
            {
                Assert.DoesNotThrow(
                    () => mutation.Apply(baseSpec),
                    $"mutation '{mutation.Name}' no longer applies to spec/AtomicCommit.tla.");
            }
        });
    }

    private static bool IsContinuousIntegration =>
        string.Equals(
            Environment.GetEnvironmentVariable("GITHUB_ACTIONS"),
            "true",
            StringComparison.OrdinalIgnoreCase);

    /// <summary>
    /// Counts TLC's violation banners. Both wordings are matched because TLC
    /// names the property for invariants and action properties but not for
    /// liveness.
    /// </summary>
    private static int CountViolationLines(string output) =>
        Regex.Matches(
            output,
            @"^Error: (?:Invariant \w+ is violated|Action property \w+ is violated|Temporal properties were violated)\.",
            RegexOptions.Multiline).Count;

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

        return (Environment.GetEnvironmentVariable("PATH") ?? string.Empty)
            .Split(Path.PathSeparator, StringSplitOptions.RemoveEmptyEntries)
            .Select(directory => Path.Combine(directory.Trim(), executable))
            .FirstOrDefault(File.Exists);
    }

    /// <summary>
    /// Writes a module and cfg into a scratch directory and runs TLC there.
    /// TLC emits its state files beside the module it checks, so running in
    /// <c>spec/</c> would leave build output in the repository.
    /// </summary>
    private TlcResult RunTlc(string moduleText, string configText, string moduleName)
    {
        var scratch = Path.Combine(Path.GetTempPath(), $"lattice-tlc-{Guid.NewGuid():N}");
        Directory.CreateDirectory(scratch);

        try
        {
            var module = $"{moduleName}.tla";
            var config = $"{moduleName}.cfg";
            File.WriteAllText(Path.Combine(scratch, module), moduleText);
            File.WriteAllText(Path.Combine(scratch, config), configText);

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

            // Drained concurrently: TLC is chatty enough to fill a pipe buffer,
            // and reading the streams in sequence would deadlock against a
            // process blocked writing to the other one.
            var stdout = process.StandardOutput.ReadToEndAsync();
            var stderr = process.StandardError.ReadToEndAsync();

            if (!process.WaitForExit((int)RunTimeout.TotalMilliseconds))
            {
                process.Kill(entireProcessTree: true);
                Assert.Fail($"TLC did not finish within {RunTimeout.TotalMinutes} minutes for {moduleName}.");
            }

            var output = string.Concat(stdout.GetAwaiter().GetResult(), stderr.GetAwaiter().GetResult());
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
                // A scratch directory the OS still holds open is not worth
                // failing a verification run over; the temp path is disposable.
            }
        }
    }

    private sealed record TlcResult(int ExitCode, string Output);
}
