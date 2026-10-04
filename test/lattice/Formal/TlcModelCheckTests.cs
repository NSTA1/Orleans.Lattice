using System.Diagnostics;
using System.Globalization;
using System.Text.RegularExpressions;
using Orleans.Lattice.Testing.Hygiene;

namespace Orleans.Lattice.Tests.Formal;

/// <summary>
/// Runs TLC over every TLA+ module under <c>spec/</c> (see
/// <see cref="SpecModuleCatalogue"/>) and over a mutation of each per checked
/// property, so that every property is required to demonstrate it can go red.
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
/// EVERY MODULE, EVERY CASE NAMED. The cases come from
/// <see cref="SpecModuleCases"/>, so each is labelled with its module and a new
/// module is checked as soon as discovery can see it.
/// <see cref="A_synthetic_module_is_model_checked_by_every_TLC_gate"/> proves
/// that against a module built in a temp directory.
/// </para>
/// <para>
/// BUDGET. Cases run in parallel, at most <see cref="TlcConcurrency"/> TLC
/// processes at once, each with an equal share of the cores as TLC workers.
/// The atomic-commit module's base model and twenty mutations are 43 TLC runs;
/// see "CI budget" in <c>spec/README.md</c> for the measured figures and how
/// they scale with modules.
/// </para>
/// <para>
/// TIER. Tagged <c>[Category("Tlc")]</c> because it needs an external toolchain
/// (a JVM and <c>tla2tools.jar</c>) - the same reason
/// <c>AzureStorageEmulator</c> exists as a category. The documented Tier 2
/// filter excludes it so a contributor without a JVM is not blocked; CI's
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
[Category(SpecModuleGates.TlcCategory)]
[Parallelizable(ParallelScope.Children)]
public sealed class TlcModelCheckTests
{
    private const string CleanBanner = "Model checking completed. No error has been found.";

    private const string DeadlockBanner = "Error: Deadlock reached.";

    /// <summary>
    /// TLC is fast on these bounded instances: the atomic-commit base model
    /// finishes in a few seconds and a mutant usually trips in about one. This
    /// ceiling is a hang guard rather than a budget, and a run approaching it
    /// means the model is wrong, not that the timeout is tight. It is per TLC
    /// run, not per fixture, so it does not shrink as modules are added; the
    /// wait for a concurrency slot is outside it.
    /// </summary>
    private static readonly TimeSpan RunTimeout = TimeSpan.FromMinutes(5);

    /// <summary>
    /// How many TLC processes may run at once: half the cores, between one and
    /// four, unless <c>LATTICE_TLC_CONCURRENCY</c> sets it (1 runs serially,
    /// which is how the serial budget in <c>spec/README.md</c> was measured).
    /// Most of a small model's wall-clock is JVM start-up, which parallelises
    /// well; capping it keeps a shared machine responsive and bounds the JVM
    /// heaps resident at once.
    /// </summary>
    private static readonly int TlcConcurrency =
        int.TryParse(Environment.GetEnvironmentVariable("LATTICE_TLC_CONCURRENCY"), NumberStyles.None, CultureInfo.InvariantCulture, out var configured) && configured > 0
            ? configured
            : Math.Clamp(Environment.ProcessorCount / 2, 1, 4);

    /// <summary>TLC worker threads per process: an equal share of the cores.</summary>
    private static readonly int TlcWorkers = Math.Max(1, Environment.ProcessorCount / TlcConcurrency);

    private static readonly SemaphoreSlim TlcSlots = new(TlcConcurrency, TlcConcurrency);

    /// <summary>TLC's switch that defers every liveness check to the end of the state search.</summary>
    private static readonly string[] LivenessAtEnd = ["-lncheck", "final"];

    private static readonly Regex DistinctStates = new(
        @"^(\d+) states generated, (\d+) distinct states found, 0 states left on queue\.",
        RegexOptions.Multiline);

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
    /// The module's base specification holds against its own full model: every
    /// invariant and every action and temporal property at once, with TLC's
    /// deadlock check on, over exactly the number of distinct states the
    /// module's manifest records.
    /// <para>
    /// This is separate from the per-mutation control arms, and both are needed.
    /// A control arm checks one property in isolation; this checks that they
    /// hold together, which is the claim the module's README makes and the one
    /// a reader of the repository relies on.
    /// </para>
    /// <para>
    /// The state count is asserted because it is the one number that moves
    /// whenever the reachable behaviour does. A change that quietly shrinks the
    /// model - a guard tightened, an action that can no longer fire - can leave
    /// every property green while checking less; an equality on the count makes
    /// that change announce itself and its README restate the count.
    /// </para>
    /// </summary>
    [TestCaseSource(typeof(SpecModuleCases), nameof(SpecModuleCases.Modules))]
    public void The_base_specification_holds(SpecModule module)
    {
        ArgumentNullException.ThrowIfNull(module);

        var result = RunTlc(module, module.ReadSpecification(), module.ReadConfig(), module.Name, []);

        Assert.That(
            result.Output,
            Does.Contain(CleanBanner),
            $"{module.Describe(module.ConfigPath)} must hold against {module.Describe(module.SpecificationPath)}."
            + Environment.NewLine + result.Output);
        Assert.That(result.ExitCode, Is.Zero, $"TLC reported a failure exit code for {module.Name}.");

        var distinct = DistinctStates.Match(result.Output);
        Assert.That(distinct.Success, Is.True, "TLC's output carried no final state-count line." + Environment.NewLine + result.Output);
        Assert.That(
            long.Parse(distinct.Groups[2].Value, CultureInfo.InvariantCulture),
            Is.EqualTo(module.Manifest.Counts.DistinctStates),
            $"TLC found a different number of distinct states for {module.Name} than "
            + $"{module.Describe(module.ManifestPath)} records. If the specification changed on purpose, restate "
            + "'distinctStates' there and the counts table in the module README; if it did not, the reachable "
            + "behaviour has changed under it.");
    }

    /// <summary>
    /// The base specification holds under each of the module's variant
    /// configurations - the same specification checked under a different bound
    /// (see "Variant configurations" in <c>spec/README.md</c>) - over exactly
    /// the number of distinct states the manifest records for that variant.
    /// <para>
    /// The count must also DIFFER from the base configuration's. That is the
    /// assertion that the variant's override took effect: TLC accepts a value
    /// assignment to a name the specification does not have and checks the
    /// unchanged model, so a variant whose bound is misspelt would otherwise
    /// pass as a second, larger check while re-checking the base. A variant
    /// that genuinely reaches the base's state count changes nothing and has no
    /// reason to exist.
    /// </para>
    /// </summary>
    [TestCaseSource(typeof(SpecModuleCases), nameof(SpecModuleCases.Variants))]
    public void Each_variant_configuration_holds(SpecModule module, string variant)
    {
        ArgumentNullException.ThrowIfNull(module);
        ArgumentException.ThrowIfNullOrEmpty(variant);

        var where = module.Describe(module.VariantConfigPath(variant));
        var result = RunTlc(module, module.ReadSpecification(), module.ReadVariantConfig(variant), module.Name, []);

        Assert.That(
            result.Output,
            Does.Contain(CleanBanner),
            $"{where} must hold against {module.Describe(module.SpecificationPath)}." + Environment.NewLine + result.Output);
        Assert.That(result.ExitCode, Is.Zero, $"TLC reported a failure exit code for {where}.");

        var distinct = DistinctStates.Match(result.Output);
        Assert.That(distinct.Success, Is.True, "TLC's output carried no final state-count line." + Environment.NewLine + result.Output);
        var states = long.Parse(distinct.Groups[2].Value, CultureInfo.InvariantCulture);

        Assert.That(
            states,
            Is.Not.EqualTo(module.Manifest.Counts.DistinctStates),
            $"{where} reached exactly the base configuration's {module.Manifest.Counts.DistinctStates} distinct "
            + "states, so its override did not take effect: it re-checked the base model under the base's bound. "
            + "Check that every name it assigns is spelt as the specification declares or defines it.");
        Assert.That(
            states,
            Is.EqualTo(module.Manifest.Variants[variant]),
            $"TLC found a different number of distinct states for {where} than {module.Describe(module.ManifestPath)} "
            + $"records under 'variants.{variant}'. If the specification or the variant changed on purpose, restate "
            + "it there; if not, the reachable behaviour under the variant's bound has changed.");
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
    /// property for a temporal violation, so for those the guard is the
    /// single-property cfg plus the violation count asserted below.
    /// </para>
    /// <para>
    /// Arm 3 runs only for a mutation that declares <c>DEADLOCK: off</c>, and
    /// is what keeps that declaration honest. Both arms above run with TLC's
    /// deadlock check off for such a mutation; arm 3 runs the mutant again
    /// with it on and requires TLC to report a deadlock. A declaration that
    /// was not needed would otherwise silently weaken the check for no reason,
    /// and one that stopped being needed would never be noticed.
    /// </para>
    /// </summary>
    [TestCaseSource(typeof(SpecModuleCases), nameof(SpecModuleCases.Mutations))]
    public void Each_property_fires_under_its_mutation_and_not_on_the_base(SpecModule module, SpecMutation mutation)
    {
        ArgumentNullException.ThrowIfNull(module);
        ArgumentNullException.ThrowIfNull(mutation);

        var baseSpec = module.ReadSpecification();
        var config = mutation.BuildConfig(module.ReadConfig());

        var control = RunTlc(module, baseSpec, config, module.Name, mutation.TlcOptions);
        Assert.That(
            control.Output,
            Does.Contain(CleanBanner),
            $"CONTROL ARM FAILED for '{mutation.Name}'. Property '{mutation.Target}' must HOLD on the "
            + $"unmutated {module.Name} specification, otherwise the mutant going red proves nothing about the "
            + "mutation. Either the base specification is broken or the generated cfg is wrong."
            + Environment.NewLine + control.Output);

        var mutantSpec = mutation.Apply(baseSpec, module.Name);
        var mutant = RunTlc(module, mutantSpec, config, mutation.Module, mutation.TlcOptions);

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

        if (mutation.DeadlockCheckDisabled)
        {
            // Liveness is checked only once the search completes, so the
            // deadlock - found during the search - is reported first if it is
            // reachable. Left to TLC's default, a periodic mid-search liveness
            // check can report the target first on a slow machine, and the arm
            // would then fail for timing rather than for the declaration.
            var withDeadlockCheck = RunTlc(module, mutantSpec, config, mutation.Module, LivenessAtEnd);
            Assert.That(
                withDeadlockCheck.Output,
                Does.Contain(DeadlockBanner),
                $"'{mutation.Name}' declares DEADLOCK: off, but with TLC's deadlock check left on the mutant "
                + "does not deadlock. The declaration weakens the experiment's checking for no reason; remove "
                + "it from the .mutation file."
                + Environment.NewLine + withDeadlockCheck.Output);
        }
    }

    /// <summary>
    /// The discovery control for the TLC gates: every TLC gate, found by
    /// signature rather than listed, is run over a module built in a temp
    /// directory and must pass; then each broken copy - a silent mutation, a wrong state count, a drifted variant count, a misspelt variant bound and an unresolved variant override - must fail the gate
    /// that owns the fault. The toolchain-free gates have the same control in
    /// <see cref="SpecModuleDiscoveryControlTests"/>.
    /// </summary>
    [Test]
    public void A_synthetic_module_is_model_checked_by_every_TLC_gate()
    {
        var gates = SpecModuleGates.All().Where(g => g.RunsTlc).ToArray();
        Assert.That(
            gates.Select(g => g.Method.Name),
            Is.SupersetOf(new[] { nameof(The_base_specification_holds), nameof(Each_variant_configuration_holds), nameof(Each_property_fires_under_its_mutation_and_not_on_the_base) }),
            "the reflection that finds TLC gates missed one this fixture declares, so the control below would "
            + "run over fewer gates than exist.");

        using (var synthetic = SyntheticSpecModule.Create())
        {
            var module = synthetic.Discover();
            foreach (var gate in gates)
            {
                Assert.That(SpecModuleGates.Run(gate, module, this), Is.GreaterThan(0), $"{gate.Name} ran no case for the synthetic module.");
            }
        }

        using (var silent = SyntheticSpecModule.Create())
        {
            silent.Replace(
                $"mutations/{SyntheticSpecModule.MutationName}.mutation",
                "    /\\ x' = (x + 1) % (Wrap + 1)",
                "    /\\ x' = (1 + x) % Wrap");
            var module = silent.Discover();
            var gate = gates.Single(g => g.Method.Name == nameof(Each_property_fires_under_its_mutation_and_not_on_the_base));
            Assert.That(
                Assert.Catch(() => SpecModuleGates.Run(gate, module, this))?.Message,
                Does.Contain("model-checked CLEAN"),
                "a mutation that changes the text but not the behaviour passed the pairing gate.");
        }

        using (var miscounted = SyntheticSpecModule.Create())
        {
            miscounted.Replace($"{SyntheticSpecModule.ModuleName}{SpecModuleManifest.FileSuffix}", "\"distinctStates\": 3", "\"distinctStates\": 4");
            var module = miscounted.Discover();
            var gate = gates.Single(g => g.Method.Name == nameof(The_base_specification_holds));
            Assert.That(
                Assert.Catch(() => SpecModuleGates.Run(gate, module, this))?.Message,
                Does.Contain("different number of distinct states"),
                "a manifest recording the wrong state count passed the base-model gate.");
        }

        var variantGate = gates.Single(g => g.Method.Name == nameof(Each_variant_configuration_holds));
        var variantFile = $"{SyntheticSpecModule.ModuleName}.{SyntheticSpecModule.VariantName}.cfg";

        using (var drifted = SyntheticSpecModule.Create())
        {
            drifted.Replace($"{SyntheticSpecModule.ModuleName}{SpecModuleManifest.FileSuffix}", "\"Narrow\": { \"distinctStates\": 2 }", "\"Narrow\": { \"distinctStates\": 5 }");
            var module = drifted.Discover();
            Assert.That(
                Assert.Catch(() => SpecModuleGates.Run(variantGate, module, this))?.Message,
                Does.Contain("records under 'variants.Narrow'"),
                "a manifest recording the wrong variant state count passed the variant gate.");
        }

        using (var misspelt = SyntheticSpecModule.Create())
        {
            // TLC accepts a value assignment to a name the specification does
            // not have and checks the unchanged model, so this run is clean at
            // the base's own state count. Only the count assertion stands
            // between it and a pass.
            misspelt.Replace(variantFile, "    Wrap = 2", "    Wrpa = 2");
            var module = misspelt.Discover();
            Assert.That(
                Assert.Catch(() => SpecModuleGates.Run(variantGate, module, this))?.Message,
                Does.Contain("override did not take effect"),
                "a variant whose bound is misspelt re-checked the base and passed the variant gate.");
        }

        using (var unresolved = SyntheticSpecModule.Create())
        {
            unresolved.Replace(variantFile, "    Wrap = 2", "    Wrap <- Narrower");
            var module = unresolved.Discover();
            Assert.That(
                Assert.Catch(() => SpecModuleGates.Run(variantGate, module, this))?.Message,
                Does.Contain("must hold against"),
                "a variant overriding with an undefined name passed the variant gate.");
        }
    }

    /// <summary>
    /// Completeness, drift, and catalogue well-formedness are checked by
    /// <see cref="SpecMutationCatalogueTests"/>, which needs no toolchain and
    /// therefore runs even where this fixture is skipped.
    /// </summary>
    private static bool IsContinuousIntegration =>
        string.Equals(
            Environment.GetEnvironmentVariable("GITHUB_ACTIONS"),
            "true",
            StringComparison.OrdinalIgnoreCase);

    /// <summary>
    /// Counts TLC's violation banners. Both wordings are matched because TLC
    /// names the property for invariants and action properties but not for
    /// temporal properties.
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
    /// <c>spec/</c> would leave build output in the repository. Every sibling
    /// <c>.tla</c> of the module is copied in first, so a module that extends
    /// or instantiates a sibling resolves it.
    /// </summary>
    private TlcResult RunTlc(
        SpecModule module,
        string moduleText,
        string configText,
        string moduleName,
        IReadOnlyList<string> options)
    {
        var scratch = Path.Combine(Path.GetTempPath(), $"lattice-tlc-{Guid.NewGuid():N}");
        Directory.CreateDirectory(scratch);

        TlcSlots.Wait();
        try
        {
            foreach (var (name, text) in module.ReadSiblingSpecifications())
            {
                File.WriteAllText(Path.Combine(scratch, name), text);
            }

            var tla = $"{moduleName}.tla";
            var cfg = $"{moduleName}.cfg";
            File.WriteAllText(Path.Combine(scratch, tla), moduleText);
            File.WriteAllText(Path.Combine(scratch, cfg), configText);

            var info = new ProcessStartInfo(_java)
            {
                WorkingDirectory = scratch,
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false,
            };

            // Each JVM gets its own temp directory because TLC extracts the
            // standard modules (Naturals, FiniteSets, ...) into java.io.tmpdir
            // and parses them from there. With the system temp directory shared,
            // one concurrent run deletes or rewrites a file another is reading,
            // and the second fails with "Cannot find source file for module".
            info.ArgumentList.Add($"-Djava.io.tmpdir={scratch}");
            info.ArgumentList.Add("-XX:+UseParallelGC");
            info.ArgumentList.Add("-cp");
            info.ArgumentList.Add(_jar);
            info.ArgumentList.Add("tlc2.TLC");
            foreach (var option in options)
            {
                info.ArgumentList.Add(option);
            }

            info.ArgumentList.Add("-config");
            info.ArgumentList.Add(cfg);
            info.ArgumentList.Add("-workers");
            info.ArgumentList.Add(TlcWorkers.ToString(CultureInfo.InvariantCulture));
            info.ArgumentList.Add("-cleanup");
            info.ArgumentList.Add(tla);

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
            TlcSlots.Release();

            try
            {
                Directory.Delete(scratch, recursive: true);
            }
            catch (IOException)
            {
                // A scratch directory the OS still holds open is not worth
                // failing a verification run over; the temp path is disposable.
            }
            catch (UnauthorizedAccessException)
            {
                // Reached on the timeout path, where a just-killed JVM may
                // still hold a handle open. On Windows that surfaces here
                // rather than as IOException, and letting it escape would
                // replace the hang diagnostic - the one that matters most in
                // that case - with an unrelated cleanup error.
            }
        }
    }

    private sealed record TlcResult(int ExitCode, string Output);
}
