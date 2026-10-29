using System.Diagnostics;
using System.Globalization;

namespace Orleans.Lattice.Tests.Formal;

public sealed partial class TlcModelCheckTests
{
    [TestCase(false)]
    [TestCase(true)]
    public void TLC_process_options_preserve_liveness_mode_and_state_exploration(bool atEnd)
    {
        var scratch = Path.Combine(Path.GetTempPath(), "replay-process-options-probe");
        string[] options = atEnd ? ["-fp", "0", .. LivenessAtEnd] : ["-fp", "0"];
        var info = CreateTlcStartInfo(scratch, "Probe.tla", "Probe.cfg", options);
        var arguments = info.ArgumentList.ToArray();
        var strategy = Array.IndexOf(arguments, "-lncheck");

        Assert.Multiple(() =>
        {
            Assert.That(arguments.Count(a => a == "-lncheck"), Is.EqualTo(atEnd ? 1 : 0));
            if (atEnd)
                Assert.That(arguments[strategy + 1], Is.EqualTo("final"));
            Assert.That(arguments, Does.Contain("-config").And.Contain("Probe.cfg"));
            Assert.That(arguments, Does.Contain("-fp").And.Contain("0"));
            Assert.That(arguments[^1], Is.EqualTo("Probe.tla"));
            Assert.That(arguments[Array.IndexOf(arguments, "-workers") + 1],
                Is.EqualTo(TlcWorkers.ToString(CultureInfo.InvariantCulture)));
            Assert.That(arguments, Does.Not.Contain("-deadlock").And.Not.Contain("-simulate"));
            Assert.That(arguments[Array.IndexOf(arguments, "-cp") + 1], Is.EqualTo(_classpath));
            Assert.That(_classpath.Split(Path.PathSeparator)[0],
                Is.EqualTo(Path.Combine(Orleans.Lattice.Testing.Hygiene.HygieneRepository.FindRepoRoot(),
                    "tools", "tlc", "build", "compiled")));
            Assert.That(info.WorkingDirectory, Is.EqualTo(scratch));
        });
    }

    [TestCase(false)]
    [TestCase(true)]
    public void Periodic_and_final_liveness_checks_retain_genuine_temporal_positive_and_negative_controls(bool atEnd)
    {
        using var synthetic = SyntheticSpecModule.Create();
        var module = synthetic.Discover();
        var source = module.ReadSpecification().Replace(
            @"Spec == Init /\ [][Next]_vars",
            "Progress == []<>(x = 2)\nSpec == Init /\\ [][Next]_vars /\\ WF_vars(Step)",
            StringComparison.Ordinal);
        var config = module.ReadConfig() + "\nPROPERTY Progress\n";
        string[] options = atEnd ? LivenessAtEnd : [];
        var control = RunTlc(module, source, config, module.Name, options, "process-options control");
        var mutant = source.Replace(
            @"/\ x' = (x + 1) % Wrap", @"/\ x' = x", StringComparison.Ordinal);
        var violation = RunTlc(module, mutant, config, module.Name, options, "process-options violation");
        var target = new SpecMutation
        {
            Name = "ProgressNeverAdvances",
            Module = module.Name,
            Target = "Progress",
            PropertyClass = SpecPropertyClass.Temporal,
            Summary = "The unchanged step preserves TypeOK but never reaches two.",
            Edits = [],
        };

        Assert.Multiple(() =>
        {
            Assert.That(control.ExitCode, Is.Zero, control.Output);
            Assert.That(control.Output, Does.Contain(CleanBanner));
            Assert.That(violation.ExitCode, Is.Not.Zero, violation.Output);
            Assert.That(HasExpectedViolation(violation.Output, target), Is.True, violation.Output);
            Assert.That(CountViolationLines(violation.Output), Is.EqualTo(1));
            Assert.That(violation.Output, Does.Not.Contain(CleanBanner).And.Not.Contain(DeadlockBanner));
        });
    }

    [TestCase(false)]
    [TestCase(true)]
    public void Holder_departure_normalization_preserves_the_unbounded_completion_premise(bool atEnd)
    {
        using var synthetic = SyntheticSpecModule.Create();
        var module = synthetic.Discover();
        const string source = """
            ---- MODULE Synthetic ----
            EXTENDS Naturals
            VARIABLE holders
            vars == <<holders>>
            TypeOK == holders \subseteq {1, 2}
            Init == holders \in SUBSET {1, 2}
            Next == holders' \in SUBSET {1, 2}
            Original == \A r \in {1, 2} : (r \in holders ~> r \notin holders)
            Normalized == \A r \in {1, 2} : []<>(r \notin holders)
            Equivalent == (Original => Normalized) /\ (Normalized => Original)
            Spec == Init /\ [][Next]_vars
            ====
            """;
        const string config = "SPECIFICATION Spec\nINVARIANT TypeOK\nPROPERTY Equivalent\n";
        string[] options = atEnd ? LivenessAtEnd : [];
        var control = RunTlc(module, source, config, module.Name, options, "process-options control");
        var mutant = source.Replace(
            @"Normalized == \A r \in {1, 2} : []<>(r \notin holders)",
            @"Normalized == \A r \in {1, 2} : []<>(r \in holders)",
            StringComparison.Ordinal);
        var violation = RunTlc(module, mutant, config, module.Name, options, "process-options violation");
        var target = new SpecMutation
        {
            Name = "HolderDepartureOppositeRecurrence",
            Module = module.Name,
            Target = "Equivalent",
            PropertyClass = SpecPropertyClass.Temporal,
            Summary = "Requiring recurring ownership is not equivalent to requiring departure.",
            Edits = [],
        };

        Assert.Multiple(() =>
        {
            Assert.That(control.ExitCode, Is.Zero, control.Output);
            Assert.That(control.Output, Does.Contain(CleanBanner));
            Assert.That(violation.ExitCode, Is.Not.Zero, violation.Output);
            Assert.That(HasExpectedViolation(violation.Output, target), Is.True, violation.Output);
            Assert.That(CountViolationLines(violation.Output), Is.EqualTo(1));
            Assert.That(violation.Output, Does.Not.Contain(CleanBanner).And.Not.Contain(DeadlockBanner));
        });
    }

    [TestCase(false)]
    [TestCase(true)]
    public void Action_conjunction_folding_preserves_both_checked_clauses(bool atEnd)
    {
        using var synthetic = SyntheticSpecModule.Create();
        var module = synthetic.Discover();
        // [A]_vars means A or stuttering. Arbitrary step observations cover
        // every action trace without TLC's unsupported nested action formulas.
        const string source = """
            ---- MODULE Synthetic ----
            EXTENDS Naturals
            VARIABLES a, b, stuttering
            vars == <<a, b, stuttering>>
            TypeOK == a \in BOOLEAN /\ b \in BOOLEAN /\ stuttering \in BOOLEAN
            Init == TypeOK
            Next == a' \in BOOLEAN /\ b' \in BOOLEAN /\ stuttering' \in BOOLEAN
            Original == [](a \/ stuttering) /\ [](b \/ stuttering)
            Folded == []((a /\ b) \/ stuttering)
            Equivalent == (Original => Folded) /\ (Folded => Original)
            Spec == Init /\ [][Next]_vars
            ====
            """;
        const string config = "SPECIFICATION Spec\nINVARIANT TypeOK\nPROPERTY Equivalent\n";
        string[] options = atEnd ? LivenessAtEnd : [];
        var control = RunTlc(module, source, config, module.Name, options, "process-options control");
        var mutant = source.Replace(
            @"Folded == []((a /\ b) \/ stuttering)",
            @"Folded == [](a \/ stuttering)",
            StringComparison.Ordinal);
        var violation = RunTlc(module, mutant, config, module.Name, options, "process-options violation");
        var target = new SpecMutation
        {
            Name = "ActionConjunctionDropsSecondClause",
            Module = module.Name,
            Target = "Equivalent",
            PropertyClass = SpecPropertyClass.Temporal,
            Summary = "Dropping the second action clause is not equivalent to folding both.",
            Edits = [],
        };
        TestContext.WriteLine($"Action-folding control (final mode: {atEnd}):{Environment.NewLine}{control.Output}");
        TestContext.WriteLine($"Drop-second-clause negative (final mode: {atEnd}):{Environment.NewLine}{violation.Output}");

        Assert.Multiple(() =>
        {
            Assert.That(control.ExitCode, Is.Zero, control.Output);
            Assert.That(control.Output, Does.Contain(CleanBanner));
            Assert.That(violation.ExitCode, Is.Not.Zero, violation.Output);
            Assert.That(HasExpectedViolation(violation.Output, target), Is.True, violation.Output);
            Assert.That(CountViolationLines(violation.Output), Is.EqualTo(1));
            Assert.That(violation.Output, Does.Not.Contain(CleanBanner).And.Not.Contain(DeadlockBanner));
        });
    }

    private ProcessStartInfo CreateTlcStartInfo(
        string scratch, string tla, string cfg, IReadOnlyList<string> options)
    {
        var info = new ProcessStartInfo(_java)
        {
            WorkingDirectory = scratch,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
        };

        // Isolate TLC's extracted standard modules from other JVMs.
        info.ArgumentList.Add($"-Djava.io.tmpdir={scratch}");
        info.ArgumentList.Add("-XX:+UseParallelGC");
        info.ArgumentList.Add("-cp");
        info.ArgumentList.Add(_classpath);
        info.ArgumentList.Add("tlc2.TLC");
        foreach (var option in options)
            info.ArgumentList.Add(option);
        info.ArgumentList.Add("-config");
        info.ArgumentList.Add(cfg);
        info.ArgumentList.Add("-workers");
        info.ArgumentList.Add(TlcWorkers.ToString(CultureInfo.InvariantCulture));
        info.ArgumentList.Add("-cleanup");
        info.ArgumentList.Add(tla);
        return info;
    }
}
