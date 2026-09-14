using System.Diagnostics;
using System.IO;
using System.Text.RegularExpressions;
using Orleans.Lattice.Testing.Hygiene;

namespace Orleans.Lattice.Tests.Hygiene;

/// <summary>
/// Asserts that the tuning overlay's resource knobs are adjudicated by MEANING and
/// not merely by PRESENCE (issue #2863).
/// </summary>
/// <remarks>
/// <para>
/// <b>The defect this fixture exists to keep fixed.</b> The overlay declares every
/// resource knob as <c>${VAR:?message}</c>. That form errors when the variable is
/// unset or empty, and it cannot inspect the value, because compose interpolation
/// offers presence operators only and has no value predicate. So it establishes that
/// something was supplied and never that what was supplied means what the supplier
/// intended.
/// </para>
/// <para>
/// Every knob has a falsy value inside its own valid syntax that means "ignore me" to
/// its final consumer: <c>0</c> is no CPU limit and no memory limit at all to Docker,
/// the host core count to ONNX Runtime and to the CLR's garbage collector, and the
/// runtime-derived WAL replay ceiling the overlay exists to pin. <c>0</c> is
/// non-empty, so it satisfies every guard on the page. An operator who forgot to
/// export a variable and one who pinned it to <c>0</c> deliberately produce identical
/// deployments, and the guard calls both satisfied.
/// </para>
/// <para>
/// <b>Why the assertions come in pairs.</b> A fixture that only proved <c>0</c> is
/// refused would pass just as well against a preflight that refused everything, and a
/// fixture that only proved a good value is accepted would pass against one that
/// accepted everything. Neither half is worth anything alone, so each direction is
/// asserted against its opposite: <c>0</c> is refused AND a pinned value is accepted;
/// an unset variable is refused AND its refusal is distinguishable from the sentinel
/// refusal; <c>auto</c> is accepted where a parser exists AND refused where none
/// does.
/// </para>
/// <para>
/// <b>What a green run does not establish.</b> That two tracked files agree, and
/// nothing about any running container. The preflight adjudicates a
/// <c>.env</c>; whether the deployment read that <c>.env</c>, and whether the image
/// it started understands the <c>auto</c> token, is what
/// <c>Assert-ContainerProvenance.ps1</c> and the embedder's startup provenance line
/// are for.
/// </para>
/// </remarks>
[TestFixture]
public sealed class TuningEnvSentinelHygieneTests
{
    private const string ComposeDirectory = "samples/RepoContextContainer";
    private const string TuningComposeFile = "docker-compose.tuning.yml";
    private const string PreflightScript = "scripts/Assert-TuningEnv.ps1";
    private const string KnobModule = "scripts/_tuningKnobs.ps1";

    /// <summary>Exit code the preflight uses for a refusal, as opposed to an unreadable file.</summary>
    private const int Refused = 2;

    /// <summary>
    /// The number of guarded knobs, asserted rather than assumed.
    /// </summary>
    /// <remarks>
    /// This is the one hand-written number in the fixture, and it is here precisely so
    /// that the two INDEPENDENTLY DERIVED sets it is compared against cannot agree by
    /// both being empty. Set equality between two computations of nothing is true, and
    /// a fixture that asserted only set equality would go green the moment either
    /// parser stopped matching. If a knob is legitimately added or removed, this number
    /// moves with it and the move is visible in review, which is the point.
    /// </remarks>
    private const int GuardedKnobCount = 7;

    /// <summary>
    /// A guarded reference in the overlay: <c>${NAME:?message}</c>. Captures the
    /// variable name so the guarded set can be read off the overlay itself rather than
    /// transcribed into this file, where it would drift.
    /// </summary>
    private static readonly Regex GuardedReference = new(
        @"\$\{(?<name>[A-Z_][A-Z0-9_]*):\?[^}]+\}",
        RegexOptions.Compiled);

    /// <summary>The token that names the derivation deliberately.</summary>
    private const string AutoToken = "auto";

    /// <summary>
    /// The two knobs whose value is finally parsed by code in THIS repository, and so
    /// the only two where a token can be introduced at all.
    /// </summary>
    /// <remarks>
    /// Held here as an expectation, and corroborated against the host sources by
    /// <see cref="Every_knob_that_accepts_auto_is_read_by_a_host_that_declares_the_token"/>,
    /// so this list cannot quietly grow to cover a knob that has no parser to honour it.
    /// </remarks>
    private static readonly string[] KnobsWithAnAutoToken =
    [
        "REPOCONTEXT_MAX_CONCURRENT_REPLAYS",
        "EMBEDDER_INTRA_THREADS",
    ];

    /// <summary>
    /// The host source files that must declare the token, paired with the knob each
    /// reads. These are the parsers the token's existence depends on.
    /// </summary>
    private static readonly (string Knob, string Source)[] AutoTokenParsers =
    [
        ("REPOCONTEXT_MAX_CONCURRENT_REPLAYS", "apps/repocontext/Hosting/RepoContextReplayConcurrency.cs"),
        ("EMBEDDER_INTRA_THREADS", "apps/embedding-onnx/Embedding/EmbedServerOptions.cs"),
    ];

    [Test]
    public void The_overlay_and_the_preflight_guard_the_same_knobs()
    {
        var fromOverlay = GuardedKnobsInOverlay();
        var fromPreflight = KnobsKnownToThePreflight();

        Assert.Multiple(() =>
        {
            Assert.That(
                fromOverlay,
                Has.Count.EqualTo(GuardedKnobCount),
                $"expected {GuardedKnobCount} `${{VAR:?}}` references in {TuningComposeFile}. If a knob "
                + "was added or removed on purpose, move GuardedKnobCount with it. If this reads 0, the "
                + "reference regex has stopped matching and every set comparison below is vacuous.");

            Assert.That(
                fromPreflight,
                Has.Count.EqualTo(GuardedKnobCount),
                $"expected {KnobModule} to register {GuardedKnobCount} knobs.");

            Assert.That(
                fromPreflight,
                Is.EquivalentTo(fromOverlay),
                "the preflight adjudicates a different set of variables from the one the overlay "
                + "guards. A knob the overlay requires but the preflight does not know is a knob "
                + "whose value is checked for presence only, which is the entire defect of #2863; a "
                + "knob the preflight refuses but the overlay never references is a deployment that "
                + "cannot start for a reason nothing explains.");
        });
    }

    [Test]
    public void A_fully_pinned_env_is_accepted()
    {
        var (exit, output) = RunPreflight(HealthyEnv());

        Assert.That(
            exit,
            Is.Zero,
            "a .env in which every knob carries a meaningful value must be ACCEPTED. This is the "
            + "control for every refusal assertion in this fixture: without it, a preflight that "
            + "refused unconditionally would satisfy all of them. "
            + $"Output was: {output.Trim()}");
    }

    [Test]
    public void An_explicit_zero_is_refused_for_every_knob()
    {
        var accepted = new List<string>();

        foreach (var knob in GuardedKnobsInOverlay())
        {
            var (exit, _) = RunPreflight(HealthyEnv(overrides: new() { [knob] = "0" }));

            if (exit != Refused)
            {
                accepted.Add($"{knob} (exit {exit})");
            }
        }

        Assert.That(
            accepted,
            Is.Empty,
            "these knobs accepted an explicit `0`. The set is walked knob by knob rather than by "
            + "counting occurrences of the fix, because an uncovered SIBLING is how this class of "
            + "defect survives a repair: the reported variable gets fixed, the six that share its "
            + "shape do not, and an occurrence count returns a clean 1.");
    }

    [Test]
    public void Every_spelling_of_zero_is_refused_not_just_the_bare_digit()
    {
        // A guard written against the literal string "0" is a guard that has not
        // understood the problem. Docker parses sizes and CPU shares, so `0m` and `0.0`
        // reach exactly the same no-limit behaviour by a different spelling, and a
        // preflight that let them through would have fixed the example rather than the
        // defect.
        var spellings = new (string Knob, string Value)[]
        {
            ("REPOCONTEXT_MEM_LIMIT", "0m"),
            ("REPOCONTEXT_MEM_LIMIT", "0g"),
            ("REPOCONTEXT_MEM_LIMIT", "0b"),
            ("EMBEDDER_MEM_LIMIT", "0m"),
            ("REPOCONTEXT_CPUS", "0.0"),
            ("EMBEDDER_CPUS", "0.00"),
            ("REPOCONTEXT_GC_HEAP_COUNT", "00"),
        };

        var accepted = new List<string>();

        foreach (var (knob, value) in spellings)
        {
            var (exit, _) = RunPreflight(HealthyEnv(overrides: new() { [knob] = value }));

            if (exit != Refused)
            {
                accepted.Add($"{knob}={value} (exit {exit})");
            }
        }

        Assert.That(
            accepted,
            Is.Empty,
            "these spellings of zero were accepted. Each reaches the same no-limit behaviour as a "
            + "bare `0` at the consumer, so admitting them would leave the defect intact behind a "
            + "guard that reports it fixed.");
    }

    [Test]
    public void A_genuinely_unset_variable_is_still_refused_for_every_knob()
    {
        var accepted = new List<string>();

        foreach (var knob in GuardedKnobsInOverlay())
        {
            var (exit, _) = RunPreflight(HealthyEnv(omit: knob));

            if (exit != Refused)
            {
                accepted.Add($"{knob} (exit {exit})");
            }
        }

        Assert.That(
            accepted,
            Is.Empty,
            "these knobs were accepted while absent from the file. This is the direction that is "
            + "easy to lose while fixing the other one: a preflight that stopped refusing an unset "
            + "variable would pass every zero-is-refused assertion in this fixture and would have "
            + "removed the original guard entirely, which is the OPPOSITE defect rather than an "
            + "incomplete fix.");
    }

    [Test]
    public void The_sentinel_refusal_and_the_unset_refusal_are_distinguishable()
    {
        const string Knob = "REPOCONTEXT_MAX_CONCURRENT_REPLAYS";

        var (sentinelExit, sentinelOutput) = RunPreflight(HealthyEnv(overrides: new() { [Knob] = "0" }));
        var (unsetExit, unsetOutput) = RunPreflight(HealthyEnv(omit: Knob));

        Assert.Multiple(() =>
        {
            Assert.That(sentinelExit, Is.EqualTo(Refused), "expected the sentinel case to be refused.");
            Assert.That(unsetExit, Is.EqualTo(Refused), "expected the unset case to be refused.");

            Assert.That(
                sentinelOutput,
                Is.Not.EqualTo(unsetOutput),
                "the two refusals are word for word identical, so the preflight has collapsed the "
                + "very distinction it was written to restore. The remedies differ: a retired "
                + "sentinel is migrated to `auto` or to a pinned value, an unset variable is one "
                + "the operator never supplied.");

            // Inequality alone is far too weak to rest on. Collapsing the unset
            // verdict into the sentinel one still yields two strings that differ,
            // because the sentinel message quotes the offending value and an unset
            // knob quotes an empty one. So each message is also required to make
            // its OWN claim and forbidden from making the other's.
            Assert.That(
                sentinelOutput,
                Does.Contain("PARSES BUT MEANS"),
                "the sentinel refusal must say that the value parsed and still meant nothing, "
                + "which is the whole content of the diagnosis.");

            Assert.That(
                unsetOutput,
                Does.Not.Contain("PARSES BUT MEANS"),
                "the unset refusal must not borrow the sentinel's diagnosis. Nothing parsed, "
                + "because nothing was supplied, and telling an operator that their value parsed "
                + "sends them to inspect a value that does not exist.");

            Assert.That(
                sentinelOutput,
                Does.Contain(AutoToken),
                "the sentinel refusal must name the migration target, or an operator who "
                + "deliberately pinned `0` to select the derivation is told only that their choice "
                + "is refused and not how to express it.");

            Assert.That(
                unsetOutput,
                Does.Contain("is UNSET"),
                "the unset refusal must say the variable is unset rather than reusing the sentinel "
                + "wording.");
        });
    }

    [Test]
    public void The_auto_token_is_accepted_on_exactly_the_knobs_whose_parser_this_repository_owns()
    {
        var wrong = new List<string>();

        foreach (var knob in GuardedKnobsInOverlay())
        {
            var expectedToBeAccepted = KnobsWithAnAutoToken.Contains(knob, StringComparer.Ordinal);
            var (exit, _) = RunPreflight(HealthyEnv(overrides: new() { [knob] = AutoToken }));
            var wasAccepted = exit == 0;

            if (wasAccepted != expectedToBeAccepted)
            {
                wrong.Add(
                    $"{knob}: expected `{AutoToken}` to be {(expectedToBeAccepted ? "accepted" : "refused")}, "
                    + $"exit was {exit}");
            }
        }

        Assert.That(
            wrong,
            Is.Empty,
            "the `auto` token must be accepted where a parser in this repository translates it, and "
            + "refused everywhere else. The split is a constraint rather than a preference: compose "
            + "cannot rewrite a value in transit, so a token invented for a variable Docker or the "
            + "CLR finally reads is just a foreign string handed to a parser that will ignore it and "
            + "carry on. Accepting `auto` on one of those five would reintroduce #2863 wearing the "
            + "name of its own fix.");
    }

    [Test]
    public void Every_knob_that_accepts_auto_is_read_by_a_host_that_declares_the_token()
    {
        var root = HygieneRepository.FindRepoRoot();
        var missing = new List<string>();

        foreach (var (knob, source) in AutoTokenParsers)
        {
            var path = Path.Combine(root, source.Replace('/', Path.DirectorySeparatorChar));

            if (!File.Exists(path))
            {
                missing.Add($"{knob}: {source} does not exist");
                continue;
            }

            var text = File.ReadAllText(path);

            if (!text.Contains($"AutoToken = \"{AutoToken}\"", StringComparison.Ordinal))
            {
                missing.Add($"{knob}: {source} declares no AutoToken constant");
            }
        }

        Assert.Multiple(() =>
        {
            Assert.That(
                AutoTokenParsers.Select(p => p.Knob),
                Is.EquivalentTo(KnobsWithAnAutoToken),
                "every knob the preflight accepts `auto` for must have a named parser here.");

            Assert.That(
                missing,
                Is.Empty,
                "the preflight accepts `auto` for a knob whose host does not declare the token. That "
                + "is a deployment that passes its own preflight and then hands a string the reader "
                + "does not recognise to the reader: for the replay ceiling it throws at startup, "
                + "which is loud and recoverable, but for the intra-op thread count it falls through "
                + "to the derived path SILENTLY, which is indistinguishable from success and is "
                + "exactly the failure mode #2863 is about.");
        });
    }

    /// <summary>
    /// Reads the guarded variable names off the overlay itself.
    /// </summary>
    private static List<string> GuardedKnobsInOverlay()
    {
        var path = Path.Combine(
            HygieneRepository.FindRepoRoot(),
            ComposeDirectory.Replace('/', Path.DirectorySeparatorChar),
            TuningComposeFile);

        Assert.That(File.Exists(path), Is.True, $"expected the tuning overlay at {path}.");

        var names = new List<string>();

        foreach (var raw in File.ReadAllLines(path))
        {
            // Comments are skipped deliberately. This fixture's own explanation of the
            // defect quotes `${VAR:?...}` in the header block, and counting those would
            // make the guarded set depend on how much prose the file carries.
            if (raw.TrimStart().StartsWith('#'))
            {
                continue;
            }

            foreach (Match match in GuardedReference.Matches(raw))
            {
                var name = match.Groups["name"].Value;

                // The memory archive path is guarded the same way but is a path rather
                // than a resource knob: it has no falsy value in its domain, so it is
                // not in scope and the preflight does not adjudicate it.
                if (name.Equals("REPOCONTEXT_MEMORY_ARCHIVE_PATH", StringComparison.Ordinal))
                {
                    continue;
                }

                if (!names.Contains(name, StringComparer.Ordinal))
                {
                    names.Add(name);
                }
            }
        }

        return names;
    }

    /// <summary>
    /// Asks the preflight's knob module which variables it registers, so the comparison
    /// is between two derivations rather than between a derivation and a copy.
    /// </summary>
    private static List<string> KnobsKnownToThePreflight()
    {
        var module = Path.Combine(
            HygieneRepository.FindRepoRoot(),
            ComposeDirectory.Replace('/', Path.DirectorySeparatorChar),
            KnobModule.Replace('/', Path.DirectorySeparatorChar));

        Assert.That(File.Exists(module), Is.True, $"expected the knob module at {module}.");

        var script =
            $". '{module}'; "
            + "Get-TuningKnob | ForEach-Object { $_.Name }";

        // -Command is safe HERE SPECIFICALLY, and only because of what the next line reads.
        // pwsh -Command collapses every non-zero exit to 1, so it destroys the distinction
        // between one failure and another; zero is the single value it preserves intact. The
        // assertion below is zero-vs-non-zero, so nothing this call can observe is lost.
        //
        // If a future edit ever reads a SPECIFIC value here (to tell "module missing" from
        // "module threw", say), it must move to -File first: measured, a script really exiting
        // 3 reports 3 through -File and 1 through -Command. RunPreflight below already reads
        // specific values (Refused = 2) and is on -File for exactly this reason.
        //
        // Do not take "use -File everywhere" away from that. -File has the opposite fault: it
        // discards a trailing $LASTEXITCODE when a script ends without an explicit exit,
        // turning a leaked 7 into a reported 0 (issue #2718). The rule is the pairing of what a
        // site reads with the channel it reads through, not a preferred flag.
        //
        // Nothing enforces this. It is prose, and no test asserts that this call site and its
        // assertion still agree.
        var (exit, output) = RunShell(["-NoProfile", "-Command", script]);

        Assert.That(exit, Is.Zero, $"listing the registered knobs failed: {output.Trim()}");

        return output
            .Split('\n')
            .Select(line => line.Trim())
            .Where(line => line.Length > 0)
            .ToList();
    }

    /// <summary>
    /// Builds a .env in which every guarded knob carries a meaningful value, optionally
    /// overriding or omitting one of them.
    /// </summary>
    /// <remarks>
    /// The healthy values are assigned by a RULE read off the variable's own name rather
    /// than transcribed per knob, so adding a knob to the overlay does not silently leave
    /// this fixture asserting against a file that omits it. The values are deliberately
    /// not the reference host's: nothing here is a recommendation, and they need only be
    /// meaningful.
    /// </remarks>
    private static string HealthyEnv(
        Dictionary<string, string>? overrides = null,
        string? omit = null)
    {
        var lines = new List<string>
        {
            "# generated by TuningEnvSentinelHygieneTests",
        };

        foreach (var knob in GuardedKnobsInOverlay())
        {
            if (omit is not null && knob.Equals(omit, StringComparison.Ordinal))
            {
                continue;
            }

            string value;

            if (overrides is not null && overrides.TryGetValue(knob, out var supplied))
            {
                value = supplied;
            }
            else if (knob.EndsWith("MEM_LIMIT", StringComparison.Ordinal))
            {
                value = "3072m";
            }
            else if (knob.EndsWith("CPUS", StringComparison.Ordinal))
            {
                value = "1.5";
            }
            else
            {
                value = "3";
            }

            lines.Add($"{knob}={value}");
        }

        return string.Join('\n', lines) + "\n";
    }

    private static (int ExitCode, string Output) RunPreflight(string envContent)
    {
        var script = Path.Combine(
            HygieneRepository.FindRepoRoot(),
            ComposeDirectory.Replace('/', Path.DirectorySeparatorChar),
            PreflightScript.Replace('/', Path.DirectorySeparatorChar));

        Assert.That(File.Exists(script), Is.True, $"expected the preflight at {script}.");

        var envFile = Path.Combine(
            Path.GetTempPath(),
            $"lattice-tuning-{Guid.NewGuid():N}.env");

        File.WriteAllText(envFile, envContent);

        try
        {
            return RunShell(["-NoProfile", "-File", script, "-EnvFile", envFile]);
        }
        finally
        {
            try { File.Delete(envFile); } catch { /* best effort */ }
        }
    }

    /// <summary>
    /// Runs a PowerShell invocation, trying <c>pwsh</c> before Windows PowerShell, and
    /// returns the exit code with stdout and stderr combined.
    /// </summary>
    private static (int ExitCode, string Output) RunShell(string[] arguments)
    {
        foreach (var shell in new[] { "pwsh", "powershell" })
        {
            var start = new ProcessStartInfo(shell)
            {
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false,
            };

            foreach (var argument in arguments)
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

            var stdout = process.StandardOutput.ReadToEnd();
            var stderr = process.StandardError.ReadToEnd();
            process.WaitForExit(milliseconds: 120_000);

            return (process.ExitCode, stdout + stderr);
        }

        RequireShell();
        return (-1, string.Empty);
    }

    /// <summary>
    /// Handles an absent PowerShell asymmetrically: visibly skipped on a developer
    /// machine, red in CI. Never <c>Assert.Inconclusive</c>, which NUnit counts as
    /// neither passed, failed, nor skipped, so a run that checked nothing still prints
    /// <c>Passed!</c> with <c>Skipped: 0</c> and only the total moves.
    /// </summary>
    private static void RequireShell()
    {
        var underCi = string.Equals(
            Environment.GetEnvironmentVariable("GITHUB_ACTIONS"),
            "true",
            StringComparison.OrdinalIgnoreCase);

        if (underCi)
        {
            Assert.Fail(
                "PowerShell is required to evaluate the tuning preflight, and it must be present in "
                + "CI: skipping here would let the sentinel guard rot behind a green check.");
        }

        Assert.Ignore("Neither `pwsh` nor `powershell` could be started, so the preflight assertions "
            + "cannot run.");
    }
}
