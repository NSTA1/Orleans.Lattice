using System;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Text.RegularExpressions;
using NUnit.Framework;
using Orleans.Lattice.Testing.Hygiene;

namespace Orleans.Lattice.Tests.Hygiene;

/// <summary>
/// A <b>report-only</b> census of instrument liveness. It does not gate anything: it
/// partitions the declared instruments by whether any fixture is in a position to
/// witness them moving, and prints the partition so the unwitnessed set can be routed
/// deliberately rather than discovered by an outage.
/// </summary>
/// <remarks>
/// <para>
/// <b>Why this is not a gate, and must not become one.</b> A blanket rule that every
/// instrument needs a liveness witness fires on instruments that are legitimately
/// quiet on every path a unit test can drive - a saturation signal, a retry-exhausted
/// counter, a partition-loss path. A guard that cries wolf earns a bypass flag and is
/// then deleted, taking its true positives with it. So this fixture reports and never
/// fails on a finding. The only assertions here are anti-vacuity ones, which fail when
/// the census itself has stopped working.
/// </para>
/// <para>
/// <b>The three states, and which are already covered.</b> An instrument that cannot
/// produce a series is not one defect but three, and only the first has a gate:
/// </para>
/// <list type="number">
/// <item><description>
/// <b>Declared, no emission site.</b> No code anywhere under <c>src/</c> can move it.
/// Covered by <see cref="InstrumentEmissionCoverageTests"/>, statically and exactly.
/// </description></item>
/// <item><description>
/// <b>Emission site exists, never reached.</b> The site is present and compiles, and
/// no deployment ever executes it. Nothing covers this.
/// </description></item>
/// <item><description>
/// <b>Site executes, value frozen.</b> The call runs and the reported value never
/// changes - an arm that is always the same constant, or a duration that is always
/// zero. Nothing covers this.
/// </description></item>
/// </list>
/// <para>
/// <b>What this census can and cannot decide.</b> States 2 and 3 are <i>not statically
/// separable from each other</i>, nor from an instrument that is perfectly healthy in
/// production and merely has no test. All three present identically in source. Stating
/// that plainly is the point: the census does not claim to resolve them. What it does
/// decide is the strictly weaker and actually-decidable question #2942 asked - <i>is
/// any fixture positioned to observe this instrument moving?</i> - which bounds states
/// 2 and 3 inside a named set instead of leaving them at large across the whole
/// declaration surface.
/// </para>
/// <para>
/// <b>The bound runs in the safe direction.</b> The witness test is name-level: a file
/// that mentions the instrument and also listens to a meter and also asserts something
/// positive. It cannot tell whether the positive assertion was about <i>this</i>
/// instrument or a sibling in the same file, so it <b>over-counts witnesses</b>.
/// Over-counting witnesses under-counts the unwitnessed, which makes the reported
/// blast radius a <b>conservative lower bound</b>: everything named is genuinely
/// unwitnessed, and the true set may be larger. A census that errs toward reporting
/// fewer problems than exist can be acted on; one that errs the other way cannot.
/// </para>
/// </remarks>
[TestFixture]
[Category("Hygiene")]
public sealed class InstrumentLivenessCensusTests
{
    /// <summary>How a declared instrument relates to the fixtures that could observe it.</summary>
    private enum Witness
    {
        /// <summary>A fixture mentions it, listens to a meter, and asserts a positive value.</summary>
        Witnessed,

        /// <summary>No fixture is positioned to observe it moving.</summary>
        Unwitnessed,

        /// <summary>Observed under a listener, but no assertion shape this scan recognises.</summary>
        Unresolved,
    }

    private sealed record Declared(string MetricName, string Kind, bool IsObservable, IReadOnlySet<string> FieldNames);

    private sealed record Census(Declared Instrument, bool HasEmissionSite, Witness Witness);

    /// <summary>Recognises a fixture that attaches a listener and can therefore see measurements.</summary>
    private static readonly Regex ListensToAMeter = new(
        @"MeterListening\.|new MeterListener|StartForInstrument|StartForMeter",
        RegexOptions.Compiled);

    /// <summary>
    /// Recognises an assertion that a measured quantity is present or positive. Kept
    /// deliberately narrow: a shape this misses lands in <see cref="Witness.Unresolved"/>,
    /// which is honest, whereas a shape it over-matches lands in
    /// <see cref="Witness.Witnessed"/> and hides a real gap.
    /// </summary>
    private static readonly Regex AssertsSomethingPositive = new(
        @"Is\.Positive|Is\.GreaterThan\(\s*0|Is\.GreaterThanOrEqualTo\(\s*[1-9]|Is\.Not\.Empty|Has\.Count\.GreaterThan|Is\.Not\.Zero|Is\.EqualTo\(\s*[1-9]",
        RegexOptions.Compiled);

    private static readonly Lazy<IReadOnlyList<Declared>> Instruments = new(DeclaredInstruments);

    private static readonly Lazy<IReadOnlySet<string>> EmittedFields = new(() =>
        MetricEmissionScanner.Scan(HygieneRepository.FindRepoRoot())
            .Select(static s => s.Instrument)
            .ToHashSet(StringComparer.Ordinal));

    private static readonly Lazy<IReadOnlyList<(string Path, string Text)>> TestCorpusHolder =
        new(LoadTestCorpus);

    private static IReadOnlyList<(string Path, string Text)> TestCorpus => TestCorpusHolder.Value;

    private static readonly Lazy<IReadOnlyList<Census>> Rows = new(Classify);

    /// <summary>
    /// The census. Report-only: it prints the partition and asserts nothing about the
    /// findings. Routing the unwitnessed set is a human decision, deliberately.
    /// </summary>
    [Test]
    public void Instrument_liveness_census_is_reported()
    {
        var rows = Rows.Value;
        var report = new StringBuilder();

        var unwitnessed = rows.Where(static r => r.Witness == Witness.Unwitnessed).ToList();
        var unresolved = rows.Where(static r => r.Witness == Witness.Unresolved).ToList();
        var witnessed = rows.Count(static r => r.Witness == Witness.Witnessed);
        var noSite = rows.Count(static r => !r.HasEmissionSite && !r.Instrument.IsObservable);
        var observableNoSite = rows.Count(static r => !r.HasEmissionSite && r.Instrument.IsObservable);

        report.AppendLine("Instrument liveness census (report-only; no finding here fails this test).")
            .AppendLine()
            .AppendLine($"  instruments examined : {rows.Count}")
            .AppendLine($"  fixtures scanned     : {TestCorpus.Count}")
            .AppendLine()
            .AppendLine("  The three states an unusable instrument can be in:")
            .AppendLine()
            .AppendLine("    state                                  | covered by                       | count")
            .AppendLine("    ---------------------------------------+----------------------------------+------")
            .AppendLine($"    1. declared, no emission site          | InstrumentEmissionCoverageTests  | {noSite,5}")
            .AppendLine($"    2. site exists, never reached          | nothing                          |   n/a")
            .AppendLine($"    3. site executes, value frozen         | nothing                          |   n/a")
            .AppendLine()
            .AppendLine($"  State 1 counts SYNCHRONOUS declarations only, which is the population that")
            .AppendLine($"  gate covers. {observableNoSite} observable instrument(s) also have no separate emission")
            .AppendLine("  site, and that is correct rather than a defect: an observable supplies its")
            .AppendLine("  callback at the declaration. Folding those into state 1 would report a")
            .AppendLine("  non-zero count against a green gate, which is the same arity mismatch this")
            .AppendLine("  epic keeps counting.")
            .AppendLine()
            .AppendLine("  States 2 and 3 are not statically separable, from each other or from a")
            .AppendLine("  healthy-but-untested instrument. They are bounded instead: both must lie")
            .AppendLine("  inside the unwitnessed set below, because an instrument some fixture drives")
            .AppendLine("  to a positive value is in neither state.")
            .AppendLine()
            .AppendLine("    witness            | count")
            .AppendLine("    -------------------+------")
            .AppendLine($"    witnessed          | {witnessed,5}")
            .AppendLine($"    unresolved         | {unresolved.Count,5}")
            .AppendLine($"    unwitnessed        | {unwitnessed.Count,5}")
            .AppendLine()
            .AppendLine("  The unwitnessed count is a LOWER bound. The witness test is name-level and")
            .AppendLine("  cannot attribute a positive assertion to one instrument among several in a")
            .AppendLine("  file, so it over-counts witnesses and therefore under-counts this set.")
            .AppendLine("  Being named here is evidence; being absent from here is not.")
            .AppendLine();

        Append(report, "Unwitnessed - no fixture is positioned to observe these moving (states 2 and 3 lie in here):", unwitnessed);
        Append(report, "Unresolved - observed under a listener, but with no assertion shape this scan recognises:", unresolved);

        TestContext.Out.WriteLine(report.ToString());

        Assert.Pass(
            $"Census reported {rows.Count} instrument(s): {witnessed} witnessed, "
            + $"{unresolved.Count} unresolved, {unwitnessed.Count} unwitnessed.");
    }

    /// <summary>
    /// Anti-vacuity, and the only thing here that can fail. Each input is asserted
    /// separately: a census that examined nothing, or scanned no fixtures, reports a
    /// clean empty partition that reads exactly like a healthy repository.
    /// </summary>
    /// <remarks>
    /// The witnessed floor is the load-bearing one. If the reflection population or the
    /// fixture corpus silently empties, every instrument classifies
    /// <see cref="Witness.Unwitnessed"/> and the census reports a spectacular blast
    /// radius that is entirely an artefact of its own machinery - a verdict assertion
    /// would pass straight through that, which is why the counts are asserted instead.
    /// </remarks>
    [Test]
    public void Census_inputs_are_not_vacuous()
    {
        Assert.Multiple(() =>
        {
            Assert.That(
                Instruments.Value,
                Is.Not.Empty,
                "Reflection found no declared instruments, so the census examined nothing and "
                + "its empty partition is an artefact rather than a measurement.");

            Assert.That(
                TestCorpus,
                Is.Not.Empty,
                "No fixture source was found under test/. Every instrument would then classify "
                + "unwitnessed, reporting a blast radius produced entirely by the scan failing.");

            Assert.That(
                EmittedFields.Value,
                Is.Not.Empty,
                "The emission scan found no sites under src/, so the emission-site column is "
                + "uniformly false and state 1 is over-reported.");

            Assert.That(
                Rows.Value.Count(static r => r.Witness == Witness.Witnessed),
                Is.GreaterThan(0),
                "No instrument was classified witnessed. The witness detector cannot "
                + "discriminate, so the unwitnessed set it reports is not evidence of anything.");
        });
    }

    /// <summary>
    /// The census must agree with the gate that already covers state 1. A disagreement
    /// means one of the two populations has drifted, and a census that contradicts a
    /// shipped gate is reporting on something other than what it claims.
    /// </summary>
    [Test]
    public void Census_state_one_agrees_with_the_shipped_emission_gate()
    {
        var emitted = EmittedFields.Value;

        var censusSaysNoSite = Rows.Value
            .Where(static r => !r.HasEmissionSite)
            .Where(static r => !r.Instrument.IsObservable)
            .Select(static r => r.Instrument.MetricName)
            .ToHashSet(StringComparer.Ordinal);

        var gateWouldReport = Instruments.Value
            .Where(static d => !d.IsObservable)
            .Where(d => !d.FieldNames.Any(emitted.Contains))
            .Select(static d => d.MetricName)
            .ToHashSet(StringComparer.Ordinal);

        Assert.That(
            censusSaysNoSite,
            Is.EquivalentTo(gateWouldReport),
            "The census and InstrumentEmissionCoverageTests disagree about which synchronous "
            + "instruments have no emission site. Two registries of the same fact that can "
            + "silently diverge is the defect class this epic exists to remove.");
    }

    private static void Append(StringBuilder report, string heading, IReadOnlyList<Census> rows)
    {
        report.AppendLine(heading);

        if (rows.Count == 0)
        {
            report.AppendLine("  (none)").AppendLine();
            return;
        }

        foreach (var row in rows.OrderBy(static r => r.Instrument.MetricName, StringComparer.Ordinal))
        {
            var site = row.HasEmissionSite ? "site" : "NO SITE";
            var kind = row.Instrument.IsObservable ? $"{row.Instrument.Kind}, observable" : row.Instrument.Kind;
            report.AppendLine($"  {row.Instrument.MetricName} [{kind}] ({site})");
        }

        report.AppendLine();
    }

    private static IReadOnlyList<Census> Classify()
    {
        var emitted = EmittedFields.Value;

        return Instruments.Value
            .Select(d => new Census(d, d.FieldNames.Any(emitted.Contains), WitnessFor(d)))
            .ToList();
    }

    private static Witness WitnessFor(Declared instrument)
    {
        var sawListener = false;

        foreach (var (_, text) in TestCorpus)
        {
            if (!Mentions(text, instrument))
            {
                continue;
            }

            if (!ListensToAMeter.IsMatch(text))
            {
                // A mention with no listener is not an observation. Fixtures that
                // enumerate instrument names for doc or dashboard coverage match here,
                // and counting them as witnesses would make every documented
                // instrument look exercised.
                continue;
            }

            sawListener = true;

            if (AssertsSomethingPositive.IsMatch(text))
            {
                return Witness.Witnessed;
            }
        }

        return sawListener ? Witness.Unresolved : Witness.Unwitnessed;
    }

    private static bool Mentions(string text, Declared instrument) =>
        text.Contains(instrument.MetricName, StringComparison.Ordinal)
        || instrument.FieldNames.Any(f => Regex.IsMatch(text, $@"\b{Regex.Escape(f)}\b"));

    private static IReadOnlyList<(string Path, string Text)> LoadTestCorpus()
    {
        var root = Path.Combine(HygieneRepository.FindRepoRoot(), "test");
        var files = new List<(string, string)>();

        if (!Directory.Exists(root))
        {
            return files;
        }

        foreach (var path in Directory.EnumerateFiles(root, "*.cs", SearchOption.AllDirectories))
        {
            if (path.Contains($"{Path.DirectorySeparatorChar}bin{Path.DirectorySeparatorChar}", StringComparison.Ordinal)
                || path.Contains($"{Path.DirectorySeparatorChar}obj{Path.DirectorySeparatorChar}", StringComparison.Ordinal))
            {
                continue;
            }

            files.Add((path, File.ReadAllText(path)));
        }

        return files;
    }

    /// <summary>
    /// Enumerates instrument declarations from the compiled core assembly. Aliases fold
    /// into one entry: a field assigned from another instrument is a second name for the
    /// same instrument, and an observation through either name observes the one series.
    /// </summary>
    private static IReadOnlyList<Declared> DeclaredInstruments()
    {
        var byName = new Dictionary<string, (string Kind, bool Observable, HashSet<string> Fields)>(StringComparer.Ordinal);

        foreach (var type in typeof(LatticeMetrics).Assembly.GetTypes())
        {
            foreach (var field in type.GetFields(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Static))
            {
                if (!typeof(Instrument).IsAssignableFrom(field.FieldType))
                {
                    continue;
                }

                Instrument? instrument;
                try
                {
                    instrument = field.GetValue(null) as Instrument;
                }
                catch (Exception)
                {
                    continue;
                }

                if (instrument is null)
                {
                    continue;
                }

                if (!byName.TryGetValue(instrument.Name, out var entry))
                {
                    entry = (instrument.GetType().Name, instrument.IsObservable, new HashSet<string>(StringComparer.Ordinal));
                    byName[instrument.Name] = entry;
                }

                entry.Fields.Add(field.Name);
            }
        }

        return byName
            .Select(static e => new Declared(e.Key, e.Value.Kind, e.Value.Observable, e.Value.Fields))
            .OrderBy(static d => d.MetricName, StringComparer.Ordinal)
            .ToList();
    }
}
