using System;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.IO;
using System.Linq;
using System.Reflection;
using NUnit.Framework;
using Orleans.Lattice.Testing.Hygiene;

namespace Orleans.Lattice.Tests.Hygiene;

/// <summary>
/// Asserts that every synchronous instrument declared in the core assembly has at
/// least one emission site in <c>src/</c>: a declaration without one is exported as
/// <c># HELP</c> and <c># TYPE</c>, is therefore resolvable by every tool that reads
/// the exposition, and can never produce a series.
/// </summary>
/// <remarks>
/// <para>
/// <b>Quantifier.</b> This gate quantifies over <i>declarations</i>: for all declared
/// synchronous instruments, there exists an emission site. That is deliberately the
/// converse of <see cref="MetricEmissionScanner"/>'s consumers, which quantify over
/// <i>emissions</i>: for all emission sites, the tags are well formed. The two
/// populations are disjoint in the failing case - an instrument with no emissions
/// contributes nothing to the emission population - so no amount of greenness in an
/// emission-side gate can detect a violation here. Three repository gates pass
/// correctly on a declared-but-never-emitted instrument for exactly this reason: the
/// dashboard token resolver finds it declared, the histogram-bucket gate finds it is
/// genuinely a <see cref="Histogram{T}"/>, and the emission scanner iterates a set it
/// is not in.
/// </para>
/// <para>
/// <b>What this gate is silent about.</b> Stated explicitly, because a gate whose
/// coverage is unstated will be assumed total.
/// </para>
/// <list type="bullet">
/// <item><description>
/// <b>Observable instruments are out of scope and must be.</b> An
/// <see cref="ObservableCounter{T}"/>, <see cref="ObservableGauge{T}"/> or
/// <see cref="ObservableUpDownCounter{T}"/> supplies its callback at the declaration
/// site, so it has no separate emission site anywhere and would be a false positive
/// in every case. The exclusion is taken from the runtime's own
/// <see cref="Instrument.IsObservable"/> rather than from a regex over the declared
/// type, because a declaration whose generic argument is inferred
/// (<c>CreateObservableGauge(Name, () =&gt; ...)</c>) is invisible to a pattern that
/// expects <c>&lt;T&gt;</c> - a narrowing that returns a smaller clean number rather
/// than an error.
/// </description></item>
/// <item><description>
/// <b>Only the core assembly's declarations are covered.</b> Other packages declare
/// instruments on their own metrics classes and this gate does not see them. Emission
/// coverage is nevertheless searched across the whole of <c>src/</c>, which is the
/// direction that matters for correctness here: several core instruments are emitted
/// from other packages (<c>TagIndexReconcileGrain</c> and <c>WalSaturationSignal</c>
/// publish onto <c>LatticeMetrics.Meter</c>), so a scan narrowed to <c>src/lattice/</c>
/// would report live instruments as dead.
/// </description></item>
/// <item><description>
/// <b>An emission reachable only from <c>test/</c> or <c>benchmark/</c> does not
/// count</b>, which is intended: an instrument exercised only by its own test still
/// produces no series in production.
/// </description></item>
/// <item><description>
/// <b>This gate proves a site exists, not that it executes.</b> An emission behind a
/// condition that is never true in a given deployment still satisfies it. That is the
/// <c>declared-unfired</c> case, which is a different question and is not evidence of
/// a dead instrument.
/// </description></item>
/// </list>
/// </remarks>
[TestFixture]
[Category("Hygiene")]
public sealed class InstrumentEmissionCoverageTests
{
    /// <summary>A synchronous instrument declared in the core assembly.</summary>
    /// <param name="MetricName">The exported metric name.</param>
    /// <param name="Kind">The runtime instrument type name.</param>
    /// <param name="FieldNames">
    /// Every static field referring to this instrument. Aliases are folded in
    /// deliberately: a field such as
    /// <c>private static readonly Histogram&lt;long&gt; ApplyLag = LatticeMetrics.ViewApplyLag;</c>
    /// is a reference, not a second declaration, and an emission through either name
    /// is an emission of the one instrument.
    /// </param>
    private sealed record DeclaredInstrument(string MetricName, string Kind, IReadOnlySet<string> FieldNames);

    private static readonly Lazy<IReadOnlyList<DeclaredInstrument>> Synchronous =
        new(() => Declared(observable: false));

    private static readonly Lazy<IReadOnlyList<DeclaredInstrument>> Observable =
        new(() => Declared(observable: true));

    private static readonly Lazy<IReadOnlySet<string>> EmittedNames = new(() =>
        MetricEmissionScanner.Scan(HygieneRepository.FindRepoRoot())
            .Select(static s => s.Instrument)
            .ToHashSet(StringComparer.Ordinal));

    /// <summary>
    /// Instruments known to be declared without an emission site, each held open by a
    /// tracked issue rather than waived.
    /// </summary>
    /// <remarks>
    /// An exemption here is a statement that the instrument <i>should</i> emit and does
    /// not yet - never that its absence is acceptable. It is deliberately falsifiable:
    /// <see cref="Every_exemption_is_still_necessary"/> fails when an exempted
    /// instrument gains an emission, so the list cannot quietly outlive its reason. A
    /// suppression that cannot be contradicted is indistinguishable from an oversight,
    /// which is the failure mode this whole gate exists to remove.
    /// </remarks>
    private static readonly IReadOnlyDictionary<string, string> Exempt =
        new Dictionary<string, string>(StringComparer.Ordinal);

    /// <summary>
    /// The gate. Every synchronous declaration must have an emission site somewhere
    /// under <c>src/</c>.
    /// </summary>
    [Test]
    public void Every_declared_synchronous_instrument_has_an_emission_site()
    {
        var emitted = EmittedNames.Value;

        var dead = Synchronous.Value
            .Where(d => !d.FieldNames.Any(emitted.Contains))
            .Where(d => !Exempt.ContainsKey(d.MetricName))
            .OrderBy(static d => d.MetricName, StringComparer.Ordinal)
            .ToList();

        Assert.That(
            dead,
            Is.Empty,
            $"{dead.Count} instrument(s) are declared and registered but have no emission site "
            + "anywhere under src/. Each is exported as # HELP and # TYPE, so every tool that "
            + "reads the exposition resolves it, and none can ever produce a series. A panel "
            + "querying one renders empty - or, with 'or vector(0)', a confident zero that is "
            + "indistinguishable from a measured one. Either wire it up or delete it, and "
            + "delete any documentation or dashboard panel that routes a reader to it:"
            + Environment.NewLine + "  "
            + string.Join(
                Environment.NewLine + "  ",
                dead.Select(d => $"{d.MetricName} [{d.Kind}] declared as {string.Join(" / ", d.FieldNames.OrderBy(static n => n, StringComparer.Ordinal))}")));
    }

    /// <summary>
    /// Every exemption must still be necessary. This is what stops the exemption list
    /// becoming a quiet permanent waiver: the moment an exempted instrument gains an
    /// emission site, the gate fails and demands the entry be removed. An exemption you
    /// cannot contradict is not an exemption, it is an oversight with a comment on it.
    /// </summary>
    [Test]
    public void Every_exemption_is_still_necessary()
    {
        var emitted = EmittedNames.Value;
        var declared = Synchronous.Value.ToDictionary(static d => d.MetricName, StringComparer.Ordinal);

        Assert.Multiple(() =>
        {
            foreach (var (name, justification) in Exempt.OrderBy(static e => e.Key, StringComparer.Ordinal))
            {
                Assert.That(
                    declared.ContainsKey(name),
                    Is.True,
                    $"'{name}' is exempted but is no longer a declared synchronous instrument. "
                    + "Remove the exemption.");

                if (declared.TryGetValue(name, out var instrument))
                {
                    Assert.That(
                        instrument.FieldNames.Any(emitted.Contains),
                        Is.False,
                        $"'{name}' is exempted as unemitted but now has an emission site. The "
                        + $"exemption is stale and must be deleted. Recorded reason: {justification}");
                }
            }
        });
    }

    /// <summary>
    /// Anti-vacuity. A repository-wide gate that silently matches nothing reports green
    /// and is worse than no gate at all, so each input population is asserted non-empty
    /// independently - a single combined assertion would let one empty side hide behind
    /// the other.
    /// </summary>
    [Test]
    public void Gate_inputs_are_not_vacuous()
    {
        Assert.Multiple(() =>
        {
            Assert.That(
                Synchronous.Value,
                Is.Not.Empty,
                "Reflection found no synchronous instruments in the core assembly. The gate "
                + "would then quantify over an empty set and pass against any source at all.");

            Assert.That(
                EmittedNames.Value,
                Is.Not.Empty,
                "The emission scan found no emission sites under src/. Every declaration would "
                + "then read as dead, or - had the assertion been inverted - nothing would.");

            Assert.That(
                Observable.Value,
                Is.Not.Empty,
                "No observable instruments were found. The observable exclusion would then be "
                + "inert, and a regression that made it over-broad could not be detected here.");
        });
    }

    /// <summary>
    /// Known-positive control: the gate's predicate, applied to a declaration that is
    /// known to have no emission site, must select it. A detector that has not been
    /// shown to fire is not evidence, and this control is independent of the repository
    /// ever containing a real violation - it keeps working after the gate goes green.
    /// </summary>
    [Test]
    public void Coverage_predicate_selects_a_declaration_with_no_emission_site()
    {
        var emitted = EmittedNames.Value;

        var planted = new DeclaredInstrument(
            "orleans_lattice_control_never_emitted",
            nameof(Histogram<long>),
            new HashSet<string>(StringComparer.Ordinal) { "ControlInstrumentThatIsNeverEmitted" });

        Assert.That(
            planted.FieldNames.Any(emitted.Contains),
            Is.False,
            "The control's field name was found in the emission set, so it is not a "
            + "known-negative and proves nothing.");

        var live = Synchronous.Value.FirstOrDefault(d => d.FieldNames.Any(emitted.Contains));

        Assert.That(
            live,
            Is.Not.Null,
            "No declared instrument has an emission site, so the control cannot demonstrate "
            + "that the predicate discriminates rather than simply always firing.");

        // Both arms through the identical predicate: it must select the planted
        // declaration and reject a real emitting one. A predicate shown only to fire
        // has not been shown to discriminate.
        Assert.Multiple(() =>
        {
            Assert.That(Selects(planted, emitted), Is.True, "The predicate failed to select a declaration with no emission site.");
            Assert.That(Selects(live!, emitted), Is.False, $"The predicate selected {live!.MetricName}, which does have an emission site.");
        });
    }

    /// <summary>
    /// The observable exclusion must be load-bearing and correct: observables are
    /// excluded, and - the half that would otherwise go unnoticed - at least one of
    /// them would have been reported dead had they not been, which is what makes the
    /// exclusion necessary rather than merely present.
    /// </summary>
    [Test]
    public void Observable_instruments_are_excluded_and_the_exclusion_is_load_bearing()
    {
        var emitted = EmittedNames.Value;
        var synchronousNames = Synchronous.Value.Select(static d => d.MetricName).ToHashSet(StringComparer.Ordinal);

        var overlap = Observable.Value.Where(o => synchronousNames.Contains(o.MetricName)).ToList();

        Assert.That(
            overlap,
            Is.Empty,
            "An instrument was classified both observable and synchronous, so the two "
            + "populations are not a partition and the exclusion is ambiguous.");

        var wouldHaveBeenReported = Observable.Value.Count(o => !o.FieldNames.Any(emitted.Contains));

        Assert.That(
            wouldHaveBeenReported,
            Is.GreaterThan(0),
            "Every observable instrument has what looks like an emission site, so excluding "
            + "them changes nothing and this exclusion is not the reason the gate is green. "
            + "That would mean the scanner is matching observables' callbacks as emissions, "
            + "which is a different defect from the one this exclusion exists to avoid.");
    }

    /// <summary>
    /// Runtime cross-check on the emission scan. A source parser can only be validated
    /// against a different kind of evidence, never against a threshold on its own
    /// output: a parse that silently narrows returns a smaller clean number that clears
    /// every floor anyone would think to write, and is indistinguishable from healthy.
    /// </summary>
    [Test]
    public void Emission_scan_resolves_instrument_identifiers_that_reflection_confirms_exist()
    {
        var declaredFields = Synchronous.Value
            .SelectMany(static d => d.FieldNames)
            .Concat(Observable.Value.SelectMany(static d => d.FieldNames))
            .ToHashSet(StringComparer.Ordinal);

        var resolved = EmittedNames.Value.Count(declaredFields.Contains);

        Assert.That(
            resolved,
            Is.GreaterThan(0),
            "No identifier reported by the emission scan corresponds to any instrument field "
            + "reflection can see in the core assembly. The scan is resolving something other "
            + "than instrument names, so its emptiness for any given instrument is not "
            + "evidence that the instrument is unemitted.");
    }

    private static bool Selects(DeclaredInstrument declaration, IReadOnlySet<string> emitted) =>
        !declaration.FieldNames.Any(emitted.Contains);

    /// <summary>
    /// Enumerates instrument declarations from the compiled core assembly, partitioned
    /// by <see cref="Instrument.IsObservable"/>.
    /// </summary>
    private static IReadOnlyList<DeclaredInstrument> Declared(bool observable)
    {
        var byName = new Dictionary<string, (string Kind, HashSet<string> Fields)>(StringComparer.Ordinal);

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
                    // A field whose static initialiser needs a host is not evidence either way.
                    continue;
                }

                if (instrument is null || instrument.IsObservable != observable)
                {
                    continue;
                }

                if (!byName.TryGetValue(instrument.Name, out var entry))
                {
                    entry = (instrument.GetType().Name, new HashSet<string>(StringComparer.Ordinal));
                    byName[instrument.Name] = entry;
                }

                entry.Fields.Add(field.Name);
            }
        }

        return byName
            .Select(kv => new DeclaredInstrument(kv.Key, kv.Value.Kind, kv.Value.Fields))
            .OrderBy(static d => d.MetricName, StringComparer.Ordinal)
            .ToList();
    }
}
