using System.Diagnostics.Metrics;
using System.Text.Json;
using System.Text.RegularExpressions;
using NUnit.Framework;
using Orleans.Lattice.Testing.Hygiene;
using Orleans.Lattice.Testing.Metrics;

namespace Orleans.Lattice.Testing;

/// <summary>
/// Reusable drift-guard base that asserts every instrument published on a
/// single <see cref="System.Diagnostics.Metrics.Meter"/> is charted by at least
/// one bundled Grafana dashboard, and that every metric token a supplied
/// dashboard references for that meter resolves to a live instrument.
/// <para>
/// A concrete subclass in a package's test project supplies the meter it owns
/// (via <see cref="Meter"/>), the canonical names of any instruments whose
/// factories are not statically wired at test time (via
/// <see cref="AdditionalInstrumentNames"/> - observable gauges in particular),
/// and the dashboard JSON that is expected to cover the meter (via
/// <see cref="DashboardJson"/>). This mirrors the shared Hygiene / size-contract
/// base fixtures: the reflection-and-JSON logic lives here once and every
/// package reuses it by construction, so an add-on package (auth, membership,
/// ...) that ships a new instrument fails CI unless it also lands a dashboard
/// panel.
/// </para>
/// </summary>
/// <remarks>
/// The library is deliberately product-agnostic: it works purely against the
/// BCL <see cref="System.Diagnostics.Metrics.Meter"/> and the JSON strings the
/// subclass hands it, so it never references any Orleans.Lattice assembly.
/// Coverage uses <em>forward</em> mapping (live instrument name to the canonical
/// Prometheus token forms the OpenTelemetry exporter would emit) rather than
/// reverse mapping, because instrument names embed underscores and the reverse
/// direction is ambiguous once both dot separators and embedded underscores map
/// to underscores.
/// </remarks>
public abstract class MeterDashboardCoverageTestsBase
{
    private static readonly Regex InstrumentTokenRegex =
        new(@"\borleans_lattice_[a-z0-9_]+\b", RegexOptions.Compiled);

    /// <summary>The name of the meter whose instruments must be paneled.</summary>
    protected abstract string MeterName { get; }

    /// <summary>
    /// The live meter that owns the instruments under test. A snapshot
    /// <see cref="MeterListener"/> enumerates every instrument published on it.
    /// </summary>
    protected abstract Meter Meter { get; }

    /// <summary>
    /// The Grafana dashboard JSON documents expected to cover this meter. A
    /// combined dashboard that also charts other meters is fine: only the tokens
    /// that belong to <see cref="MeterName"/> are considered.
    /// </summary>
    protected abstract IEnumerable<string> DashboardJson { get; }

    /// <summary>
    /// Canonical dotted names of instruments whose factories are not statically
    /// wired at test time (observable gauges created only when the owning
    /// subsystem starts), so a snapshot <see cref="MeterListener"/> does not see
    /// them. Override to include them in the coverage guard.
    /// </summary>
    protected virtual IEnumerable<string> AdditionalInstrumentNames => Array.Empty<string>();

    /// <summary>
    /// Canonical dotted names of instruments that are intentionally not charted.
    /// Override (with a justifying comment) to tolerate a deliberate gap.
    /// </summary>
    protected virtual IReadOnlySet<string> IntentionallyUnpaneledInstruments { get; } =
        new HashSet<string>(StringComparer.Ordinal);

    /// <summary>
    /// Whether an instrument published on <see cref="Meter"/> is in scope for
    /// this fixture. The default includes every instrument on the meter, which
    /// is right for a package that owns its meter outright.
    /// </summary>
    /// <remarks>
    /// Override it when a package publishes its instruments on a <em>shared</em>
    /// meter it does not own - the grain-index package adds its series to the
    /// core <c>orleans.lattice</c> meter, for instance - so the guard covers the
    /// package's own instruments without demanding that its dashboard also chart
    /// every instrument the meter's owner publishes.
    /// </remarks>
    /// <param name="instrumentName">The instrument's canonical dotted name.</param>
    /// <returns><c>true</c> when the instrument is this fixture's to cover.</returns>
    protected virtual bool IncludeInstrument(string instrumentName) => true;

    /// <summary>
    /// Forward guard: every instrument published on <see cref="Meter"/> exposes
    /// at least one canonical Prometheus token that appears in some supplied
    /// dashboard, so a new instrument cannot ship without a panel.
    /// </summary>
    [Test]
    public void Every_instrument_on_the_meter_is_referenced_by_a_dashboard_panel()
    {
        var referenced = ReferencedTokens();

        var unpaneled = new List<string>();
        foreach (var (name, series) in Instruments())
        {
            if (IntentionallyUnpaneledInstruments.Contains(name))
            {
                continue;
            }

            if (!series.Any(referenced.Contains))
            {
                unpaneled.Add(name);
            }
        }

        unpaneled.Sort(StringComparer.Ordinal);
        Assert.That(unpaneled, Is.Empty,
            $"The following instruments on meter '{MeterName}' are not referenced by any supplied Grafana " +
            "dashboard panel. Add a panel (and a docs/lattice.dashboards/metrics-to-panel-map.md row), or - " +
            "if the omission is intentional - override IntentionallyUnpaneledInstruments with a justifying " +
            $"comment:{Environment.NewLine}  - " + string.Join(Environment.NewLine + "  - ", unpaneled));
    }

    /// <summary>
    /// Reverse guard: every metric token a supplied dashboard references that
    /// belongs to this meter (shares its underscored prefix) resolves to a live
    /// instrument, so a rename or removal fails before the dashboard ships stale.
    /// </summary>
    [Test]
    public void Every_meter_token_referenced_by_the_dashboards_resolves_to_a_live_instrument()
    {
        var known = new HashSet<string>(StringComparer.Ordinal);
        foreach (var (_, series) in Instruments())
        {
            known.UnionWith(series);
        }

        var prefix = MeterName.Replace('.', '_') + "_";

        var unknown = new List<string>();
        foreach (var token in ReferencedTokens())
        {
            if (token.StartsWith(prefix, StringComparison.Ordinal) && !known.Contains(token))
            {
                unknown.Add(token);
            }
        }

        unknown.Sort(StringComparer.Ordinal);
        Assert.That(unknown, Is.Empty,
            $"The supplied dashboards reference metric tokens under meter '{MeterName}' that do not resolve to " +
            $"any live instrument (renamed or removed?):{Environment.NewLine}  - " +
            string.Join(Environment.NewLine + "  - ", unknown));
    }

    /// <summary>
    /// Every instrument this fixture covers has a declared unit and kind the
    /// exporter naming model can read, so its exact series names are known.
    /// </summary>
    /// <remarks>
    /// The two guards above match dashboard tokens against exactly the series
    /// <c>.AddPrometheusExporter()</c> emits (issue #3260), which depends on the
    /// instrument's unit and kind. A statically wired instrument supplies both
    /// through the live <see cref="Instrument"/>; one named only in
    /// <see cref="AdditionalInstrumentNames"/> is read from its declaration in
    /// <c>src/</c>. An instrument neither route can place would otherwise contribute
    /// no series and surface only as an unexplained unpaneled or unresolved entry,
    /// so it is reported here by name.
    /// </remarks>
    [Test]
    public void Every_instrument_on_the_meter_has_a_known_exporter_series_name()
    {
        var unknown = Instruments()
            .Where(static pair => pair.Value.Count == 0)
            .Select(static pair => pair.Key)
            .OrderBy(static name => name, StringComparer.Ordinal)
            .ToList();

        Assert.That(unknown, Is.Empty,
            $"These instruments on meter '{MeterName}' are neither published at test time nor declared in src/ " +
            "with a readable unit and kind, so the exporter series name they render as cannot be derived:" +
            $"{Environment.NewLine}  - " + string.Join(Environment.NewLine + "  - ", unknown));
    }

    private HashSet<string> ReferencedTokens()
    {
        var tokens = new HashSet<string>(StringComparer.Ordinal);
        foreach (var json in DashboardJson)
        {
            using var doc = JsonDocument.Parse(json);
            WalkForExpr(doc.RootElement, tokens);
        }

        return tokens;
    }

    /// <summary>
    /// Every instrument this fixture covers, mapped to the exact series names
    /// <c>.AddPrometheusExporter()</c> emits for it (empty when they cannot be
    /// derived).
    /// </summary>
    private Dictionary<string, IReadOnlyList<string>> Instruments()
    {
        var names = new Dictionary<string, IReadOnlyList<string>>(StringComparer.Ordinal);

        // Force the owning type-initialiser so statically-wired instruments are
        // published before the snapshot listener enumerates them.
        _ = MeterName;
        var meter = Meter;

        using (var listener = new MeterListener())
        {
            listener.InstrumentPublished = (instrument, _) =>
            {
                if (ReferenceEquals(instrument.Meter, meter) && IncludeInstrument(instrument.Name))
                {
                    names[instrument.Name] = PrometheusExporterNaming.SeriesNames(
                        instrument.Name, instrument.Unit, PrometheusExporterNaming.FamilyTypeOf(instrument));
                }
            };
            listener.Start();
        }

        foreach (var name in AdditionalInstrumentNames)
        {
            if (!names.ContainsKey(name))
            {
                names[name] = DeclaredSeriesNames(name);
            }
        }

        // Anti-vacuity control on the SHARED discovery root of both tests in this
        // fixture. Discovering nothing renders identically to discovering a fully
        // paneled meter: the forward gate's `unpaneled` list stays empty and the
        // reverse gate's `known` set is never consulted, so both report a pass
        // while auditing nothing. The collapse is reachable without any obvious
        // mistake - MeterListener.Start() replays already-published instruments
        // re-entrantly, so a metrics class whose `Meter` field is declared below
        // its instruments hands this callback a null meter and ReferenceEquals
        // silently matches nothing (see the Metrics section of
        // .github/copilot-instructions.md). A subclass exists precisely because
        // its package owns instruments on this meter, so zero is never correct.
        HygieneDenominator.RequireExamined(
            names.Count,
            GetType().Name,
            "instruments",
            $"meter '{MeterName}' (plus {AdditionalInstrumentNames.Count()} explicitly named instrument(s))");

        return names;
    }

    /// <summary>
    /// The exporter series names for an instrument read from its declaration in
    /// <c>src/</c>, or an empty list when the declaration does not record both its
    /// kind and its unit.
    /// </summary>
    /// <remarks>
    /// Exactly the series <c>.AddPrometheusExporter()</c> emits (issue #3260). No
    /// alternative spelling is accepted: the repository-context container's
    /// unsuffixed exposition is not what the bundled dashboards target, and
    /// accepting it certified panel queries naming series that exporter never
    /// emits.
    /// </remarks>
    private static IReadOnlyList<string> DeclaredSeriesNames(string instrumentName)
    {
        if (DeclaredInstruments.ByDottedName.TryGetValue(instrumentName, out var kind)
            && DeclaredInstruments.UnitByDottedName.TryGetValue(instrumentName, out var unit))
        {
            return PrometheusExporterNaming.SeriesNames(instrumentName, unit, PrometheusExporterNaming.FamilyTypeOf(kind));
        }

        return [];
    }

    private static void WalkForExpr(JsonElement element, HashSet<string> tokens)
    {
        switch (element.ValueKind)
        {
            case JsonValueKind.Object:
                foreach (var prop in element.EnumerateObject())
                {
                    if ((prop.Name == "expr" || prop.Name == "query") && prop.Value.ValueKind == JsonValueKind.String)
                    {
                        var s = prop.Value.GetString();
                        if (!string.IsNullOrEmpty(s))
                        {
                            foreach (Match m in InstrumentTokenRegex.Matches(s))
                            {
                                tokens.Add(m.Value);
                            }
                        }
                    }

                    WalkForExpr(prop.Value, tokens);
                }

                break;

            case JsonValueKind.Array:
                foreach (var item in element.EnumerateArray())
                {
                    WalkForExpr(item, tokens);
                }

                break;
        }
    }
}
