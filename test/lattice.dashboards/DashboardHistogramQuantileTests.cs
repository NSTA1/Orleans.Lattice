using System.Diagnostics.Metrics;
using System.Text.Json;
using System.Text.RegularExpressions;
using Orleans.Lattice;
using Orleans.Lattice.Dashboards;
using Orleans.Lattice.Replication;
using Orleans.Lattice.Testing.Hygiene;

namespace Orleans.Lattice.Dashboards.Tests;

/// <summary>
/// Maps every Prometheus <c>_bucket</c> token a declared instrument could be
/// written as back to the instrument and the kind that declares it.
/// </summary>
/// <remarks>
/// <para>
/// This map is a <b>binder</b>, not a naming gate. Its job is to attach a bucket
/// token to the instrument a panel author meant, so that the kind gate below can
/// report a panel reading buckets off a counter as a kind violation, and the unit
/// gate can report a wrong unit suffix as a wrong suffix - rather than both being
/// reported as an unknown instrument. It is deliberately permissive for that
/// reason: it generates the unsuffixed and both time-suffixed forms for every
/// instrument. Whether a token is the exact family the exporter emits is judged by
/// <see cref="PrometheusExporterNaming"/>, through <c>DashboardJsonTests</c>.
/// </para>
/// <para>
/// Bucket forms are generated for <b>every</b> kind, not only histograms, so that
/// a panel naming <c>some_counter_bucket</c> resolves to a counter and is reported
/// as a kind violation, rather than failing to resolve at all.
/// </para>
/// </remarks>
internal static class DeclaredBucketTokens
{
    private static readonly Lazy<IReadOnlyDictionary<string, (string Dotted, DeclaredInstrumentKind Kind)>> TokensLazy =
        new(Build, isThreadSafe: true);

    /// <summary>Every candidate bucket token, bound to its declaring instrument and kind.</summary>
    public static IReadOnlyDictionary<string, (string Dotted, DeclaredInstrumentKind Kind)> BucketTokens => TokensLazy.Value;

    /// <summary>Generates every Prometheus bucket token a dotted instrument name could be written as.</summary>
    public static IEnumerable<string> BucketFormsOf(string dottedName)
    {
        ArgumentNullException.ThrowIfNull(dottedName);

        var underscored = dottedName.Replace('.', '_');
        yield return underscored + "_bucket";
        yield return underscored + "_milliseconds_bucket";
        yield return underscored + "_seconds_bucket";
    }

    private static IReadOnlyDictionary<string, (string Dotted, DeclaredInstrumentKind Kind)> Build()
    {
        var bucketTokens = new Dictionary<string, (string, DeclaredInstrumentKind)>(StringComparer.Ordinal);
        foreach (var (dotted, kind) in DeclaredInstruments.ByDottedName)
        {
            foreach (var token in BucketFormsOf(dotted))
            {
                // A longer dotted name can generate a token another name also
                // generates via a unit suffix. Prefer a histogram binding so the
                // gate never reports a false violation on an ambiguous token.
                if (bucketTokens.TryGetValue(token, out var prior) && prior.Item2 == DeclaredInstrumentKind.Histogram)
                {
                    continue;
                }

                bucketTokens[token] = (dotted, kind);
            }
        }

        return bucketTokens;
    }
}


/// <summary>
/// Asserts that every Prometheus bucket series a bundled Grafana panel reads is
/// backed by an instrument that can actually produce bucket series, and that every
/// <c>histogram_quantile</c> call reads a bucket series rather than a family that
/// has none.
/// </summary>
/// <remarks>
/// <para>
/// <b>Only a <c>Histogram&lt;T&gt;</c> ever exports <c>_bucket</c>.</b> A counter,
/// an up-down counter, and every observable instrument export a single sample per
/// series under every exporter, so <c>histogram_quantile</c> over one of them
/// returns an empty vector. That is invisible in Grafana, and the common
/// <c>or vector(0)</c> idiom then substitutes a literal zero - a confident
/// measurement of nothing, which is strictly worse than a blank panel because a
/// blank panel prompts investigation and a zero terminates it.
/// </para>
/// <para>
/// <b>Why no existing gate catches this.</b>
/// <see cref="DashboardJsonTests"/> validates that every metric token a panel
/// names belongs to a live instrument, but its token map registered
/// <c>_bucket</c> forms for <em>every</em> instrument unconditionally, so
/// <c>orleans_lattice_wal_gc_passes_bucket</c> - a counter - passed the name
/// check. The registration asserted that a form exists without establishing that
/// any exporter can produce it. That generosity is narrowed alongside this
/// fixture, and this fixture is what states the invariant directly.
/// <c>DashboardPanelTagDomainTests</c> is out of scope by construction: it
/// compares a PromQL <em>tag value</em> against an instrument's <em>value
/// domain</em>, whereas this compares a PromQL <em>function choice</em> against an
/// instrument's <em>export kind</em>. No shared machinery.
/// </para>
/// <para>
/// <b>Scope.</b> This gate is deliberately exporter-independent. The bundled
/// dashboards are documented to target a Prometheus-exported OpenTelemetry
/// pipeline, which does emit real bucket series for a <c>Histogram&lt;T&gt;</c>,
/// so a histogram-backed quantile panel is correct there and is not flagged. What
/// is wrong under <em>every</em> exporter is reading buckets from an instrument
/// that is not a histogram at all, and that is what is asserted here. The separate
/// question of whether a particular deployment's scrape endpoint buckets
/// histograms is a property of the endpoint, not of the panel; the RepoContext
/// container endpoint documents its own summary-only behaviour in
/// <c>docs/lattice.api.mcp.repocontext/container.md</c>.
/// </para>
/// </remarks>
[TestFixture]
public sealed class DashboardHistogramQuantileTests
{
    /// <summary>
    /// A floor on the number of instrument declarations the source scan must find.
    /// The scan is the foundation every assertion below rests on, so a scan that
    /// silently matched nothing - a refactor to a different factory shape, a moved
    /// source root - would turn this whole fixture green and vacuous. The floor is
    /// set well below the current count so ordinary churn never trips it.
    /// </summary>
    private const int MinimumInstrumentDeclarations = 150;

    /// <summary>
    /// A floor on the number of panel targets that must invoke
    /// <c>histogram_quantile</c>. Same reasoning as
    /// <see cref="MinimumInstrumentDeclarations"/>, applied to the other input.
    /// </summary>
    private const int MinimumQuantileTargets = 50;

    private static readonly Regex InstrumentTokenRegex =
        new(@"\borleans_lattice(?:_replication)?_[a-z0-9_]+\b", RegexOptions.Compiled);

    private static readonly Lazy<IReadOnlyList<QuerySite>> SitesLazy = new(CollectSites, isThreadSafe: true);

    /// <summary>
    /// Every query expression bundled in a dashboard, with its location. Exposed to
    /// the assembly so that a sibling gate reuses this walk rather than growing a
    /// second one that can drift from it.
    /// </summary>
    internal static IReadOnlyList<QuerySite> Sites => SitesLazy.Value;

    /// <summary>
    /// Anti-vacuity. Every other test in this fixture is a statement about a set of
    /// scanned sites, so each one passes trivially if the scan finds nothing. This
    /// asserts both inputs are non-empty and above a floor, and fails loudly rather
    /// than reporting a green that means only that the fixture stopped looking.
    /// </summary>
    [Test]
    public void Scan_finds_instrument_declarations_and_quantile_targets_to_check()
    {
        Assert.That(DeclaredInstruments.DeclarationCount, Is.GreaterThanOrEqualTo(MinimumInstrumentDeclarations),
            $"The source scan for instrument declarations matched only {DeclaredInstruments.DeclarationCount} "
            + "site(s). Every assertion in this fixture is a statement about that scan, so a scan that matches "
            + "nothing reports a vacuous green. Either the Meter.Create* declaration shape changed, or the "
            + "source root moved; fix the scan rather than lowering the floor.");

        Assert.That(DeclaredInstruments.Unresolved, Is.Empty,
            "Some instrument declarations have a name argument this fixture could not resolve to a literal. "
            + "An unresolved declaration is an instrument the gate silently stops covering, so it is failed "
            + "rather than skipped. Give the instrument a 'const string ...Name' or a literal name:"
            + Environment.NewLine + "  - " + string.Join(Environment.NewLine + "  - ", DeclaredInstruments.Unresolved));

        var quantileTargets = Sites.Count(s => s.CallsHistogramQuantile);
        Assert.That(quantileTargets, Is.GreaterThanOrEqualTo(MinimumQuantileTargets),
            $"The dashboard scan found only {quantileTargets} target(s) invoking histogram_quantile. "
            + "The panel walk is the other input this fixture rests on; a scan that matches nothing is a "
            + "vacuous green, not a clean bill of health.");

        var bucketReferences = Sites.Count(s => s.BucketTokens.Count > 0);
        Assert.That(bucketReferences, Is.GreaterThan(0),
            "No panel target references a _bucket series at all, which cannot be right while "
            + $"{quantileTargets} target(s) invoke histogram_quantile.");
    }

    /// <summary>
    /// The forward half: every bucket series a panel reads must be backed by a
    /// <c>Histogram&lt;T&gt;</c>. There is no waiver list - a non-histogram cannot
    /// produce buckets under any exporter, so there is no deployment in which such
    /// a panel is correct and nothing to except.
    /// </summary>
    [Test]
    public void Every_bucket_series_a_panel_reads_is_backed_by_a_histogram()
    {
        var violations = EvaluateBucketBackings(Sites);

        Assert.That(violations, Is.Empty,
            "The following panel targets read a Prometheus _bucket series from an instrument that cannot "
            + "produce one. Only a Histogram<T> exports buckets; every other instrument kind exports a single "
            + "sample per series, so histogram_quantile over it returns an empty vector - and where the target "
            + "also carries 'or vector(0)', the panel renders a confident literal zero instead of rendering "
            + "empty. Either point the panel at a histogram, or read the instrument with the function its kind "
            + "supports (rate() for a counter, the bare series for a gauge):"
            + Environment.NewLine + "  - " + string.Join(Environment.NewLine + "  - ", violations));
    }

    /// <summary>
    /// The companion half: a <c>histogram_quantile</c> call must actually read a
    /// bucket series. A quantile computed over <c>_sum</c> or <c>_count</c> - the
    /// two series a summary-rendering endpoint does expose - returns nothing, and
    /// is the shape a reader reaches for when buckets turn out to be missing.
    /// </summary>
    [Test]
    public void Every_histogram_quantile_call_reads_a_bucket_series()
    {
        var violations = new List<string>();

        foreach (var site in Sites.Where(s => s.CallsHistogramQuantile))
        {
            foreach (var (argument, tokens) in site.QuantileArguments)
            {
                if (tokens.Count == 0)
                {
                    continue;
                }

                if (tokens.Any(t => t.EndsWith("_bucket", StringComparison.Ordinal)))
                {
                    continue;
                }

                violations.Add(
                    $"{site.Describe()} calls histogram_quantile over [{string.Join(", ", tokens)}] "
                    + $"with no _bucket series: {Truncate(argument)}");
            }
        }

        Assert.That(violations, Is.Empty,
            "The following panel targets compute a quantile over a series that carries no buckets. "
            + "histogram_quantile reads the 'le' dimension of a _bucket family; over a _sum or _count series "
            + "it returns an empty vector, which renders blank - or as a literal zero if the target carries "
            + "'or vector(0)'. Read a mean as rate(_sum) / rate(_count) and relabel the panel, rather than "
            + "presenting it as a percentile:"
            + Environment.NewLine + "  - " + string.Join(Environment.NewLine + "  - ", violations));
    }

    /// <summary>
    /// Known-positive control. The two gates above assert an absence, and an
    /// assertion that something is absent is only evidence if the detector is shown
    /// to fire when it is present. This plants the exact defect - a bucket read off
    /// a counter, carrying the <c>or vector(0)</c> that makes it render as a
    /// confident zero - and asserts the detector reports it. If this test ever fails
    /// while the repository gates pass, the gates above are asleep, not clean.
    /// </summary>
    [Test]
    public void Detector_flags_a_synthetic_panel_reading_buckets_from_a_counter()
    {
        // The control must plant a token the panel scanner actually recognises, so
        // it is drawn from the instruments the bundled dashboards can name: those
        // on the Lattice meters. Picking any counter in src/ would risk a name the
        // token regex does not match, which would make the control silently fail to
        // plant anything - the precise failure mode a control exists to rule out.
        var counter = DeclaredInstruments.ByDottedName
            .Where(kv => kv.Value == DeclaredInstrumentKind.Counter
                && kv.Key.StartsWith("orleans.lattice", StringComparison.Ordinal))
            .Select(kv => kv.Key)
            .OrderBy(n => n, StringComparer.Ordinal)
            .FirstOrDefault();

        Assert.That(counter, Is.Not.Null,
            "No Counter<T> instrument on an 'orleans.lattice' meter was found in source, so the control "
            + "cannot plant a realistic defect. That is itself a sign the declaration scan has broken.");

        var token = counter!.Replace('.', '_') + "_bucket";
        var expr = $"histogram_quantile(0.95, sum by (le) (rate({token}{{tree=~\"$tree\"}}[5m]))) or vector(0)";
        var synthetic = new QuerySite("SyntheticControl", "panel 'p95 of a counter'", "A", expr);

        Assert.Multiple(() =>
        {
            Assert.That(synthetic.CallsHistogramQuantile, Is.True,
                "The synthetic control must itself parse as a histogram_quantile target, or it proves nothing.");
            Assert.That(synthetic.BucketTokens, Does.Contain(token),
                "The synthetic control must expose the planted bucket token to the detector.");

            var violations = EvaluateBucketBackings([synthetic]);
            Assert.That(violations, Is.Not.Empty,
                $"The detector did not flag a panel reading '{token}' - a bucket series on a Counter<T>, which "
                + "no exporter can produce. The repository-wide assertions above are therefore not evidence of "
                + "anything: a detector that has never been shown to fire cannot distinguish a clean repository "
                + "from a broken scan.");
            Assert.That(violations[0], Does.Contain("Counter"),
                "The violation message should name the offending instrument kind so the remedy is obvious.");
        });
    }

    /// <summary>
    /// Resolver control. The gates above are only as good as the source-derived
    /// kind map, so this pins that map against the authoritative runtime answer:
    /// for every instrument that is live at test time, the kind parsed from its
    /// declaration must equal the kind of the object the meter actually created.
    /// A parse that silently drifted - a new factory overload, a changed generic
    /// arity - shows up here rather than as a quietly narrowed gate.
    /// </summary>
    [Test]
    public void Source_derived_instrument_kinds_agree_with_live_reflection()
    {
        var live = EnumerateLiveInstrumentKinds();

        Assert.That(live, Is.Not.Empty,
            "No live instruments were observed, so this cross-check would pass vacuously.");

        var mismatches = new List<string>();
        var missing = new List<string>();

        foreach (var (name, runtimeKind) in live.OrderBy(k => k.Key, StringComparer.Ordinal))
        {
            if (!DeclaredInstruments.ByDottedName.TryGetValue(name, out var declaredKind))
            {
                missing.Add(name);
                continue;
            }

            if (declaredKind != runtimeKind)
            {
                mismatches.Add($"{name}: source says {declaredKind}, runtime says {runtimeKind}");
            }
        }

        Assert.Multiple(() =>
        {
            Assert.That(missing, Is.Empty,
                "These instruments exist at runtime but were not found by the source declaration scan, so the "
                + "gate does not cover them:"
                + Environment.NewLine + "  - " + string.Join(Environment.NewLine + "  - ", missing));
            Assert.That(mismatches, Is.Empty,
                "The source-derived instrument kind disagrees with the runtime type for these instruments. The "
                + "declaration parse has drifted from what the meter actually creates:"
                + Environment.NewLine + "  - " + string.Join(Environment.NewLine + "  - ", mismatches));
        });
    }

    /// <summary>
    /// Pins the concrete shape of the registry, so that a change which quietly
    /// reclassifies instruments is visible as a diff rather than absorbed silently.
    /// Asserts that histograms and non-histograms both exist in useful numbers -
    /// a registry that classified everything as a histogram would make
    /// <see cref="Every_bucket_series_a_panel_reads_is_backed_by_a_histogram"/>
    /// unfalsifiable without failing anything itself.
    /// </summary>
    [Test]
    public void Registry_distinguishes_histograms_from_every_other_instrument_kind()
    {
        var byKind = DeclaredInstruments.ByDottedName
            .GroupBy(kv => kv.Value)
            .ToDictionary(g => g.Key, g => g.Count());

        var histograms = byKind.GetValueOrDefault(DeclaredInstrumentKind.Histogram);
        var others = byKind.Where(kv => kv.Key != DeclaredInstrumentKind.Histogram).Sum(kv => kv.Value);

        Assert.Multiple(() =>
        {
            Assert.That(histograms, Is.GreaterThan(0),
                "The registry classified no instrument as a Histogram<T>, which cannot be right while panels "
                + "read bucket series. The declaration parse has broken.");
            Assert.That(others, Is.GreaterThan(0),
                "The registry classified every instrument as a Histogram<T>. That would make the bucket-backing "
                + "gate unfalsifiable - every bucket read would resolve to a histogram by construction - while "
                + "still reporting green.");
        });
    }

    private static List<string> EvaluateBucketBackings(IReadOnlyList<QuerySite> sites)
    {
        var violations = new List<string>();

        foreach (var site in sites)
        {
            foreach (var token in site.BucketTokens)
            {
                if (!DeclaredBucketTokens.BucketTokens.TryGetValue(token, out var binding))
                {
                    violations.Add(
                        $"{site.Describe()} reads '{token}', which does not correspond to any instrument "
                        + "declared under src/. Either the instrument was renamed and the panel was not "
                        + "updated, or the token is a typo.");
                    continue;
                }

                if (binding.Kind == DeclaredInstrumentKind.Histogram)
                {
                    continue;
                }

                var orVector = site.CarriesOrVector ? " and carries 'or vector(0)', so it renders a literal zero" : string.Empty;
                violations.Add(
                    $"{site.Describe()} reads '{token}', but '{binding.Dotted}' is declared as a "
                    + $"{binding.Kind}, which exports no bucket series{orVector}.");
            }
        }

        violations.Sort(StringComparer.Ordinal);
        return violations;
    }

    private static Dictionary<string, DeclaredInstrumentKind> EnumerateLiveInstrumentKinds()
    {
        _ = LatticeMetrics.MeterName;
        _ = LatticeReplicationMetrics.MeterName;

        var collected = new Dictionary<string, DeclaredInstrumentKind>(StringComparer.Ordinal);
        using var listener = new MeterListener();
        listener.InstrumentPublished = (instrument, _) =>
        {
            if (!ReferenceEquals(instrument.Meter, LatticeMetrics.Meter)
                && !ReferenceEquals(instrument.Meter, LatticeReplicationMetrics.Meter))
            {
                return;
            }

            var kind = RuntimeKindOf(instrument);
            if (kind is not null)
            {
                collected[instrument.Name] = kind.Value;
            }
        };
        listener.Start();
        return collected;
    }

    private static DeclaredInstrumentKind? RuntimeKindOf(Instrument instrument)
    {
        var type = instrument.GetType();
        if (!type.IsGenericType)
        {
            return null;
        }

        var definition = type.GetGenericTypeDefinition();
        if (definition == typeof(Histogram<>))
        {
            return DeclaredInstrumentKind.Histogram;
        }

        if (definition == typeof(Counter<>))
        {
            return DeclaredInstrumentKind.Counter;
        }

        if (definition == typeof(UpDownCounter<>))
        {
            return DeclaredInstrumentKind.UpDownCounter;
        }

        if (definition == typeof(ObservableGauge<>))
        {
            return DeclaredInstrumentKind.ObservableGauge;
        }

        if (definition == typeof(ObservableCounter<>))
        {
            return DeclaredInstrumentKind.ObservableCounter;
        }

        if (definition == typeof(ObservableUpDownCounter<>))
        {
            return DeclaredInstrumentKind.ObservableUpDownCounter;
        }

        return null;
    }

    private static List<QuerySite> CollectSites()
    {
        var sites = new List<QuerySite>();

        foreach (var kind in LatticeDashboards.All)
        {
            using var doc = JsonDocument.Parse(LatticeDashboards.GetGrafanaDashboardJson(kind));
            WalkJson(doc.RootElement, kind.ToString(), "dashboard", null, sites);
        }

        return sites;
    }

    private static void WalkJson(JsonElement node, string dashboard, string site, string? refId, List<QuerySite> sink)
    {
        switch (node.ValueKind)
        {
            case JsonValueKind.Object:
                site = SiteLabel(node) ?? site;
                refId = node.TryGetProperty("refId", out var r) && r.ValueKind == JsonValueKind.String
                    ? r.GetString()
                    : refId;

                foreach (var property in node.EnumerateObject())
                {
                    if ((property.NameEquals("expr") || property.NameEquals("query"))
                        && property.Value.ValueKind == JsonValueKind.String
                        && property.Value.GetString() is { Length: > 0 } expr)
                    {
                        sink.Add(new QuerySite(dashboard, site, refId ?? "-", expr));
                        continue;
                    }

                    WalkJson(property.Value, dashboard, site, refId, sink);
                }

                break;

            case JsonValueKind.Array:
                foreach (var element in node.EnumerateArray())
                {
                    WalkJson(element, dashboard, site, refId, sink);
                }

                break;
        }
    }

    private static string? SiteLabel(JsonElement node)
    {
        if (node.TryGetProperty("id", out var id) && id.ValueKind == JsonValueKind.Number
            && node.TryGetProperty("title", out var title) && title.ValueKind == JsonValueKind.String)
        {
            return $"panel {id.GetRawText()} '{title.GetString()}'";
        }

        if (node.TryGetProperty("name", out var name) && name.ValueKind == JsonValueKind.String
            && node.TryGetProperty("enable", out _))
        {
            return $"annotation '{name.GetString()}'";
        }

        return null;
    }

    private static string Truncate(string value) =>
        value.Length <= 160 ? value : value[..160] + " ...";

    /// <summary>One PromQL expression on one dashboard, with what the gate needs to judge it.</summary>
    internal sealed class QuerySite
    {
        public QuerySite(string dashboard, string site, string refId, string expression)
        {
            Dashboard = dashboard;
            Site = site;
            RefId = refId;
            Expression = expression;

            var tokens = InstrumentTokenRegex.Matches(expression).Select(m => m.Value).ToList();
            BucketTokens = tokens
                .Where(t => t.EndsWith("_bucket", StringComparison.Ordinal))
                .Distinct(StringComparer.Ordinal)
                .OrderBy(t => t, StringComparer.Ordinal)
                .ToList();

            QuantileArguments = ExtractQuantileArguments(expression);
            CallsHistogramQuantile = QuantileArguments.Count > 0;
            CarriesOrVector = expression.Contains("or vector(", StringComparison.Ordinal);
        }

        public string Dashboard { get; }

        public string Site { get; }

        public string RefId { get; }

        public string Expression { get; }

        public IReadOnlyList<string> BucketTokens { get; }

        public IReadOnlyList<(string Argument, IReadOnlyList<string> Tokens)> QuantileArguments { get; }

        public bool CallsHistogramQuantile { get; }

        public bool CarriesOrVector { get; }

        public string Describe() => $"{Dashboard} {Site} [refId {RefId}]";

        /// <summary>
        /// Extracts the argument text of each <c>histogram_quantile(...)</c> call by
        /// scanning for the balanced closing parenthesis, so a nested call such as
        /// <c>sum by (le) (rate(...))</c> is captured whole rather than truncated at
        /// its first inner <c>)</c>.
        /// </summary>
        private static List<(string, IReadOnlyList<string>)> ExtractQuantileArguments(string expression)
        {
            var results = new List<(string, IReadOnlyList<string>)>();
            const string needle = "histogram_quantile(";

            var at = expression.IndexOf(needle, StringComparison.Ordinal);
            while (at >= 0)
            {
                var open = at + needle.Length;
                var depth = 1;
                var i = open;
                while (i < expression.Length && depth > 0)
                {
                    if (expression[i] == '(')
                    {
                        depth++;
                    }
                    else if (expression[i] == ')')
                    {
                        depth--;
                    }

                    i++;
                }

                var argument = expression[open..(depth == 0 ? i - 1 : expression.Length)];
                var tokens = InstrumentTokenRegex.Matches(argument)
                    .Select(m => m.Value)
                    .Distinct(StringComparer.Ordinal)
                    .OrderBy(t => t, StringComparer.Ordinal)
                    .ToList();
                results.Add((argument, tokens));

                at = expression.IndexOf(needle, at + needle.Length, StringComparison.Ordinal);
            }

            return results;
        }
    }
}
