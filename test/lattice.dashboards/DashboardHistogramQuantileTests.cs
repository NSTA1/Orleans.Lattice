using System.Diagnostics.Metrics;
using System.Text.Json;
using System.Text.RegularExpressions;
using Orleans.Lattice;
using Orleans.Lattice.Dashboards;
using Orleans.Lattice.Replication;
using Orleans.Lattice.Testing.Hygiene;

namespace Orleans.Lattice.Dashboards.Tests;

/// <summary>
/// The Prometheus family a .NET instrument declaration renders as, derived from
/// the <c>Meter.Create*</c> factory that declares it.
/// </summary>
internal enum DeclaredInstrumentKind
{
    /// <summary>A <c>Counter&lt;T&gt;</c>.</summary>
    Counter,

    /// <summary>An <c>UpDownCounter&lt;T&gt;</c>.</summary>
    UpDownCounter,

    /// <summary>A <c>Histogram&lt;T&gt;</c> - the only kind that exports bucket series.</summary>
    Histogram,

    /// <summary>An <c>ObservableGauge&lt;T&gt;</c>.</summary>
    ObservableGauge,

    /// <summary>An <c>ObservableCounter&lt;T&gt;</c>.</summary>
    ObservableCounter,

    /// <summary>An <c>ObservableUpDownCounter&lt;T&gt;</c>.</summary>
    ObservableUpDownCounter,
}

/// <summary>
/// The source-derived registry of every instrument declared anywhere under
/// <c>src/</c>, keyed by its canonical dotted name and carrying the factory kind
/// that declares it.
/// </summary>
/// <remarks>
/// <para>
/// The registry is built by parsing source rather than by reflecting over live
/// instruments, because many instruments - observable gauges in particular - are
/// created only when the host starts the subsystem that owns them, so a snapshot
/// <see cref="MeterListener"/> at test time never sees them. Parsing the
/// declaration covers the lazily-created and the eagerly-created alike, and does
/// so uniformly.
/// </para>
/// <para>
/// <b>The mapping is built forward, never reverse.</b> The exporter translates
/// both <c>'.'</c> and any underscore already present in the .NET name into
/// <c>'_'</c>, so a Prometheus token cannot be parsed back into a dotted name:
/// <c>orleans_lattice_wal_gc_passes_total</c> is consistent with several distinct
/// dotted names and the mangling is not injective. Every lookup here therefore
/// generates the candidate Prometheus forms from a known dotted name and matches
/// tokens against that generated set.
/// </para>
/// </remarks>
internal static class DeclaredInstruments
{
    private static readonly Regex CreateRegex = new(
        @"Create(?<kind>Histogram|UpDownCounter|Counter|ObservableGauge|ObservableCounter|ObservableUpDownCounter)"
        + @"\s*(?:<[^>()]*>)?\s*(?<open>\()\s*(?<arg>@?""(?:[^""\\]|\\.)*""|[A-Za-z_][A-Za-z0-9_.]*)",
        RegexOptions.Compiled);

    private static readonly Regex NamedUnitRegex = new(
        @"^unit\s*:\s*""(?<u>[^""\\]*)""$",
        RegexOptions.Compiled);

    private static readonly Regex ConstRegex = new(
        @"const\s+string\s+(?<id>[A-Za-z_][A-Za-z0-9_]*)\s*=\s*""(?<val>[^""\\]*)""\s*;",
        RegexOptions.Compiled);

    private static readonly Lazy<Registry> RegistryLazy = new(Build, isThreadSafe: true);

    /// <summary>Every declared instrument, keyed by canonical dotted name.</summary>
    public static IReadOnlyDictionary<string, DeclaredInstrumentKind> ByDottedName => RegistryLazy.Value.ByDottedName;

    /// <summary>
    /// Declarations whose name argument could not be resolved to a literal. This
    /// must stay empty: an unresolved declaration is an instrument the gates
    /// silently stop covering, so it is asserted rather than tolerated.
    /// </summary>
    public static IReadOnlyList<string> Unresolved => RegistryLazy.Value.Unresolved;

    /// <summary>The number of <c>Create*</c> declarations the scan matched.</summary>
    public static int DeclarationCount => RegistryLazy.Value.DeclarationCount;

    /// <summary>
    /// Maps every Prometheus <c>_bucket</c> token any declared instrument could
    /// produce - under any unit suffix the exporter may insert - back to the kind
    /// that declares it.
    /// </summary>
    /// <remarks>
    /// Bucket forms are generated for <b>every</b> kind, not only histograms, so
    /// that a panel naming <c>some_counter_bucket</c> resolves to a counter and is
    /// reported as a kind violation, rather than failing to resolve at all and
    /// being reported as an unknown instrument. The two are different defects with
    /// different remedies and must not be conflated.
    /// </remarks>
    public static IReadOnlyDictionary<string, (string Dotted, DeclaredInstrumentKind Kind)> BucketTokens =>
        RegistryLazy.Value.BucketTokens;

    /// <summary>
    /// The unit string each instrument declares, keyed by canonical dotted name.
    /// An instrument that declares no unit maps to the empty string.
    /// </summary>
    /// <remarks>
    /// The unit is read from the declaration's argument list, which is located by
    /// balancing parentheses from the factory call rather than by matching a
    /// trailing anchor such as <c>description:</c>. That distinction is
    /// load-bearing: an anchored pattern reads only the declarations shaped the
    /// way its author happened to look at, and silently reports every other
    /// declaration as <i>no unit</i> rather than as <i>not read</i>. Four
    /// instruments in <c>src/</c> supply the unit positionally, and an anchored
    /// scan misses all four while reporting a clean result.
    /// </remarks>
    public static IReadOnlyDictionary<string, string> UnitByDottedName => RegistryLazy.Value.UnitByDottedName;

    /// <summary>
    /// Declarations whose argument list could not be read to its closing
    /// parenthesis, so the declared unit is <b>unknown</b> rather than absent.
    /// Asserted empty, because a parser that classifies what it could not read as
    /// "no unit" reports its own depth as the repository's content.
    /// </summary>
    public static IReadOnlyList<string> UnitUnresolved => RegistryLazy.Value.UnitUnresolved;

    /// <summary>
    /// The number of instruments whose unit was supplied positionally rather than
    /// with a <c>unit:</c> label. Asserted non-zero, so that the parser's ability
    /// to read positional units stays proven rather than assumed.
    /// </summary>
    public static int PositionalUnitCount => RegistryLazy.Value.PositionalUnitCount;

    /// <summary>Generates every Prometheus bucket token a dotted instrument name could produce.</summary>
    public static IEnumerable<string> BucketFormsOf(string dottedName)
    {
        ArgumentNullException.ThrowIfNull(dottedName);

        var underscored = dottedName.Replace('.', '_');
        yield return underscored + "_bucket";
        yield return underscored + "_milliseconds_bucket";
        yield return underscored + "_seconds_bucket";
    }

    private static Registry Build()
    {
        var root = HygieneRepository.FindRepoRoot();
        var src = Path.Combine(root, "src");

        var constants = new Dictionary<string, string>(StringComparer.Ordinal);
        var ambiguous = new HashSet<string>(StringComparer.Ordinal);
        var files = new List<(string Path, string Text)>();

        foreach (var file in HygieneRepository.EnumerateFiles(src, "*.cs"))
        {
            var text = File.ReadAllText(file);
            files.Add((file, text));

            foreach (Match m in ConstRegex.Matches(text))
            {
                var id = m.Groups["id"].Value;
                var val = m.Groups["val"].Value;
                if (constants.TryGetValue(id, out var existing))
                {
                    if (!string.Equals(existing, val, StringComparison.Ordinal))
                    {
                        ambiguous.Add(id);
                    }
                }
                else
                {
                    constants[id] = val;
                }
            }
        }

        var byDotted = new Dictionary<string, DeclaredInstrumentKind>(StringComparer.Ordinal);
        var unitByDotted = new Dictionary<string, string>(StringComparer.Ordinal);
        var unresolved = new List<string>();
        var unitUnresolved = new List<string>();
        var positionalUnits = 0;
        var count = 0;

        foreach (var (path, text) in files)
        {
            foreach (Match m in CreateRegex.Matches(text))
            {
                count++;
                var kind = Enum.Parse<DeclaredInstrumentKind>(m.Groups["kind"].Value);
                var arg = m.Groups["arg"].Value;
                var dotted = ResolveName(arg, constants, ambiguous);

                if (dotted is null)
                {
                    unresolved.Add($"{Path.GetFileName(path)}: Create{kind}(... {arg} ...)");
                    continue;
                }

                // A name declared twice must agree on its kind; the exporter would
                // otherwise emit two conflicting "# TYPE" lines for one family.
                if (byDotted.TryGetValue(dotted, out var existing) && existing != kind)
                {
                    unresolved.Add($"{dotted}: declared as both {existing} and {kind}");
                    continue;
                }

                byDotted[dotted] = kind;

                var arguments = ReadArgumentList(text, m.Groups["open"].Index);
                if (arguments is null)
                {
                    unitUnresolved.Add($"{Path.GetFileName(path)}: {dotted} - argument list did not close");
                    continue;
                }

                unitByDotted[dotted] = ResolveUnit(SplitTopLevelArguments(arguments), ref positionalUnits);
            }
        }

        var bucketTokens = new Dictionary<string, (string, DeclaredInstrumentKind)>(StringComparer.Ordinal);
        foreach (var (dotted, kind) in byDotted)
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

        unresolved.Sort(StringComparer.Ordinal);
        unitUnresolved.Sort(StringComparer.Ordinal);
        return new Registry(byDotted, unitByDotted, unresolved, unitUnresolved, count, positionalUnits, bucketTokens);
    }

    /// <summary>
    /// Returns the text between the parenthesis at <paramref name="openIndex"/> and
    /// its match, or <see langword="null"/> when the list does not close. String
    /// literals, character literals, and comments are skipped so that a parenthesis
    /// inside one cannot unbalance the scan.
    /// </summary>
    private static string? ReadArgumentList(string text, int openIndex)
    {
        var depth = 0;

        for (var i = openIndex; i < text.Length; i++)
        {
            var c = text[i];

            if (c == '"')
            {
                i = SkipStringLiteral(text, i);
                if (i < 0)
                {
                    return null;
                }

                continue;
            }

            if (c == '\'')
            {
                i = SkipCharLiteral(text, i);
                if (i < 0)
                {
                    return null;
                }

                continue;
            }

            if (c == '/' && i + 1 < text.Length && text[i + 1] == '/')
            {
                while (i < text.Length && text[i] != '\n')
                {
                    i++;
                }

                continue;
            }

            if (c == '/' && i + 1 < text.Length && text[i + 1] == '*')
            {
                var end = text.IndexOf("*/", i + 2, StringComparison.Ordinal);
                if (end < 0)
                {
                    return null;
                }

                i = end + 1;
                continue;
            }

            if (c == '(')
            {
                depth++;
                continue;
            }

            if (c == ')')
            {
                depth--;
                if (depth == 0)
                {
                    return text[(openIndex + 1)..i];
                }
            }
        }

        return null;
    }

    /// <summary>Returns the index of the closing quote, or -1 when unterminated.</summary>
    private static int SkipStringLiteral(string text, int quoteIndex)
    {
        var verbatim = quoteIndex > 0 && text[quoteIndex - 1] == '@';

        for (var i = quoteIndex + 1; i < text.Length; i++)
        {
            if (verbatim)
            {
                if (text[i] != '"')
                {
                    continue;
                }

                // A doubled quote inside a verbatim literal is an escaped quote.
                if (i + 1 < text.Length && text[i + 1] == '"')
                {
                    i++;
                    continue;
                }

                return i;
            }

            if (text[i] == '\\')
            {
                i++;
                continue;
            }

            if (text[i] == '"')
            {
                return i;
            }
        }

        return -1;
    }

    /// <summary>Returns the index of the closing quote, or -1 when unterminated.</summary>
    private static int SkipCharLiteral(string text, int quoteIndex)
    {
        for (var i = quoteIndex + 1; i < text.Length; i++)
        {
            if (text[i] == '\\')
            {
                i++;
                continue;
            }

            if (text[i] == '\'')
            {
                return i;
            }
        }

        return -1;
    }

    /// <summary>Splits an argument list on its top-level commas.</summary>
    private static List<string> SplitTopLevelArguments(string arguments)
    {
        var parts = new List<string>();
        var depth = 0;
        var start = 0;

        for (var i = 0; i < arguments.Length; i++)
        {
            var c = arguments[i];

            if (c == '"')
            {
                var end = SkipStringLiteral(arguments, i);
                i = end < 0 ? arguments.Length : end;
                continue;
            }

            if (c is '(' or '[' or '{' or '<')
            {
                depth++;
                continue;
            }

            if (c is ')' or ']' or '}' or '>')
            {
                depth--;
                continue;
            }

            if (c == ',' && depth <= 0)
            {
                parts.Add(arguments[start..i].Trim());
                start = i + 1;
            }
        }

        parts.Add(arguments[start..].Trim());
        return parts;
    }

    /// <summary>
    /// Reads the declared unit from a split argument list. A <c>unit:</c> label
    /// wins; failing that the second positional argument is the unit, which is the
    /// shape the <c>Meter.Create*</c> overloads define.
    /// </summary>
    private static string ResolveUnit(List<string> arguments, ref int positionalUnits)
    {
        foreach (var argument in arguments)
        {
            var named = NamedUnitRegex.Match(argument);
            if (named.Success)
            {
                return named.Groups["u"].Value;
            }
        }

        if (arguments.Count >= 2 && arguments[1].Length > 1 && arguments[1][0] == '"' && arguments[1][^1] == '"')
        {
            positionalUnits++;
            return arguments[1][1..^1];
        }

        return string.Empty;
    }

    private static string? ResolveName(string arg, Dictionary<string, string> constants, HashSet<string> ambiguous)
    {
        if (arg.Length > 1 && arg[0] == '"')
        {
            return arg[1..^1];
        }

        if (arg.Length > 2 && arg[0] == '@' && arg[1] == '"')
        {
            return arg[2..^1];
        }

        var identifier = arg.Split('.')[^1];
        if (ambiguous.Contains(identifier))
        {
            return null;
        }

        return constants.TryGetValue(identifier, out var value) ? value : null;
    }

    private sealed record Registry(
        IReadOnlyDictionary<string, DeclaredInstrumentKind> ByDottedName,
        IReadOnlyDictionary<string, string> UnitByDottedName,
        IReadOnlyList<string> Unresolved,
        IReadOnlyList<string> UnitUnresolved,
        int DeclarationCount,
        int PositionalUnitCount,
        IReadOnlyDictionary<string, (string Dotted, DeclaredInstrumentKind Kind)> BucketTokens);
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
                if (!DeclaredInstruments.BucketTokens.TryGetValue(token, out var binding))
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
