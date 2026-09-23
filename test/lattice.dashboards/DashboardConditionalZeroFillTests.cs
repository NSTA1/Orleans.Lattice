using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using Orleans.Lattice.Testing.Hygiene;

namespace Orleans.Lattice.Dashboards.Tests;

/// <summary>
/// One recording call site of one instrument, with whether it sits inside a
/// conditional construct.
/// </summary>
/// <param name="DottedName">The canonical dotted instrument name being recorded.</param>
/// <param name="File">The repository-relative source file holding the call.</param>
/// <param name="Line">The 1-based line the call begins on.</param>
/// <param name="Guarded">Whether the call is lexically enclosed by a conditional construct.</param>
internal readonly record struct InstrumentCallSite(string DottedName, string File, int Line, bool Guarded);

/// <summary>
/// Classifies every instrument declared under <c>src/</c> as <i>conditionally</i>
/// or <i>unconditionally</i> emitted, by walking each recording call site and
/// asking whether it sits on a straight-line path through its method.
/// </summary>
/// <remarks>
/// <para>
/// <b>The rule.</b> An instrument is <i>conditional</i> when <b>every</b> one of
/// its recording call sites (<c>.Add(</c> / <c>.Record(</c>) is lexically
/// enclosed by a conditional construct - <c>if</c>, <c>else</c>, <c>catch</c>,
/// <c>for</c>, <c>foreach</c>, <c>while</c>, <c>switch</c>, <c>case</c>, or
/// <c>do</c>. It is <i>unconditional</i> as soon as one call site sits on a
/// straight-line path, because reaching the enclosing method then necessarily
/// records the measurement.
/// </para>
/// <para>
/// <b>The rule is sound, and deliberately incomplete.</b> A fully-guarded
/// instrument provably <i>can</i> be absent on a healthy estate, which is the
/// discriminating question issue #2520 poses. The converse does not hold: an
/// instrument recorded on a straight-line path inside a method that is itself
/// only ever called conditionally is also legitimately absent, and this walk
/// cannot see that because it is intraprocedural. The census is therefore a
/// lower bound on the conditional population - every name it reports as
/// conditional is one, and it does not claim to have found them all. That
/// direction is the safe one: it never asserts that a legitimately-absent
/// series is guaranteed present.
/// </para>
/// <para>
/// <b>Why source rather than reflection.</b> Emission conditions are a property
/// of the code path, not of the live object, so no <c>MeterListener</c> snapshot
/// can recover them. Deriving them from source also means the census follows the
/// code: the day somebody moves a recording call behind an <c>if</c>, the
/// instrument reclassifies and the dashboard gate reddens, which is exactly the
/// regression this fixture exists to catch.
/// </para>
/// </remarks>
internal static class InstrumentEmissionCensus
{
    /// <summary>
    /// Conditional constructs. <c>try</c>, <c>using</c>, <c>lock</c>, and
    /// <c>finally</c> are deliberately absent: their bodies run whenever the
    /// enclosing path is reached, so they do not make a measurement optional.
    /// </summary>
    private static readonly Regex GuardRegex = new(
        @"\b(?:if|else|catch|for|foreach|while|switch|case|do)\b",
        RegexOptions.Compiled);

    private static readonly Regex RecordRegex = new(
        @"([A-Za-z_][A-Za-z0-9_]*)\s*\.\s*(?:Add|Record)\s*\(",
        RegexOptions.Compiled);

    /// <summary>
    /// Binds the field an instrument is declared into to the dotted name it
    /// carries, so a later <c>Field.Add(...)</c> resolves to an instrument.
    /// </summary>
    private static readonly Regex DeclarationRegex = new(
        @"(?<field>[A-Za-z_][A-Za-z0-9_]*)\s*=\s*(?:[A-Za-z_][A-Za-z0-9_]*\s*\.\s*)*"
        + @"Create(?:Histogram|UpDownCounter|Counter|ObservableGauge|ObservableCounter|ObservableUpDownCounter)"
        + @"\s*(?:<[^>()]*>)?\s*\(\s*(?<arg>@?""(?:[^""\\]|\\.)*""|[A-Za-z_][A-Za-z0-9_.]*)",
        RegexOptions.Compiled);

    /// <summary>
    /// A per-file alias of an instrument field, as in
    /// <c>private static readonly Counter&lt;long&gt; Rejected = LatticeMetrics.ViewAggregationRejected;</c>.
    /// Six instruments in <c>src/</c> are recorded only through such an alias and
    /// are invisible to a scan keyed on the declaring field alone.
    /// </summary>
    private static readonly Regex AliasRegex = new(
        @"(?m)^[^\r\n=]*?\b(?<alias>[A-Za-z_][A-Za-z0-9_]*)\s*=\s*"
        + @"(?:[A-Za-z_][A-Za-z0-9_]*\s*\.\s*)*?(?<source>[A-Za-z_][A-Za-z0-9_]*)\s*;",
        RegexOptions.Compiled);

    private static readonly Regex ConstRegex = new(
        @"const\s+string\s+(?<id>[A-Za-z_][A-Za-z0-9_]*)\s*=\s*""(?<val>[^""\\]*)""\s*;",
        RegexOptions.Compiled);

    private static readonly Lazy<Census> CensusLazy = new(Build, isThreadSafe: true);

    /// <summary>Dotted names every recording call site of which is conditionally reached.</summary>
    public static IReadOnlySet<string> Conditional => CensusLazy.Value.Conditional;

    /// <summary>Dotted names with at least one straight-line recording call site.</summary>
    public static IReadOnlySet<string> Unconditional => CensusLazy.Value.Unconditional;

    /// <summary>Every recording call site the walk resolved to an instrument.</summary>
    public static IReadOnlyList<InstrumentCallSite> CallSites => CensusLazy.Value.CallSites;

    /// <summary>Maps every Prometheus token a declared instrument can render as back to its dotted name.</summary>
    public static IReadOnlyDictionary<string, string> DottedByToken => CensusLazy.Value.DottedByToken;

    /// <summary>The number of source files the walk lexed.</summary>
    public static int FilesWalked => CensusLazy.Value.FilesWalked;

    /// <summary>
    /// Generates every Prometheus family name the exporter could render the given
    /// dotted name as.
    /// </summary>
    /// <remarks>
    /// The forms mirror <c>DashboardJsonTests.AddInstrumentForms</c>, including its
    /// deliberate retention of the <c>_milliseconds_*</c> and <c>_seconds_*</c>
    /// spellings that issue #3260 tracks. Those spellings are dead on a live
    /// scrape, but they are what the dashboards currently write, so resolving them
    /// is what lets this gate adjudicate those panels rather than report them as
    /// unknown. Repairing them is issue #3260's job, not this gate's.
    /// </remarks>
    /// <param name="dottedName">The canonical dotted instrument name.</param>
    /// <returns>The candidate Prometheus family names, including the bare form.</returns>
    public static IEnumerable<string> TokenFormsOf(string dottedName)
    {
        ArgumentNullException.ThrowIfNull(dottedName);

        var underscored = dottedName.Replace('.', '_');

        yield return underscored;
        yield return underscored + "_total";
        yield return underscored + "_bucket";
        yield return underscored + "_count";
        yield return underscored + "_sum";
        yield return underscored + "_milliseconds_bucket";
        yield return underscored + "_milliseconds_count";
        yield return underscored + "_milliseconds_sum";
        yield return underscored + "_seconds_bucket";
        yield return underscored + "_seconds_count";
        yield return underscored + "_seconds_sum";
    }

    private static Census Build()
    {
        var src = Path.Combine(HygieneRepository.FindRepoRoot(), "src");

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

        // Field identifier -> dotted instrument name, global across src/.
        var dottedByField = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (var (_, text) in files)
        {
            foreach (Match m in DeclarationRegex.Matches(text))
            {
                var dotted = ResolveName(m.Groups["arg"].Value, constants, ambiguous);
                if (dotted is not null)
                {
                    dottedByField[m.Groups["field"].Value] = dotted;
                }
            }
        }

        var callSites = new List<InstrumentCallSite>();
        var totals = new Dictionary<string, (int Total, int Guarded)>(StringComparer.Ordinal);
        var walked = 0;

        foreach (var (path, text) in files)
        {
            if (!text.Contains(".Add(", StringComparison.Ordinal)
                && !text.Contains(".Record(", StringComparison.Ordinal))
            {
                continue;
            }

            var resolved = ResolveCallSites(text, dottedByField);
            if (resolved.Count == 0)
            {
                continue;
            }

            walked++;
            WalkFile(text, RelativePath(path), resolved, callSites, totals);
        }

        var conditional = new HashSet<string>(StringComparer.Ordinal);
        var unconditional = new HashSet<string>(StringComparer.Ordinal);
        foreach (var (name, counts) in totals)
        {
            _ = counts.Guarded == counts.Total ? conditional.Add(name) : unconditional.Add(name);
        }

        var dottedByToken = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (var dotted in dottedByField.Values)
        {
            foreach (var token in TokenFormsOf(dotted))
            {
                dottedByToken[token] = dotted;
            }
        }

        return new Census(conditional, unconditional, callSites, dottedByToken, walked);
    }

    /// <summary>
    /// Indexes the character offset of every <c>.Add(</c> / <c>.Record(</c> whose
    /// receiver resolves to an instrument, including through a per-file alias.
    /// </summary>
    private static Dictionary<int, string> ResolveCallSites(string text, Dictionary<string, string> dottedByField)
    {
        Dictionary<string, string>? local = null;
        foreach (Match a in AliasRegex.Matches(text))
        {
            if (dottedByField.TryGetValue(a.Groups["source"].Value, out var dotted))
            {
                local ??= new Dictionary<string, string>(StringComparer.Ordinal);
                local[a.Groups["alias"].Value] = dotted;
            }
        }

        var calls = new Dictionary<int, string>();
        foreach (Match m in RecordRegex.Matches(text))
        {
            var receiver = m.Groups[1].Value;
            if (dottedByField.TryGetValue(receiver, out var name)
                || (local is not null && local.TryGetValue(receiver, out name)))
            {
                calls[m.Index] = name;
            }
        }

        return calls;
    }

    /// <summary>
    /// Single-pass C#-aware lex of one file, tracking brace nesting and whether
    /// each open brace was introduced by a conditional construct.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Comments and string literals are skipped <b>inline</b> rather than removed
    /// by a pre-pass substitution. A block-comment regex applied to the whole file
    /// over-matches across an intervening <c>*/</c> and silently deletes tens of
    /// kilobytes of real code, which yields confident and wrong verdicts.
    /// </para>
    /// <para>
    /// The block-opener test reads <b>code-only</b> text accumulated since the last
    /// statement boundary. Reading the raw span instead lets XML documentation prose
    /// leak the word <c>if</c> into the test, which misclassifies most of the
    /// repository as conditional.
    /// </para>
    /// </remarks>
    private static void WalkFile(
        string text,
        string relativePath,
        Dictionary<int, string> calls,
        List<InstrumentCallSite> callSites,
        Dictionary<string, (int Total, int Guarded)> totals)
    {
        var stack = new List<bool>();
        var segment = new StringBuilder();
        var guardDepth = 0;
        var line = 1;
        var i = 0;

        while (i < text.Length)
        {
            var ch = text[i];

            if (ch == '/' && i + 1 < text.Length)
            {
                if (text[i + 1] == '/')
                {
                    while (i < text.Length && text[i] != '\n')
                    {
                        i++;
                    }

                    continue;
                }

                if (text[i + 1] == '*')
                {
                    i += 2;
                    while (i + 1 < text.Length && !(text[i] == '*' && text[i + 1] == '/'))
                    {
                        if (text[i] == '\n')
                        {
                            line++;
                        }

                        i++;
                    }

                    i += 2;
                    continue;
                }
            }

            if (ch == '@' && i + 1 < text.Length && text[i + 1] == '"')
            {
                i += 2;
                while (i < text.Length)
                {
                    if (text[i] == '"')
                    {
                        if (i + 1 < text.Length && text[i + 1] == '"')
                        {
                            i += 2;
                            continue;
                        }

                        i++;
                        break;
                    }

                    if (text[i] == '\n')
                    {
                        line++;
                    }

                    i++;
                }

                segment.Append(' ');
                continue;
            }

            if (ch == '"' && i + 2 < text.Length && text[i + 1] == '"' && text[i + 2] == '"')
            {
                var quotes = 0;
                while (i < text.Length && text[i] == '"')
                {
                    quotes++;
                    i++;
                }

                var run = 0;
                while (i < text.Length)
                {
                    if (text[i] == '"')
                    {
                        run++;
                        if (run == quotes)
                        {
                            i++;
                            break;
                        }
                    }
                    else
                    {
                        run = 0;
                        if (text[i] == '\n')
                        {
                            line++;
                        }
                    }

                    i++;
                }

                segment.Append(' ');
                continue;
            }

            if (ch is '"' or '\'')
            {
                i++;
                while (i < text.Length)
                {
                    if (text[i] == '\\')
                    {
                        i += 2;
                        continue;
                    }

                    if (text[i] == ch)
                    {
                        i++;
                        break;
                    }

                    if (text[i] == '\n')
                    {
                        break;
                    }

                    i++;
                }

                segment.Append(' ');
                continue;
            }

            if (calls.TryGetValue(i, out var instrument))
            {
                var guarded = guardDepth > 0;
                callSites.Add(new InstrumentCallSite(instrument, relativePath, line, guarded));

                totals.TryGetValue(instrument, out var counts);
                totals[instrument] = (counts.Total + 1, counts.Guarded + (guarded ? 1 : 0));
            }

            if (ch == '\n')
            {
                line++;
            }

            switch (ch)
            {
                case ';':
                    segment.Clear();
                    break;

                case '}':
                    if (stack.Count > 0)
                    {
                        if (stack[^1])
                        {
                            guardDepth--;
                        }

                        stack.RemoveAt(stack.Count - 1);
                    }

                    segment.Clear();
                    break;

                case '{':
                    var opensGuard = GuardRegex.IsMatch(segment.ToString());
                    segment.Clear();
                    stack.Add(opensGuard);
                    if (opensGuard)
                    {
                        guardDepth++;
                    }

                    break;

                default:
                    segment.Append(ch);
                    break;
            }

            i++;
        }
    }

    private static string? ResolveName(string argument, Dictionary<string, string> constants, HashSet<string> ambiguous)
    {
        if (argument.Length > 0 && (argument[0] == '"' || argument.StartsWith("@\"", StringComparison.Ordinal)))
        {
            var start = argument.IndexOf('"') + 1;
            return argument[start..^1];
        }

        var lastDot = argument.LastIndexOf('.');
        var identifier = lastDot >= 0 ? argument[(lastDot + 1)..] : argument;

        return ambiguous.Contains(identifier) ? null
            : constants.TryGetValue(identifier, out var value) ? value
            : null;
    }

    private static string RelativePath(string path)
    {
        var marker = $"{Path.DirectorySeparatorChar}src{Path.DirectorySeparatorChar}";
        var index = path.IndexOf(marker, StringComparison.Ordinal);
        return index < 0 ? Path.GetFileName(path) : path[(index + 1)..].Replace('\\', '/');
    }

    private sealed record Census(
        IReadOnlySet<string> Conditional,
        IReadOnlySet<string> Unconditional,
        IReadOnlyList<InstrumentCallSite> CallSites,
        IReadOnlyDictionary<string, string> DottedByToken,
        int FilesWalked);
}

/// <summary>
/// Asserts that no bundled dashboard zero-fills a query over an instrument that
/// can legitimately be absent on a healthy estate (issue #2520).
/// </summary>
/// <remarks>
/// <para>
/// <c>or vector(0)</c> replaces an <i>empty</i> PromQL result with a literal
/// zero. On an instrument that is always emitted that is a reasonable way to
/// render a scrape gap, because a real reading of zero and a missing reading mean
/// the same thing. On an instrument that is emitted only past a threshold, only
/// on a fault path, or only when an option is set, absence is the <i>healthy</i>
/// state and the zero-fill manufactures a confident "measured zero" for something
/// that was never measured. A panel is better off failing to draw than drawing a
/// reassuring number nothing produced.
/// </para>
/// <para>
/// The classification is derived live from <c>src/</c> by
/// <see cref="InstrumentEmissionCensus"/> rather than pinned in a checked-in
/// list, so moving a recording call behind a condition reddens this gate at the
/// moment of the move.
/// </para>
/// </remarks>
[TestFixture]
public sealed class DashboardConditionalZeroFillTests
{
    /// <summary>
    /// Floors that keep a silent scan failure from reading as a clean result. All
    /// are set well under the measured population so ordinary churn does not trip
    /// them, and well over zero so an empty scan cannot pass.
    /// </summary>
    /// <remarks>
    /// Measured when this fixture was written, so the headroom each floor carries is
    /// a stated quantity rather than a guess: 121 files walked, 631 recording call
    /// sites, 87 conditional and 260 unconditional instruments, 181 zero-filled
    /// queries, and 54 panel descriptions discussing the idiom.
    /// </remarks>
    private const int MinimumFilesWalked = 100;

    private const int MinimumCallSites = 550;

    private const int MinimumConditionalInstruments = 70;

    private const int MinimumUnconditionalInstruments = 220;

    private const int MinimumZeroFilledQueries = 150;

    private const int MinimumDescriptionMentions = 30;

    private static readonly Regex InstrumentTokenRegex =
        new(@"\borleans_lattice(?:_replication)?_[a-z0-9_]+\b", RegexOptions.Compiled);

    private static readonly Lazy<IReadOnlyList<ZeroFilledQuery>> QueriesLazy =
        new(CollectZeroFilledQueries, isThreadSafe: true);

    /// <summary>
    /// The census must actually have read the repository. Every later assertion in
    /// this fixture is a statement about a population, and a population of zero
    /// satisfies all of them.
    /// </summary>
    [Test]
    public void Census_reads_a_population_large_enough_to_judge()
    {
        Assert.Multiple(() =>
        {
            Assert.That(
                InstrumentEmissionCensus.FilesWalked,
                Is.GreaterThanOrEqualTo(MinimumFilesWalked),
                "The emission census walked too few source files to be reading the repository.");

            Assert.That(
                InstrumentEmissionCensus.CallSites,
                Has.Count.GreaterThanOrEqualTo(MinimumCallSites),
                "The emission census resolved too few recording call sites.");

            Assert.That(
                InstrumentEmissionCensus.Conditional,
                Has.Count.GreaterThanOrEqualTo(MinimumConditionalInstruments),
                "The emission census found too few conditionally-emitted instruments.");

            Assert.That(
                InstrumentEmissionCensus.Unconditional,
                Has.Count.GreaterThanOrEqualTo(MinimumUnconditionalInstruments),
                "The emission census found too few unconditionally-emitted instruments.");
        });
    }

    /// <summary>
    /// Pins the census against two instruments whose emission condition is settled
    /// independently of this walk, one in each direction.
    /// </summary>
    /// <remarks>
    /// A classifier that answered "conditional" for everything, or "unconditional"
    /// for everything, would satisfy the population floors above. Only a two-sided
    /// ground truth rejects both. <c>materialiser.lagging_consumers</c> is recorded
    /// only for consumers found to be behind, and issue #2520 cites the CommitPath
    /// panel that already omits the zero-fill over it as the correct precedent.
    /// <c>get.duration</c> is timed on every read.
    /// </remarks>
    [Test]
    public void Census_agrees_with_ground_truth_in_both_directions()
    {
        Assert.Multiple(() =>
        {
            Assert.That(
                InstrumentEmissionCensus.Conditional,
                Does.Contain("orleans.lattice.materialiser.lagging_consumers"),
                "A counter recorded only for lagging consumers must classify as conditional.");

            Assert.That(
                InstrumentEmissionCensus.Unconditional,
                Does.Contain("orleans.lattice.get.duration"),
                "A histogram recorded on every read must classify as unconditional.");

            Assert.That(
                InstrumentEmissionCensus.Conditional.Overlaps(InstrumentEmissionCensus.Unconditional),
                Is.False,
                "An instrument cannot be both conditionally and unconditionally emitted.");
        });
    }

    /// <summary>
    /// The detector must separate the two cases on a controlled input, so a green
    /// result on the real dashboards is evidence the matcher discriminates rather
    /// than evidence it matches nothing.
    /// </summary>
    /// <remarks>
    /// This is the known-positive half of the fixture. A token matcher that keys on
    /// a prefix, or that greps a rendered label set whose order it guessed, passes
    /// every real query while detecting nothing at all; the only way to tell that
    /// apart from a genuinely clean corpus is to hand it an expression that must
    /// fail and confirm it does.
    /// </remarks>
    [Test]
    public void Detector_flags_a_conditional_instrument_and_spares_an_unconditional_one()
    {
        var conditional = FirstTokenOf("orleans.lattice.materialiser.lagging_consumers");
        var unconditional = FirstTokenOf("orleans.lattice.get.duration");

        var flagged = ConditionalTokensIn(
            $"sum by (tree) (rate({conditional}{{tree=~\"$tree\"}}[5m])) or vector(0)");

        var spared = ConditionalTokensIn(
            $"histogram_quantile(0.95, sum by (le) (rate({unconditional}_bucket[5m]))) or vector(0)");

        Assert.Multiple(() =>
        {
            Assert.That(
                flagged,
                Does.Contain("orleans.lattice.materialiser.lagging_consumers"),
                "A zero-filled query over a conditionally-emitted instrument must be flagged.");

            Assert.That(
                spared,
                Is.Empty,
                "A zero-filled query over an unconditionally-emitted instrument must not be flagged.");
        });
    }

    /// <summary>
    /// The scan must read query text only. The idiom is also discussed in panel
    /// prose, and a scan that cannot tell the two apart would demand edits to
    /// documentation that correctly describes the very rule being enforced.
    /// </summary>
    [Test]
    public void Scan_reads_query_expressions_and_never_panel_prose()
    {
        var descriptions = CollectDescriptionsMentioningZeroFill();
        var expressions = QueriesLazy.Value.Select(q => q.Expression).ToHashSet(StringComparer.Ordinal);

        Assert.Multiple(() =>
        {
            Assert.That(
                descriptions,
                Has.Count.GreaterThanOrEqualTo(MinimumDescriptionMentions),
                "Panel prose discussing the zero-fill idiom must exist for this discrimination to be non-vacuous.");

            Assert.That(
                QueriesLazy.Value,
                Has.Count.GreaterThanOrEqualTo(MinimumZeroFilledQueries),
                "Too few zero-filled queries were collected for the gate to be judging the dashboards.");

            Assert.That(
                descriptions.Where(expressions.Contains),
                Is.Empty,
                "A panel description was collected as if it were a query expression.");
        });
    }

    /// <summary>
    /// The gate. No dashboard query may zero-fill a series that a healthy estate is
    /// entitled to omit.
    /// </summary>
    [Test]
    public void No_query_zero_fills_a_conditionally_emitted_instrument()
    {
        var offenders = new List<string>();

        foreach (var query in QueriesLazy.Value)
        {
            var conditional = ConditionalTokensIn(query.Expression);
            if (conditional.Count > 0)
            {
                offenders.Add(
                    $"{query.Dashboard} {query.Site}: 'or vector(0)' over conditionally-emitted "
                    + string.Join(", ", conditional));
            }
        }

        Assert.That(
            offenders,
            Is.Empty,
            "These queries render an absent series as a measured zero. Drop the 'or vector(0)', "
            + "set spanNulls to false, and say in the panel description that absence is the healthy "
            + "reading. See issue #2520."
            + Environment.NewLine
            + string.Join(Environment.NewLine, offenders));
    }

    private static string FirstTokenOf(string dottedName) =>
        InstrumentEmissionCensus.TokenFormsOf(dottedName).First();

    /// <summary>
    /// Resolves every instrument token in an expression and returns the distinct
    /// dotted names that are conditionally emitted.
    /// </summary>
    private static List<string> ConditionalTokensIn(string expression)
    {
        List<string>? hits = null;

        foreach (Match match in InstrumentTokenRegex.Matches(expression))
        {
            if (!InstrumentEmissionCensus.DottedByToken.TryGetValue(match.Value, out var dotted)
                || !InstrumentEmissionCensus.Conditional.Contains(dotted))
            {
                continue;
            }

            hits ??= [];
            if (!hits.Contains(dotted, StringComparer.Ordinal))
            {
                hits.Add(dotted);
            }
        }

        return hits ?? [];
    }

    private static List<ZeroFilledQuery> CollectZeroFilledQueries()
    {
        var sink = new List<ZeroFilledQuery>();

        foreach (var kind in LatticeDashboards.All)
        {
            using var document = JsonDocument.Parse(LatticeDashboards.GetGrafanaDashboardJson(kind));
            WalkQueries(document.RootElement, kind.ToString(), "dashboard", sink);
        }

        return sink;
    }

    private static void WalkQueries(JsonElement node, string dashboard, string site, List<ZeroFilledQuery> sink)
    {
        switch (node.ValueKind)
        {
            case JsonValueKind.Object:
                site = SiteLabel(node) ?? site;

                foreach (var property in node.EnumerateObject())
                {
                    if ((property.NameEquals("expr") || property.NameEquals("query"))
                        && property.Value.ValueKind == JsonValueKind.String
                        && property.Value.GetString() is { Length: > 0 } expression)
                    {
                        if (expression.Contains("or vector(", StringComparison.Ordinal))
                        {
                            sink.Add(new ZeroFilledQuery(dashboard, site, expression));
                        }

                        continue;
                    }

                    WalkQueries(property.Value, dashboard, site, sink);
                }

                break;

            case JsonValueKind.Array:
                foreach (var element in node.EnumerateArray())
                {
                    WalkQueries(element, dashboard, site, sink);
                }

                break;
        }
    }

    private static List<string> CollectDescriptionsMentioningZeroFill()
    {
        var sink = new List<string>();

        foreach (var kind in LatticeDashboards.All)
        {
            using var document = JsonDocument.Parse(LatticeDashboards.GetGrafanaDashboardJson(kind));
            WalkDescriptions(document.RootElement, sink);
        }

        return sink;
    }

    private static void WalkDescriptions(JsonElement node, List<string> sink)
    {
        switch (node.ValueKind)
        {
            case JsonValueKind.Object:
                foreach (var property in node.EnumerateObject())
                {
                    if (property.NameEquals("description")
                        && property.Value.ValueKind == JsonValueKind.String
                        && property.Value.GetString() is { } text
                        && text.Contains("or vector(", StringComparison.Ordinal))
                    {
                        sink.Add(text);
                        continue;
                    }

                    WalkDescriptions(property.Value, sink);
                }

                break;

            case JsonValueKind.Array:
                foreach (var element in node.EnumerateArray())
                {
                    WalkDescriptions(element, sink);
                }

                break;
        }
    }

    private static string? SiteLabel(JsonElement node) =>
        node.TryGetProperty("id", out var id) && id.ValueKind == JsonValueKind.Number
        && node.TryGetProperty("title", out var title) && title.ValueKind == JsonValueKind.String
            ? $"panel {id.GetRawText()} '{title.GetString()}'"
            : null;

    private sealed record ZeroFilledQuery(string Dashboard, string Site, string Expression);
}
