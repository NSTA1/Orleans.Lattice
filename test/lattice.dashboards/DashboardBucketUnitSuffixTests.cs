using System.Text.RegularExpressions;

namespace Orleans.Lattice.Dashboards.Tests;

/// <summary>
/// The Prometheus unit suffixes a declared instrument unit could imply, together
/// with the evidence behind them.
/// </summary>
/// <remarks>
/// A unit carries a <b>set</b> of candidates rather than one suffix, because for
/// some units this repository cannot establish which of two exporter behaviours
/// applies. That is not a weakness to be papered over with a guess: where the
/// candidates predict the same series name for every instrument that declares the
/// unit, the disagreement is irrelevant and the gate asserts the shared prediction;
/// where they would predict different names, the gate says so and refuses to judge
/// rather than encoding a guess and then enforcing it as though it were known.
/// </remarks>
/// <param name="Suffixes">Candidate suffixes, without a leading underscore. Empty string means none.</param>
/// <param name="Basis">Why the repository believes this, recorded so a later reader can re-judge it.</param>
internal readonly record struct UnitSuffixRule(IReadOnlyList<string> Suffixes, string Basis);

/// <summary>
/// Asserts that every Prometheus <c>_bucket</c> series a bundled Grafana panel
/// reads carries the unit suffix the instrument's own declared unit implies.
/// </summary>
/// <remarks>
/// <para>
/// A sibling gate already asserts that every bucket series a panel reads is backed
/// by a histogram. That check is unit-blind: it accepts <c>x_bucket</c>,
/// <c>x_milliseconds_bucket</c>, and <c>x_seconds_bucket</c> interchangeably,
/// because it generates all three forms for every instrument and asks only whether
/// the token resolves to <i>something</i>. A panel that drops the suffix therefore
/// resolves cleanly and renders empty, which is the defect this gate exists to
/// catch.
/// </para>
/// <para>
/// <b>A gate is not exempt from the rule it enforces.</b> Forward coverage - every
/// live instrument is referenced by at least one panel - has been asserted here for
/// far longer than anything asserted the reverse, and the asymmetry was read as
/// though it were symmetry. The quantifiers differ and neither implies the other:
/// the older gate is <i>for every instrument there exists a panel</i>, while this
/// one is <i>for every panel token there exists a series an exporter can name</i>.
/// The same error recurs one level down, which is why the sibling gate's
/// accept-any-form matching is itself a <c>Sigma</c> standing in for an
/// <c>Exists</c>, and reports a clean result for a panel naming a series no
/// exporter will produce.
/// </para>
/// <para>
/// <b>Scope, declared rather than derived.</b> This gate reads every
/// <c>*_bucket</c> token in a bundled dashboard, not only those carrying an
/// <c>orleans_lattice</c> prefix, and it fails rather than skips when it cannot
/// decide a token. A scope computed by a parser tracks the parser's depth, not the
/// repository's content, and silently reports what it could not read as
/// <i>not applicable</i>.
/// </para>
/// <para>
/// <b>What this gate does and does not claim.</b> It compares panels against a
/// contract of exporter naming rules stated once, in one place, with the evidence
/// for each rule beside it - a single reviewable assumption in place of one silent
/// assumption per panel. It is not a measurement of any particular exporter build.
/// Where two candidate behaviours exist, the gate asserts only what they agree on.
/// </para>
/// <para>
/// These dashboards target a host exporting through
/// <c>.AddPrometheusExporter()</c>, which appends unit suffixes. They are not
/// written for the repocontext container's hand-rolled exposition, which renders
/// every histogram as a Prometheus <c>summary</c> carrying only <c>_sum</c> and
/// <c>_count</c> and appends no unit suffix at all. Against that endpoint every
/// quantile panel here is unavailable rather than zero, whatever suffix it names.
/// See <c>docs/lattice.api.mcp.repocontext/container.md</c>.
/// </para>
/// </remarks>
[TestFixture]
internal sealed class DashboardBucketUnitSuffixTests
{
    /// <summary>
    /// Floor on the histograms the source scan must resolve, so that a scan which
    /// silently stops finding declarations fails instead of passing vacuously.
    /// </summary>
    private const int MinimumHistogramDeclarations = 90;

    /// <summary>Floor on the distinct bucket tokens the panel scan must find.</summary>
    private const int MinimumBucketTokens = 80;

    /// <summary>
    /// Floor on the tokens that must be judged conforming. This is the
    /// known-positive control for a gate whose headline verdict is an absence: a
    /// detector that resolved nothing would report no violations, and would be
    /// indistinguishable from a clean repository without this.
    /// </summary>
    private const int MinimumConformingTokens = 50;

    /// <summary>
    /// An instrument known to declare unit <c>ms</c>, used to build the synthetic
    /// arms. Asserted to exist and to declare that unit.
    /// </summary>
    private const string ControlInstrument = "orleans.lattice.atomic_write.duration";

    /// <summary>
    /// Matches any Prometheus bucket token, whatever its prefix. Deliberately wider
    /// than the sibling gate's <c>orleans_lattice</c>-anchored pattern.
    /// </summary>
    private static readonly Regex BucketTokenRegex =
        new(@"\b[A-Za-z_][A-Za-z0-9_]*_bucket\b", RegexOptions.Compiled);

    /// <summary>Matches an OpenTelemetry annotation unit such as <c>{entry}</c>.</summary>
    private static readonly Regex AnnotationUnitRegex = new(@"^\{[^{}]*\}$", RegexOptions.Compiled);

    /// <summary>
    /// The declared contract: what suffix each instrument unit could imply, and on
    /// what evidence. Annotation units are handled by pattern rather than
    /// enumerated.
    /// </summary>
    private static readonly IReadOnlyDictionary<string, UnitSuffixRule> Contract =
        new Dictionary<string, UnitSuffixRule>(StringComparer.Ordinal)
        {
            [string.Empty] = new(
                [string.Empty],
                "An instrument that declares no unit gives the exporter nothing to append."),

            ["ms"] = new(
                ["milliseconds"],
                "Of the ms-unit histograms a bundled panel reads bucket series from, all but three "
                + "carry '_milliseconds'. The three that do not are the defect this gate was written to catch."),

            ["s"] = new(
                ["seconds"],
                "The one s-unit histogram a bundled panel reads bucket series from carries '_seconds', "
                + "with no counter-example anywhere in the bundled dashboards."),

            ["By"] = new(
                ["bytes"],
                "Measured, not inferred. The rival candidate previously carried here was resolvable "
                + "from this repository's instruments only if one of them declared 'By' without "
                + "already being named '*_bytes', and none does - so the question was settled by "
                + "scraping OpenTelemetry.Exporter.Prometheus.AspNetCore 1.15.3-beta.1, the version "
                + "every host here pins, against a synthetic control instrument named without the "
                + "'_bytes' suffix. The exporter appended '_bytes' to the control and did not double "
                + "it on the repository's own '*_bytes' names (issue #2941). The do-not-double rule "
                + "lives in PredictedBucketTokens, so this single candidate still predicts the bare "
                + "name for every instrument already carrying the suffix."),

            ["1"] = new(
                [string.Empty, "ratio"],
                "OpenTelemetry gives the dimensionless unit '1' special treatment that varies by "
                + "instrument kind, and its handling for a histogram is not established from this "
                + "repository. Both candidates are carried, and the gate asserts only where they agree."),
        };

    /// <summary>
    /// Instruments whose unit carries more than one candidate suffix, and which the
    /// gate can therefore judge only because those candidates collapse to the same
    /// prediction. Declared explicitly so the set cannot change in silence: an
    /// instrument leaving this list means the collapse no longer holds and the panel
    /// is no longer provably correct.
    /// </summary>
    /// <remarks>
    /// The three <c>By</c>-unit instruments that were listed here left when that
    /// unit stopped being a two-candidate rule: it is now a measured single rule
    /// (see the contract entry), so those panels are provable outright rather than
    /// only where rival rules happen to agree. That is a strictly stronger verdict
    /// for them, and it is why this list shrinking is the expected outcome of
    /// issue #2941 rather than a loss of coverage.
    /// </remarks>
    private static readonly IReadOnlyList<string> ExpectedCollapsedInstruments =
    [
        "orleans.lattice.leaf.tombstone.ratio",
    ];

    private static readonly Lazy<Assessment> AssessmentLazy =
        new(() => Assess(CollectPanelTokens()), isThreadSafe: true);

    private static IReadOnlyDictionary<string, string> Histograms { get; } =
        DeclaredInstruments.UnitByDottedName
            .Where(kv => DeclaredInstruments.ByDottedName.TryGetValue(kv.Key, out var k)
                && k == DeclaredInstrumentKind.Histogram)
            .ToDictionary(kv => kv.Key, kv => kv.Value, StringComparer.Ordinal);

    /// <summary>
    /// Anti-vacuity, plus the known-positive control for the source parser itself.
    /// </summary>
    /// <remarks>
    /// The positional-unit assertion is the important one. An earlier scan of these
    /// declarations located the unit by matching forward to a <c>description:</c>
    /// label, which reads only the declarations written with named arguments and
    /// reports every other declaration as carrying no unit - not as unread. It
    /// missed four instruments and produced a clean, confident, wrong result.
    /// Asserting that positional units are still being read keeps that failure from
    /// returning silently.
    /// </remarks>
    [Test]
    public void Scan_resolves_every_histogram_declaration_and_reads_positional_units()
    {
        Assert.That(
            DeclaredInstruments.UnitUnresolved,
            Is.Empty,
            "A declaration whose argument list could not be read has an unknown unit, not an absent "
            + "one. Classifying it as 'no unit' would report the parser's depth as the repository's "
            + "content.");

        Assert.That(
            DeclaredInstruments.PositionalUnitCount,
            Is.GreaterThan(0),
            "No instrument unit was read from a positional argument. Either every declaration now "
            + "uses a 'unit:' label, or the parser has regressed to reading only labelled ones - and "
            + "the second failure is silent, so it is asserted here rather than assumed.");

        Assert.That(
            Histograms,
            Has.Count.GreaterThanOrEqualTo(MinimumHistogramDeclarations),
            "The source scan resolved fewer histograms than the repository is known to declare.");

        Assert.That(
            AssessmentLazy.Value.TokensScanned,
            Is.GreaterThanOrEqualTo(MinimumBucketTokens),
            "The panel scan found fewer bucket tokens than the bundled dashboards are known to read.");

        // A concrete anchor: if this instrument is renamed or re-united, the synthetic
        // controls below silently stop testing what they claim to test.
        Assert.That(
            DeclaredInstruments.UnitByDottedName,
            Does.ContainKey(ControlInstrument),
            $"The control instrument '{ControlInstrument}' is no longer declared, so the synthetic "
            + "arms of this fixture no longer prove anything.");

        Assert.That(
            DeclaredInstruments.UnitByDottedName[ControlInstrument],
            Is.EqualTo("ms"),
            $"The control instrument '{ControlInstrument}' no longer declares unit 'ms'.");
    }

    /// <summary>
    /// The gate. Every bucket series a panel reads must carry the suffix its
    /// instrument's declared unit implies.
    /// </summary>
    [Test]
    public void Every_bucket_series_a_panel_reads_carries_the_suffix_its_unit_implies()
    {
        var assessment = AssessmentLazy.Value;

        // Known-positive control, inside the test whose verdict is an absence. A
        // blind detector reports an empty violation set, exactly as a clean
        // repository does; only a non-zero conforming count separates them.
        Assert.That(
            assessment.Conforming,
            Has.Count.GreaterThanOrEqualTo(MinimumConformingTokens),
            "The detector judged almost nothing conforming, so its empty violation list is not "
            + "evidence of a clean repository - it is evidence that the detector is not working.");

        Assert.That(
            assessment.Violations,
            Is.Empty,
            "A panel reads a bucket series whose name omits the unit suffix its instrument's "
            + "declared unit implies. The series does not exist, so the panel renders empty against "
            + "a correctly configured exporter, and any 'or vector(0)' on the expression renders a "
            + "literal zero instead.\n"
            + string.Join("\n", assessment.Violations));
    }

    /// <summary>
    /// Every histogram-backed token is accounted for in exactly one outcome.
    /// </summary>
    /// <remarks>
    /// This is the tally that makes the unasserted outcomes interpretable. A token
    /// the gate declines to judge is indistinguishable from a token the gate never
    /// saw, unless the judged and declined counts are required to sum to the scanned
    /// total. Without this, the undecided category could absorb an arbitrary number
    /// of tokens while the headline verdict stayed green.
    /// </remarks>
    [Test]
    public void Every_histogram_backed_token_is_accounted_for_in_exactly_one_outcome()
    {
        var a = AssessmentLazy.Value;

        var accounted = a.Conforming.Count
            + a.Violations.Count
            + a.UndecidedTokens.Count
            + a.AmbiguouslyBoundTokens.Count;

        Assert.That(
            accounted,
            Is.EqualTo(a.HistogramBackedTokens),
            "The outcome categories do not sum to the number of histogram-backed bucket tokens "
            + "scanned, so at least one token is being silently dropped between the scan and the "
            + "verdict.");

        Assert.That(
            a.HistogramBackedTokens + a.NonHistogramTokens + a.UnresolvedTokens,
            Is.EqualTo(a.TokensScanned),
            "The token classification does not sum to the number of tokens scanned.");
    }

    /// <summary>
    /// Nothing is excluded from this gate. A unit whose exporter behaviour is not
    /// established still yields a verdict, because the gate asserts what its
    /// candidate rules agree on.
    /// </summary>
    /// <remarks>
    /// <para>
    /// This is how <c>unit: "1"</c> is settled without measuring an exporter, and
    /// the reasoning generalises. The sole instrument declaring it is
    /// <c>orleans.lattice.leaf.tombstone.ratio</c>, whose name <b>already ends in
    /// <c>_ratio</c></b>. If the exporter maps <c>1</c> to <c>ratio</c>, the suffix
    /// is already present and is not appended twice; if it maps <c>1</c> to nothing,
    /// no suffix is appended either. Both candidates predict the same series name,
    /// so the panel is correct under either behaviour and no experiment against any
    /// exporter could distinguish them using this repository's instruments.
    /// <c>By</c> was settled the other way: the same argument held for it, which is
    /// exactly why no instrument here could decide it, so it was decided by scraping
    /// the pinned exporter against a synthetic control named without the suffix
    /// (issue #2941). It is now a single measured rule and no longer reaches this
    /// path.
    /// </para>
    /// <para>
    /// The collapse is what makes those panels provable, so it is asserted rather
    /// than assumed - and this test has teeth precisely because the collapse is
    /// contingent. An instrument declaring <c>unit: "1"</c> whose name did not end
    /// in <c>_ratio</c> would break the agreement, and this fails rather than
    /// quietly picking a side.
    /// </para>
    /// </remarks>
    [Test]
    public void Units_with_rival_candidate_rules_are_judged_only_where_the_rules_agree()
    {
        var collapsed = new List<string>();

        foreach (var (dotted, unit) in Histograms.OrderBy(kv => kv.Key, StringComparer.Ordinal))
        {
            var predictions = PredictedBucketTokens(dotted, unit);

            if (RuleFor(unit).Suffixes.Count <= 1)
            {
                continue;
            }

            Assert.That(
                predictions,
                Is.Not.Empty,
                $"'{dotted}' produced no prediction at all, which is a parser fault rather than an "
                + "undecidable unit.");

            if (predictions.Count == 1)
            {
                collapsed.Add(dotted);
            }
        }

        // Known-positive control: if no instrument exercised the collapse, this test
        // is asserting a property of the empty set.
        Assert.That(
            collapsed,
            Is.Not.Empty,
            "No instrument exercised the candidate-collapse path, so this test proves nothing about "
            + "how rival rules are handled.");

        Assert.That(
            collapsed,
            Is.EqualTo(ExpectedCollapsedInstruments.OrderBy(n => n, StringComparer.Ordinal).ToList()),
            "The set of instruments judged only because their rival candidate rules collapse to one "
            + "prediction has changed. Each is a panel whose correctness depends on that collapse "
            + "still holding, so the set is declared explicitly rather than allowed to drift.");

        Assert.That(
            AssessmentLazy.Value.UndecidedTokens,
            Is.Empty,
            "A bucket token could not be judged because its unit's candidate rules disagree for that "
            + "instrument. Establish the exporter's behaviour for the unit, or rename the instrument "
            + "so the candidates agree.\n"
            + string.Join("\n", AssessmentLazy.Value.UndecidedTokens.Select(t => $"{t.Token} <- {t.Dotted}")));
    }

    /// <summary>
    /// Known-positive control: a synthetic panel that drops the unit suffix is
    /// flagged. A detector that has not been shown to fire is not evidence.
    /// </summary>
    [Test]
    public void Detector_flags_a_synthetic_panel_that_drops_the_unit_suffix()
    {
        var bare = ControlInstrument.Replace('.', '_') + "_bucket";
        var assessment = Assess([new ScannedToken(bare, "SyntheticControl", "panel 'p95 without the unit suffix'")]);

        Assert.That(
            assessment.Violations,
            Has.Count.EqualTo(1),
            "The detector did not flag a panel reading a bucket series with the unit suffix removed, "
            + "so its silence on the real dashboards means nothing.");

        Assert.That(assessment.Violations[0], Does.Contain(bare));
        Assert.That(assessment.Conforming, Is.Empty);
    }

    /// <summary>
    /// Adversarial arm: the same detector, the same instrument, the correct suffix.
    /// Without this, a detector that flagged every token would pass the control
    /// above while proving nothing.
    /// </summary>
    [Test]
    public void Detector_accepts_a_synthetic_panel_that_carries_the_unit_suffix()
    {
        var suffixed = ControlInstrument.Replace('.', '_') + "_milliseconds_bucket";
        var assessment = Assess([new ScannedToken(suffixed, "SyntheticControl", "panel 'p95 with the unit suffix'")]);

        Assert.That(
            assessment.Violations,
            Is.Empty,
            "The detector flagged a correctly suffixed bucket series, so it is reporting the same "
            + "verdict regardless of input.");

        Assert.That(
            assessment.Conforming,
            Has.Count.EqualTo(1),
            "The correctly suffixed series was neither flagged nor counted as conforming, so it was "
            + "silently dropped rather than judged.");
    }

    /// <summary>
    /// The sibling gate anchors its token scan to an <c>orleans_lattice</c> prefix,
    /// while this repository also declares <c>repocontext.*</c> instruments. Today
    /// every bundled bucket token happens to carry the anchored prefix, so the
    /// narrower scan is complete by coincidence rather than by construction. This
    /// turns that coincidence into a gated invariant: the first panel to read a
    /// bucket series outside the anchor fails here, naming the token, instead of
    /// being silently excluded from the sibling gate.
    /// </summary>
    [Test]
    public void Sibling_gates_narrower_token_scan_still_covers_every_bucket_token()
    {
        var wide = CollectPanelTokens()
            .Select(t => t.Token)
            .Distinct(StringComparer.Ordinal)
            .ToList();

        var narrow = DashboardHistogramQuantileTests.Sites
            .SelectMany(s => s.BucketTokens)
            .Distinct(StringComparer.Ordinal)
            .ToHashSet(StringComparer.Ordinal);

        // Known-positive control for a test whose verdict is an empty difference.
        Assert.That(
            wide,
            Has.Count.GreaterThanOrEqualTo(MinimumBucketTokens),
            "The wide scan found almost no tokens, so an empty difference against the narrow scan "
            + "proves nothing.");

        var missed = wide.Where(t => !narrow.Contains(t)).OrderBy(t => t, StringComparer.Ordinal).ToList();

        Assert.That(
            missed,
            Is.Empty,
            "A bundled panel reads a bucket series the sibling gate's token pattern does not match, "
            + "so that gate is not checking it. Widen the pattern in "
            + "DashboardHistogramQuantileTests rather than accepting the gap.\n"
            + string.Join("\n", missed));
    }

    /// <summary>Resolves the rule governing a declared unit.</summary>
    private static UnitSuffixRule RuleFor(string unit)
    {
        if (AnnotationUnitRegex.IsMatch(unit))
        {
            return new UnitSuffixRule(
                [string.Empty],
                "OpenTelemetry treats a braced annotation unit as documentation rather than a "
                + "dimension and appends nothing. Every bundled panel reading an annotation-unit "
                + "histogram reads it bare, with no counter-example.");
        }

        return Contract.TryGetValue(unit, out var rule)
            ? rule
            : new UnitSuffixRule(
                [string.Empty, unit],
                $"The unit '{unit}' is not covered by the declared contract, so both appending it "
                + "and appending nothing are carried as candidates.");
    }

    /// <summary>
    /// The distinct bucket tokens an instrument could produce under its unit's
    /// candidate rules. One element means the rules agree and the gate can assert.
    /// </summary>
    private static IReadOnlyList<string> PredictedBucketTokens(string dotted, string unit)
    {
        var underscored = dotted.Replace('.', '_');
        var predictions = new List<string>();

        foreach (var suffix in RuleFor(unit).Suffixes)
        {
            // An exporter that appends a unit suffix declines to append one the name
            // already carries, so both behaviours predict the bare name in that case.
            var token = suffix.Length == 0 || underscored.EndsWith("_" + suffix, StringComparison.Ordinal)
                ? underscored + "_bucket"
                : underscored + "_" + suffix + "_bucket";

            if (!predictions.Contains(token, StringComparer.Ordinal))
            {
                predictions.Add(token);
            }
        }

        return predictions;
    }

    private static Assessment Assess(IReadOnlyList<ScannedToken> tokens)
    {
        var conforming = new List<string>();
        var violations = new List<string>();
        var undecided = new List<(string Token, string Dotted)>();
        var ambiguous = new List<string>();
        var nonHistogram = 0;
        var unresolved = 0;
        var histogramBacked = 0;

        var distinct = tokens
            .GroupBy(t => t.Token, StringComparer.Ordinal)
            .OrderBy(g => g.Key, StringComparer.Ordinal)
            .ToList();

        foreach (var group in distinct)
        {
            var token = group.Key;
            var candidates = Histograms
                .Where(kv => CouldProduce(kv.Key, token))
                .Select(kv => kv.Key)
                .OrderBy(n => n, StringComparer.Ordinal)
                .ToList();

            if (candidates.Count == 0)
            {
                // Either a non-histogram instrument, or nothing at all. The sibling
                // gate owns both of those verdicts; this gate must not restate them
                // in its own vocabulary, because a confident wrong explanation
                // terminates the inquiry that would have found the real cause.
                if (DeclaredInstruments.BucketTokens.ContainsKey(token))
                {
                    nonHistogram++;
                }
                else
                {
                    unresolved++;
                }

                continue;
            }

            histogramBacked++;

            if (candidates.Count > 1)
            {
                ambiguous.Add(
                    $"'{token}' could be produced by {candidates.Count} distinct instruments "
                    + $"({string.Join(", ", candidates)}), so no single expected form can be derived.");
                continue;
            }

            var dotted = candidates[0];
            var unit = Histograms[dotted];
            var predictions = PredictedBucketTokens(dotted, unit);

            if (predictions.Count != 1)
            {
                undecided.Add((token, dotted));
                continue;
            }

            if (string.Equals(predictions[0], token, StringComparison.Ordinal))
            {
                conforming.Add(token);
                continue;
            }

            var site = group.First();
            violations.Add(
                $"{site.Dashboard} {site.Site} reads '{token}', but '{dotted}' declares unit "
                + $"'{unit}', so the exporter names the series '{predictions[0]}'. "
                + $"Basis: {RuleFor(unit).Basis}");
        }

        violations.Sort(StringComparer.Ordinal);
        ambiguous.Sort(StringComparer.Ordinal);

        return new Assessment(
            conforming,
            violations,
            undecided,
            ambiguous,
            distinct.Count,
            histogramBacked,
            nonHistogram,
            unresolved);
    }

    /// <summary>
    /// Whether a dotted name could produce a token under any candidate suffix rule.
    /// Deliberately permissive: binding must not depend on the very rule under test,
    /// or a panel using the wrong suffix would fail to bind and be reported as an
    /// unknown instrument rather than as a wrong suffix.
    /// </summary>
    private static bool CouldProduce(string dotted, string token)
    {
        var underscored = dotted.Replace('.', '_');

        foreach (var suffix in new[] { string.Empty, "milliseconds", "seconds", "bytes", "ratio" })
        {
            var candidate = suffix.Length == 0
                ? underscored + "_bucket"
                : underscored + "_" + suffix + "_bucket";

            if (string.Equals(candidate, token, StringComparison.Ordinal))
            {
                return true;
            }
        }

        return false;
    }

    private static IReadOnlyList<ScannedToken> CollectPanelTokens()
    {
        var tokens = new List<ScannedToken>();

        foreach (var site in DashboardHistogramQuantileTests.Sites)
        {
            foreach (Match match in BucketTokenRegex.Matches(site.Expression))
            {
                tokens.Add(new ScannedToken(match.Value, site.Dashboard, site.Site));
            }
        }

        return tokens;
    }

    private sealed record ScannedToken(string Token, string Dashboard, string Site);

    private sealed record Assessment(
        IReadOnlyList<string> Conforming,
        IReadOnlyList<string> Violations,
        IReadOnlyList<(string Token, string Dotted)> UndecidedTokens,
        IReadOnlyList<string> AmbiguouslyBoundTokens,
        int TokensScanned,
        int HistogramBackedTokens,
        int NonHistogramTokens,
        int UnresolvedTokens);
}
