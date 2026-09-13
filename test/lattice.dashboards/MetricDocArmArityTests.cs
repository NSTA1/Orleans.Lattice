using System.Reflection;
using System.Text.RegularExpressions;
using Orleans.Lattice;
using Orleans.Lattice.Replication;
using Orleans.Lattice.Testing.Hygiene;

namespace Orleans.Lattice.Dashboards.Tests;

/// <summary>
/// Asserts that when a metric documentation row states how many tag arms an
/// instrument has, that number is the number the instrument actually arms in
/// source.
/// </summary>
/// <remarks>
/// <para>
/// Every doc-coverage gate in the repository is an <i>enrolment</i> check: it
/// answers "does this instrument have a row?", which is a question about
/// presence. None of them reads what the row says, so a row whose content is
/// false is indistinguishable from a correct one. Issue #2950 was filed after
/// exactly that: an instrument's armed set grew from five outcome arms to
/// eight, both documentation rows went on claiming "all five outcomes", and
/// every gate stayed green. The mismatch was closed one commit later by hand.
/// It was caught by a person reading prose, which is not a mechanism.
/// </para>
/// <para>
/// <b>Arity only.</b> This guard deliberately checks a count and nothing else.
/// The armed tag set is discoverable from source, so "the row claims N arms and
/// the instrument arms M" is mechanically decidable; general prose correctness
/// is not, and an attempt at it would produce either a gate that cannot fail or
/// one that fails on every rewording. Arity is the checkable part, and it is the
/// part that was wrong.
/// </para>
/// <para>
/// <b>The armed set is derived from recording sites, never from an enum.</b>
/// An instrument's tag space is a union of whatever its call sites stamp, and on
/// the instrument that prompted this issue the union is strictly larger than any
/// one enum: four arms are lifecycle events that are not enum members at all.
/// A guard that derived the expected set from <c>Enum.GetValues</c> would
/// therefore report those legitimate arms as undocumented and author the next
/// arity mismatch into the gate meant to catch the last one. The derivation used
/// here is <see cref="DashboardPanelTagDomainTests.ArmedValues"/>, which is the
/// same source scan the panel tag-domain guard already runs on; sharing it means
/// the documented arity and the charted domain cannot drift apart while both
/// still pass.
/// </para>
/// <para>
/// <b>The decision this guard encodes.</b> Issue #2950 asks whether doc rows
/// should enumerate arms at all, since a row that claims no arm set cannot
/// misstate one. They may, and they should be free to: the enumeration is
/// genuinely useful to a reader, fourteen rows already carry one, and removing a
/// claim because it is unverified is only the right move when the claim cannot
/// be verified. This one can be, so it is gated instead of banned.
/// </para>
/// <para>
/// <b>Loud when it matches nothing.</b> A gate whose scan silently finds no work
/// is worse than no gate, because it reports the same green as a gate that
/// checked everything. Each stage of the scan therefore has its own test
/// asserting the stage found something, on the model of
/// <c>MeterFieldDeclarationOrderTests</c>.
/// </para>
/// </remarks>
[TestFixture]
public sealed class MetricDocArmArityTests
{
    /// <summary>The documentation files whose rows carry arity claims.</summary>
    private static readonly string[] DocumentationFiles =
    [
        "docs/lattice/metrics.md",
        "docs/lattice.dashboards/metrics-to-panel-map.md",
    ];

    /// <summary>
    /// Rows whose arity claim cannot be decided from source, with the reason.
    /// An entry here is a narrow, justified exemption and is itself gated by
    /// <see cref="EveryExemptionIsStillLoadBearing"/>, so an exemption that stops
    /// being needed fails the build rather than quietly widening the hole.
    /// </summary>
    private static readonly IReadOnlyDictionary<string, string> UndecidableClaims =
        new Dictionary<string, string>(StringComparer.Ordinal)
        {
        };

    /// <summary>
    /// Maps the noun a claim uses onto the tag key it is a claim about. A claim
    /// whose noun appears here is checked against that specific tag; one whose
    /// noun does not (a bare "arms", say) is checked against every tag key the
    /// instrument could carry. Narrowing where the noun says so is what stops a
    /// wrong count passing because some unrelated tag happens to share it.
    /// </summary>
    private static readonly IReadOnlyDictionary<string, string> NounTagKeys =
        new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
        {
            ["outcome arms"] = "outcome",
            ["fault arms"] = "fault",
            ["outcomes"] = "outcome",
            ["phases"] = "phase",
            ["states"] = "state",
            ["stages"] = "stage",
            ["statuses"] = "status",
            ["reasons"] = "reason",
            ["kinds"] = "kind",
            ["decisions"] = "decision",
        };

    private static readonly IReadOnlyDictionary<string, int> NumberWords =
        new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase)
        {
            ["two"] = 2,
            ["three"] = 3,
            ["four"] = 4,
            ["five"] = 5,
            ["six"] = 6,
            ["seven"] = 7,
            ["eight"] = 8,
            ["nine"] = 9,
            ["ten"] = 10,
            ["eleven"] = 11,
            ["twelve"] = 12,
        };

    /// <summary>
    /// Matches a markdown table row whose first cell is a backticked instrument
    /// name. The instrument is taken from the first cell only, never from the
    /// prose, because a description routinely names sibling instruments it should
    /// be charted beside and attributing a claim to one of those would check the
    /// wrong instrument.
    /// </summary>
    private static readonly Regex RowInstrumentRegex = new(
        @"^\|\s*`(?<name>orleans\.[a-z0-9_.]+)`\s*\|",
        RegexOptions.Compiled);

    /// <summary>
    /// Matches an explicit arity claim. Deliberately anchored on "all" or
    /// "every", which is what makes the statement a claim about the complete set
    /// rather than a passing mention of some arms.
    /// </summary>
    private static readonly Regex ArityClaimRegex = new(
        @"\b(?:all|every one of the|each of the)\s+"
        + @"(?<count>two|three|four|five|six|seven|eight|nine|ten|eleven|twelve|\d{1,2})\s+"
        + @"(?<noun>outcome arms|fault arms|outcomes|arms|phases|states|stages|statuses|reasons|kinds|decisions|values)\b",
        RegexOptions.IgnoreCase | RegexOptions.Compiled);

    /// <summary>One arity claim found on one documentation row.</summary>
    /// <param name="File">The repo-relative documentation path.</param>
    /// <param name="Line">The 1-based line the row occupies.</param>
    /// <param name="Instrument">The instrument the row documents.</param>
    /// <param name="Claimed">The arm count the row claims.</param>
    /// <param name="Noun">The noun the claim uses.</param>
    /// <param name="Phrase">The matched phrase, for diagnostics.</param>
    internal sealed record ArityClaim(
        string File, int Line, string Instrument, int Claimed, string Noun, string Phrase)
    {
        /// <summary>The stable key an exemption is registered under.</summary>
        internal string Key => $"{File}|{Instrument}|{Claimed} {Noun}";
    }

    /// <summary>The verdict on one claim.</summary>
    /// <param name="Claim">The claim evaluated.</param>
    /// <param name="Violation">The mismatch found, or null.</param>
    /// <param name="Undecidable">Why no armed set could be derived, or null.</param>
    internal sealed record Verdict(ArityClaim Claim, string? Violation, string? Undecidable);

    // ------------------------------------------------------------- the gate

    /// <summary>
    /// Every arity claim in the metric documentation states the number of tag
    /// values the instrument actually arms.
    /// </summary>
    [Test]
    public void DocumentedArmArityMatchesTheArmedSet()
    {
        var claims = ScanClaims();
        var verdicts = ScannedVerdicts();

        var violations = verdicts
            .Where(static v => v.Violation is not null)
            .Select(static v => $"  {v.Claim.File}:{v.Claim.Line}  {v.Violation}")
            .ToArray();

        var undecidable = verdicts
            .Where(static v => v.Undecidable is not null)
            .Where(static v => !UndecidableClaims.ContainsKey(v.Claim.Key))
            .Select(static v => $"  {v.Claim.File}:{v.Claim.Line}  {v.Undecidable}")
            .ToArray();

        Assert.Multiple(() =>
        {
            Assert.That(violations, Is.Empty,
                "A documentation row states an arm count the instrument does not arm. The row is "
                + "the thing to fix, not this guard: the armed set is derived from the recording "
                + "sites under src/, so it is what the process really stamps.\n"
                + string.Join('\n', violations));

            Assert.That(undecidable, Is.Empty,
                "A documentation row claims an arm count for an instrument whose armed set could "
                + "not be derived from source. Extend the resolver, or register the row in "
                + "UndecidableClaims with a reason. Do not delete the claim to silence this.\n"
                + string.Join('\n', undecidable));
        });
    }

    // --------------------------------------------------- loud-on-empty scan

    /// <summary>
    /// The scan finds documentation rows. Without this, deleting or renaming a
    /// documentation file turns the gate into a permanent green.
    /// </summary>
    [Test]
    public void TheScanDiscoversInstrumentRows()
    {
        var rows = ScanRows();

        Assert.That(rows, Is.Not.Empty,
            "No instrument rows were found in the metric documentation. Either the documentation "
            + "moved or RowInstrumentRegex stopped matching the table shape; in both cases this "
            + "guard is checking nothing and must fail rather than report green.");

        foreach (var file in DocumentationFiles)
        {
            Assert.That(rows.Any(r => string.Equals(r.File, file, StringComparison.Ordinal)), Is.True,
                $"No instrument rows were found in {file}, so that file is unguarded.");
        }
    }

    /// <summary>
    /// The scan finds arity claims. Without this, a regex that stops matching the
    /// prose reports the same green as a documentation set with no claims in it.
    /// </summary>
    [Test]
    public void TheScanDiscoversArityClaims()
    {
        var claims = ScanClaims();

        Assert.That(claims, Is.Not.Empty,
            "No arity claims were found. ArityClaimRegex has stopped matching the way the "
            + "documentation phrases an arm count, so DocumentedArmArityMatchesTheArmedSet is "
            + "evaluating an empty set and cannot fail.");

        Assert.That(claims.Select(static c => c.Instrument).Distinct().Count(), Is.GreaterThan(1),
            "Arity claims were found for only one instrument, which is too narrow to be the real "
            + "population and suggests the row-to-claim attribution is broken.");
    }

    /// <summary>
    /// The resolver derives a real armed set for at least one claimed row.
    /// </summary>
    /// <remarks>
    /// This is the clause that separates "every claim checked out" from "no claim
    /// could be checked". Both produce an empty violation list, and only this
    /// test can tell them apart, which is the same distinction the instrument in
    /// issue #2950 failed to make about its own zeroes.
    /// </remarks>
    [Test]
    public void TheResolverDerivesAnArmedSetForAtLeastOneClaimedRow()
    {
        var decided = ScannedVerdicts().Count(static v => v.Undecidable is null);

        Assert.That(decided, Is.GreaterThan(0),
            "No arity claim could be decided against source. The armed-set resolver is returning "
            + "nothing for every documented instrument, so the gate is vacuous.");
    }

    /// <summary>
    /// Every registered exemption still names a claim the resolver genuinely
    /// cannot decide.
    /// </summary>
    [Test]
    public void EveryExemptionIsStillLoadBearing()
    {
        var undecidable = ScannedVerdicts()
            .Where(static v => v.Undecidable is not null)
            .Select(static v => v.Claim.Key)
            .ToHashSet(StringComparer.Ordinal);

        var stale = UndecidableClaims.Keys
            .Where(k => !undecidable.Contains(k))
            .ToArray();

        Assert.That(stale, Is.Empty,
            "An entry in UndecidableClaims no longer names an undecidable claim. Remove it: a "
            + "stale exemption silently excuses whatever row later takes that key.\n  "
            + string.Join("\n  ", stale));
    }

    // ------------------------------------- generic-noun ambiguity (issue #2956)

    /// <summary>
    /// No arity claim states a completeness count with a generic noun on an
    /// instrument that arms more than one tag, where the claim could be satisfied
    /// by a tag other than the one the row means.
    /// </summary>
    /// <remarks>
    /// <para>
    /// A claim phrased "all three arms" names a count but not a <i>tag</i>. When
    /// the noun maps to a tag key, <see cref="Evaluate"/> pins the comparison to
    /// that tag. When it does not, the evaluator falls through to a search across
    /// every candidate tag and accepts the claim as soon as <b>any</b> of them arms
    /// that many values - see the <c>matched = true</c> branch. On an instrument
    /// with a single tag domain that is harmless, because the only tag it could
    /// match is the one meant. On an instrument with several, the claim is
    /// certified by a coincidence of cardinality: the row says "three" about
    /// <c>outcome</c>, some unrelated tag happens to arm three values, and the gate
    /// reports green over a claim it never checked.
    /// </para>
    /// <para>
    /// That is the epic's defect in miniature. The verdict reads "claim verified";
    /// the denominator is a tag nobody was talking about. This guard removes the
    /// ambiguity at the source - the wording - rather than trying to guess the
    /// intended tag, because a guess would be one more hand-maintained mapping of
    /// exactly the kind that goes stale.
    /// </para>
    /// <para>
    /// The rule is deliberately about <b>ambiguity, not correctness</b>: a generic
    /// claim is reported whenever more than one tag domain is derivable, even if
    /// only one currently has the claimed cardinality. A claim that is right today
    /// only because no sibling tag happens to share its size is still unpinned, and
    /// it starts certifying the wrong tag the moment one does - silently, with no
    /// edit to the row to attribute it to.
    /// </para>
    /// </remarks>
    [Test]
    public void NoArityClaimUsesAGenericNounWhereTheTagIsAmbiguous()
    {
        var ambiguous = AmbiguousGenericClaims(ScanClaims());

        Assert.That(ambiguous, Is.Empty,
            "A documentation row states a completeness count with a generic noun on an instrument "
            + "that arms more than one tag. The count can be satisfied by a tag the row does not "
            + "mean, so the arity gate can certify it without checking the intended tag. Name the "
            + "tag in the row - \"all three outcome arms\" rather than \"all three arms\" - which "
            + "pins the comparison to that tag.\n  "
            + string.Join("\n  ", ambiguous));
    }

    /// <summary>
    /// The ambiguity rule has a population it could apply to: at least one
    /// documented instrument arms more than one tag domain.
    /// </summary>
    /// <remarks>
    /// <see cref="NoArityClaimUsesAGenericNounWhereTheTagIsAmbiguous"/> passes today
    /// because every such row was reworded, so its claim list is empty - and an
    /// empty list is exactly what a broken scan produces too. This asserts the
    /// condition the rule needs in order to be capable of firing, so the guard
    /// cannot quietly become a test of nothing if the tag resolver stops deriving
    /// domains.
    /// </remarks>
    [Test]
    public void TheAmbiguityRuleHasAPopulationItCouldApplyTo()
    {
        var multiTagged = DocumentedInstruments()
            .Count(name => DerivableTagCount(name) > 1);

        Assert.That(multiTagged, Is.GreaterThan(0),
            "No documented instrument arms more than one derivable tag domain, so no generic "
            + "claim could ever be ambiguous and this rule is incapable of firing. The tag "
            + "resolver has most likely stopped deriving domains; fix it rather than deleting "
            + "this test.");
    }

    /// <summary>
    /// A generic claim on a multi-tag instrument is reported - the positive control
    /// for the ambiguity rule.
    /// </summary>
    /// <remarks>
    /// Built against a real instrument resolved at run time, and driven through the
    /// same helper the gate calls, so it proves the shipping rule rather than a
    /// parallel reimplementation of it.
    /// </remarks>
    [Test]
    public void AGenericClaimOnAMultiTagInstrumentIsReported()
    {
        var instrument = DocumentedInstruments().FirstOrDefault(name => DerivableTagCount(name) > 1);

        Assert.That(instrument, Is.Not.Null,
            "No documented instrument arms more than one derivable tag domain, so the control "
            + "cannot be constructed.");

        var reported = AmbiguousGenericClaims([
            new ArityClaim("docs/synthetic.md", 1, instrument!, 3, "arms", "all three arms")]);

        Assert.That(reported, Is.Not.Empty,
            $"A generic claim on {instrument}, which arms more than one tag, was not reported. "
            + "The ambiguity rule is not applying and the real scan above proves nothing.");
    }

    /// <summary>
    /// The same claim on the same instrument is <b>not</b> reported once its noun
    /// names a tag - the negative control.
    /// </summary>
    /// <remarks>
    /// Without this, the positive control would be satisfied by a rule that
    /// reported every claim on a multi-tag instrument regardless of its wording,
    /// which would make the reword pointless and the guard unsatisfiable. Holding
    /// the instrument fixed and varying only the noun isolates genericness as the
    /// thing being detected.
    /// </remarks>
    [Test]
    public void AMappedNounOnTheSameInstrumentIsNotReported()
    {
        var instrument = DocumentedInstruments().FirstOrDefault(name => DerivableTagCount(name) > 1);

        Assert.That(instrument, Is.Not.Null,
            "No documented instrument arms more than one derivable tag domain, so the control "
            + "cannot be constructed.");

        var reported = AmbiguousGenericClaims([
            new ArityClaim("docs/synthetic.md", 1, instrument!, 3, "outcome arms",
                "all three outcome arms")]);

        Assert.That(reported, Is.Empty,
            $"A claim naming the outcome tag on {instrument} was reported as ambiguous. The rule "
            + "is firing on the instrument rather than on the wording, so rewording a row could "
            + "never satisfy it.\n  " + string.Join("\n  ", reported));
    }

    /// <summary>
    /// Reports the claims whose noun does not name a tag and whose instrument arms
    /// more than one tag domain.
    /// </summary>
    /// <remarks>
    /// Takes its claims as a parameter so the controls drive the same code the gate
    /// runs, rather than a copy that could agree with itself while both are wrong.
    /// </remarks>
    internal static IReadOnlyList<string> AmbiguousGenericClaims(IEnumerable<ArityClaim> claims) =>
        claims
            .Where(static c => !NounTagKeys.ContainsKey(c.Noun))
            .Where(c => DerivableTagCount(c.Instrument) > 1)
            .Select(c =>
                $"{c.File}:{c.Line}  \"{c.Phrase}\" on {c.Instrument}, which arms "
                + $"{DerivableTagCount(c.Instrument)} tag domains ("
                + $"{string.Join(", ", DerivableTags(c.Instrument))}), so the count does not say "
                + "which tag is meant")
            .ToList();

    /// <summary>The tag keys an instrument actually arms, derived from source.</summary>
    private static IReadOnlyList<string> DerivableTags(string instrument) =>
        CandidateTagKeys()
            .Where(k => DashboardPanelTagDomainTests.ArmedValues(instrument, k) is { Count: > 0 })
            .ToList();

    private static int DerivableTagCount(string instrument) => DerivableTags(instrument).Count;

    // ------------------------------------------------------ positive controls

    /// <summary>
    /// The evaluator reports a violation for a row that overstates its arity.
    /// </summary>
    /// <remarks>
    /// The control is built against a real instrument and a real armed count read
    /// from source at run time, never a literal, so it keeps proving the same
    /// thing after the instrument's arm count changes.
    /// </remarks>
    [Test]
    public void AnOverstatedArityIsReportedAsAViolation()
    {
        var (instrument, armed) = AnInstrumentWithADerivableOutcomeDomain();

        var verdicts = Evaluate([
            new ArityClaim("docs/synthetic.md", 1, instrument, armed + 1, "outcomes",
                $"all {armed + 1} outcomes")]);

        Assert.Multiple(() =>
        {
            Assert.That(verdicts.Single().Undecidable, Is.Null,
                "The control instrument must be decidable, or the control proves nothing.");
            Assert.That(verdicts.Single().Violation, Is.Not.Null,
                $"A claim of {armed + 1} outcomes against {instrument}, which arms {armed}, was "
                + "not reported. The evaluator cannot see an over-claim.");
        });
    }

    /// <summary>
    /// The evaluator accepts a row that states its arity correctly, so the guard
    /// is not simply rejecting everything.
    /// </summary>
    [Test]
    public void AnAccurateArityIsAccepted()
    {
        var (instrument, armed) = AnInstrumentWithADerivableOutcomeDomain();

        var verdict = Evaluate([
            new ArityClaim("docs/synthetic.md", 1, instrument, armed, "outcomes",
                $"all {armed} outcomes")]).Single();

        Assert.Multiple(() =>
        {
            Assert.That(verdict.Undecidable, Is.Null);
            Assert.That(verdict.Violation, Is.Null,
                $"A claim of {armed} outcomes against {instrument}, which arms exactly {armed}, "
                + "was rejected. The guard would fail every correct row.");
        });
    }

    /// <summary>
    /// A noun that names a tag key pins the check to that key, rather than
    /// accepting any tag on the instrument whose cardinality happens to match.
    /// </summary>
    /// <remarks>
    /// Without this the guard would be satisfiable by coincidence, which is how a
    /// wrong count survives a check that only asks whether the number appears
    /// somewhere. The control asserts the narrowing is live by finding an
    /// instrument that arms two differently sized tag domains and claiming the
    /// wrong one's size under the other one's noun.
    /// </remarks>
    [Test]
    public void TheNounMappedTagIsPreferredOverAnyMatchingCardinality()
    {
        var candidate = DocumentedInstruments()
            .Select(name => new
            {
                Name = name,
                Outcome = DashboardPanelTagDomainTests.ArmedValues(name, "outcome"),
                Other = CandidateTagKeys()
                    .Where(static k => !string.Equals(k, "outcome", StringComparison.Ordinal))
                    .Select(k => DashboardPanelTagDomainTests.ArmedValues(name, k))
                    .FirstOrDefault(static v => v is { Count: > 0 }),
            })
            .FirstOrDefault(x => x.Outcome is { Count: > 0 }
                && x.Other is { Count: > 0 }
                && x.Other.Count != x.Outcome.Count);

        Assert.That(candidate, Is.Not.Null,
            "No documented instrument arms two differently sized tag domains, so the narrowing "
            + "this guard depends on cannot be demonstrated and the control proves nothing. Widen "
            + "the search rather than deleting the test.");

        var verdict = Evaluate([
            new ArityClaim("docs/synthetic.md", 1, candidate!.Name, candidate.Other!.Count,
                "outcomes", $"all {candidate.Other.Count} outcomes")]).Single();

        Assert.That(verdict.Violation, Is.Not.Null,
            $"{candidate.Name} arms {candidate.Outcome!.Count} outcome values, and a claim of "
            + $"{candidate.Other.Count} outcomes was accepted because some other tag on the same "
            + "instrument has that many values. The noun-to-tag narrowing is not being applied.");
    }

    // ------------------------------------------------------------ evaluation

    /// <summary>
    /// Decides every claim against the armed set derived from source.
    /// </summary>
    /// <remarks>
    /// Takes its claims as a parameter rather than scanning, so a control can
    /// drive the same evaluator the gate runs on. A control that exercised a
    /// parallel code path would demonstrate only that the parallel path works.
    /// </remarks>
    /// <summary>
    /// The verdicts for the documentation population, computed once. Several
    /// tests interrogate the same scan, and resolving an armed set walks source
    /// files, so sharing the result keeps the fixture's cost proportional to the
    /// number of claims rather than to the number of tests.
    /// </summary>
    private static IReadOnlyList<Verdict> ScannedVerdicts() => ScannedVerdictsLazy.Value;

    private static readonly Lazy<IReadOnlyList<Verdict>> ScannedVerdictsLazy = new(()
        => Evaluate(ScanClaims()));

    internal static IReadOnlyList<Verdict> Evaluate(IEnumerable<ArityClaim> claims)
    {        var verdicts = new List<Verdict>();

        foreach (var claim in claims)
        {
            if (NounTagKeys.TryGetValue(claim.Noun, out var tag)
                && DashboardPanelTagDomainTests.ArmedValues(claim.Instrument, tag) is { } pinned)
            {
                verdicts.Add(new Verdict(
                    claim,
                    pinned.Count == claim.Claimed
                        ? null
                        : $"\"{claim.Phrase}\" on {claim.Instrument}, which arms {pinned.Count} "
                          + $"'{tag}' value(s): {string.Join(", ", pinned.OrderBy(static v => v, StringComparer.Ordinal))}",
                    null));
                continue;
            }

            var sizes = new SortedSet<int>();
            var matched = false;
            foreach (var key in CandidateTagKeys())
            {
                if (DashboardPanelTagDomainTests.ArmedValues(claim.Instrument, key) is not { Count: > 0 } values)
                {
                    continue;
                }

                sizes.Add(values.Count);
                if (values.Count == claim.Claimed)
                {
                    // The claim is satisfied by this tag. Resolving the rest
                    // would only enlarge a diagnostic that will not be printed,
                    // and resolution is the expensive part of this guard.
                    matched = true;
                    break;
                }
            }

            if (matched)
            {
                verdicts.Add(new Verdict(claim, null, null));
                continue;
            }

            if (sizes.Count == 0)
            {
                verdicts.Add(new Verdict(
                    claim,
                    null,
                    $"\"{claim.Phrase}\" on {claim.Instrument}, for which no tag domain could be "
                    + "derived from any recording site"));
                continue;
            }

            verdicts.Add(new Verdict(
                claim,
                $"\"{claim.Phrase}\" on {claim.Instrument}, which arms no tag with that many "
                + $"values (armed cardinalities: {string.Join(", ", sizes)})",
                null));
        }

        return verdicts;
    }

    // ---------------------------------------------------------------- scan

    /// <summary>One documentation row that names an instrument.</summary>
    /// <param name="File">The repo-relative documentation path.</param>
    /// <param name="Line">The 1-based line number.</param>
    /// <param name="Instrument">The instrument named in the first cell.</param>
    /// <param name="Text">The whole row.</param>
    internal sealed record DocumentedRow(string File, int Line, string Instrument, string Text);

    private static IReadOnlyList<DocumentedRow> ScanRows() => ScanRowsLazy.Value;

    private static readonly Lazy<IReadOnlyList<DocumentedRow>> ScanRowsLazy = new(ScanRowsCore);

    private static IReadOnlyList<DocumentedRow> ScanRowsCore()
    {
        var root = HygieneRepository.FindRepoRoot();
        var rows = new List<DocumentedRow>();

        foreach (var relative in DocumentationFiles)
        {
            var path = Path.Combine(root, relative.Replace('/', Path.DirectorySeparatorChar));
            if (!File.Exists(path))
            {
                continue;
            }

            var lines = File.ReadAllLines(path);
            for (var i = 0; i < lines.Length; i++)
            {
                var match = RowInstrumentRegex.Match(lines[i]);
                if (match.Success)
                {
                    rows.Add(new DocumentedRow(relative, i + 1, match.Groups["name"].Value, lines[i]));
                }
            }
        }

        return rows;
    }

    private static IReadOnlyList<ArityClaim> ScanClaims() => ScanClaimsLazy.Value;

    private static readonly Lazy<IReadOnlyList<ArityClaim>> ScanClaimsLazy = new(ScanClaimsCore);

    private static IReadOnlyList<ArityClaim> ScanClaimsCore()
    {
        var claims = new List<ArityClaim>();

        foreach (var row in ScanRows())
        {
            foreach (Match match in ArityClaimRegex.Matches(row.Text))
            {
                var raw = match.Groups["count"].Value;
                var count = NumberWords.TryGetValue(raw, out var word)
                    ? word
                    : int.Parse(raw, System.Globalization.CultureInfo.InvariantCulture);

                claims.Add(new ArityClaim(
                    row.File,
                    row.Line,
                    row.Instrument,
                    count,
                    match.Groups["noun"].Value,
                    match.Value));
            }
        }

        return claims;
    }

    /// <summary>
    /// Every tag key the metric surfaces declare, read off the <c>Tag*</c>
    /// constants rather than listed here, so a new tag key joins the generic
    /// search without an edit.
    /// </summary>
    /// <remarks>
    /// Ordered so that the keys an arm-counting sentence is most likely to be
    /// about come first. Resolving one armed set walks source, and the generic
    /// search stops at the first key whose cardinality matches, so the order is
    /// the difference between a handful of resolutions per claim and the whole
    /// key space. It is a cost ordering only: every key is still reachable, so no
    /// claim is decided differently because of it, and a claim that matches
    /// nothing still pays for the full sweep before it is reported.
    /// </remarks>
    private static IReadOnlyList<string> CandidateTagKeys() => CandidateTagKeysLazy.Value;

    /// <summary>
    /// Tag keys tried before the rest, in this order.
    /// </summary>
    private static readonly string[] PreferredTagKeys =
        ["outcome", "arm", "phase", "state", "stage", "status", "reason", "kind", "decision", "result"];

    private static readonly Lazy<IReadOnlyList<string>> CandidateTagKeysLazy = new(() =>
    {
        var keys = new SortedSet<string>(StringComparer.Ordinal);

        foreach (var owner in new[] { typeof(LatticeMetrics), typeof(LatticeReplicationMetrics) })
        {
            foreach (var field in owner.GetFields(BindingFlags.Public | BindingFlags.Static))
            {
                if (!field.IsLiteral || field.FieldType != typeof(string)) continue;
                if (!field.Name.StartsWith("Tag", StringComparison.Ordinal)) continue;
                if (field.GetRawConstantValue() is string value && value.Length > 0) keys.Add(value);
            }
        }

        return keys
            .OrderBy(static k => Array.IndexOf(PreferredTagKeys, k) is var i && i >= 0 ? i : int.MaxValue)
            .ThenBy(static k => k, StringComparer.Ordinal)
            .ToArray();
    });

    /// <summary>
    /// An instrument with a derivable <c>outcome</c> domain, and that domain's
    /// size, for use as a control. Chosen at run time from the documented
    /// instruments rather than named, so the control cannot rot against a renamed
    /// instrument or a changed arm count.
    /// </summary>
    private static (string Instrument, int Armed) AnInstrumentWithADerivableOutcomeDomain()
    {
        foreach (var name in DocumentedInstruments())
        {
            if (DashboardPanelTagDomainTests.ArmedValues(name, "outcome") is { Count: > 1 } values)
            {
                return (name, values.Count);
            }
        }

        throw new InvalidOperationException(
            "No documented instrument with a derivable multi-value 'outcome' domain was found, so "
            + "the positive controls cannot be built. The resolver is broken.");
    }

    /// <summary>
    /// The instruments a documentation row makes an arity claim about, ordered so
    /// the controls pick deterministically. Bounded to this set rather than every
    /// known instrument because resolving an armed set is the expensive part of
    /// this guard, and the claim population is the only part it has to decide.
    /// </summary>
    private static IReadOnlyList<string> DocumentedInstruments() => DocumentedInstrumentsLazy.Value;

    private static readonly Lazy<IReadOnlyList<string>> DocumentedInstrumentsLazy = new(()
        => ScanClaims()
            .Select(static c => c.Instrument)
            .Distinct(StringComparer.Ordinal)
            .OrderBy(static n => n, StringComparer.Ordinal)
            .ToArray());
}
