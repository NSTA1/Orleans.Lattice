using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Text.RegularExpressions;
using NUnit.Framework;
using Orleans.Lattice.Testing.Hygiene;

namespace Orleans.Lattice.Tests.Hygiene;

/// <summary>
/// Asserts that every instrument declaration under <c>src/</c> is accounted for by
/// exactly one enrolment in <c>InstrumentPrimingEnrolment.tsv</c>, and that each
/// enrolment is one the parser is able to contradict.
/// </summary>
/// <remarks>
/// <para>
/// The defect class this closes: a metric series that is absent because nothing ever
/// wired it is byte-identical, at the query, to a series that is absent because the
/// measured quantity was genuinely zero. An unprimed instrument therefore breaks the
/// epic's first rule - an absence is evidence only if the detector is independently
/// known to work - and it breaks it silently.
/// </para>
/// <para>
/// The rule deliberately implemented here is NOT "every instrument must be primed".
/// That rule is wrong in both directions. It forbids too much, because a bounded
/// taxonomy forced to prime every arm manufactures series that exist but are never
/// written, which a reader cannot distinguish from a measured zero any better than
/// before; priming can thus create the very false absence it was meant to prevent.
/// And it helps too little, because a high-cardinality or free-form dimension cannot
/// be primed at all, so the mechanical rule is silent on the case most in need of it.
/// The correct form is: every unprimed dimension must be totalled by a primed one,
/// and the totalling must be asserted.
/// </para>
/// <para>
/// <b>Scope is declared, never derived.</b> This is the load-bearing design decision
/// and it was reached the expensive way. Deriving the in-scope set by parsing tag
/// domains was attempted three times and produced 13, then 70, then 42 - three
/// coherent, internally consistent, entirely unverified numbers, none of which threw.
/// The 70 was larger than the 13 and so read as progress while being unsound. The
/// general property is that a gate whose SCOPE is derived by a parser has a scope
/// that tracks the parser's depth rather than the repository's content: it excludes
/// what it cannot parse, and excludes it as "out of scope" rather than as "unknown".
/// A deeper parser does not fix that; it relocates the boundary and keeps it silent.
/// So the enrolment file is the scope, and the parser's only job is to FALSIFY what
/// the file claims. A shallow parser then yields a false alarm, which is loud, rather
/// than a false exclusion, which is not.
/// </para>
/// <para>
/// <b>Every category is falsifiable.</b> There is deliberately no "exempt" category.
/// An exemption would assert that an instrument cannot be primed or anchored, which
/// is a promise about capability that nothing can contradict; a gate whose categories
/// include an unfalsifiable one reports coverage it did not measure.
/// <see cref="Enrolment.None"/> instead claims something about CONTENT - that the
/// instrument carries no bounded tag dimension - which the parser contradicts the
/// moment it resolves one there.
/// </para>
/// <para>
/// <b><see cref="Enrolment.Unresolved"/> exists so that the parser's blind spot cannot
/// be laundered into a negative claim.</b> Total accounting over two collections that
/// can both omit the same member is not total coverage: an instrument the parser
/// cannot read AND nobody declared is absent from both sides, the two sides agree,
/// and the gate is green. That is the same silent-exclusion failure one level up, in
/// the comparison rather than in the derivation. Splitting "no bounded dimension"
/// from "a dimension I cannot resolve" keeps the blind spot named and counted instead
/// of invisible, and it is falsifiable in exactly the same way: resolving the domain
/// later contradicts the checked-in claim.
/// </para>
/// <para>
/// <b>Ambiguity is never unioned.</b> Resolution of a tag value through a helper or a
/// constant is file-local first, and widens to the repository only when the name is
/// unique there. Four helper names in <c>src/</c> - <c>DescribeCause</c>,
/// <c>DescribeOutcome</c>, <c>DescribeRejection</c>, <c>DescribeState</c> - are each
/// declared in two different files, so a repository-wide lookup by bare name silently
/// unions two unrelated taxonomies and yields a superset that looks like a richer
/// answer. Every such case resolves to <see cref="Enrolment.Unresolved"/> here.
/// </para>
/// <para>
/// <b>Quantifier, stated so the next reader does not have to infer it.</b> This gate
/// iterates <b>instrument declarations</b> - the syntactic <c>Create*</c> call sites
/// under <c>src/</c> - and asserts a universal over that population: every declaration
/// resolves to exactly one enrolment, and no enrolment is contradicted by the parser.
/// It is therefore <b>silent</b> about three adjacent populations that a reader may
/// mistake it for covering:
/// <list type="bullet">
/// <item><description><b>Tag values.</b> An enrolment is a claim about an instrument,
/// not about each arm of its taxonomy. <see cref="Enrolment.Primed"/> asserts a zero
/// sample exists per value at the sites the parser can see; it does not enumerate the
/// value set a running silo can actually emit.</description></item>
/// <item><description><b>Panels.</b> Nothing here observes whether any dashboard
/// references the instrument. That existential lives in
/// <c>DashboardJsonTests.Every_live_instrument_is_referenced_by_at_least_one_dashboard_panel</c>,
/// in the <c>lattice.dashboards</c> test project - a different assembly, unreachable
/// by any filter scoped to <c>test/lattice/</c>.</description></item>
/// <item><description><b>Runtime instruments with no declaration site.</b> An
/// instrument created somewhere this parser does not read is outside the iterated
/// population entirely. The reflection cross-check narrows that gap for the core
/// package only, and is not a general guarantee.</description></item>
/// </list>
/// A gate over one of these populations looks like a stronger version of a gate over
/// another, and is not. Quantifier confusion between them is the single most repeated
/// defect on this epic.
/// </para>
/// <para>
/// <b>The same indirection hides the PRIMING, and this fix does not close that hole.</b>
/// Naming it here because an unnamed blind spot is the defect this gate exists to prevent,
/// and leaving one unnamed inside the fix for it would be the same error one level down.
/// <c>ZeroEmittedValues</c> recognises a prime only as a literal zero in <c>args[0]</c> of an
/// <c>owner.Add(</c> / <c>owner.Record(</c> call, and it reads tags through the same
/// literal-only path as <c>ResolveDimensions</c>. An instrument primed through a shared
/// recorder therefore has invisible primes as well as invisible dimensions - one indirection,
/// two blind spots.
/// </para>
/// <para>
/// <c>LatticeMetrics.WalGcBlockedLeafReactivations</c> is the worked example of BOTH halves.
/// Its single write seam is <c>LatticeWalGcScheduler.RecordBlockedLeafReactivation</c> (that
/// file, line 1895), through which all ten call sites route; five of them prime, at lines
/// 1161-1164 and 1176. Every one passes the zero as the recorder's trailing <c>delta</c>
/// argument, so at the <c>.Add(</c> site <c>args[0]</c> is the variable <c>delta</c> and no
/// prime is visible. The instrument is in fact fully primed and is nonetheless enrolled here
/// as <see cref="Enrolment.Unresolved"/>.
/// </para>
/// <para>
/// That enrolment is correct rather than a concession. <see cref="Enrolment.Unresolved"/>
/// claims only that a dimension is present and its domain is not resolvable from source, which
/// is exactly true of this parser's reach; it makes no claim either way about priming.
/// Enrolling it <see cref="Enrolment.Primed"/> would assert a zero sample per value that this
/// parser cannot see and so cannot falsify - trading a false negative claim for an
/// unfalsifiable positive one, which is strictly worse. Widening the prime detection is a
/// second parser change with its own blast radius and is deliberately left undone here rather
/// than done partially and reported as complete.
/// </para>
/// <para>
/// To regenerate the enrolment file after adding instruments, set
/// <c>LATTICE_REWRITE_PRIMING_ENROLMENT=1</c> and run this fixture. The rewritten file
/// records the parser's current view; the <c>none</c> rows it emits are a checked-in
/// baseline of that view and not an independent audit, which is precisely what makes a
/// later parser improvement show up as a visible contradiction rather than as a silent
/// change of scope.
/// </para>
/// </remarks>
[TestFixture]
[Category("Hygiene")]
public sealed class InstrumentPrimingEnrolmentTests
{
    /// <summary>Enrolment categories. Every one of these is falsifiable by the parser.</summary>
    public enum Enrolment
    {
        /// <summary>A zero sample is emitted for every value of the bounded dimension at startup.</summary>
        Primed,

        /// <summary>Arms are deliberately unprimed but sum to a primed total, and a named test asserts the tally.</summary>
        Anchored,

        /// <summary>A tag dimension is present but its domain is not resolvable from source. Named, not hidden.</summary>
        Unresolved,

        /// <summary>No bounded tag dimension. A claim about content, contradicted by resolving one.</summary>
        None,

        /// <summary>
        /// A bounded dimension that is currently neither primed nor anchored, recorded with a
        /// tracking reference. This is a claim about CURRENT STATE, not about capability, so it
        /// is falsifiable in both directions: the gate reddens if a new one appears, and equally
        /// if one of these is fixed and the row is left behind.
        /// </summary>
        Unprimed,
    }

    /// <summary>One instrument declaration discovered in source.</summary>
    public sealed record Declaration(
        string Key,
        string RelativePath,
        string Owner,
        string Kind,
        string? InstrumentName,
        IReadOnlyDictionary<string, DomainResult> Dimensions);

    /// <summary>The outcome of resolving one tag key's value domain.</summary>
    public sealed record DomainResult(IReadOnlyList<string> Values, bool Ambiguous, string? Note);

    /// <summary>One row of the checked-in enrolment file.</summary>
    public sealed record EnrolmentRow(string Key, Enrolment Enrolment, string Detail);

    // A floor, not a pinned count: a pinned count coincides, and the number of
    // unparseable instruments and the number of genuinely tagless ones can move in
    // opposite directions and cancel. The floor exists only so the scan cannot go
    // vacuous and report green over nothing.
    private const int MinimumDeclarations = 300;

    // A floor on the named-blind-spot population, for the same reason as the floor above. The
    // measured value when this landed was 140 of the 212 rows that had previously claimed no
    // dimension; the floor sits well below that so ordinary churn does not trip it, and well
    // above zero so the marker silently ceasing to be emitted cannot read as green.
    private const int MinimumNamedBlindSpots = 100;

    private const string EnrolmentFileName = "InstrumentPrimingEnrolment.tsv";

    /// <summary>
    /// The literal that opens the header comment listing the enrolment vocabulary. Held here so
    /// the generator that writes the line and the gate that reads it back agree by construction.
    /// </summary>
    private const string VocabularyPrefix = "# enrolment:";

    private static readonly string[] FactoryNames =
    {
        "CreateCounter",
        "CreateUpDownCounter",
        "CreateHistogram",
        "CreateObservableGauge",
        "CreateObservableCounter",
        "CreateObservableUpDownCounter",
    };

    private static readonly Lazy<SourceCorpus> Corpus = new(SourceCorpus.Load);

    [Test]
    public void Instrument_declaration_scan_is_not_vacuous()
    {
        var declarations = Corpus.Value.Declarations;

        Assert.That(
            declarations,
            Is.Not.Empty,
            "The instrument declaration scan matched nothing. A repository-wide gate that "
            + "silently matches nothing reports green over an unexamined repository, which is "
            + "worse than having no gate at all.");

        Assert.That(
            declarations.Count,
            Is.GreaterThanOrEqualTo(MinimumDeclarations),
            $"Only {declarations.Count} instrument declarations were found, below the floor of "
            + $"{MinimumDeclarations}. Either the scan has been narrowed by a change to the "
            + "factory-name list or the declaration shapes, or instruments were removed "
            + "wholesale. A parse that silently narrows returns a smaller clean number rather "
            + "than an error, so this floor is the only thing that separates the two.");

        Assert.That(
            Corpus.Value.Files.Count,
            Is.GreaterThan(0),
            "No source files were enumerated under src/.");
    }

    [Test]
    public void Every_instrument_declaration_carries_exactly_one_enrolment()
    {
        var declarations = Corpus.Value.Declarations;
        var rows = ReadEnrolmentFile(out var path);

        MaybeRewrite(declarations, path);

        var declaredKeys = rows.Select(r => r.Key).ToList();
        var duplicates = declaredKeys
            .GroupBy(k => k, StringComparer.Ordinal)
            .Where(g => g.Count() > 1)
            .Select(g => g.Key)
            .ToList();

        Assert.That(
            duplicates,
            Is.Empty,
            $"The enrolment file lists these keys more than once, so the accounting is not "
            + $"one-to-one:{Environment.NewLine}  {string.Join($"{Environment.NewLine}  ", duplicates)}");

        var sourceKeys = declarations.Select(d => d.Key).ToHashSet(StringComparer.Ordinal);
        var fileKeys = declaredKeys.ToHashSet(StringComparer.Ordinal);

        var unaccounted = sourceKeys.Except(fileKeys).OrderBy(k => k, StringComparer.Ordinal).ToList();
        var stale = fileKeys.Except(sourceKeys).OrderBy(k => k, StringComparer.Ordinal).ToList();

        Assert.Multiple(() =>
        {
            Assert.That(
                unaccounted,
                Is.Empty,
                $"{unaccounted.Count} instrument declaration(s) exist in source with no enrolment. "
                + "An instrument that is in neither category is invisible to this gate, which is "
                + "the condition the gate exists to prevent. Add a row to "
                + $"{EnrolmentFileName} choosing primed, anchored, unresolved or none:"
                + $"{Environment.NewLine}  {string.Join($"{Environment.NewLine}  ", unaccounted.Take(40))}");

            Assert.That(
                stale,
                Is.Empty,
                $"{stale.Count} enrolment row(s) name a declaration that no longer exists. A "
                + "registry that is trusted rather than validated rots into a record of what "
                + "used to be true:"
                + $"{Environment.NewLine}  {string.Join($"{Environment.NewLine}  ", stale.Take(40))}");
        });
    }

    [Test]
    public void No_bounded_dimension_claims_are_not_contradicted_by_the_parser()
    {
        var byKey = Corpus.Value.Declarations.ToDictionary(d => d.Key, StringComparer.Ordinal);
        var rows = ReadEnrolmentFile(out _);

        var contradicted = new List<string>();
        foreach (var row in rows.Where(r => r.Enrolment == Enrolment.None))
        {
            if (!byKey.TryGetValue(row.Key, out var declaration))
            {
                continue;
            }

            // A single resolved value contradicts the claim just as a multi-valued domain
            // does. None asserts that the instrument carries NO bounded tag dimension, and a
            // tag key that resolved to exactly one literal demonstrably has one. Treating
            // Count == 1 as "not bounded" was a live defect in this gate: the emission site
            // for LatticeReplicationMetrics.ShipDuration tags outcome from a local that is
            // initialised to "error" and reassigned to "ok" on the success path, and the
            // resolver reads initialisers but not reassignment, so the domain came back as
            // the single value ['error']. Classified None, that read as "no taxonomy here"
            // rather than as "a taxonomy I failed to read" - a parse failure laundered into
            // a negative claim, which is the precise failure Unresolved exists to prevent.
            // Any dimension the parser found contradicts the claim, resolved or not. None
            // asserts that the instrument carries NO bounded tag dimension; an ambiguous or
            // unreadable dimension means a dimension demonstrably exists and could not be
            // read, which is Unresolved's claim, not None's. Counting only fully-resolved
            // dimensions here would let every parse failure settle into a negative claim.
            var resolved = declaration.Dimensions
                .Where(d => d.Value.Ambiguous || d.Value.Values.Count > 0)
                .ToList();

            if (resolved.Count > 0)
            {
                var detail = string.Join(
                    ", ",
                    resolved.Select(d => d.Value.Ambiguous
                        ? $"{d.Key} -> unresolved ({d.Value.Note})"
                        : $"{d.Key} -> [{string.Join(", ", d.Value.Values)}]"));
                contradicted.Add($"{row.Key}: {detail}");
            }
        }

        Assert.That(
            contradicted,
            Is.Empty,
            $"{contradicted.Count} instrument(s) claim no bounded tag dimension, but the parser "
            + "resolved one. This is the negative claim doing its job: the claim is about content, "
            + "so it is contradictable, and a parser improvement surfaces here as a visible "
            + "contradiction of a checked-in claim rather than as a silent change of scope. "
            + "Re-enrol each as primed, anchored or unresolved:"
            + $"{Environment.NewLine}  {string.Join($"{Environment.NewLine}  ", contradicted.Take(40))}");
    }

    [Test]
    public void Primed_enrolments_emit_a_zero_sample_for_every_value()
    {
        var byKey = Corpus.Value.Declarations.ToDictionary(d => d.Key, StringComparer.Ordinal);
        var all = ReadEnrolmentFile(out _);
        var rows = all
            .Where(r => r.Enrolment is Enrolment.Primed or Enrolment.Anchored)
            .ToList();

        Assert.That(
            all.Count(r => r.Enrolment == Enrolment.Primed),
            Is.GreaterThan(0),
            "No instrument is enrolled as primed. A category nobody uses is as informative as one "
            + "everybody uses, and an empty primed set would mean this arm of the gate has never "
            + "been exercised against real data.");

        var failures = new List<string>();
        foreach (var row in rows)
        {
            if (!byKey.TryGetValue(row.Key, out var declaration))
            {
                continue;
            }

            // An instrument may carry several bounded dimensions with different remedies:
            // the worked example primes 'progress' and deliberately leaves 'cause' unprimed,
            // anchored by the primed total. So enrolment is per row, but this check is per
            // dimension, and only the dimension named by an anchored row is excused.
            var anchoredDimension = row.Enrolment == Enrolment.Anchored
                ? ExtractDetail(row.Detail, "dimension")
                : null;

            var primedValues = Corpus.Value.ZeroEmittedValues(declaration.Owner);
            foreach (var (tag, domain) in declaration.Dimensions)
            {
                if (domain.Ambiguous || domain.Values.Count <= 1)
                {
                    continue;
                }

                if (string.Equals(tag, anchoredDimension, StringComparison.Ordinal))
                {
                    continue;
                }

                primedValues.TryGetValue(tag, out var covered);
                covered ??= new HashSet<string>(StringComparer.Ordinal);

                var missing = domain.Values.Where(v => !covered.Contains(v)).ToList();
                if (missing.Count > 0)
                {
                    failures.Add(
                        $"{row.Key} [{tag}] never emits a zero sample for: {string.Join(", ", missing)}");
                }
            }
        }

        Assert.That(
            failures,
            Is.Empty,
            $"{failures.Count} enrolment(s) do not prime every value of a bounded dimension that "
            + "is not excused by an anchor. An arm with no zero sample is absent until it first "
            + "fires, and an absent series is indistinguishable from a measured zero:"
            + $"{Environment.NewLine}  {string.Join($"{Environment.NewLine}  ", failures)}");
    }

    [Test]
    public void Anchored_enrolments_name_a_primed_total_and_an_existing_assertion()
    {
        var byKey = Corpus.Value.Declarations.ToDictionary(d => d.Key, StringComparer.Ordinal);
        var rows = ReadEnrolmentFile(out _).Where(r => r.Enrolment == Enrolment.Anchored).ToList();

        var failures = new List<string>();
        foreach (var row in rows)
        {
            var dimension = ExtractDetail(row.Detail, "dimension");
            var total = ExtractDetail(row.Detail, "total");
            var assertedBy = ExtractDetail(row.Detail, "assertedBy");

            if (string.IsNullOrWhiteSpace(dimension)
                || string.IsNullOrWhiteSpace(total)
                || string.IsNullOrWhiteSpace(assertedBy))
            {
                failures.Add(
                    $"{row.Key}: an anchored enrolment must carry dimension=<tag>, total=<value> "
                    + "and assertedBy=<test method>. Without all three it is an unfalsifiable "
                    + "promise that the arms add up.");
                continue;
            }

            if (!byKey.TryGetValue(row.Key, out var declaration))
            {
                continue;
            }

            if (!declaration.Dimensions.ContainsKey(dimension))
            {
                failures.Add(
                    $"{row.Key}: dimension={dimension} is not a tag this instrument emits, so the "
                    + "anchor excuses a dimension that does not exist.");
                continue;
            }

            // The total must itself be a primed value on this instrument: anchoring an unprimed
            // dimension to another unprimed one relocates the ambiguity rather than removing it.
            var primedValues = Corpus.Value.ZeroEmittedValues(declaration.Owner);
            var totalIsPrimed = primedValues.Values.Any(set => set.Contains(total));

            if (!totalIsPrimed)
            {
                failures.Add(
                    $"{row.Key}: total={total} is never emitted as a zero sample, so the anchor "
                    + "rests on a series that is itself absent until it first fires.");
            }

            if (!Corpus.Value.TestMethodNames.Contains(assertedBy))
            {
                failures.Add(
                    $"{row.Key}: assertedBy={assertedBy} names no test method anywhere under "
                    + "test/. The tally must actually be asserted, not merely claimed.");
            }
        }

        Assert.That(
            failures,
            Is.Empty,
            $"{failures.Count} anchored enrolment(s) are not backed by a primed total and an "
            + $"existing assertion:{Environment.NewLine}  "
            + string.Join($"{Environment.NewLine}  ", failures));
    }

    [Test]
    public void Documented_enrolment_vocabulary_matches_the_enum()
    {
        ReadEnrolmentFile(out var path);
        Assert.That(
            File.Exists(path),
            Is.True,
            $"{EnrolmentFileName} is missing, so the documented vocabulary cannot be compared "
                + "to the enum at all. This assertion exists so an absent file fails rather "
                + "than silently producing an empty comparison that passes.");

        var headerLines = File
            .ReadAllLines(path)
            .Select(static line => line.Trim())
            .Where(static line => line.StartsWith(VocabularyPrefix, StringComparison.Ordinal))
            .ToList();

        // Exactly one, not at least one: two header lines would let a stale copy sit above or
        // below a corrected one with the gate reading whichever it happened to find first.
        Assert.That(
            headerLines,
            Has.Count.EqualTo(1),
            $"{EnrolmentFileName} must carry exactly one '{VocabularyPrefix}' header line; "
                + $"found {headerLines.Count}. A parse that matches nothing must fail rather "
                + "than pass, because a comment-parsing assertion that silently matches zero "
                + "states is the identical defect one level up.");

        var documented = headerLines[0][VocabularyPrefix.Length..]
            .Split('|', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            .ToList();

        var declared = Enum.GetValues<Enrolment>().Select(Render).ToList();

        Assert.Multiple(() =>
        {
            Assert.That(
                documented,
                Is.Not.Empty,
                $"The '{VocabularyPrefix}' header in {EnrolmentFileName} parsed to zero states. "
                    + "The separator or the prefix has changed and this gate is now reading "
                    + "nothing, which would otherwise compare empty against empty and pass.");

            Assert.That(
                declared,
                Is.Not.Empty,
                "Enum.GetValues<Enrolment>() yielded nothing, so the computed side of this "
                    + "comparison is empty and every documented state would read as unknown.");

            // Render's default arm maps every unhandled member onto "unresolved". A sixth state
            // added without its own arm would therefore render as an EXISTING token: it would
            // not fail the set comparison below, it would vanish into an entry that is already
            // there and the two sides would still match. This is the arity mismatch hiding
            // inside the remedy for the arity mismatch, so it is asserted separately.
            Assert.That(
                declared.Distinct(StringComparer.Ordinal).Count(),
                Is.EqualTo(declared.Count),
                $"Render is not injective over Enrolment: {declared.Count} members render to "
                    + $"{declared.Distinct(StringComparer.Ordinal).Count()} distinct tokens "
                    + $"([{string.Join(", ", declared)}]). A member with no Render arm falls "
                    + "through to the default and aliases onto an existing token, so it would "
                    + "pass the vocabulary comparison while being undocumented in practice. "
                    + "Give every member its own arm.");

            var undocumented = declared.Except(documented, StringComparer.Ordinal).ToList();
            Assert.That(
                undocumented,
                Is.Empty,
                $"Enrolment states exist that {EnrolmentFileName} does not document: "
                    + $"{string.Join(", ", undocumented)}. A contributor enrolling a row into "
                    + "one of these is writing a state the file's own header says does not "
                    + "exist. Regenerate the file so the header is derived from the enum.");

            var unknown = documented.Except(declared, StringComparer.Ordinal).ToList();
            Assert.That(
                unknown,
                Is.Empty,
                $"{EnrolmentFileName} documents states the enum does not declare: "
                    + $"{string.Join(", ", unknown)}. A documented state with no member cannot "
                    + "be parsed out of a row, so a row claiming it is skipped silently by "
                    + "ReadEnrolmentFile rather than rejected.");
        });
    }

    [Test]
    public void Unprimed_records_are_still_unprimed_and_carry_a_tracking_reference()
    {
        var byKey = Corpus.Value.Declarations.ToDictionary(d => d.Key, StringComparer.Ordinal);
        var rows = ReadEnrolmentFile(out _).Where(r => r.Enrolment == Enrolment.Unprimed).ToList();

        // This row set is legitimately empty today, and empty is the DESIRED state: it means
        // nothing sits in the known-broken-but-tracked bucket. So this test must not demand a
        // row exist - a gate that only passes while a defect is recorded is worse than one that
        // is quiet. What it must do instead is distinguish "absent because none exist" from
        // "absent because the filter stopped working", which is the priming doctrine applied to
        // a fixture rather than to an instrument: assert the detector, not the population.
        Assert.That(
            Enum.TryParse<Enrolment>(Render(Enrolment.Unprimed), ignoreCase: true, out var probe)
                && probe == Enrolment.Unprimed,
            Is.True,
            "The 'unprimed' token no longer round-trips from Render back through the parse that "
                + "ReadEnrolmentFile uses, so every unprimed row in the file would be skipped "
                + "silently and the filter below would report zero for a reason unrelated to "
                + "the rows. Zero unprimed rows is a good state; an unreadable one is not, and "
                + "without this assertion the two are the same observation.");

        var failures = new List<string>();
        foreach (var row in rows)
        {
            if (string.IsNullOrWhiteSpace(ExtractDetail(row.Detail, "tracked")))
            {
                failures.Add(
                    $"{row.Key}: an unprimed record must carry tracked=<issue>. A recorded "
                    + "violation with no owner is a waiver wearing a finding's clothes.");
            }

            if (!byKey.TryGetValue(row.Key, out var declaration))
            {
                continue;
            }

            // The staleness half, and the reason this category is not an exemption in disguise:
            // it asserts the dimension IS currently unprimed, so priming it contradicts the row
            // and reddens the gate until the record is removed. A category that only ever
            // permits, and never expires, would report coverage it did not measure.
            var primedValues = Corpus.Value.ZeroEmittedValues(declaration.Owner);
            foreach (var (tag, domain) in declaration.Dimensions)
            {
                if (domain.Ambiguous || domain.Values.Count <= 1)
                {
                    continue;
                }

                primedValues.TryGetValue(tag, out var covered);
                if (covered is not null && domain.Values.All(covered.Contains))
                {
                    failures.Add(
                        $"{row.Key} [{tag}] is now fully primed, so this unprimed record is stale. "
                        + "Re-enrol it as primed and drop the tracking reference.");
                }
            }
        }

        Assert.That(
            failures,
            Is.Empty,
            $"{failures.Count} unprimed record(s) are not in the state they claim:"
            + $"{Environment.NewLine}  {string.Join($"{Environment.NewLine}  ", failures)}");
    }

    [Test]
    public void Unresolved_enrolments_state_a_reason_and_are_not_a_negative_claim()
    {
        var rows = ReadEnrolmentFile(out _).Where(r => r.Enrolment == Enrolment.Unresolved).ToList();

        var missing = rows
            .Where(r => string.IsNullOrWhiteSpace(ExtractDetail(r.Detail, "reason")))
            .Select(r => r.Key)
            .ToList();

        Assert.That(
            missing,
            Is.Empty,
            $"{missing.Count} unresolved enrolment(s) carry no reason=. The whole point of this "
            + "category is that the parser's blind spot stays named and counted rather than being "
            + $"absorbed into a claim of no bounded dimension:{Environment.NewLine}  "
            + string.Join($"{Environment.NewLine}  ", missing));
    }

    [Test]
    public void Resolution_of_a_name_declared_in_two_files_is_ambiguous_not_unioned()
    {
        // Known-positive control. A detector that has not been shown to fire is not
        // evidence, and this particular failure is the one that produced a wrong answer
        // during development: a helper name declared in more than one file, looked up by
        // bare name across the repository, unioned two unrelated taxonomies into a
        // plausible superset. Note the caller here does NOT declare the helper itself,
        // because file-local resolution legitimately disambiguates that case; the residual
        // ambiguity is a name the caller's own file does not declare.
        var corpus = SourceCorpus.ForTesting(new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["src/probe/Caller.cs"] = """
                internal static class Caller
                {
                    private const string StateTagKey = "state";
                    private static readonly Counter<long> _probe = Meter.CreateCounter<long>("probe.a");
                    public void Emit(int s) => _probe.Add(1, new KeyValuePair<string, object?>(StateTagKey, DescribeState(s)));
                }
                """,
            ["src/probe/A.cs"] = """
                internal static class A
                {
                    private static string DescribeState(int s) => s switch { 0 => "alpha", _ => "beta" };
                }
                """,
            ["src/probe/B.cs"] = """
                internal static class B
                {
                    private static string DescribeState(int s) => s switch { 0 => "gamma", _ => "delta" };
                }
                """,
        });

        var probe = corpus.Declarations.Single(d => d.Owner == "_probe");
        var domain = probe.Dimensions["state"];

        Assert.Multiple(() =>
        {
            Assert.That(
                domain.Ambiguous,
                Is.True,
                "The control did not fire. DescribeState is declared in two probe files and in "
                + "neither is it local to the caller, so its domain must resolve as ambiguous.");

            Assert.That(
                domain.Values,
                Does.Not.Contain("gamma"),
                "The resolver unioned a same-named helper from another file. A union on ambiguity "
                + "produces a superset that reads as a richer answer while being unsound.");
        });

        // The negative half of the control: an unambiguous name still resolves, so the
        // ambiguity rule has not simply disabled resolution altogether. A control that can
        // only fire one way does not distinguish a working detector from a stuck one.
        var single = SourceCorpus.ForTesting(new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["src/probe/Caller.cs"] = """
                internal static class Caller
                {
                    private const string StateTagKey = "state";
                    private static readonly Counter<long> _probe = Meter.CreateCounter<long>("probe.a");
                    private static string DescribeState(int s) => s switch { 0 => "alpha", _ => "beta" };
                    public void Emit(int s) => _probe.Add(1, new KeyValuePair<string, object?>(StateTagKey, DescribeState(s)));
                }
                """,
        });

        var resolved = single.Declarations.Single(d => d.Owner == "_probe").Dimensions["state"];

        Assert.Multiple(() =>
        {
            Assert.That(resolved.Ambiguous, Is.False, "An unambiguous helper must still resolve.");
            Assert.That(resolved.Values, Is.EquivalentTo(new[] { "alpha", "beta" }));
        });
    }

    [Test]
    public void Tag_arguments_that_are_not_literal_pairs_are_named_rather_than_dropped()
    {
        // Known-positive control for issue #2968. This parser reads a tag only from a literal
        // `new KeyValuePair<string, object?>(...)` at the emission site. An emission whose tags
        // are PRE-BUILT KeyValuePair VARIABLES matched nothing, produced no dimensions at all,
        // and therefore seeded the enrolment `none` - a positive claim that the instrument
        // carries no bounded taxonomy, manufactured out of the parser's own blindness.
        //
        // This is a DIFFERENT hole from the unresolved-key path: that path needs a literal to
        // have been found before it can report that the literal's key would not resolve. With
        // no literal there is no pair whose key failed, so neither it nor the
        // None-contradiction check that consumes it had anything to fire on.
        var blind = SourceCorpus.ForTesting(new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["src/probe/Caller.cs"] = """
                internal static class Caller
                {
                    private static readonly Counter<long> _probe = Meter.CreateCounter<long>("probe.a");
                    public void Emit(long delta, KeyValuePair<string, object?> outcome)
                        => _probe.Add(delta, treeTag, outcome, tenantTag);
                }
                """,
        });

        var unread = blind.Declarations.Single(d => d.Owner == "_probe").Dimensions
            .Where(d => d.Key.StartsWith("(unread-tag-argument:", StringComparison.Ordinal))
            .ToList();

        Assert.Multiple(() =>
        {
            Assert.That(
                unread,
                Is.Not.Empty,
                "The control did not fire. Tag arguments that are not literal KeyValuePair "
                + "constructions must surface as a named blind spot. Dropping them leaves the "
                + "declaration with no dimensions, which seeds `none` - a parse failure "
                + "laundered into a measured absence, inside the gate that exists to prevent "
                + "exactly that.");

            Assert.That(
                unread.All(d => d.Value.Ambiguous),
                Is.True,
                "An unread tag argument must be marked ambiguous. An unread domain recorded as "
                + "resolved-and-empty is the same collapse in a new field.");
        });

        // The negative half of the control, and the half that makes the positive half mean
        // anything: a detector that fires on every emission is indistinguishable from one
        // stuck on. Fully literal tags must produce NO marker, and must still resolve.
        var readable = SourceCorpus.ForTesting(new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["src/probe/Caller.cs"] = """
                internal static class Caller
                {
                    private static readonly Counter<long> _probe = Meter.CreateCounter<long>("probe.a");
                    public void EmitAlpha() => _probe.Add(1, new KeyValuePair<string, object?>("state", "alpha"));
                    public void EmitBeta() => _probe.Add(1, new KeyValuePair<string, object?>("state", "beta"));
                }
                """,
        });

        var readableDimensions = readable.Declarations.Single(d => d.Owner == "_probe").Dimensions;

        Assert.Multiple(() =>
        {
            Assert.That(
                readableDimensions.Keys.Where(k => k.StartsWith("(unread-tag-argument:", StringComparison.Ordinal)),
                Is.Empty,
                "The detector fired on tags it CAN read, so it does not distinguish a blind spot "
                + "from a successful parse and its output carries no information.");

            // Guards the residue analysis against a specific regression. SplitArguments is not
            // generic-aware, so it splits `KeyValuePair<string, object?>` at the comma inside
            // the type argument list; the callers rejoin the fragments with a comma before
            // matching, and that rejoin is load-bearing. Analysing the arguments individually
            // instead - which reads as the obvious simplification - makes the literal
            // unmatchable, and every readable tag in the repository silently becomes a blind
            // spot. That inverts this fix while still looking like it is working.
            Assert.That(
                readableDimensions.ContainsKey("state"),
                Is.True,
                "A literal tag pair no longer resolves. The residue analysis must remove the "
                + "spans it consumed from the REJOINED argument text, not inspect arguments "
                + "one at a time.");

            Assert.That(readableDimensions["state"].Values, Is.EquivalentTo(new[] { "alpha", "beta" }));
        });
    }

    [Test]
    public void An_observation_callback_the_parser_cannot_read_is_never_recorded_as_an_absent_dimension()
    {
        // Known-positive control for issue #3318, and a THIRD blind spot distinct from the two
        // the control above covers. Both of those need an `owner.Add(...)` site to exist before
        // they have anything to report on: one names a tag argument that is not a literal pair,
        // the other names a literal whose key would not resolve. An OBSERVABLE instrument has
        // no Add() site at all. Its tag surface lives inside the measurement callback handed to
        // the factory, and that callback body routinely sits in a DIFFERENT FILE from the
        // declaration - so the scan matched nothing, produced no dimensions, and the generator
        // wrote `none`. `none` is not a placeholder: it is an affirmative claim that the
        // instrument carries no bounded tag dimension, and a satisfied claim SUPPRESSES the
        // contradiction check that would otherwise guard the tag surface. The parser was
        // manufacturing the evidence that silenced the gate.
        var across = SourceCorpus.ForTesting(new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["src/probe/Metrics.cs"] = """
                internal static class Metrics
                {
                    public static readonly ObservableGauge<long> Depth = Meter.CreateObservableGauge(
                        "probe.depth",
                        static () => Sampler.ObserveDepth(),
                        unit: "1",
                        description: "Depth per lane.");
                }
                """,
            ["src/probe/Sampler.cs"] = """
                internal static class Sampler
                {
                    public static IEnumerable<Measurement<long>> ObserveDepth()
                    {
                        yield return new Measurement<long>(1, new KeyValuePair<string, object?>("lane", "read"));
                        yield return new Measurement<long>(2, new KeyValuePair<string, object?>("lane", "write"));
                    }
                }
                """,
        });

        var resolved = across.Declarations.Single(d => d.Owner == "Depth").Dimensions;

        Assert.Multiple(() =>
        {
            Assert.That(
                resolved.ContainsKey("lane"),
                Is.True,
                "A callback defined in another file was not followed, so the gauge's tag key was "
                + "never named. Leaving the declaration with no dimensions is what seeds `none` - "
                + "a parse failure laundered into a measured absence.");

            Assert.That(
                resolved["lane"].Values,
                Is.EquivalentTo(new[] { "read", "write" }),
                "The callback was reached but its literal tag values were not read.");

            // The half that keeps the recovery honest. Following ONE callback path with a
            // single forwarder hop is a LOWER BOUND on the tag surface, never a closed set: a
            // second contributing helper, or a value yielded only under a runtime condition,
            // is invisible here. Reporting it as bounded would turn a partial read into an
            // affirmative claim of completeness - the same false positive this test exists to
            // remove, relocated one field to the left.
            Assert.That(
                resolved["lane"].Ambiguous,
                Is.True,
                "A domain recovered from an observation callback was marked bounded. A partial "
                + "read presented as a closed domain is the identical false positive this gate "
                + "was added to prevent.");
        });

        // A callback whose target is nowhere in the corpus. The parser must say it cannot see,
        // not that there is nothing there.
        var orphan = SourceCorpus.ForTesting(new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["src/probe/Orphan.cs"] = """
                internal static class Orphan
                {
                    public static readonly ObservableGauge<long> Stalls = Meter.CreateObservableGauge(
                        "probe.stalls",
                        static () => Nowhere.ObserveStalls(),
                        unit: "s",
                        description: "Stall age.");
                }
                """,
        });

        var blind = orphan.Declarations.Single(d => d.Owner == "Stalls").Dimensions
            .Where(d => d.Key.StartsWith(SourceCorpus.ObservableBlindSpotPrefix, StringComparison.Ordinal))
            .ToList();

        Assert.Multiple(() =>
        {
            Assert.That(
                blind,
                Is.Not.Empty,
                "An unreachable observation callback produced no dimension at all, so the "
                + "declaration seeds `none` - the parser asserting an absence it never "
                + "established. An instrument the parser cannot resolve must land `unresolved`.");

            Assert.That(
                blind.All(d => d.Value.Ambiguous),
                Is.True,
                "The blind spot must be ambiguous. Recorded as resolved-and-empty it is a "
                + "negative claim again, wearing a different name.");
        });

        // The negative half, and the half that makes every assertion above mean anything: a
        // detector that fires on every observable is indistinguishable from one stuck on, and
        // would collapse `none` and `unresolved` into one value just as surely as the defect
        // did. A callback the parser CAN read all the way to a genuinely tagless measurement
        // must produce no dimension and no marker, leaving `none` legitimately claimable.
        var tagless = SourceCorpus.ForTesting(new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["src/probe/Plain.cs"] = """
                internal static class Plain
                {
                    public static readonly ObservableGauge<int> Count = Meter.CreateObservableGauge(
                        "probe.count",
                        static () => new Measurement<int>(0),
                        unit: "1",
                        description: "Untagged count.");
                }
                """,
        });

        Assert.That(
            tagless.Declarations.Single(d => d.Owner == "Count").Dimensions,
            Is.Empty,
            "The detector fired on a callback it CAN read to a tagless measurement, so it does "
            + "not distinguish a blind spot from a successful parse and its output carries no "
            + "information. `none` must remain claimable for an instrument that genuinely "
            + "carries no tags.");
    }

    [Test]
    public void Observable_instruments_in_the_repository_do_not_claim_an_absence_the_parser_never_established()
    {
        // The production half of issue #3318, run against the real corpus rather than a
        // synthetic one. The synthetic control above proves the MECHANISM; this proves it is
        // wired to the instruments that actually ship - a gate verified only against its own
        // helper is the vacuous shape this issue is about.
        var byKey = Corpus.Value.Declarations.ToDictionary(d => d.Key, StringComparer.Ordinal);
        var rows = ReadEnrolmentFile(out _);

        var laundered = rows
            .Where(r => r.Enrolment == Enrolment.None)
            .Where(r => byKey.TryGetValue(r.Key, out var d)
                && d.Dimensions.Keys.Any(k =>
                    k.StartsWith(SourceCorpus.ObservableBlindSpotPrefix, StringComparison.Ordinal)))
            .Select(r => r.Key)
            .ToList();

        // The issue's own reproduction, asserted by name. `orleans.lattice.materialiser.pin
        // .shed_stall_seconds` is declared in LatticeMetrics.cs while its tags are built in
        // WalMaterialiserPinPressure.ObserveShedStalls, in another file - and it seeded `none`
        // despite carrying a bounded pin_shard dimension. Pinning the key here is deliberate:
        // a floor over the aggregate population would stay green if the cross-file resolution
        // regressed to a blind spot, because the marker count would simply rise.
        const string ReproKey = "src/lattice/LatticeMetrics.cs#MaterialiserPinShedStallSeconds";
        var repro = byKey.TryGetValue(ReproKey, out var declaration)
            ? declaration.Dimensions
            : new Dictionary<string, DomainResult>(StringComparer.Ordinal);

        // A floor over the parser's LIVE output, not over rows of the checked-in file: the
        // file does not change when the resolver breaks, so a floor over rows would stay green
        // through exactly the regression it is meant to witness.
        var resolvedObservables = Corpus.Value.Declarations.Count(d =>
            d.Kind.StartsWith("CreateObservable", StringComparison.Ordinal)
            && d.Dimensions.Count > 0
            && !d.Dimensions.Keys.Any(k =>
                k.StartsWith(SourceCorpus.ObservableBlindSpotPrefix, StringComparison.Ordinal)));

        Assert.Multiple(() =>
        {
            Assert.That(
                laundered,
                Is.Empty,
                $"{laundered.Count} observable instrument(s) are enrolled as carrying no bounded "
                + "tag dimension while the parser is telling you it could not read their "
                + "observation callback. `none` is a claim about the instrument's CONTENT; a "
                + "callback the parser cannot reach is a fact about its REACH. Re-enrol each as "
                + $"unresolved:{Environment.NewLine}  "
                + string.Join($"{Environment.NewLine}  ", laundered.Take(20)));

            Assert.That(
                repro.ContainsKey("pin_shard"),
                Is.True,
                "The reproduction from issue #3318 no longer resolves. "
                + $"`{ReproKey}` declares a gauge whose tags are built in "
                + "WalMaterialiserPinPressure.ObserveShedStalls, in another file; its bounded "
                + "pin_shard dimension must be recovered across that file boundary, or the row "
                + "falls back to claiming an absence nobody established. Dimensions seen: "
                + $"[{string.Join(", ", repro.Keys)}].");

            Assert.That(
                repro.ContainsKey("tree"),
                Is.True,
                $"`{ReproKey}` also carries a `tree` tag built in the same cross-file callback. "
                + $"Dimensions seen: [{string.Join(", ", repro.Keys)}].");

            Assert.That(
                resolvedObservables,
                Is.GreaterThan(0),
                "Not one observable instrument in the repository had its callback resolved. The "
                + "cross-file resolution has stopped working and every observable is now a named "
                + "blind spot - which passes the clause above vacuously, because a declaration "
                + "that is all marker can never be caught claiming `none`.");
        });
    }

    [Test]
    public void An_unread_tag_domain_is_never_recorded_as_an_absent_one()
    {
        // The audit issue #2968 asked for, expressed as an assertion rather than as a number
        // in a report: of the instruments the enrolment file records as carrying no bounded
        // tag dimension, how many are saying "there is nothing to find" and how many were
        // saying "I found nothing"? Before this fix the two shared the value `none`, so the
        // split was not derivable from the output at all - which is the defect, not merely a
        // consequence of it.
        var byKey = Corpus.Value.Declarations.ToDictionary(d => d.Key, StringComparer.Ordinal);
        var rows = ReadEnrolmentFile(out _);

        var laundered = new List<string>();
        foreach (var row in rows.Where(r => r.Enrolment == Enrolment.None))
        {
            if (!byKey.TryGetValue(row.Key, out var declaration))
            {
                continue;
            }

            var unread = declaration.Dimensions
                .Where(d => d.Key.StartsWith("(unread-tag-argument:", StringComparison.Ordinal))
                .ToList();

            if (unread.Count > 0)
            {
                laundered.Add($"{row.Key}: {unread[0].Value.Note}");
            }
        }

        // A floor, not a pinned count, for the reason given at MinimumDeclarations: the two
        // populations move independently and a pinned total lets them cancel. The floor is
        // what stops this test passing because the marker stopped being produced - in which
        // case `laundered` is empty for the wrong reason and every assertion above it is
        // vacuously true.
        //
        // It counts the PARSER's live output, not rows of the checked-in file. Counting rows
        // was the first form of this clause and it was decorative: the file does not change
        // when the detector breaks, so disabling the detector left the clause green and only
        // the synthetic control caught it. A floor over an artefact cannot witness a
        // regression in the machinery that produced the artefact.
        var named = Corpus.Value.Declarations.Count(d =>
            d.Dimensions.Keys.Any(k => k.StartsWith("(unread-tag-argument:", StringComparison.Ordinal)));

        Assert.Multiple(() =>
        {
            Assert.That(
                laundered,
                Is.Empty,
                $"{laundered.Count} instrument(s) are enrolled as carrying no bounded tag "
                + "dimension while the parser can see tag text it never read. `none` is a claim "
                + "about the instrument's CONTENT; this is a claim about the parser's REACH. "
                + "Re-enrol each as unresolved:"
                + $"{Environment.NewLine}  {string.Join($"{Environment.NewLine}  ", laundered.Take(20))}");

            Assert.That(
                named,
                Is.GreaterThanOrEqualTo(MinimumNamedBlindSpots),
                $"Only {named} instrument declaration(s) carry a named unread tag argument, below "
                + $"the floor of {MinimumNamedBlindSpots}. The blind spot is not hypothetical - it "
                + "covered 140 of the 212 rows that previously claimed no dimension - so a count "
                + "near zero means the marker stopped being emitted and every `none` row is once "
                + "again unfalsifiable, not that the repository got tidier.");

            Assert.That(
                rows.Count(r => r.Enrolment == Enrolment.None),
                Is.GreaterThan(0),
                "No row claims `none` any more. The point of naming the blind spot is to make "
                + "the two states DISTINGUISHABLE; collapsing every row into `unresolved` "
                + "destroys the distinction just as surely as collapsing them into `none` did, "
                + "and leaves nothing falsifiable.");
        });
    }

    [Test]
    public void Source_derived_declarations_agree_with_live_reflection_for_the_core_package()
    {
        // The runtime cross-check. A parser can only be validated against a different kind
        // of evidence, never against a threshold on its own output: a parse that silently
        // narrows returns a smaller clean number that clears every floor anyone would think
        // to write. Reflection over the compiled assembly is that different kind.
        //
        // It compares instrument NAMES rather than field names, and that detail is the whole
        // point rather than an implementation convenience. Several fields in this assembly are
        // aliases - `private static readonly Histogram<long> ApplyLag = LatticeMetrics.ViewApplyLag;`
        // - which are instrument-typed references, not declarations. Comparing field names
        // makes every alias look like a declaration the parser missed, so the check would fail
        // for a reason that has nothing to do with the parser. Comparing names collapses each
        // alias onto the instrument it refers to, and additionally validates that the parser
        // resolved the name expression correctly, which a field-name comparison never touches.
        var assembly = typeof(LatticeMetrics).Assembly;
        var reflected = new HashSet<string>(StringComparer.Ordinal);

        foreach (var type in assembly.GetTypes())
        {
            foreach (var field in type.GetFields(
                BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Static))
            {
                if (!typeof(System.Diagnostics.Metrics.Instrument).IsAssignableFrom(field.FieldType))
                {
                    continue;
                }

                try
                {
                    if (field.GetValue(null) is System.Diagnostics.Metrics.Instrument instrument)
                    {
                        reflected.Add(instrument.Name);
                    }
                }
                catch (Exception)
                {
                    // A field whose static initialiser needs a host is not evidence either way.
                }
            }
        }

        Assert.That(
            reflected,
            Is.Not.Empty,
            "Reflection found no instruments in the core assembly, so the cross-check would be "
            + "vacuous and would agree with any parse at all.");

        var parsed = Corpus.Value.Declarations
            .Where(d => d.RelativePath.StartsWith("src/lattice/", StringComparison.Ordinal))
            .Select(d => d.InstrumentName)
            .Where(n => n is not null)
            .ToHashSet(StringComparer.Ordinal)!;

        var missed = reflected.Except(parsed).OrderBy(n => n, StringComparer.Ordinal).ToList();

        Assert.That(
            missed,
            Is.Empty,
            $"{missed.Count} instrument(s) are live in the compiled core assembly but were not "
            + "found by the source scan. The parse has narrowed against a declaration shape or a "
            + "name expression it does not recognise, and would have reported a smaller clean "
            + $"number rather than an error:{Environment.NewLine}  "
            + string.Join($"{Environment.NewLine}  ", missed.Take(40)));
    }

    private static string? ExtractDetail(string detail, string name)
    {
        foreach (var part in detail.Split(';', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
        {
            var eq = part.IndexOf('=');
            if (eq > 0 && part[..eq].Trim().Equals(name, StringComparison.Ordinal))
            {
                return part[(eq + 1)..].Trim();
            }
        }

        return null;
    }

    private static IReadOnlyList<EnrolmentRow> ReadEnrolmentFile(out string path)
    {
        path = Path.Combine(
            HygieneRepository.FindRepoRoot(), "test", "lattice", "Hygiene", EnrolmentFileName);

        if (!File.Exists(path))
        {
            return Array.Empty<EnrolmentRow>();
        }

        var rows = new List<EnrolmentRow>();
        foreach (var raw in File.ReadAllLines(path))
        {
            var line = raw.Trim();
            if (line.Length == 0 || line.StartsWith('#'))
            {
                continue;
            }

            var parts = line.Split('\t');
            if (parts.Length < 2)
            {
                continue;
            }

            if (!Enum.TryParse<Enrolment>(parts[1].Replace("-", string.Empty), ignoreCase: true, out var enrolment))
            {
                continue;
            }

            rows.Add(new EnrolmentRow(parts[0], enrolment, parts.Length > 2 ? parts[2] : string.Empty));
        }

        return rows;
    }

    private static void MaybeRewrite(IReadOnlyList<Declaration> declarations, string path)
    {
        if (Environment.GetEnvironmentVariable("LATTICE_REWRITE_PRIMING_ENROLMENT") != "1")
        {
            return;
        }

        var existing = ReadEnrolmentFile(out _).ToDictionary(r => r.Key, StringComparer.Ordinal);
        var builder = new StringBuilder();
        builder.AppendLine("# Instrument priming enrolment. One row per instrument declaration under src/.");
        builder.AppendLine("# key<TAB>enrolment<TAB>detail");
        builder.AppendLine(VocabularyHeaderLine);
        builder.AppendLine("# Regenerate with LATTICE_REWRITE_PRIMING_ENROLMENT=1; review every row it changes.");

        foreach (var declaration in declarations.OrderBy(d => d.Key, StringComparer.Ordinal))
        {
            // Preserve a curated row, EXCEPT a None row the parser now contradicts. None is a
            // claim that the instrument carries no tag dimension; once a dimension is visible
            // the row is a stale negative claim, and preserving it would let the regeneration
            // path quietly re-assert something the gate already knows to be false.
            var hasDimension = declaration.Dimensions.Count > 0;
            if (existing.TryGetValue(declaration.Key, out var row)
                && !(row.Enrolment == Enrolment.None && hasDimension))
            {
                builder.AppendLine($"{row.Key}\t{Render(row.Enrolment)}\t{row.Detail}");
                continue;
            }

            var bounded = declaration.Dimensions
                .Where(d => !d.Value.Ambiguous && d.Value.Values.Count > 1)
                .ToList();
            var single = declaration.Dimensions
                .Where(d => !d.Value.Ambiguous && d.Value.Values.Count == 1)
                .ToList();
            var ambiguous = declaration.Dimensions.Where(d => d.Value.Ambiguous).ToList();

            if (bounded.Count > 0)
            {
                builder.AppendLine(
                    $"{declaration.Key}\tunresolved\treason=bounded domain found, enrolment not yet chosen");
            }
            else if (single.Count > 0)
            {
                // A tag key resolving to exactly one literal is far likelier to be an
                // under-resolved dimension than a genuine constant, so it is seeded as a
                // named blind spot rather than as a negative claim.
                builder.AppendLine(
                    $"{declaration.Key}\tunresolved\treason=single-value domain {single[0].Key} -> "
                    + $"[{string.Join(", ", single[0].Value.Values)}]; likely under-resolved, not a constant");
            }
            else if (ambiguous.Count > 0)
            {
                builder.AppendLine(
                    $"{declaration.Key}\tunresolved\treason=dimension {ambiguous[0].Key}: "
                    + $"{ambiguous[0].Value.Note ?? "domain not resolvable"}");
            }
            else
            {
                builder.AppendLine($"{declaration.Key}\tnone\t");
            }
        }

        File.WriteAllText(path, builder.ToString());
    }

    private static string Render(Enrolment enrolment) => enrolment switch
    {
        Enrolment.None => "none",
        Enrolment.Primed => "primed",
        Enrolment.Anchored => "anchored",
        Enrolment.Unprimed => "unprimed",
        _ => "unresolved",
    };

    /// <summary>
    /// The enrolment vocabulary line as the file's header must state it, derived from
    /// <see cref="Enrolment"/> rather than hand-authored. It used to be a literal, which is how
    /// the file came to document four of the five states: the sentence and the type it described
    /// were maintained independently and nothing compared them. Deriving it means the generator
    /// is the only author, so the header cannot drift from the enum without the enum changing.
    /// </summary>
    private static string VocabularyHeaderLine =>
        VocabularyPrefix + " " + string.Join(" | ", Enum.GetValues<Enrolment>().Select(Render));

    /// <summary>The parsed source corpus and everything derived from it.</summary>
    public sealed class SourceCorpus
    {
        /// <summary>
        /// Prefix of the dimension key recorded when an observable instrument's measurement
        /// callback could not be reached. It marks an admission about the parser's REACH, and
        /// is deliberately shaped so the existing contradiction gate treats it as a dimension:
        /// a row carrying it can never be enrolled as a negative claim.
        /// </summary>
        public const string ObservableBlindSpotPrefix = "(unread-observable-callback:";

        private static readonly Regex ConstStringPattern = new(
            @"\bconst\s+string\s+(\w+)\s*=\s*""([^""]*)""", RegexOptions.Compiled);

        private static readonly Regex DescribeHelperPattern = new(
            @"\bstatic\s+string\s+(Describe\w+)\s*\([^)]*\)\s*=>\s*(\w+)\s+switch\s*\{",
            RegexOptions.Compiled);

        private static readonly Regex TestMethodPattern = new(
            @"public\s+(?:async\s+)?(?:void|Task)\s+(\w+)\s*\(", RegexOptions.Compiled);

        private static readonly Regex MeasurementConstructionPattern = new(
            @"new\s+Measurement\s*<[^<>]*>\s*\(", RegexOptions.Compiled);

        private readonly Dictionary<string, string> _files;
        private readonly Dictionary<string, List<(string File, List<string> Values)>> _describeHelpers = new(StringComparer.Ordinal);
        private readonly Dictionary<string, List<(string File, string Value)>> _consts = new(StringComparer.Ordinal);
        private readonly Dictionary<string, List<(string Path, string Body)>> _methodBodies = new(StringComparer.Ordinal);

        private SourceCorpus(Dictionary<string, string> files, IReadOnlyCollection<string> testMethodNames)
        {
            _files = files;
            TestMethodNames = testMethodNames.ToHashSet(StringComparer.Ordinal);

            foreach (var (path, text) in files)
            {
                foreach (Match m in ConstStringPattern.Matches(text))
                {
                    if (!_consts.TryGetValue(m.Groups[1].Value, out var list))
                    {
                        list = new List<(string, string)>();
                        _consts[m.Groups[1].Value] = list;
                    }

                    list.Add((path, m.Groups[2].Value));
                }

                foreach (Match m in DescribeHelperPattern.Matches(text))
                {
                    var values = ExtractSwitchArms(text, m.Index + m.Length - 1);
                    if (!_describeHelpers.TryGetValue(m.Groups[1].Value, out var list))
                    {
                        list = new List<(string, List<string>)>();
                        _describeHelpers[m.Groups[1].Value] = list;
                    }

                    list.Add((path, values));
                }
            }

            Declarations = BuildDeclarations();
        }

        /// <summary>Every source file scanned, keyed by repo-relative path.</summary>
        public IReadOnlyDictionary<string, string> Files => _files;

        /// <summary>Every instrument declaration discovered.</summary>
        public IReadOnlyList<Declaration> Declarations { get; }

        /// <summary>Every test method name found under test/, used to validate anchors.</summary>
        public HashSet<string> TestMethodNames { get; }

        /// <summary>Loads the real repository corpus.</summary>
        public static SourceCorpus Load()
        {
            var root = HygieneRepository.FindRepoRoot();
            var files = new Dictionary<string, string>(StringComparer.Ordinal);

            foreach (var file in HygieneRepository.EnumerateFiles(Path.Combine(root, "src"), "*.cs"))
            {
                files[Relative(root, file)] = File.ReadAllText(file);
            }

            var testMethods = new HashSet<string>(StringComparer.Ordinal);
            foreach (var file in HygieneRepository.EnumerateFiles(Path.Combine(root, "test"), "*.cs"))
            {
                foreach (Match m in TestMethodPattern.Matches(File.ReadAllText(file)))
                {
                    testMethods.Add(m.Groups[1].Value);
                }
            }

            return new SourceCorpus(files, testMethods);
        }

        /// <summary>Builds a corpus from in-memory sources, for the known-positive control.</summary>
        public static SourceCorpus ForTesting(Dictionary<string, string> files) =>
            new(files, Array.Empty<string>());

        /// <summary>Tag values for which <paramref name="owner"/> emits a zero sample.</summary>
        public Dictionary<string, HashSet<string>> ZeroEmittedValues(string owner)
        {
            var result = new Dictionary<string, HashSet<string>>(StringComparer.Ordinal);
            var pattern = new Regex(@"\b" + Regex.Escape(owner) + @"\s*\.\s*(?:Add|Record)\s*\(", RegexOptions.Compiled);

            foreach (var (path, text) in _files)
            {
                foreach (Match m in pattern.Matches(text))
                {
                    var args = SplitArguments(text, m.Index + m.Length);
                    if (args.Count == 0 || !IsZeroLiteral(args[0]))
                    {
                        continue;
                    }

                    foreach (var (key, value) in ExtractTagPairs(string.Join(",", args.Skip(1)), path))
                    {
                        if (value.Ambiguous || value.Values.Count != 1)
                        {
                            continue;
                        }

                        if (!result.TryGetValue(key, out var set))
                        {
                            set = new HashSet<string>(StringComparer.Ordinal);
                            result[key] = set;
                        }

                        set.Add(value.Values[0]);
                    }
                }
            }

            return result;
        }

        private static bool IsZeroLiteral(string argument)
        {
            var a = argument.Trim();
            return a is "0" or "0L" or "0d" or "0.0" or "0f";
        }

        private static string Relative(string root, string file) =>
            Path.GetRelativePath(root, file).Replace('\\', '/');

        private List<Declaration> BuildDeclarations()
        {
            var declarations = new List<Declaration>();
            var factoryPattern = new Regex(
                @"\b(" + string.Join("|", FactoryNames) + @")\s*(?:<[^>()]*>)?\s*\(", RegexOptions.Compiled);

            foreach (var (path, text) in _files)
            {
                foreach (Match m in factoryPattern.Matches(text))
                {
                    var arguments = SplitArguments(text, m.Index + m.Length);
                    var owner = FindOwner(text, m.Index)
                        ?? $"(unassigned:{DescribeInstrumentName(arguments)})";
                    var key = $"{path}#{owner}";

                    if (declarations.Any(d => string.Equals(d.Key, key, StringComparison.Ordinal)))
                    {
                        continue;
                    }

                    var dimensions = owner.StartsWith('(')
                        ? new Dictionary<string, DomainResult>(StringComparer.Ordinal)
                        : ResolveDimensions(owner);

                    // An observable instrument has no Add()/Record() site at all - its tag
                    // surface lives inside the measurement callback handed to the factory, and
                    // that callback body routinely sits in a different file from the
                    // declaration. ResolveDimensions scans only for owner.Add/Record, so before
                    // this the scan found nothing and the generator wrote that down as "no
                    // bounded tag dimension" - an affirmative negative claim manufactured out
                    // of the parser's own blindness, which then SUPPRESSED the contradiction
                    // check that would have guarded the tag surface. See issue #3318.
                    if (m.Groups[1].Value.StartsWith("CreateObservable", StringComparison.Ordinal))
                    {
                        MergeObservableSurface(dimensions, path, arguments);
                    }

                    var nameDomain = arguments.Count > 0
                        ? ResolveDomain(arguments[0], path)
                        : new DomainResult(Array.Empty<string>(), true, "no name argument");
                    var instrumentName = !nameDomain.Ambiguous && nameDomain.Values.Count == 1
                        ? nameDomain.Values[0]
                        : null;

                    declarations.Add(
                        new Declaration(key, path, owner, m.Groups[1].Value, instrumentName, dimensions));
                }
            }

            return declarations;
        }

        private static string? FindOwner(string text, int index)
        {
            // Walk back to the assignment that RECEIVES this factory call. Both real shapes
            // in this repository are covered: a static readonly field initialiser, and an
            // instance-field assignment inside a constructor.
            //
            // The check that the span between the '=' and the call contains no statement
            // boundary is load-bearing rather than defensive. Fifty observable gauges here
            // are registered inside a RegisterGauges() method with the return value
            // discarded, so they have no assignment at all; a nearest-'=' search attributes
            // each one to whatever unrelated assignment happens to precede it, silently
            // misattributing a real declaration to an existing owner and then deduplicating
            // it out of existence. That is a smaller clean number rather than an error,
            // which is the failure mode this whole fixture is built against.
            var start = Math.Max(0, index - 400);
            var window = text[start..index];
            var eq = window.LastIndexOf('=');
            if (eq < 0)
            {
                return null;
            }

            var between = window[(eq + 1)..];
            if (between.IndexOfAny(new[] { ';', '{', '}' }) >= 0)
            {
                return null;
            }

            var before = window[..eq].TrimEnd();
            if (before.EndsWith("=", StringComparison.Ordinal)
                || before.EndsWith("!", StringComparison.Ordinal)
                || before.EndsWith("<", StringComparison.Ordinal)
                || before.EndsWith(">", StringComparison.Ordinal))
            {
                return null;
            }

            var match = Regex.Match(before, @"(\w+)\s*$");
            return match.Success ? match.Groups[1].Value : null;
        }

        private static string DescribeInstrumentName(IReadOnlyList<string> arguments)
        {
            if (arguments.Count == 0)
            {
                return "unknown";
            }

            var first = arguments[0].Trim().TrimEnd('!');
            if (first.StartsWith('"') && first.EndsWith('"') && first.Count(c => c == '"') == 2)
            {
                return first[1..^1];
            }

            var identifier = Regex.Match(first, @"[\w\.]+$");
            return identifier.Success ? identifier.Value : "unknown";
        }

        private Dictionary<string, DomainResult> ResolveDimensions(string owner)
        {
            var dimensions = new Dictionary<string, DomainResult>(StringComparer.Ordinal);
            var pattern = new Regex(@"\b" + Regex.Escape(owner) + @"\s*\.\s*(?:Add|Record)\s*\(", RegexOptions.Compiled);

            foreach (var (path, text) in _files)
            {
                foreach (Match m in pattern.Matches(text))
                {
                    var args = SplitArguments(text, m.Index + m.Length);
                    if (args.Count <= 1 || IsZeroLiteral(args[0]))
                    {
                        continue;
                    }

                    foreach (var (key, domain) in ExtractTagPairs(string.Join(",", args.Skip(1)), path))
                    {
                        MergeDimension(dimensions, key, domain);
                    }
                }
            }

            return dimensions;
        }

        /// <summary>Unions <paramref name="domain"/> into the accumulated dimension set.</summary>
        private static void MergeDimension(
            Dictionary<string, DomainResult> dimensions, string key, DomainResult domain)
        {
            if (!dimensions.TryGetValue(key, out var existing))
            {
                dimensions[key] = domain;
                return;
            }

            // Unioning ACROSS EMISSION SITES of one instrument is correct and is
            // not an ambiguity: a bounded taxonomy is emitted exactly that way,
            // one arm per call site. This is a different operation from unioning
            // two same-named declarations in different files, which is unsound
            // and is refused in ResolveByName. Conflating the two marked every
            // genuinely bounded instrument here as ambiguous.
            var merged = existing.Values
                .Union(domain.Values, StringComparer.Ordinal)
                .OrderBy(v => v, StringComparer.Ordinal)
                .ToList();
            var ambiguous = existing.Ambiguous || domain.Ambiguous;
            dimensions[key] = new DomainResult(
                merged, ambiguous, ambiguous ? existing.Note ?? domain.Note : null);
        }

        /// <summary>
        /// Merges the tag dimensions of an observable instrument's measurement callback into
        /// <paramref name="dimensions"/>, or records a named blind spot when the callback's
        /// measurement construction cannot be reached.
        /// </summary>
        /// <param name="dimensions">The accumulated dimension set for the declaration.</param>
        /// <param name="path">Repo-relative path of the file holding the declaration.</param>
        /// <param name="arguments">The factory call's argument list.</param>
        private void MergeObservableSurface(
            Dictionary<string, DomainResult> dimensions, string path, IReadOnlyList<string> arguments)
        {
            // Argument 1 is the observation callback for every CreateObservable* overload the
            // corpus uses: (name, callback, unit, description).
            var callback = arguments.Count > 1 ? Collapse(arguments[1]) : string.Empty;

            if (callback.Length == 0)
            {
                MergeDimension(
                    dimensions,
                    $"{ObservableBlindSpotPrefix}no callback argument)",
                    new DomainResult(
                        Array.Empty<string>(),
                        true,
                        "the factory call has no observation callback argument, so the tag "
                        + "surface of this instrument was never reached"));
                return;
            }

            var blobs = ReadMeasurementBlobs(callback, path, depth: 0);

            if (blobs is null)
            {
                MergeDimension(
                    dimensions,
                    $"{ObservableBlindSpotPrefix}{Render(callback)})",
                    new DomainResult(
                        Array.Empty<string>(),
                        true,
                        "the tags of this observable instrument are built inside its measurement "
                        + "callback, whose body this parser could not reach, so its tag surface "
                        + "was never read. This is an admission of reach, never a claim that the "
                        + "instrument carries no bounded dimension"));
                return;
            }

            foreach (var (blob, blobPath) in blobs)
            {
                foreach (var (key, domain) in ExtractTagPairs(blob, blobPath))
                {
                    // A callback read is a LOWER BOUND on the tag surface, never a closed
                    // set: this parser follows one callback path with a single forwarder
                    // hop, so a second contributing helper - or a value the callback yields
                    // only under a runtime condition - is invisible to it. Reporting the
                    // recovered domain as bounded would turn a partial read into an
                    // affirmative claim of completeness, which is the same false positive
                    // this whole change exists to remove. So the tag key is NAMED (which is
                    // what falsifies a 'none' row) while the domain stays ambiguous (which
                    // is what keeps the downstream priming check from acting on a subset).
                    MergeDimension(
                        dimensions,
                        key,
                        new DomainResult(
                            domain.Values,
                            true,
                            domain.Values.Count > 0
                                ? "read from this observable instrument's measurement callback, "
                                    + $"which yields [{string.Join(", ", domain.Values)}]. A callback "
                                    + "read is a lower bound on the tag surface, not a closed domain"
                                : domain.Note
                                    ?? "read from this observable instrument's measurement callback, "
                                    + "whose domain did not resolve to literals"));
                }
            }
        }

        /// <summary>
        /// The tag-argument text of every measurement constructed by an observation callback,
        /// or null when the callback body could not be reached or held no measurement
        /// construction. An empty list is the honest "reached it, and it carries no tags".
        /// </summary>
        private List<(string Blob, string Path)>? ReadMeasurementBlobs(
            string expression, string path, int depth)
        {
            if (depth > 2)
            {
                return null;
            }

            var body = expression;
            var bodyPath = path;
            var arrow = body.IndexOf("=>", StringComparison.Ordinal);
            if (arrow >= 0)
            {
                body = body[(arrow + 2)..].Trim();
            }

            // A construction that WAS found but carried no tag arguments is the honest
            // "reached it, and it carries no tags" - the one reading that still entitles a
            // declaration to claim `none`. Testing only `direct.Count > 0` would fold it into
            // the unreachable branch below, marking every callback a blind spot and leaving
            // the detector indistinguishable from one stuck on.
            var direct = ExtractMeasurementBlobs(body, bodyPath);
            if (direct.Count > 0 || MeasurementConstructionPattern.IsMatch(body))
            {
                return direct;
            }

            // Not constructed inline, so the callback delegates: either a method group
            // (Observe, Registry.ObserveProcessed) or a lambda invoking one. Resolve the
            // target's body and look there, file-local first then repo-wide-unique, the same
            // discipline ResolveByName uses - a name declared in two files is ambiguous, never
            // unioned.
            var target = CallbackTargetName(body);
            if (target is null || !TryFindMethodBody(target, bodyPath, out var found, out var foundPath))
            {
                return null;
            }

            var blobs = ExtractMeasurementBlobs(found, foundPath);
            if (blobs.Count > 0 || MeasurementConstructionPattern.IsMatch(found))
            {
                return blobs;
            }

            // A one-hop forwarder (static () => Instance.ObservePeak(), or a body that is a
            // single delegation) is common enough to follow; beyond that the parser stops and
            // says so rather than guessing.
            var forwarded = Regex.Match(found.Trim(), @"^(?:return\s+)?([\w\.]+\s*\([^;]*\))\s*;?\s*$");
            return forwarded.Success
                ? ReadMeasurementBlobs(Collapse(forwarded.Groups[1].Value), foundPath, depth + 1)
                : null;
        }

        /// <summary>Tag-argument text of every <c>new Measurement&lt;T&gt;(...)</c> in a body.</summary>
        private static List<(string Blob, string Path)> ExtractMeasurementBlobs(string body, string path)
        {
            var blobs = new List<(string, string)>();

            foreach (Match m in MeasurementConstructionPattern.Matches(body))
            {
                var args = SplitArguments(body, m.Index + m.Length);
                if (args.Count <= 1)
                {
                    continue;
                }

                blobs.Add((string.Join(",", args.Skip(1)), path));
            }

            return blobs;
        }

        /// <summary>The simple name of the method an observation callback delegates to.</summary>
        private static string? CallbackTargetName(string body)
        {
            var invocation = Regex.Match(body.Trim(), @"^(?:return\s+)?([\w\.]+)\s*\(");
            if (invocation.Success)
            {
                return invocation.Groups[1].Value.Split('.')[^1];
            }

            var group = Regex.Match(body.Trim(), @"^([\w\.]+)$");
            return group.Success ? group.Groups[1].Value.Split('.')[^1] : null;
        }

        /// <summary>
        /// Locates the body of a method by simple name, preferring the declaring file and
        /// widening to the corpus only when the name resolves to exactly one file.
        /// </summary>
        private bool TryFindMethodBody(
            string name, string preferredPath, out string body, out string path)
        {
            // Memoised and pre-filtered deliberately. A naive implementation compiles a
            // regex and scans all ~3,400 corpus files once per observable declaration,
            // which is enough work to trip the test host's inactivity guard. The ordinal
            // Contains() pre-filter rejects the overwhelming majority of files at memcmp
            // speed before any regex runs, and the cache makes a repeated name free.
            if (!_methodBodies.TryGetValue(name, out var hits))
            {
                var pattern = new Regex(
                    @"(?<![\w\.])[\w\<\>\,\?\[\]\.]+\s+" + Regex.Escape(name) + @"\s*\(");
                hits = new List<(string Path, string Body)>();

                foreach (var (file, text) in _files)
                {
                    if (!text.Contains(name, StringComparison.Ordinal))
                    {
                        continue;
                    }

                    foreach (Match m in pattern.Matches(text))
                    {
                        if (TryReadBody(text, ArgumentListEnd(text, m.Index + m.Length), out var found))
                        {
                            hits.Add((file, found));
                        }
                    }
                }

                _methodBodies[name] = hits;
            }

            var local = hits.Where(h => string.Equals(h.Path, preferredPath, StringComparison.Ordinal)).ToList();
            var chosen = local.Count > 0 ? local : hits;

            if (chosen.Count == 0
                || chosen.Select(h => h.Path).Distinct(StringComparer.Ordinal).Count() > 1)
            {
                body = string.Empty;
                path = string.Empty;
                return false;
            }

            body = string.Join(Environment.NewLine, chosen.Select(h => h.Body));
            path = chosen[0].Path;
            return true;
        }

        /// <summary>Reads a method body that begins at or after <paramref name="start"/>.</summary>
        private static bool TryReadBody(string text, int start, out string body)
        {
            body = string.Empty;
            var i = start;
            while (i < text.Length && char.IsWhiteSpace(text[i]))
            {
                i++;
            }

            if (i >= text.Length)
            {
                return false;
            }

            if (text[i] == '{')
            {
                var depth = 1;
                int end;
                for (end = i + 1; end < text.Length && depth > 0; end++)
                {
                    if (text[end] == '{')
                    {
                        depth++;
                    }
                    else if (text[end] == '}')
                    {
                        depth--;
                    }
                }

                body = text[(i + 1)..Math.Min(end, text.Length)];
                return true;
            }

            if (i + 1 < text.Length && text[i] == '=' && text[i + 1] == '>')
            {
                var end = text.IndexOf(';', i);
                body = end < 0 ? text[(i + 2)..] : text[(i + 2)..end];
                return true;
            }

            return false;
        }

        /// <summary>Marker text bounded to <see cref="ResidueRenderLimit"/> characters.</summary>
        private static string Render(string expression) =>
            expression.Length <= ResidueRenderLimit
                ? expression
                : expression[..ResidueRenderLimit] + "...";

        private IEnumerable<(string Key, DomainResult Domain)> ExtractTagPairs(string blob, string path)
        {
            var pattern = new Regex(
                @"new\s+KeyValuePair\s*<\s*string\s*,\s*object\??\s*>\s*\(", RegexOptions.Compiled);

            // Every region of the blob consumed by a literal KeyValuePair construction. What is
            // left over after removing them is tag text this parser never read, and it is the
            // difference between "there was nothing to find" and "I found nothing". See the
            // residue check below.
            var consumed = new List<(int Start, int End)>();

            foreach (Match m in pattern.Matches(blob))
            {
                var args = SplitArguments(blob, m.Index + m.Length);
                consumed.Add((m.Index, ArgumentListEnd(blob, m.Index + m.Length)));

                if (args.Count < 2)
                {
                    // Never drop a tag pair silently. A discarded pair leaves the instrument
                    // with no dimensions at all, which seeds as None - a claim that the
                    // instrument carries NO bounded dimension. That converts a parse failure
                    // into a negative claim, which is precisely the silent exclusion this gate
                    // exists to prevent, occurring inside the gate's own parser.
                    yield return (
                        "(unreadable-tag-pair)",
                        new DomainResult(
                            Array.Empty<string>(),
                            true,
                            "a tag pair could not be split into key and value"));
                    continue;
                }

                var key = ResolveSingle(args[0], path);
                if (key is null)
                {
                    yield return (
                        $"(unresolved-key:{Collapse(args[0])})",
                        new DomainResult(
                            Array.Empty<string>(),
                            true,
                            $"tag key {Collapse(args[0])} did not resolve to a single literal"));
                    continue;
                }

                yield return (key, ResolveDomain(args[1], path));
            }

            // The residue: tag text outside every literal KeyValuePair construction. An earlier
            // revision of this parser yielded pairs only from the literals and discarded the
            // rest in silence, so an emission whose tags are PRE-BUILT KeyValuePair VARIABLES -
            // `Add(delta, treeTag, outcome, tenantTag)` - matched nothing, produced no
            // dimensions, and seeded None: a positive claim that the instrument carries no
            // bounded taxonomy, manufactured out of the parser's own blindness.
            //
            // Note this is a DIFFERENT hole from the unresolved-key path above, which needs a
            // literal to have been found before it can report that the literal's key would not
            // resolve. With no literal there is no pair whose key failed, so there was nothing
            // for that path - or for the None-contradiction check that consumes it - to fire on.
            // The worked example is LatticeMetrics.WalGcBlockedLeafReactivations, whose sole
            // emission passes `outcome` as a method parameter (issue #2968). That instrument's
            // domain is real: LatticeMetrics.cs declares one `BlockedLeafReactivation*` tag
            // literal per arm, which is where the domain is to be read from.
            //
            // The arity is DELIBERATELY NOT WRITTEN DOWN HERE, and that is not squeamishness.
            // It moves: this branch's base carries eight arms, and issue #2692 then added five
            // drive verdicts, taking it to thirteen. A count typed into a comment inside the
            // gate whose whole purpose is to stop stale claims about instruments would be that
            // defect in its purest form, and a comment cannot fail when it goes stale. Derive
            // the arm set by enumerating those declarations; do not quote a number for it.
            //
            // Where the domain is read from is equally deliberate: from those declarations, and
            // NOT from the dashboards tag-domain resolver. The resolver happens to agree on this
            // instrument, but it is not a sound oracle in general (issue #2970 records it
            // fabricating a `grain_type` domain by descending into every method that shares a
            // simple name and harvesting the string literals it finds there). A control that
            // borrows its authority from a detector with known false positives is not a control.
            //
            // Residue is reported as ONE marker per emission site rather than one per token.
            // The claim being made is deliberately weak - "there is tag text here I did not
            // read" - because a parser that cannot read the text equally cannot count how many
            // dimensions it holds. Asserting a number would be a second invented measurement.
            var residue = Collapse(Remove(blob, consumed)).Trim().Trim(',').Trim();
            if (residue.Length > 0 && residue.Any(char.IsLetterOrDigit))
            {
                var rendered = residue.Length <= ResidueRenderLimit
                    ? residue
                    : residue[..ResidueRenderLimit] + " (truncated)";

                yield return (
                    $"(unread-tag-argument:{rendered})",
                    new DomainResult(
                        Array.Empty<string>(),
                        true,
                        $"tag argument(s) {rendered} are not literal KeyValuePair constructions, "
                        + "so neither their keys nor their domains were ever read"));
            }
        }

        /// <summary>Longest residue rendered verbatim into a marker key. Keeps enrolment rows readable.</summary>
        private const int ResidueRenderLimit = 120;

        /// <summary>Index just past the argument list whose contents start at <paramref name="start"/>.</summary>
        private static int ArgumentListEnd(string text, int start)
        {
            var depth = 0;

            for (var i = start; i < text.Length; i++)
            {
                var c = text[i];
                if (c is '(' or '[' or '{')
                {
                    depth++;
                }
                else if (c is ')' or ']' or '}')
                {
                    if (c == ')' && depth == 0)
                    {
                        return i + 1;
                    }

                    depth--;
                }
                else if (c == '"')
                {
                    var j = i + 1;
                    while (j < text.Length && !(text[j] == '"' && text[j - 1] != '\\'))
                    {
                        j++;
                    }

                    i = j;
                }
            }

            return text.Length;
        }

        /// <summary>The text outside every span in <paramref name="spans"/>.</summary>
        private static string Remove(string text, List<(int Start, int End)> spans)
        {
            if (spans.Count == 0)
            {
                return text;
            }

            var builder = new StringBuilder();
            var cursor = 0;

            foreach (var (start, end) in spans.OrderBy(s => s.Start))
            {
                if (start > cursor)
                {
                    builder.Append(text, cursor, start - cursor);
                }

                cursor = Math.Max(cursor, Math.Min(end, text.Length));
            }

            if (cursor < text.Length)
            {
                builder.Append(text, cursor, text.Length - cursor);
            }

            return builder.ToString();
        }

        private static string Collapse(string expression) =>
            Regex.Replace(expression.Trim(), @"\s+", " ");

        private string? ResolveSingle(string expression, string path)
        {
            var domain = ResolveDomain(expression, path);
            return !domain.Ambiguous && domain.Values.Count == 1 ? domain.Values[0] : null;
        }

        private DomainResult ResolveDomain(string expression, string path) =>
            ResolveDomain(expression, path, depth: 0);

        private DomainResult ResolveDomain(string expression, string path, int depth)
        {
            var e = expression.Trim().TrimEnd('!');

            if (depth > 4)
            {
                return new DomainResult(Array.Empty<string>(), true, "resolution nested too deeply");
            }

            if (e.StartsWith('(') && e.EndsWith(')') && e.Count(c => c == '(') == 1)
            {
                e = e[1..^1].Trim();
            }

            if (e.StartsWith('"') && e.EndsWith('"') && e.Count(c => c == '"') == 2)
            {
                return new DomainResult(new[] { e[1..^1] }, false, null);
            }

            // Ternary: both arms contribute to the domain. Splitting on the top-level '?'
            // and ':' only, so a nested call or a null-coalesce inside an arm is preserved.
            var ternary = SplitTernary(e);
            if (ternary is not null)
            {
                var left = ResolveDomain(ternary.Value.WhenTrue, path, depth + 1);
                var right = ResolveDomain(ternary.Value.WhenFalse, path, depth + 1);
                if (left.Ambiguous || right.Ambiguous)
                {
                    return new DomainResult(
                        Array.Empty<string>(), true, left.Note ?? right.Note ?? "ternary arm unresolved");
                }

                return new DomainResult(
                    left.Values.Union(right.Values, StringComparer.Ordinal)
                        .OrderBy(v => v, StringComparer.Ordinal).ToList(),
                    false,
                    null);
            }

            var describe = Regex.Match(e, @"^(Describe\w+)\s*\(");
            if (describe.Success)
            {
                return ResolveDescribeHelper(describe.Groups[1].Value, path, depth);
            }

            if (Regex.IsMatch(e, @"^[\w\.]+$"))
            {
                // Keep the qualifier. LatticeReplicationMetrics.TagTree names its declaring
                // type, and discarding that left a bare TagTree, which collides with the
                // identically-named const in LatticeMetrics and so resolved as ambiguous. The
                // never-union rule was right to refuse it; throwing away the information that
                // disambiguates it was the defect.
                var segments = e.Split('.');
                var qualifier = segments.Length >= 2 ? segments[^2] : null;
                return ResolveConstant(segments[^1], path, qualifier);
            }

            return new DomainResult(Array.Empty<string>(), true, $"unrecognised value expression: {Truncate(e)}");
        }

        private static (string WhenTrue, string WhenFalse)? SplitTernary(string expression)
        {
            var depth = 0;
            var question = -1;

            for (var i = 0; i < expression.Length; i++)
            {
                var c = expression[i];
                if (c is '(' or '[' or '<')
                {
                    depth++;
                }
                else if (c is ')' or ']' or '>')
                {
                    depth--;
                }
                else if (c == '"')
                {
                    var j = i + 1;
                    while (j < expression.Length && !(expression[j] == '"' && expression[j - 1] != '\\'))
                    {
                        j++;
                    }

                    i = j;
                }
                else if (depth == 0 && c == '?' && question < 0)
                {
                    // '??' is null-coalescing, not a conditional.
                    if (i + 1 < expression.Length && expression[i + 1] == '?')
                    {
                        i++;
                        continue;
                    }

                    question = i;
                }
                else if (depth == 0 && c == ':' && question >= 0)
                {
                    if (i + 1 < expression.Length && expression[i + 1] == ':')
                    {
                        i++;
                        continue;
                    }

                    return (expression[(question + 1)..i], expression[(i + 1)..]);
                }
            }

            return null;
        }

        private DomainResult ResolveConstant(string name, string path, string? qualifier = null)
        {
            if (!_consts.TryGetValue(name, out var candidates) || candidates.Count == 0)
            {
                return new DomainResult(Array.Empty<string>(), true, $"no declaration found for {name}");
            }

            // A qualified reference narrows the candidates to the declaring type. The repository
            // convention is one top-level type per file, so the file's base name is a sound proxy
            // for the type name; if it narrows to nothing the qualifier is not a type here and the
            // ordinary file-local-first rule still applies.
            if (qualifier is not null)
            {
                var qualified = candidates
                    .Where(c => string.Equals(
                        Path.GetFileNameWithoutExtension(c.File), qualifier, StringComparison.Ordinal))
                    .ToList();
                if (qualified.Count > 0)
                {
                    candidates = qualified;
                }
            }

            var chosen = ChooseUnique(candidates, path, name, out var failure);
            return failure ?? new DomainResult(new[] { chosen!.Value.Payload }, false, null);
        }

        private DomainResult ResolveDescribeHelper(string name, string path, int depth)
        {
            if (!_describeHelpers.TryGetValue(name, out var candidates) || candidates.Count == 0)
            {
                return new DomainResult(Array.Empty<string>(), true, $"no declaration found for {name}");
            }

            var chosen = ChooseUnique(candidates, path, name, out var failure);
            if (failure is not null)
            {
                return failure;
            }

            // The arms are resolved in the HELPER's file, not the caller's, and each arm is an
            // expression rather than a literal: these switch bodies return tag constants, so an
            // arm scan that only collected string literals resolved every one of them to nothing.
            var values = new List<string>();
            foreach (var arm in chosen!.Value.Payload)
            {
                var resolved = ResolveDomain(arm, chosen.Value.File, depth + 1);
                if (resolved.Ambiguous)
                {
                    return new DomainResult(
                        Array.Empty<string>(), true, $"{name} arm unresolved: {resolved.Note}");
                }

                values.AddRange(resolved.Values);
            }

            values = values.Distinct(StringComparer.Ordinal).OrderBy(v => v, StringComparer.Ordinal).ToList();
            return values.Count == 0
                ? new DomainResult(Array.Empty<string>(), true, $"{name} resolved to nothing")
                : new DomainResult(values, false, null);
        }

        private static (string File, T Payload)? ChooseUnique<T>(
            List<(string File, T Payload)> candidates,
            string path,
            string name,
            out DomainResult? failure)
        {
            // File-local first. Widening to the repository by bare name is what unioned two
            // unrelated taxonomies during development; four helper names in src/ collide.
            var local = candidates.Where(c => string.Equals(c.File, path, StringComparison.Ordinal)).ToList();
            var chosen = local.Count > 0 ? local : candidates;

            if (chosen.Count > 1)
            {
                failure = new DomainResult(
                    Array.Empty<string>(),
                    true,
                    $"{name} is declared in {chosen.Count} files; resolving it would union unrelated taxonomies");
                return null;
            }

            failure = null;
            return chosen[0];
        }

        private static string Truncate(string value) =>
            value.Length <= 60 ? value : value[..60] + "...";

        private static List<string> ExtractSwitchArms(string text, int openBrace)
        {
            // Returns each arm's VALUE EXPRESSION, not the string literals inside the block.
            // These switch bodies overwhelmingly return tag constants rather than literals, so
            // a scan that collected only literals resolved every helper in this repository to
            // nothing - a clean empty answer that reads as "no taxonomy here" rather than as a
            // parse failure.
            var arms = new List<string>();
            var depth = 0;
            var current = new StringBuilder();
            var capturing = false;

            for (var i = openBrace; i < text.Length; i++)
            {
                var c = text[i];

                if (c == '"')
                {
                    var j = i + 1;
                    while (j < text.Length && !(text[j] == '"' && text[j - 1] != '\\'))
                    {
                        j++;
                    }

                    if (capturing)
                    {
                        current.Append(text[i..Math.Min(j + 1, text.Length)]);
                    }

                    i = j;
                    continue;
                }

                if (c is '{' or '(' or '[')
                {
                    depth++;
                    if (capturing && depth > 1)
                    {
                        current.Append(c);
                    }

                    continue;
                }

                if (c is '}' or ')' or ']')
                {
                    depth--;
                    if (depth == 0)
                    {
                        break;
                    }

                    if (capturing)
                    {
                        current.Append(c);
                    }

                    continue;
                }

                if (depth == 1 && c == '=' && i + 1 < text.Length && text[i + 1] == '>')
                {
                    capturing = true;
                    current.Clear();
                    i++;
                    continue;
                }

                if (depth == 1 && c == ',' && capturing)
                {
                    arms.Add(current.ToString().Trim());
                    capturing = false;
                    current.Clear();
                    continue;
                }

                if (capturing)
                {
                    current.Append(c);
                }
            }

            if (capturing && current.Length > 0)
            {
                arms.Add(current.ToString().Trim());
            }

            return arms
                .Where(a => a.Length > 0)
                .Distinct(StringComparer.Ordinal)
                .ToList();
        }

        private static List<string> SplitArguments(string text, int start)
        {
            var args = new List<string>();
            var current = new StringBuilder();
            var depth = 0;

            for (var i = start; i < text.Length; i++)
            {
                var c = text[i];
                if (c is '(' or '[' or '{')
                {
                    depth++;
                }
                else if (c is ')' or ']' or '}')
                {
                    if (c == ')' && depth == 0)
                    {
                        args.Add(current.ToString());
                        return args;
                    }

                    depth--;
                }
                else if (c == '"')
                {
                    var j = i + 1;
                    while (j < text.Length && !(text[j] == '"' && text[j - 1] != '\\'))
                    {
                        j++;
                    }

                    current.Append(text[i..Math.Min(j + 1, text.Length)]);
                    i = j;
                    continue;
                }
                else if (c == ',' && depth == 0)
                {
                    args.Add(current.ToString());
                    current.Clear();
                    continue;
                }

                current.Append(c);
            }

            args.Add(current.ToString());
            return args;
        }
    }
}
