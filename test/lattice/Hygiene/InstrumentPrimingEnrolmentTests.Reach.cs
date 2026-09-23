using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using NUnit.Framework;

namespace Orleans.Lattice.Tests.Hygiene;

/// <summary>
/// Issue #3231: the analyser's REACH. Tag arguments passed through a variable, a member, or a
/// label factory are followed to the pair they hold; a dimension that is open by nature (a
/// runtime tree, tenant, shard, or cluster) is reported apart from one the analyser could not
/// read; and the file records what its status column does and does not assert, plus a floor on
/// the number of declarations read in full.
/// </summary>
public sealed partial class InstrumentPrimingEnrolmentTests
{
    private const string UnreadTagPrefix = "(unread-tag-argument:";

    private static IReadOnlyDictionary<string, DomainResult> DimensionsOf(SourceCorpus corpus, string owner) =>
        corpus.Declarations.Single(d => d.Owner == owner).Dimensions;

    private static List<string> UnreadKeys(IReadOnlyDictionary<string, DomainResult> dimensions) =>
        dimensions.Keys.Where(k => k.StartsWith(UnreadTagPrefix, StringComparison.Ordinal)).ToList();

    [Test]
    public void A_tag_variable_is_followed_to_the_pair_it_binds()
    {
        var corpus = SourceCorpus.ForTesting(new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["src/probe/Labels.cs"] = """
                internal static class Labels
                {
                    public const string TagTenant = "tenant";
                    public static readonly KeyValuePair<string, object?> Platform = new(TagTenant, "_platform_");
                }
                """,
            ["src/probe/Caller.Tags.cs"] = """
                internal static partial class Caller
                {
                    private static readonly KeyValuePair<string, object?> StateTag = new("state", "ready");
                }
                """,
            ["src/probe/Caller.cs"] = """
                internal static partial class Caller
                {
                    private const string TagTree = "tree";
                    private static readonly Counter<long> _local = Meter.CreateCounter<long>("probe.local");
                    private static readonly Counter<long> _qualified = Meter.CreateCounter<long>("probe.qualified");
                    private static readonly Counter<long> _partial = Meter.CreateCounter<long>("probe.partial");

                    public void EmitLocal(string treeId)
                    {
                        var treeTag = new KeyValuePair<string, object?>(TagTree, treeId);
                        _local.Add(1, treeTag);
                    }

                    public void EmitQualified() => _qualified.Add(1, Labels.Platform);

                    public void EmitPartial() => _partial.Add(1, StateTag);
                }
                """,
        });

        var local = DimensionsOf(corpus, "_local");
        var qualified = DimensionsOf(corpus, "_qualified");
        var partial = DimensionsOf(corpus, "_partial");

        Assert.Multiple(() =>
        {
            Assert.That(UnreadKeys(local), Is.Empty, "A local tag variable bound once in the same file must be followed, not named as a blind spot.");
            Assert.That(local.ContainsKey("tree"), Is.True, "The local tag variable's key was not read through the binding.");
            Assert.That(SourceCorpus.IsOpenDimension(local["tree"]), Is.True, "A runtime tree id under the tree key is open by nature.");
            Assert.That(local["tree"].Via, Is.EqualTo("treeTag"), "The dimension must record the argument it was read through.");

            Assert.That(UnreadKeys(qualified), Is.Empty, "A qualified static tag member must be followed into the file its qualifier names.");
            Assert.That(qualified.ContainsKey("tenant"), Is.True);
            Assert.That(qualified["tenant"].Ambiguous, Is.False);
            Assert.That(qualified["tenant"].Values, Is.EqualTo(new[] { "_platform_" }));
            Assert.That(qualified["tenant"].Note, Does.StartWith(SourceCorpus.ConstantTagNotePrefix), "A single literal read through a static member is a constant tag.");

            Assert.That(UnreadKeys(partial), Is.Empty, "An unqualified tag member declared in another part of the same partial type must be followed.");
            Assert.That(partial.ContainsKey("state") ? partial["state"].Values : Array.Empty<string>(), Is.EqualTo(new[] { "ready" }));
        });
    }

    [Test]
    public void A_tag_name_the_analyser_cannot_bind_uniquely_stays_unread()
    {
        // The negative half of the test above: following a name is only sound when exactly one
        // binding can be meant. A name bound only in an UNRELATED type, or bound twice to
        // different expressions in scope, must stay a named blind spot rather than be guessed.
        var corpus = SourceCorpus.ForTesting(new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["src/other/Elsewhere.cs"] = """
                internal static class Elsewhere
                {
                    private static readonly KeyValuePair<string, object?> orphanTag = new("state", "alpha");
                }
                """,
            ["src/probe/Caller.cs"] = """
                internal static class Caller
                {
                    private static readonly Counter<long> _unrelated = Meter.CreateCounter<long>("probe.unrelated");
                    private static readonly Counter<long> _twice = Meter.CreateCounter<long>("probe.twice");

                    public void EmitUnrelated() => _unrelated.Add(1, orphanTag);

                    public void EmitA()
                    {
                        var tag = new KeyValuePair<string, object?>("state", "alpha");
                        _twice.Add(1, tag);
                    }

                    public void EmitB()
                    {
                        var tag = new KeyValuePair<string, object?>("state", "beta");
                        _twice.Add(1, tag);
                    }
                }
                """,
        });

        var unrelated = DimensionsOf(corpus, "_unrelated");
        var twice = DimensionsOf(corpus, "_twice");

        Assert.Multiple(() =>
        {
            Assert.That(UnreadKeys(unrelated), Is.Not.Empty, "A name bound only in an unrelated type was resolved, so the binding scope is not being enforced.");
            Assert.That(unrelated.ContainsKey("state"), Is.False);
            Assert.That(UnreadKeys(twice), Is.Not.Empty, "A name bound twice to different expressions was resolved by picking one.");
            Assert.That(twice.ContainsKey("state"), Is.False);
            Assert.That(twice.Values.All(d => d.Ambiguous && !SourceCorpus.IsOpenDimension(d)), Is.True, "An unread tag must stay ambiguous and must not be reported as open.");
        });
    }

    [Test]
    public void A_label_factory_is_an_open_dimension_and_an_unknown_factory_is_not()
    {
        var corpus = SourceCorpus.ForTesting(new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["src/probe/LatticeTenantLabel.cs"] = """
                public static class LatticeTenantLabel
                {
                    public const string TagTenant = "tenant";
                }
                """,
            ["src/probe/Caller.cs"] = """
                internal static class Caller
                {
                    private static readonly Counter<long> _known = Meter.CreateCounter<long>("probe.known");
                    private static readonly Counter<long> _unknown = Meter.CreateCounter<long>("probe.unknown");
                    public void EmitKnown(string treeId) => _known.Add(1, LatticeTenantLabel.ForTree(treeId));
                    public void EmitUnknown(string treeId) => _unknown.Add(1, OtherLabel.ForTree(treeId));
                }
                """,
        });

        var known = DimensionsOf(corpus, "_known");
        var unknown = DimensionsOf(corpus, "_unknown");

        Assert.Multiple(() =>
        {
            Assert.That(UnreadKeys(known), Is.Empty);
            Assert.That(known.ContainsKey("tenant"), Is.True, "The label factory's key constant was not read.");
            Assert.That(SourceCorpus.IsOpenDimension(known["tenant"]), Is.True, "A tenant derived at runtime by a label factory is open by nature.");

            Assert.That(UnreadKeys(unknown), Is.Not.Empty, "A factory not on the reviewed list must stay a named blind spot.");
            Assert.That(unknown.ContainsKey("tenant"), Is.False);
        });
    }

    [Test]
    public void Only_an_identifier_key_is_open_and_an_unread_taxonomy_is_not()
    {
        // The distinction issue #3231 asked for, in miniature. A runtime value under an
        // identifier key is a resolved answer (there is no finite domain); a runtime value under
        // any other key is a bounded taxonomy the analyser failed to read, and calling it open
        // would launder that failure into a finding.
        var corpus = SourceCorpus.ForTesting(new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["src/probe/Caller.cs"] = """
                internal static class Caller
                {
                    private static readonly Counter<long> _probe = Meter.CreateCounter<long>("probe.a");
                    public void Emit(string outcome, string treeId, string peer)
                        => _probe.Add(
                            1,
                            new KeyValuePair<string, object?>("outcome", outcome),
                            new KeyValuePair<string, object?>("tree", treeId),
                            new KeyValuePair<string, object?>("peer", peer));
                }
                """,
        });

        var dimensions = DimensionsOf(corpus, "_probe");

        Assert.Multiple(() =>
        {
            Assert.That(dimensions.Keys, Is.EquivalentTo(new[] { "outcome", "tree", "peer" }));
            Assert.That(SourceCorpus.IsOpenDimension(dimensions["tree"]), Is.True);
            Assert.That(SourceCorpus.IsOpenDimension(dimensions["peer"]), Is.True);
            Assert.That(dimensions["outcome"].Ambiguous, Is.True);
            Assert.That(SourceCorpus.IsOpenDimension(dimensions["outcome"]), Is.False, "An outcome parameter is an unread taxonomy, not an identifier.");
            Assert.That(SourceCorpus.OpenDimensionKeys, Does.Not.Contain("outcome"));
        });
    }

    [Test]
    public void A_tag_collection_is_read_whole_or_not_at_all()
    {
        var corpus = SourceCorpus.ForTesting(new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["src/probe/Caller.cs"] = """
                internal static class Caller
                {
                    private static readonly Counter<long> _whole = Meter.CreateCounter<long>("probe.whole");
                    private static readonly Counter<long> _partial = Meter.CreateCounter<long>("probe.partial");
                    private static readonly Counter<long> _array = Meter.CreateCounter<long>("probe.array");
                    public void EmitWhole(string treeId) => _whole.Add(1, new TagList { { "tree", treeId }, { "state", "a" } });
                    public void EmitPartial(string treeId) => _partial.Add(1, new TagList { { "tree", treeId }, mystery });
                    public void EmitArray(string treeId) => _array.Add(1, [new KeyValuePair<string, object?>("tree", treeId), new("state", "b")]);
                }
                """,
        });

        var whole = DimensionsOf(corpus, "_whole");
        var partial = DimensionsOf(corpus, "_partial");
        var array = DimensionsOf(corpus, "_array");

        Assert.Multiple(() =>
        {
            Assert.That(UnreadKeys(whole), Is.Empty);
            Assert.That(whole.Keys, Is.EquivalentTo(new[] { "tree", "state" }));

            // The generic-aware split: the comma inside KeyValuePair<string, object?> must not
            // split the first element in two.
            Assert.That(UnreadKeys(array), Is.Empty, "A collection expression of pairs was not read; the top-level split is breaking a generic argument list.");
            Assert.That(array.Keys, Is.EquivalentTo(new[] { "tree", "state" }));

            Assert.That(UnreadKeys(partial), Is.Not.Empty, "A collection with one unread element must stay unread as a whole.");
            Assert.That(partial.ContainsKey("tree"), Is.False, "Reporting the element that happened to resolve would understate what went unread.");
        });
    }

    [Test]
    public void A_ternary_or_switch_of_tag_members_resolves_to_the_union_of_its_arms()
    {
        var corpus = SourceCorpus.ForTesting(new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["src/probe/Caller.cs"] = """
                internal static class Caller
                {
                    private static readonly KeyValuePair<string, object?> Hit = new("result", "hit");
                    private static readonly KeyValuePair<string, object?> Miss = new("result", "miss");
                    private static readonly KeyValuePair<string, object?> Other = new("kind", "x");
                    private static readonly Counter<long> _ternary = Meter.CreateCounter<long>("probe.ternary");
                    private static readonly Counter<long> _switch = Meter.CreateCounter<long>("probe.switch");
                    private static readonly Counter<long> _mixed = Meter.CreateCounter<long>("probe.mixed");
                    public void EmitTernary(bool ok) => _ternary.Add(1, ok ? Hit : Miss);
                    public void EmitSwitch(int cause) => _switch.Add(1, cause switch { 1 => Hit, 2 => Miss, _ => throw new InvalidOperationException() });
                    public void EmitMixed(bool ok) => _mixed.Add(1, ok ? Hit : Other);
                }
                """,
        });

        var ternary = DimensionsOf(corpus, "_ternary");
        var switched = DimensionsOf(corpus, "_switch");
        var mixed = DimensionsOf(corpus, "_mixed");

        Assert.Multiple(() =>
        {
            Assert.That(ternary.ContainsKey("result"), Is.True);
            Assert.That(ternary["result"].Ambiguous, Is.False);
            Assert.That(ternary["result"].Values, Is.EquivalentTo(new[] { "hit", "miss" }));

            Assert.That(switched.ContainsKey("result"), Is.True, "A switch over tag members was not read; a throw arm must be skipped, not fail the whole switch.");
            Assert.That(switched["result"].Values, Is.EquivalentTo(new[] { "hit", "miss" }));

            Assert.That(UnreadKeys(mixed), Is.Not.Empty, "Arms carrying different keys are not one dimension and must stay unread.");
            Assert.That(mixed.ContainsKey("result") || mixed.ContainsKey("kind"), Is.False);
        });
    }

    [Test]
    public void Seed_reports_open_only_when_every_unread_dimension_is_open_by_nature()
    {
        var corpus = SourceCorpus.ForTesting(new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["src/probe/Caller.cs"] = """
                internal static class Caller
                {
                    private static readonly KeyValuePair<string, object?> Platform = new("tenant", "_platform_");
                    private static readonly Counter<long> _open = Meter.CreateCounter<long>("probe.open");
                    private static readonly Counter<long> _openConstant = Meter.CreateCounter<long>("probe.open_constant");
                    private static readonly Counter<long> _constant = Meter.CreateCounter<long>("probe.constant");
                    private static readonly Counter<long> _mixed = Meter.CreateCounter<long>("probe.mixed");
                    private static readonly Counter<long> _bounded = Meter.CreateCounter<long>("probe.bounded");
                    private static readonly Counter<long> _tagless = Meter.CreateCounter<long>("probe.tagless");
                    public void A(string treeId) => _open.Add(1, new KeyValuePair<string, object?>("tree", treeId));
                    public void B(string treeId) => _openConstant.Add(1, new KeyValuePair<string, object?>("tree", treeId), Platform);
                    public void C() => _constant.Add(1, Platform);
                    public void D(string treeId, string outcome) => _mixed.Add(1, new KeyValuePair<string, object?>("tree", treeId), new KeyValuePair<string, object?>("outcome", outcome));
                    public void E(string treeId) => _bounded.Add(1, new KeyValuePair<string, object?>("tree", treeId), new KeyValuePair<string, object?>("state", "a"));
                    public void F(string treeId) => _bounded.Add(1, new KeyValuePair<string, object?>("tree", treeId), new KeyValuePair<string, object?>("state", "b"));
                    public void G() => _tagless.Add(1);
                }
                """,
        });

        (Enrolment Enrolment, string Detail) SeedOf(string owner) =>
            Seed(corpus.Declarations.Single(d => d.Owner == owner));

        var open = SeedOf("_open");
        var openConstant = SeedOf("_openConstant");
        var constant = SeedOf("_constant");
        var mixed = SeedOf("_mixed");
        var bounded = SeedOf("_bounded");
        var tagless = SeedOf("_tagless");

        Assert.Multiple(() =>
        {
            Assert.That(open.Enrolment, Is.EqualTo(Enrolment.Open));
            Assert.That(open.Detail, Does.StartWith("open=tree;"));
            Assert.That(openConstant.Enrolment, Is.EqualTo(Enrolment.Open), "A constant tag beside an open dimension leaves nothing unread.");
            Assert.That(openConstant.Detail, Does.StartWith("open=tree;"));

            Assert.That(constant.Enrolment, Is.EqualTo(Enrolment.Unresolved), "A constant tag alone is read but not yet enrolled; it is not open.");
            Assert.That(constant.Detail, Does.StartWith("reason=constant tag tenant -> [_platform_]"));

            Assert.That(mixed.Enrolment, Is.EqualTo(Enrolment.Unresolved), "One unread dimension beside an open one must keep the row unresolved.");
            Assert.That(mixed.Detail, Does.StartWith("reason=dimension outcome:"));

            Assert.That(bounded.Enrolment, Is.EqualTo(Enrolment.Unresolved), "A bounded domain still owes a priming decision whatever else the row carries.");
            Assert.That(bounded.Detail, Does.StartWith("reason=bounded domain found"));

            Assert.That(tagless.Enrolment, Is.EqualTo(Enrolment.None), "The negative control: a tagless emission must still seed none.");
            Assert.That(tagless.Detail, Is.Empty);
        });
    }

    [Test]
    public void Enrolment_file_states_what_its_status_column_asserts()
    {
        ReadEnrolmentFile(out var path);
        Assert.That(File.Exists(path), Is.True, $"{EnrolmentFileName} is missing.");

        var reach = File.ReadAllLines(path)
            .Select(static line => line.Trim())
            .Where(static line => line.StartsWith(ReachPrefix, StringComparison.Ordinal))
            .ToList();

        Assert.That(
            reach,
            Is.EqualTo(new[] { ReachHeaderLine }),
            $"{EnrolmentFileName} must carry exactly one '{ReachPrefix}' line, equal to the generator's. "
            + "Issue #3231 recorded the status column being read as a runtime claim in both "
            + "directions; the file must say that it records the analyser's reach, not what a "
            + "process emits. Regenerate the file.");
    }

    [Test]
    public void Analyser_coverage_does_not_fall_below_the_recorded_floor()
    {
        ReadEnrolmentFile(out var path);
        var floorLines = File.Exists(path)
            ? File.ReadAllLines(path).Count(l => l.Trim().StartsWith(CoverageFloorPrefix, StringComparison.Ordinal))
            : 0;
        var floor = ReadCoverageFloor(path);
        var covered = CoveredDeclarationCount(Corpus.Value.Declarations);

        Assert.Multiple(() =>
        {
            Assert.That(floorLines, Is.EqualTo(1), $"{EnrolmentFileName} must carry exactly one '{CoverageFloorPrefix}' line.");
            Assert.That(floor, Is.GreaterThan(0), "The coverage floor is absent or zero, so a fall in the analyser's reach would produce no signal.");
            Assert.That(
                covered,
                Is.GreaterThanOrEqualTo(floor ?? int.MaxValue),
                $"The analyser now reads every tag dimension of {covered.ToString(CultureInfo.InvariantCulture)} "
                + $"declaration(s), below the recorded floor of {floor}. Its reach has eroded: a "
                + "resolver change stopped following an indirection it used to follow. This is a "
                + "statement about the TOOL, not about the estate. Restore the reach, or lower the "
                + "floor in a reviewed edit that says why.");
        });
    }

    [Test]
    public void Open_enrolments_are_backed_by_a_fully_read_declaration()
    {
        var byKey = Corpus.Value.Declarations.ToDictionary(d => d.Key, StringComparer.Ordinal);
        var rows = ReadEnrolmentFile(out _).Where(r => r.Enrolment == Enrolment.Open).ToList();

        // Non-vacuity: the estate's tree-scoped instruments are the population this state was
        // added for, so zero open rows means the classifier stopped firing, not that the
        // estate has no runtime identifiers.
        Assert.That(rows, Is.Not.Empty, "No row is enrolled open, so this gate would pass over nothing.");

        var failures = new List<string>();
        foreach (var row in rows)
        {
            if (!byKey.TryGetValue(row.Key, out var declaration))
            {
                continue;
            }

            if (!SourceCorpus.IsFullyRead(declaration))
            {
                var unread = declaration.Dimensions.First(d =>
                    d.Key.StartsWith('(') || (d.Value.Ambiguous && !SourceCorpus.IsOpenDimension(d.Value)));
                failures.Add($"{row.Key}: dimension {unread.Key} was not read ({unread.Value.Note}); open claims it was.");
            }
            else if (!declaration.Dimensions.Values.Any(SourceCorpus.IsOpenDimension))
            {
                failures.Add($"{row.Key}: no dimension is open by nature, so open is not the answer.");
            }
            else if (declaration.Dimensions.Values.Any(d => !d.Ambiguous && d.Values.Count > 1))
            {
                failures.Add($"{row.Key}: carries a bounded dimension that still owes a priming decision.");
            }
        }

        Assert.That(
            failures,
            Is.Empty,
            $"{failures.Count} open enrolment(s) are contradicted by the parser. open is a claim "
            + "that every dimension the analyser did not resolve to literals is a runtime "
            + "identifier by design:"
            + $"{Environment.NewLine}  {string.Join($"{Environment.NewLine}  ", failures)}");
    }

    [Test]
    public void A_row_an_earlier_generator_wrote_is_reseeded_and_a_curated_row_is_kept()
    {
        const string Key = "src/lattice/X.cs#Y";

        Assert.That(
            IsGeneratedRow(new EnrolmentRow(Key, Enrolment.Unresolved,
                "reason=unrecognised value expression: context.GrainId.ToString()")),
            Is.True,
            "the pre-dimension generator form must be re-seeded, or a call-site change never reaches its row");
        Assert.That(
            IsGeneratedRow(new EnrolmentRow(Key, Enrolment.Unresolved,
                "reason=dimension tree: unrecognised value expression: treeId")),
            Is.True);
        Assert.That(
            IsGeneratedRow(new EnrolmentRow(Key, Enrolment.Unresolved,
                "curated: the phase tag is primed by the coordinator on activation")),
            Is.False,
            "a curated unresolved row must be preserved verbatim");
        Assert.That(
            IsGeneratedRow(new EnrolmentRow(Key, Enrolment.Primed,
                "reason=unrecognised value expression: x")),
            Is.False,
            "only an unresolved or open row is ever generator-owned");
    }

    [Test]
    public void No_checked_in_row_carries_a_frozen_generator_reason()
    {
        var frozen = ReadEnrolmentFile(out _)
            .Where(r => r.Enrolment == Enrolment.Unresolved
                && r.Detail.StartsWith("reason=", StringComparison.Ordinal)
                && !IsGeneratedRow(r))
            .Select(r => $"{r.Key}: {r.Detail}")
            .ToList();

        Assert.That(
            frozen,
            Is.Empty,
            $"{frozen.Count} unresolved row(s) carry a reason= detail the rewriter treats as curated, "
            + "so regeneration can never correct them. Add the prefix to GeneratedReasonPrefixes:"
            + $"{Environment.NewLine}  {string.Join($"{Environment.NewLine}  ", frozen)}");
    }
}
