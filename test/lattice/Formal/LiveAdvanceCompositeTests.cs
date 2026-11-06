using System.Text.RegularExpressions;

namespace Orleans.Lattice.Tests.Formal;

/// <summary>
/// Pins every formula <c>spec/wal/WalLiveAdvanceComposite.tla</c> copies from
/// the five live-advance modules (epic #5605, member 6c) to its source. The
/// composite checks that each producer guarantee meets the consumer assumption
/// that relies on it, and it can only do that over copies: a module never
/// imports another's state. A copy that drifts from its source would leave the
/// composite checking an interface nobody runs.
/// <para>
/// Two directions are pinned. Each composite copy must EQUAL, after normalising
/// whitespace and comments and applying its declared identifier renames, the
/// source text it copies, so text added to a copy fails. And each source action
/// a copy is taken from has every top-level conjunct classified as copied or
/// explicitly excluded with a reason, so a guard added to a source action fails
/// until somebody decides whether the composite must copy it. Conjuncts that are
/// <c>UNCHANGED</c> clauses are frame conditions, never guards, and are excluded
/// by rule.
/// </para>
/// String work over <c>spec/</c> only; it needs no JVM.
/// </summary>
[TestFixture]
public sealed class LiveAdvanceCompositeTests
{
    private const string Composite = "WalLiveAdvanceComposite";

    private const string Bound = "a model bound, not an assumption about another module";

    private const string OwnReport = "C's report of the WAL shard's foreignHigh to its own activations (#5833), assumed of no other module";
    private const string ComposedAsResolve = "composed as the composite's ResolveDeferred step, checked by DeferredCloseMeetsCoreResolve (#5839)";
    private const string FreeAdvance = "the claimless advance (#5837), which the composite does not compose";
    private const string Effect = "an effect the composite's own step performs; the edge checks what is admitted, not how it is recorded";

    private static readonly Regex Definition = new(
        @"^(?<name>[A-Za-z_][A-Za-z0-9_]*)(\([^)]*\))?\s*==(?<rest>.*)$",
        RegexOptions.Compiled);

    private static readonly Regex BlockComment = new(@"\(\*.*?\*\)", RegexOptions.Compiled | RegexOptions.Singleline);

    private static readonly Regex LineComment = new(@"\\\*.*$", RegexOptions.Compiled);

    private static readonly Regex Whitespace = new(@"\s+", RegexOptions.Compiled);

    private static readonly Regex ActionProperty = new(@"^\[\]\[(?<body>.*)\]_vars$", RegexOptions.Compiled);

    private static readonly Dictionary<string, string> None = new(StringComparer.Ordinal);

    /// <summary>What part of a source definition a copy takes.</summary>
    public enum CopyShape
    {
        /// <summary>The whole definition.</summary>
        Whole,

        /// <summary>The action inside an action property <c>[][A]_vars</c>.</summary>
        ActionProperty,

        /// <summary>The listed top-level conjuncts of an action, joined in order.</summary>
        Conjuncts,
    }

    /// <summary>
    /// The composite definition <paramref name="Copy"/>, which must equal
    /// <paramref name="CopyShape"/> of definition <paramref name="Source"/> in
    /// <paramref name="Module"/> after <paramref name="Renames"/>.
    /// </summary>
    public sealed record CompositeCopy(
        string Copy,
        string Module,
        string Source,
        CopyShape CopyShape,
        IReadOnlyList<string> Conjuncts,
        IReadOnlyDictionary<string, string> Renames)
    {
        /// <inheritdoc />
        public override string ToString() => $"{Copy} <- {Module}.{Source}";
    }

    /// <summary>A source action's conjunct the composite deliberately does not copy.</summary>
    public sealed record ExcludedConjunct(string Module, string Source, string Conjunct, string Reason);

    /// <summary>A source action whose conjuncts are classified, for the test name.</summary>
    public sealed record CopiedSourceAction(string Module, string Source)
    {
        /// <inheritdoc />
        public override string ToString() => $"{Module}.{Source}";
    }

    private const string E = "WalAppendEpoch";
    private const string H = "WalHandOffFence";
    private const string Rf = "WalRoutingFloor";
    private const string B = "WalBulkSwitch";
    private const string C = "WalLiveAdvanceCore";

    /// <summary>Every definition the composite copies.</summary>
    public static IEnumerable<CompositeCopy> Copies()
    {
        yield return Whole("EAppendAdmits", E, "AppendAdmits", R("epoch[a]", "s", "memFence", "memFence[a]"));
        yield return Whole("EAlreadyDurable", E, "AlreadyDurable", R("epoch[a]", "s", "fence", "fence[a]", "memFence", "memFence[a]"));
        yield return Whole("ClaimHeadCoversPriorAppends", E, "ClaimHeadCoversPriorAppends", R("Acts", "Claimants"));
        yield return Whole("RaisedEpochUnique", E, "RaisedEpochUnique", None);
        yield return Whole("StaleEpochNeverAppends", E, "StaleEpochNeverAppends", None);
        yield return Of("EAppendLoaded", E, "OwnAlloc", R("memFence", "memFence[a]"), "memFence # Unloaded");
        yield return Of("EStaleUpdate", E, "OwnAlloc", None,
            @"staleAccepted' = (staleAccepted \/ epoch[a] < maxClaimed \/ epoch[a] <= lRetired)");
        yield return Of("EClaimAboveFloor", E, "ClaimWrite", R("epoch[a]", "s", "retired", "retired[a]"), "epoch[a] > retired");
        yield return Of("EClaimAboveEntry", E, "ClaimWrite",
            R("epoch[a]", "s", "fence", "fence[a]", "memFence", "memFence[a]", "AlreadyDurable(a)", "EAlreadyDurable(a, s)"),
            @"epoch[a] > Max2(fence, memFence) \/ AlreadyDurable(a)");
        yield return Of("ERaiseAboveEntry", E, "ClaimRaise", R("epoch[a]", "e", "Max2(fence, memFence)", "maxClaimed"),
            "epoch[a] >= Max2(fence, memFence)");
        yield return Of("ERaiseAboveFloor", E, "ClaimRaise", R("epoch[a]", "e", "retired", "lRetired"), "epoch[a] > retired");

        yield return Whole("MarkerReadsClosed", H, "MarkerReadsClosed", None);
        yield return Whole("HandOffBound", H, "HandOffBound", R("Stamped", "{Foreign}"));
        yield return Whole("CloseHeads", H, "CloseHeads", R("Stamped", "{Foreign}"));
        yield return Whole("DeferredHeadAtRaisedGeneration", H, "DeferredHeadAtRaisedGeneration", R("fEpoch", "epoch[\"F\"]"));
        yield return new("CloseHeadMonotone", H, "CloseHeadMonotone", CopyShape.ActionProperty, [], None);
        yield return Of("HAllocGuard", H, "ForeignAlloc", None, "fstamp >= fFence");

        yield return Whole("MarkerReadsInit", Rf, "MarkerReadsInit", None);
        yield return Whole("StraddlerBelowInit", Rf, "StraddlerBelowInit", R("author", "rAuthor", "closedAt", "hoClosedAt"));
        yield return Whole("ClaimAdmits", Rf, "ClaimAdmits", None);
        yield return new("InitHeadCoversPriorAllocations", Rf, "InitHeadCoversPriorAllocations", CopyShape.ActionProperty, [],
            R("closedAt", "hoClosedAt"));
        yield return Of("RAllocGuard", Rf, "Alloc", None, "loaded[i] # Unloaded", "pstamp[i] >= loaded[i]", "fo[i] => pstamp[i] > loaded[i]");
        yield return Of("RInitHead", Rf, "FirstInit", R("closedAt'", "claimHead'[\"S\"]"), "closedAt' = next");

        yield return Whole("BulkHintBound", B, "BulkHintBound", R("nUp", "born", "rpN", "rp[\"a1\"]", "heldN", "cache[\"a1\"]"));
        yield return new("SwitchCloseCoversPriorAllocations", B, "SwitchCloseCoversPriorAllocations", CopyShape.ActionProperty, [], R("closeHead", "bulkHead"));

        yield return Whole("HeadsAbove", C, "HeadsAbove", None);
        yield return Of("CClaim", C, "Claim", None,
            @"a \in up", @"a \notin stale", "~claimed[a]", "claimHead' = [claimHead EXCEPT ![a] = next]", "cur' = a",
            @"stale' = IF cur \in Acts THEN stale \cup {cur} ELSE stale");
        yield return Of("COwnAlloc", C, "OwnAlloc", None, @"a \in up", "cur = a");
        yield return Of("CForeignAlloc", C, "ForeignAlloc", None, @"hoOpen \/ ~mInit");
        yield return Of("CKeyMove", C, "KeyMove", R("gD + 1", "epoch'[\"F\"]"), "pend' = TRUE", "pendGen' = gD + 1");
        // The composite does not compose the claimless advance (FreeOn = FALSE), under which C's
        // resolved head is the allocator head; with it the head is the stale high plus one (#5837).
        yield return Of("CResolveDeferred", C, "ResolveDeferred", R("IF FreeOn THEN sHi + 1 ELSE next", "next"),
            "pend' = FALSE", "hoClosedAt' = Max2(hoClosedAt, IF FreeOn THEN sHi + 1 ELSE next)");
        // An eager foreign close covers foreign records only; the bulk switch's close is
        // composed against the lazy arm's bulk coverage (#5833, #5839).
        yield return Of("CClose", C, "Close", R("CloseCovers", "{Foreign}"),
            "phase = \"live\"", "hoOpen",
            @"\E h \in IF mInit THEN HeadsAbove(CloseCovers) ELSE 0..next : hoClosedAt' = Max2(hoClosedAt, h)");
        yield return Of("CBulkClose", C, "Close", R("CloseCovers", "{Foreign, PreAuthor}"),
            "phase = \"live\"", "hoOpen",
            @"\E h \in IF mInit THEN HeadsAbove(CloseCovers) ELSE 0..next : hoClosedAt' = Max2(hoClosedAt, h)");
        yield return Of("CStraddlerAlloc", C, "StraddlerAlloc", None, "recreated", "~reinit");
        yield return Of("CMarkInit", C, "MarkInit", None,
            "recreated", "~mInit", @"\E h \in HeadsAbove({Foreign, Straddler}) : hoClosedAt' = Max2(hoClosedAt, h)");
        yield return Of("CCreate", C, "Create", None,
            "phase = \"window\"",
            @"\E r \in -1..(next - 1), held \in SUBSET Allocated : /\ \A o \in Allocated : o <= r => o \in held /\ cache' = [cache EXCEPT ![""a1""] = held] /\ rp' = [rp EXCEPT ![""a1""] = r]");
    }

    /// <summary>Every source-action conjunct the composite does not copy, with why.</summary>
    public static IEnumerable<ExcludedConjunct> Exclusions()
    {
        yield return new(E, "OwnAlloc", @"a \in up", "the composite's append steps name their author; L's running activations are C's");
        yield return new(E, "OwnAlloc", "cstage[a] = 3", "vocabulary: only a raised claimant appends stamped");
        yield return new(E, "OwnAlloc", "AppendAdmits(a)", "copied as its definition, EAppendAdmits");
        yield return new(E, "OwnAlloc", "next < MaxOff", Bound);
        yield return new(E, "OwnAlloc", "next' = next + 1", Effect);
        yield return new(E, "OwnAlloc", "author' = [author EXCEPT ![next] = a]", Effect);
        yield return new(E, "ClaimWrite", @"a \in up", "the composite's claim steps name their claimant");
        yield return new(E, "ClaimWrite", "cstage[a] = 1", "E's claim stages are collapsed into one admitted claim");
        yield return new(E, "ClaimWrite", "fence' = Max2(fence, epoch[a])", Effect);
        yield return new(E, "ClaimWrite", "cstage' = [cstage EXCEPT ![a] = 2]", "E's claim stages are collapsed into one admitted claim");
        yield return new(E, "ClaimRaise", @"a \in up", "the composite's claim steps name their claimant");
        yield return new(E, "ClaimRaise", "cstage[a] = 2", "E's claim stages are collapsed into one admitted claim");
        yield return new(E, "ClaimRaise", "memFence' = epoch[a]", Effect);
        yield return new(E, "ClaimRaise", "claimHead' = [claimHead EXCEPT ![a] = next]",
            "the head is free in the composite and kept only if ClaimHeadCoversPriorAppends holds, E's guarantee");
        yield return new(E, "ClaimRaise", @"prior' = [prior EXCEPT ![a] = {o \in Offs : author[o] # NoAuthor}]", Effect);
        yield return new(E, "ClaimRaise", "maxClaimed' = Max2(maxClaimed, epoch[a])", Effect);
        yield return new(E, "ClaimRaise", "cstage' = [cstage EXCEPT ![a] = 3]", Effect);

        yield return new(H, "ForeignAlloc", "fpend", "H's admitted-write bookkeeping; the composite's step is the admitted write");
        yield return new(H, "ForeignAlloc", "next < MaxOff", Bound);
        yield return new(H, "ForeignAlloc", "fpend' = FALSE", Effect);
        yield return new(H, "ForeignAlloc", "fkind' = \"point\"", Effect);
        yield return new(H, "ForeignAlloc", "fstamp' = 0", Effect);
        yield return new(H, "ForeignAlloc", "next' = next + 1", Effect);
        yield return new(H, "ForeignAlloc", "author' = [author EXCEPT ![next] = \"f\"]", Effect);

        yield return new(Rf, "Alloc", "pend[i]", "R's admitted-write bookkeeping; the composite's step is the admitted write");
        yield return new(Rf, "Alloc", "next < MaxOff", Bound);
        yield return new(Rf, "Alloc", "next' = next + 1", Effect);
        yield return new(Rf, "Alloc", "author' = [author EXCEPT ![next] = i]", Effect);
        yield return new(Rf, "Alloc", "pend' = [pend EXCEPT ![i] = FALSE]", Effect);
        yield return new(Rf, "Alloc", "pstamp' = [pstamp EXCEPT ![i] = 0]", Effect);
        yield return new(Rf, "FirstInit", "recreated", "the composite's FirstInit carries the same precondition");
        yield return new(Rf, "FirstInit", "~mInit", "the composite's FirstInit carries the same precondition");
        yield return new(Rf, "FirstInit", @"~legacy \/ caught", "the composite's FirstInit carries the same precondition");
        yield return new(Rf, "FirstInit", "rowFloor[\"s\"] + 1 <= MaxGen", Bound);
        yield return new(Rf, "FirstInit", "gen' = [gen EXCEPT ![\"s\"] = rowFloor[\"s\"] + 1]",
            "R's protocol picks the generation; the composite claims any generation E's claim write admits");
        yield return new(Rf, "FirstInit", "entryRow' = [entryRow EXCEPT ![\"s\"] = Max2(@, gen'[\"s\"])]", Effect);
        yield return new(Rf, "FirstInit", "loaded' = [loaded EXCEPT ![\"s\"] = Max2(@, gen'[\"s\"])]", Effect);
        yield return new(Rf, "FirstInit", "mInit' = TRUE", Effect);
        yield return new(Rf, "FirstInit", "markNew' = TRUE", "R's re-seed identity (#5716), outside the vocabulary");
        yield return new(Rf, "FirstInit", "live' = [live EXCEPT ![\"s\"] = TRUE]", "R's shard-root liveness, outside the vocabulary");

        yield return new(C, "Claim", "claimed' = [claimed EXCEPT ![a] = TRUE]", "the composite raises cstage, from which claimed is derived");
        yield return new(C, "Claim", "ReportForeign(a)", OwnReport);
        yield return new(C, "Claim", "IF pend /\\ gD >= pendGen THEN ResolveDeferred ELSE UNCHANGED <<moveVars, hoClosedAt>>", ComposedAsResolve);
        yield return new(C, "Claim", "LatchAdmitsClaim", FreeAdvance);
        yield return new(C, "Claim", @"dClaim' = (dClaim \/ (WithLatch /\ BoundPending))", "a ghost of C's own UnlinkedNeverClaims (#5866 B2), not a guard");
        yield return new(C, "OwnAlloc", "ReportForeign(a)", OwnReport);
        yield return new(C, "OwnAlloc", "ReportStale(a)", FreeAdvance);
        yield return new(C, "ForeignAlloc", "~WithSplitBirth", FreeAdvance);
        yield return new(C, "Close", "~WithSplitBirth", FreeAdvance);
        yield return new(C, "KeyMove", "WithKeyMove", "a configuration switch");
        yield return new(C, "KeyMove", @"WithSplitBirth => ~hoOpen /\ LinkDriven", FreeAdvance);
        yield return new(C, "KeyMove", "phase = \"live\"", "the composite's DeferredClose needs F's open window, which a live leaf's marker holds");
        yield return new(C, "KeyMove", "~moved", "C's budget of one key-moving close, a model bound");
        yield return new(C, "KeyMove", "moved' = TRUE", Effect);
        yield return new(C, "KeyMove", "gD' = gD + 1", "the raise is the composite step's own effect; C raises by one, and any raise above F's generation serves");
        yield return new(C, "ResolveDeferred", "gL' = Max2(gL, pendGen)", "the re-claim is the composite step's E claim at the partition, kept only if E's claim write admits it");
        yield return new(C, "ResolveDeferred", "reclaims' = reclaims + (IF pendGen > gL THEN 1 ELSE 0)", "C's cost witness, outside the vocabulary");
        yield return new(Rf, "FirstInit", "fo' = [fo EXCEPT ![\"s\"] = FALSE]", Effect);
        yield return new(C, "OwnAlloc", "next < MaxOff", Bound);
        yield return new(C, "OwnAlloc", "next' = next + 1", Effect);
        yield return new(C, "OwnAlloc", "author' = [author EXCEPT ![next] = a]", Effect);
        yield return new(C, "OwnAlloc", @"inflight' = inflight \cup {next}", "C's durability state, outside the vocabulary");
        yield return new(C, "OwnAlloc", "seenU' = [seenU EXCEPT ![a] = Max2(@, ReportU)]", "C's own unstamped watermark, assumed of no other module");
        yield return new(C, "ForeignAlloc", "next < MaxOff", Bound);
        yield return new(C, "ForeignAlloc", "next' = next + 1", Effect);
        yield return new(C, "ForeignAlloc", "author' = [author EXCEPT ![next] = Foreign]", Effect);
        yield return new(C, "ForeignAlloc", @"inflight' = inflight \cup {next}", "C's durability state, outside the vocabulary");
        yield return new(C, "Close", "hoOpen' = FALSE", Effect);
        yield return new(C, "StraddlerAlloc", "~spUsed", "C's budget of one straddler, a model bound");
        yield return new(C, "StraddlerAlloc", "next < MaxOff", Bound);
        yield return new(C, "StraddlerAlloc", "spUsed' = TRUE", Effect);
        yield return new(C, "StraddlerAlloc", "next' = next + 1", Effect);
        yield return new(C, "StraddlerAlloc", "author' = [author EXCEPT ![next] = Straddler]", Effect);
        yield return new(C, "StraddlerAlloc", @"inflight' = inflight \cup {next}", "C's durability state, outside the vocabulary");
        yield return new(C, "MarkInit", "mInit' = TRUE", Effect);
        yield return new(C, "MarkInit", "reinit' = TRUE", Effect);
        yield return new(C, "Create", "phase' = \"live\"", Effect);
        yield return new(C, "Create", "up' = {\"a1\"}", Effect);
    }

    /// <summary>Every source action some copy takes conjuncts from.</summary>
    public static IEnumerable<CopiedSourceAction> SourceActions() =>
        Copies().Where(copied => copied.CopyShape == CopyShape.Conjuncts)
            .Select(copied => new CopiedSourceAction(copied.Module, copied.Source))
            .Distinct();

    [TestCaseSource(nameof(Copies))]
    public void Each_composite_copy_equals_its_source(CompositeCopy copied)
    {
        ArgumentNullException.ThrowIfNull(copied);

        var copy = Body(DefinitionOf(Composite, copied.Copy));
        Assert.That(copy, Is.EqualTo(Expected(copied, DefinitionOf(copied.Module, copied.Source))),
            $"{Composite}.{copied.Copy} must equal its source {copied.Module}.{copied.Source} after the declared renames");
    }

    [TestCaseSource(nameof(SourceActions))]
    public void Each_source_action_conjunct_is_copied_or_excluded(CopiedSourceAction action)
    {
        ArgumentNullException.ThrowIfNull(action);

        var conjuncts = Classifiable(TopLevelConjuncts(DefinitionOf(action.Module, action.Source)));
        // One source action may be copied more than once under different renames (C's Close
        // for the eager foreign close and for the bulk switch's close, #5839); a conjunct
        // either copy takes counts once, and a conjunct both copied and excluded is still caught.
        var classified = Copies()
            .Where(copied => copied.CopyShape == CopyShape.Conjuncts && copied.Module == action.Module && copied.Source == action.Source)
            .SelectMany(copied => copied.Conjuncts.Select(Normalise))
            .Distinct(StringComparer.Ordinal)
            .Concat(Exclusions()
                .Where(excluded => excluded.Module == action.Module && excluded.Source == action.Source)
                .Select(excluded => Normalise(excluded.Conjunct)))
            .ToArray();

        Assert.Multiple(() =>
        {
            Assert.That(conjuncts, Is.Not.Empty, $"{action} parsed to no conjuncts");
            Assert.That(conjuncts.Except(classified), Is.Empty,
                $"{action} has a conjunct neither copied into the composite nor excluded with a reason");
            Assert.That(classified.Except(conjuncts), Is.Empty,
                $"{action} no longer has a conjunct this test classifies; update the composite and this table together");
            Assert.That(classified, Is.Unique, $"{action} classifies a conjunct twice");
        });
    }

    [Test]
    public void Every_exclusion_names_an_action_a_copy_is_taken_from()
    {
        var actions = SourceActions().ToHashSet();

        Assert.That(Exclusions().Where(excluded => !actions.Contains(new CopiedSourceAction(excluded.Module, excluded.Source))),
            Is.Empty);
    }

    [Test]
    public void Every_live_advance_module_contributes_a_copy()
    {
        Assert.That(Copies().Select(copied => copied.Module).Distinct(), Is.EquivalentTo(new[] { E, H, Rf, B, C }),
            "a module the composite copies nothing from is a module the interface check cannot see drift in");
    }

    [Test]
    public void A_guard_added_to_a_source_action_is_unclassified()
    {
        const string source = "Act(a) ==\n    /\\ a \\in up\n    /\\ x > 0 \\* note\n    /\\ \\E y \\in S :\n        /\\ y = a\n    /\\ UNCHANGED v\n";
        const string widened = "Act(a) ==\n    /\\ a \\in up\n    /\\ x > 0\n    /\\ z = 1\n    /\\ \\E y \\in S :\n        /\\ y = a\n    /\\ UNCHANGED v\n";

        Assert.Multiple(() =>
        {
            Assert.That(Classifiable(TopLevelConjuncts(DefinitionIn(source, "Act", "test"))),
                Is.EqualTo(new[] { @"a \in up", "x > 0", @"\E y \in S : /\ y = a" }));
            Assert.That(Classifiable(TopLevelConjuncts(DefinitionIn(widened, "Act", "test")))
                    .Except(Classifiable(TopLevelConjuncts(DefinitionIn(source, "Act", "test")))),
                Is.EqualTo(new[] { "z = 1" }), "the new conjunct is what the classification test reports");
        });
    }

    [Test]
    public void Text_added_to_a_copy_breaks_equality()
    {
        var copied = Of("Copy", "M", "Act", R("x", "y"), @"a \in up", "x > 0");
        var source = DefinitionIn("Act(a) ==\n    /\\ a \\in up\n    /\\ x > 0\n    /\\ UNCHANGED v\n", "Act", "test");

        Assert.Multiple(() =>
        {
            Assert.That(Body(DefinitionIn("Copy(a) ==\n    /\\ a \\in up\n    /\\ y > 0\n", "Copy", "test")),
                Is.EqualTo(Expected(copied, source)));
            Assert.That(Body(DefinitionIn("Copy(a) ==\n    /\\ a \\in up\n    /\\ y > 0\n    /\\ TRUE\n", "Copy", "test")),
                Is.Not.EqualTo(Expected(copied, source)));
            Assert.That(Rename("memFence # fence", R("fence", "fence[a]")), Is.EqualTo("memFence # fence[a]"));
            Assert.That(() => DefinitionIn("A == 1\n", "D", "test"), Throws.InvalidOperationException);
        });
    }

    private static CompositeCopy Whole(string copy, string module, string source, IReadOnlyDictionary<string, string> renames) =>
        new(copy, module, source, CopyShape.Whole, [], renames);

    private static CompositeCopy Of(string copy, string module, string source, IReadOnlyDictionary<string, string> renames,
        params string[] conjuncts) =>
        new(copy, module, source, CopyShape.Conjuncts, conjuncts, renames);

    private static Dictionary<string, string> R(params string[] pairs)
    {
        var map = new Dictionary<string, string>(StringComparer.Ordinal);
        for (var i = 0; i < pairs.Length; i += 2)
        {
            map.Add(pairs[i], pairs[i + 1]);
        }

        return map;
    }

    private static string Expected(CompositeCopy copied, IReadOnlyList<string> source)
    {
        switch (copied.CopyShape)
        {
            case CopyShape.Whole:
                return Rename(Body(source), copied.Renames);
            case CopyShape.ActionProperty:
                var match = ActionProperty.Match(Body(source));
                if (!match.Success)
                {
                    throw new InvalidOperationException($"{copied.Module}.{copied.Source} is not an action property [][A]_vars.");
                }

                return Rename(match.Groups["body"].Value, copied.Renames);
            default:
                var conjuncts = TopLevelConjuncts(source);
                var missing = copied.Conjuncts.Select(Normalise).Where(conjunct => !conjuncts.Contains(conjunct)).ToArray();
                if (missing.Length > 0)
                {
                    throw new InvalidOperationException(
                        $"{copied.Module}.{copied.Source} has no conjunct {string.Join(", ", missing)}.");
                }

                return string.Join(@" /\ ", copied.Conjuncts.Select(conjunct => Rename(Normalise(conjunct), copied.Renames)));
        }
    }

    private static IReadOnlyList<string> DefinitionOf(string module, string name)
    {
        var spec = SpecModuleCatalogue.Repository().Single(m => m.Name == module);
        return DefinitionIn(spec.ReadSpecification(), name, module);
    }

    /// <summary>The definition's lines: the remainder of its header line, then its indented body.</summary>
    private static IReadOnlyList<string> DefinitionIn(string text, string name, string label)
    {
        var lines = BlockComment.Replace(text.ReplaceLineEndings("\n"), " ").Split('\n')
            .Select(line => LineComment.Replace(line, string.Empty))
            .ToArray();
        for (var i = 0; i < lines.Length; i++)
        {
            var match = Definition.Match(lines[i]);
            if (!match.Success || match.Groups["name"].Value != name)
            {
                continue;
            }

            var body = new List<string> { match.Groups["rest"].Value };
            for (var j = i + 1; j < lines.Length; j++)
            {
                if (lines[j].Length > 0 && !char.IsWhiteSpace(lines[j][0]))
                {
                    break;
                }

                body.Add(lines[j]);
            }

            return body;
        }

        throw new InvalidOperationException($"{label} defines no '{name}'.");
    }

    /// <summary>The whole body, normalised, with a leading conjunction bullet dropped.</summary>
    private static string Body(IReadOnlyList<string> definition)
    {
        var body = Normalise(string.Join('\n', definition));
        return body.StartsWith(@"/\ ", StringComparison.Ordinal) ? body[3..] : body;
    }

    /// <summary>
    /// The top-level conjuncts of a bulleted body: each line at the first
    /// bullet's indentation that opens with <c>/\</c> starts one, and deeper
    /// lines continue it. A body that is not a bulleted list is one conjunct.
    /// </summary>
    private static IReadOnlyList<string> TopLevelConjuncts(IReadOnlyList<string> definition)
    {
        var lines = definition.Skip(1).Where(line => line.Trim().Length > 0).ToArray();
        if (definition[0].Trim().Length > 0 || lines.Length == 0 || !lines[0].TrimStart().StartsWith(@"/\", StringComparison.Ordinal))
        {
            return [Body(definition)];
        }

        var indent = lines[0].Length - lines[0].TrimStart().Length;
        var conjuncts = new List<string>();
        foreach (var line in lines)
        {
            var depth = line.Length - line.TrimStart().Length;
            var trimmed = line.Trim();
            if (depth == indent && trimmed.StartsWith(@"/\", StringComparison.Ordinal))
            {
                conjuncts.Add(trimmed[2..]);
            }
            else if (conjuncts.Count > 0)
            {
                conjuncts[^1] += " " + trimmed;
            }
        }

        return conjuncts.Select(Normalise).ToArray();
    }

    private static string[] Classifiable(IReadOnlyList<string> conjuncts) =>
        conjuncts.Where(conjunct => !conjunct.StartsWith("UNCHANGED", StringComparison.Ordinal)).ToArray();

    private static string Normalise(string text) => Whitespace.Replace(text, " ").Trim();

    private static string Rename(string text, IReadOnlyDictionary<string, string> renames)
    {
        if (renames.Count == 0)
        {
            return text;
        }

        var pattern = string.Join('|', renames.Keys
            .OrderByDescending(key => key.Length)
            .Select(key => @"(?<![A-Za-z0-9_])" + Regex.Escape(key)
                + (char.IsLetterOrDigit(key[^1]) || key[^1] == '_' ? @"(?![A-Za-z0-9_])" : string.Empty)));
        return Regex.Replace(text, pattern, match => renames[match.Value]);
    }
}
