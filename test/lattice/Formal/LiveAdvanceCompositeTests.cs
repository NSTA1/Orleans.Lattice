using System.Text.RegularExpressions;

namespace Orleans.Lattice.Tests.Formal;

/// <summary>
/// Pins every formula <c>spec/wal/WalLiveAdvanceComposite.tla</c> copies from
/// the five live-advance modules (epic #5605, member 6c) to its source. The
/// composite checks that each producer guarantee meets the consumer assumption
/// that relies on it, and it can only do that over copies: a module never
/// imports another's state. A copy that drifts from its source would leave the
/// composite checking an interface nobody runs, so each copy is compared, after
/// its declared identifier renames, with the definition it was taken from.
/// String work over <c>spec/</c> only; it needs no JVM.
/// </summary>
[TestFixture]
public sealed class LiveAdvanceCompositeTests
{
    private const string Composite = "WalLiveAdvanceComposite";

    private static readonly Regex Definition = new(
        @"^(?<name>[A-Za-z_][A-Za-z0-9_]*)(\([^)]*\))?\s*==(?<rest>.*)$",
        RegexOptions.Compiled);

    private static readonly Regex BlockComment = new(@"\(\*.*?\*\)", RegexOptions.Compiled | RegexOptions.Singleline);

    private static readonly Regex LineComment = new(@"\\\*.*$", RegexOptions.Compiled | RegexOptions.Multiline);

    private static readonly Regex Whitespace = new(@"\s+", RegexOptions.Compiled);

    private static readonly Dictionary<string, string> None = new(StringComparer.Ordinal);

    /// <summary>
    /// One copied formula: <paramref name="Fragment"/> of definition
    /// <paramref name="Source"/> in module <paramref name="Module"/>, or the
    /// whole definition when <paramref name="Fragment"/> is null, appears in the
    /// composite's definition <paramref name="Copy"/> after
    /// <paramref name="Renames"/>.
    /// </summary>
    public sealed record Copied(
        string Module,
        string Source,
        string? Fragment,
        string Copy,
        IReadOnlyDictionary<string, string> Renames)
    {
        /// <inheritdoc />
        public override string ToString() =>
            $"{Module}.{Source} -> {Copy}" + (Fragment is null ? string.Empty : $" [{Fragment}]");
    }

    /// <summary>Every formula the composite copies, with its renames.</summary>
    public static IEnumerable<Copied> Copies()
    {
        const string e = "WalAppendEpoch";
        yield return new(e, "OwnAlloc", "memFence # Unloaded", "EAppendLoaded", R("memFence", "memFence[a]"));
        yield return new(e, "AppendAdmits", null, "EAppendAdmits", R("epoch[a]", "s", "memFence", "memFence[a]"));
        yield return new(e, "AlreadyDurable", null, "EAlreadyDurable",
            R("epoch[a]", "s", "fence", "fence[a]", "memFence", "memFence[a]"));
        yield return new(e, "ClaimWrite", "epoch[a] > retired", "EClaimAboveFloor", R("epoch[a]", "s", "retired", "retired[a]"));
        yield return new(e, "ClaimWrite", @"epoch[a] > Max2(fence, memFence) \/ AlreadyDurable(a)", "EClaimAboveEntry",
            R("epoch[a]", "s", "fence", "fence[a]", "memFence", "memFence[a]", "AlreadyDurable(a)", "EAlreadyDurable(a, s)"));
        yield return new(e, "ClaimRaise", "epoch[a] >= Max2(fence, memFence)", "ERaiseAboveEntry",
            R("epoch[a]", "e", "Max2(fence, memFence)", "maxClaimed"));
        yield return new(e, "ClaimRaise", "epoch[a] > retired", "ERaiseAboveFloor", R("epoch[a]", "e", "retired", "lRetired"));
        yield return new(e, "ClaimHeadCoversPriorAppends", null, "ClaimHeadCoversPriorAppends", R("Acts", "Claimants"));
        yield return new(e, "RaisedEpochUnique", null, "RaisedEpochUnique", None);
        yield return new(e, "StaleEpochNeverAppends", null, "StaleEpochNeverAppends", None);
        yield return new(e, "OwnAlloc", @"staleAccepted' = (staleAccepted \/ epoch[a] < maxClaimed \/ epoch[a] <= lRetired)",
            "EStaleUpdate", None);

        const string h = "WalHandOffFence";
        yield return new(h, "MarkerReadsClosed", null, "MarkerReadsClosed", None);
        yield return new(h, "HandOffBound", null, "HandOffBound", R("Stamped", "{Foreign}"));
        yield return new(h, "CloseHeadMonotone", "hoClosedAt' >= hoClosedAt", "CloseHeadMonotone", None);
        yield return new(h, "ForeignAlloc", "fstamp >= fFence", "HAllocGuard", None);
        yield return new(h, "CloseHeads", null, "CloseHeads", R("Stamped", "{Foreign}"));

        const string r = "WalRoutingFloor";
        yield return new(r, "MarkerReadsInit", null, "MarkerReadsInit", None);
        yield return new(r, "StraddlerBelowInit", null, "StraddlerBelowInit", R("author", "rAuthor", "closedAt", "hoClosedAt"));
        yield return new(r, "Alloc", "loaded[i] # Unloaded", "RAllocGuard", None);
        yield return new(r, "Alloc", "pstamp[i] >= loaded[i]", "RAllocGuard", None);
        yield return new(r, "ClaimAdmits", null, "ClaimAdmits", None);
        yield return new(r, "InitHeadCoversPriorAllocations",
            @"~mInit /\ mInit' => \A o \in Offs : author[o] # NoAuthor => o < closedAt'", "InitHeadCoversPriorAllocations",
            R("closedAt'", "hoClosedAt'"));
        yield return new(r, "FirstInit", "closedAt' = next", "FirstInit", R("closedAt'", "claimHead'[\"S\"]"));

        const string b = "WalBulkSwitch";
        yield return new(b, "BulkHintBound", null, "BulkHintBound",
            R("nUp", "born", "rpN", "rp[\"a1\"]", "heldN", "cache[\"a1\"]"));

        const string c = "WalLiveAdvanceCore";
        yield return new(c, "Claim", @"a \in up", "CClaim", None);
        yield return new(c, "Claim", @"a \notin stale", "CClaim", None);
        yield return new(c, "Claim", "~claimed[a]", "CClaim", None);
        yield return new(c, "Claim", "claimHead' = [claimHead EXCEPT ![a] = next]", "CClaim", None);
        yield return new(c, "Claim", "cur' = a", "CClaim", None);
        yield return new(c, "OwnAlloc", @"a \in up", "COwnAlloc", None);
        yield return new(c, "OwnAlloc", "cur = a", "COwnAlloc", None);
        yield return new(c, "ForeignAlloc", @"hoOpen \/ ~mInit", "CForeignAlloc", None);
        yield return new(c, "Close", @"\E h \in IF mInit THEN HeadsAbove({Foreign}) ELSE 0..next : hoClosedAt' = Max2(hoClosedAt, h)",
            "CClose", None);
        yield return new(c, "HeadsAbove", null, "HeadsAbove", None);
        yield return new(c, "StraddlerAlloc", "recreated", "CStraddlerAlloc", None);
        yield return new(c, "StraddlerAlloc", "~reinit", "CStraddlerAlloc", None);
        yield return new(c, "MarkInit", @"\E h \in HeadsAbove({Foreign, Straddler}) : hoClosedAt' = Max2(hoClosedAt, h)",
            "CMarkInit", None);
        yield return new(c, "Create",
            @"\E r \in -1..(hint - 1), held \in SUBSET Allocated : /\ \A o \in Allocated : o <= r => o \in held /\ cache' = [cache EXCEPT ![""a1""] = held] /\ rp' = [rp EXCEPT ![""a1""] = r]",
            "CCreate", None);
    }

    [TestCaseSource(nameof(Copies))]
    public void Each_copied_formula_matches_its_source(Copied copied)
    {
        ArgumentNullException.ThrowIfNull(copied);

        var source = DefinitionOf(copied.Module, copied.Source);
        var fragment = copied.Fragment is null ? source : Normalise(copied.Fragment);
        var copy = DefinitionOf(Composite, copied.Copy);
        var expected = Rename(fragment, copied.Renames);

        Assert.Multiple(() =>
        {
            Assert.That(source, Does.Contain(fragment),
                $"{copied.Module}.{copied.Source} no longer contains the copied text; update the composite's copy and this entry together");
            Assert.That(copy, Does.Contain(expected),
                $"{Composite}.{copied.Copy} has drifted from {copied.Module}.{copied.Source}");
        });
    }

    [Test]
    public void Every_live_advance_module_contributes_a_copy()
    {
        var modules = Copies().Select(copied => copied.Module).Distinct().ToArray();

        Assert.That(modules, Is.EquivalentTo(new[]
        {
            "WalAppendEpoch", "WalHandOffFence", "WalRoutingFloor", "WalBulkSwitch", "WalLiveAdvanceCore",
        }), "a module the composite copies nothing from is a module the interface check cannot see drift in");
    }

    [Test]
    public void Definition_extraction_reads_a_whole_definition_and_strips_comments()
    {
        const string text = "A == 1\nB(x) ==\n    /\\ x > 0 \\* note\n    /\\ (* gap *) x < 2\n\nC == 3\n";

        Assert.Multiple(() =>
        {
            Assert.That(DefinitionIn(text, "B", "test"), Is.EqualTo(@"/\ x > 0 /\ x < 2"));
            Assert.That(() => DefinitionIn(text, "D", "test"), Throws.InvalidOperationException);
            Assert.That(Rename("memFence # fence", R("fence", "fence[a]")), Is.EqualTo("memFence # fence[a]"));
        });
    }

    private static Dictionary<string, string> R(params string[] pairs)
    {
        var map = new Dictionary<string, string>(StringComparer.Ordinal);
        for (var i = 0; i < pairs.Length; i += 2)
        {
            map.Add(pairs[i], pairs[i + 1]);
        }

        return map;
    }

    private static string DefinitionOf(string module, string name)
    {
        var spec = SpecModuleCatalogue.Repository().Single(m => m.Name == module);
        return DefinitionIn(spec.ReadSpecification(), name, module);
    }

    private static string DefinitionIn(string text, string name, string label)
    {
        var lines = BlockComment.Replace(text.ReplaceLineEndings("\n"), " ").Split('\n');
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

            return Normalise(string.Join('\n', body));
        }

        throw new InvalidOperationException($"{label} defines no '{name}'.");
    }

    private static string Normalise(string text) =>
        Whitespace.Replace(LineComment.Replace(text, string.Empty), " ").Trim();

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
