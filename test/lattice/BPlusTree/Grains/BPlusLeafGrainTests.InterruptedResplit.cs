using NUnit.Framework;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Testing.Hygiene;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;
using System.Text;
using System.Text.RegularExpressions;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Regression cover for issue #3265: an interrupted <em>second or later</em>
/// division of a leaf must be resumed, never restarted with a freshly minted
/// sibling identity.
/// </summary>
/// <remarks>
/// <para>
/// The defect was a type error in the recovery predicate rather than in the
/// split itself. Every site asking "was a split interrupted?" tested
/// <c>SplitState == SplitInProgress</c>, but <see cref="SplitState"/> is a
/// monotone max lattice merged with <c>max</c> and never decreasing, so
/// <c>SplitComplete</c> absorbs the <c>Merge(SplitInProgress)</c> that opens
/// every later division. For any leaf that had already split once - which is
/// every leaf in a tree under sustained write load - the predicate answered
/// "no" unconditionally, and it answered "no" most confidently for the leaves
/// dividing most often.
/// </para>
/// <para>
/// What followed was not a stale flag. The next overflow fell through to a
/// fresh split, which minted a new sibling identity, spliced it into the
/// doubly-linked sibling chain ahead of the abandoned one, and overwrote
/// <c>SplitSiblingId</c>. The abandoned sibling stayed durable, stayed in the
/// chain, and kept the WAL materialiser pin it had published at birth, but no
/// parent was ever taught to route to it. A leaf no descent reaches receives no
/// writes, materialises nothing, and checkpoints nothing, so its pin can never
/// advance - and a pin that can never advance freezes the shard's trim floor
/// permanently. Trim is upstream of compaction, so the tree's write-ahead log
/// then grows with no bound at all.
/// </para>
/// <para>
/// These fixtures drive the public <c>SetAsync</c> entry point rather than
/// reaching past it, because the defect was never that resumption did not work
/// when it was invoked - it was that nothing invoked it. A fixture that called
/// the recovery helper directly would have passed throughout.
/// </para>
/// </remarks>
public partial class BPlusLeafGrainTests
{
    /// <summary>
    /// Pins the lattice fact the defect rested on. If this ever fails, the
    /// merge semantics changed and the recovery predicate should be revisited
    /// alongside it.
    /// </summary>
    [Test]
    public void Merge_of_SplitInProgress_into_SplitComplete_cannot_reopen_the_split()
    {
        Assert.That(
            SplitState.SplitComplete.Merge(SplitState.SplitInProgress),
            Is.EqualTo(SplitState.SplitComplete),
            "SplitState is a monotone max lattice, so a leaf that has completed one division "
            + "can never read SplitInProgress again. Any recovery predicate testing equality "
            + "against SplitInProgress is therefore dead for every leaf after its first split.");
    }

    /// <summary>
    /// The caller-level regression. A leaf carrying the exact durable state a
    /// second division leaves behind when it is interrupted must resume that
    /// division on its next write, not mint a second sibling and strand the
    /// first.
    /// </summary>
    [Test]
    public async Task Interrupted_second_split_resumes_instead_of_minting_a_second_sibling()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state, maxLeafKeys: 4);

        await grain.SetAsync("a", Encoding.UTF8.GetBytes("1"));
        await grain.SetAsync("b", Encoding.UTF8.GetBytes("2"));
        await grain.SetAsync("m", Encoding.UTF8.GetBytes("3"));

        // The durable state a leaf is actually in after an interrupted SECOND
        // division. Every field here is what the split's intent block persists:
        // SplitState is the result of Merge(SplitInProgress) against a leaf that
        // already completed a division, so it reads SplitComplete - that is the
        // whole point, not a shortcut. SplitSiblingId and OldNextSibling are the
        // outstanding intent.
        var downstreamNeighbour = GrainId.Create("leaf", Guid.NewGuid().ToString());
        var firstMintedSibling = GrainId.Create("leaf", Guid.NewGuid().ToString());

        state.State.TreeId = "test-tree";
        state.State.SplitState = SplitState.SplitComplete;
        state.State.SplitKey = "m";
        state.State.SplitSiblingId = firstMintedSibling;
        state.State.OldNextSibling = downstreamNeighbour;
        state.State.NextSibling = firstMintedSibling;

        await grain.SetAsync("z", Encoding.UTF8.GetBytes("4"));

        // Drive the leaf well past its capacity. This is the part that must not
        // be trimmed away: without enough keys to actually overflow, SplitAsync
        // is never reached, SplitSiblingId is never reassigned, and the fixture
        // passes against the unfixed source while proving nothing.
        //
        // Reaching the mint at all is the violation, so it is reported as one.
        // Against the unfixed source the overflow re-enters SplitAsync and
        // executes the unconditional Guid.NewGuid() mint; what that hits first
        // is incidental, and letting it surface raw would report a defect in
        // the invariant this fixture exists to state as a defect in the test
        // harness.
        try
        {
            for (var i = 0; i < 12; i++)
            {
                await grain.SetAsync($"n{i:D2}", Encoding.UTF8.GetBytes("v"));
            }
        }
        catch (Exception ex)
        {
            Assert.Fail(
                "The overflow re-entered the split mint instead of resuming the outstanding "
                + "division. Minting a second sibling abandons the first one in the chain with "
                + "no parent routing to it, and the WAL materialiser pin it published at birth "
                + "then gates the shard's trim floor for ever. Underlying failure: " + ex.Message);
        }

        // Before the fix this is a different Guid: the recovery predicate read
        // SplitComplete, declined to resume, and the overflow minted afresh.
        // firstMintedSibling was then unreachable by descent for good, while
        // still holding the pin that gates the trim floor.
        Assert.That(
            state.State.SplitSiblingId,
            Is.EqualTo(firstMintedSibling),
            "The interrupted division must be resumed against its existing sibling. A second "
            + "mint abandons the first sibling in the chain with no parent routing to it, and "
            + "the WAL materialiser pin it published at birth then gates the trim floor for ever.");

        Assert.That(
            state.State.NextSibling,
            Is.EqualTo(firstMintedSibling),
            "Resuming must leave the originally minted sibling spliced into the chain rather "
            + "than displacing it with a newer one.");
    }

    /// <summary>
    /// The same invariant stated from the other side: a leaf whose division is
    /// still outstanding must report itself as interrupted regardless of what
    /// <see cref="SplitState"/> says, because <c>OldNextSibling</c> is the only
    /// field that tracks an outstanding division across a leaf's whole life.
    /// </summary>
    [Test]
    public async Task First_split_of_a_leaf_still_resumes_on_the_legacy_in_progress_marker()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state, maxLeafKeys: 4);

        await grain.SetAsync("a", Encoding.UTF8.GetBytes("1"));
        await grain.SetAsync("m", Encoding.UTF8.GetBytes("2"));

        var siblingId = GrainId.Create("leaf", Guid.NewGuid().ToString());
        state.State.TreeId = "test-tree";
        state.State.SplitState = SplitState.SplitInProgress;
        state.State.SplitKey = "m";
        state.State.SplitSiblingId = siblingId;
        state.State.NextSibling = siblingId;

        var result = await grain.SetAsync("z", Encoding.UTF8.GetBytes("3"));

        Assert.That(result, Is.Not.Null);
        Assert.That(
            result!.NewSiblingId,
            Is.EqualTo(siblingId),
            "Widening the recovery predicate must not regress the first-division case it "
            + "already covered.");
    }

    /// <summary>
    /// Census gate for issue #3265: no site in <c>src/lattice</c> may ask
    /// "is a division of this node outstanding?" through a direct comparison
    /// against <see cref="SplitState.SplitInProgress"/>. Only the two
    /// <c>HasInterruptedSplit</c> predicates may name it.
    /// </summary>
    /// <remarks>
    /// <para>
    /// This gate exists because the behavioural test above cannot reach every
    /// site, and the sites it cannot reach are the ones that actually hid the
    /// defect. The first repair of this issue converted the ten recovery
    /// guards and left five standing, and the behavioural test stayed green
    /// across that gap because it exercises the mint seam rather than the
    /// recovery seam. One of the five was
    /// <c>CompleteRecoverySplitUnderGateAsync</c>'s inverted re-check, which
    /// reads <c>!= SplitInProgress</c> and therefore evaluated <em>true</em>
    /// on every leaf that had ever split: a recovery the caller had correctly
    /// decided to enter returned without resuming anything. The converted
    /// caller and the unconverted callee cancelled out, so the fix was
    /// inert at that seam and nothing reported it.
    /// </para>
    /// <para>
    /// A count is the only thing that catches that class of partial
    /// conversion, which is why this is a census rather than a spot check.
    /// The two permitted sites are the predicate definitions themselves - the
    /// one place the comparison is still correct, because each is combined
    /// there with the discriminator that survives the ratchet
    /// (<c>OldNextSibling</c> for a leaf, <c>SplitRightChildren</c> for an
    /// internal node).
    /// </para>
    /// <para>
    /// Do not raise the allowance to accommodate a new site. The comparison is
    /// unreachable on any node that has split before, so a new site naming it
    /// directly is a new instance of this defect, not a new exception to it.
    /// </para>
    /// </remarks>
    [Test]
    public void No_site_outside_the_interrupted_split_predicates_tests_SplitState_directly()
    {
            var root = HygieneRepository.FindRepoRoot();
            var src = Path.Combine(root, "src", "lattice");

            // Both polarities. The inverted form is the one that inverts the
            // failure mode too - it takes a branch it should not, where the
            // equality form skips one it should take - so a gate that matched
            // only `==` would have missed the single most damaging site.
            var comparison = new Regex(
                @"(==|!=)\s*(Primitives\.)?SplitState\.SplitInProgress",
                RegexOptions.Compiled);

            var sites = HygieneRepository.EnumerateFiles(src, "*.cs")
                .Where(f => !HygieneRepository.HasExcludedSegment(f))
                .SelectMany(f => File.ReadAllLines(f)
                    .Select((line, i) => (File: Path.GetFileName(f), No: i + 1, Text: line))
                    .Where(x => comparison.IsMatch(x.Text)))
                .OrderBy(x => x.File, StringComparer.Ordinal)
                .ThenBy(x => x.No)
                .ToList();

            // The predicate definitions, named individually. A pattern-based
            // exclusion broad enough to cover them would be broad enough to hide
            // the next real site, which is the failure this gate is for.
            var permitted = new[] { "BPlusLeafGrain.Split.cs", "BPlusInternalGrain.cs" };

            Assert.Multiple(() =>
            {
                // Anti-vacuity. A scan matching nothing passes every assertion
                // below it, so without this the gate silently becomes a no-op the
                // day its pattern, its root or its file filter stops matching -
                // and reports green while doing it.
                Assert.That(
                    sites,
                    Is.Not.Empty,
                    "The census matched no comparison at all, so it has stopped "
                    + "scanning what it claims to scan. The two predicate "
                    + "definitions must always match. Repair the scan rather "
                    + "than trusting this green.");

                var offenders = sites
                    .Where(x => !permitted.Contains(x.File))
                    .Select(x => $"{x.File}:{x.No}")
                    .ToList();

                Assert.That(
                    offenders,
                    Is.Empty,
                    "These sites ask whether a division is outstanding through a "
                    + "comparison that is unreachable on any node which has split "
                    + "before (issue #3265). Route each through "
                    + "HasInterruptedSplit instead of widening the allowance: "
                    + string.Join(", ", offenders));

                // The permitted sites are an allowance, not a floor, so assert
                // they are all still present. A predicate silently reverted to a
                // bare SplitState read would otherwise pass every check above.
                Assert.That(
                    sites.Select(x => x.File).Distinct(),
                    Is.EquivalentTo(permitted),
                    "Exactly the two HasInterruptedSplit predicates may name "
                    + "SplitInProgress. A missing one means a predicate lost the "
                    + "comparison it legitimately needs.");
            });
    }
}
