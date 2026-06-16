using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Replication.Tests.PublicApiContract;

/// <summary>
/// Cross-cluster coverage for coupling a typed CRDT mutation into a cross-tree
/// atomic write. A <c>Stage*</c> token minted by a CRDT accessor and added to a
/// <see cref="LatticeAtomicWriteBuilder"/> slice via
/// <see cref="LatticeAtomicWriteBuilder.Set(LatticeStagedCrdtWrite)"/> stages the
/// accessor's <em>merged post-mutation state</em> as the entry value; the
/// cross-tree saga commits that value all-or-nothing with its sibling entries and
/// replicates it to every peer.
/// <para>
/// <b>Replication contract (prepared-saga path).</b> Cross-tree atomic writes ride
/// the two-phase prepared / terminal replication path. That path ships and applies
/// the merged-state <em>value</em> last-writer-wins by HLC; it does <b>not</b> carry
/// the typed delta to the receiver fold (see
/// <see cref="Orleans.Lattice.Replication.Tests.PublicApiContract.PublicReplicationApiContractTests.Set_staged_crdt_writes_from_both_sites_converge_by_lww_not_typed_delta_union"/>).
/// So a single authoring site's staged CRDT write converges everywhere on its merged
/// value, but two clusters writing the <em>same</em> key concurrently reconcile by
/// LWW of their merged states, not by the typed-delta union the live (non-atomic)
/// accessor path provides. Folding staged deltas through the prepared path is tracked
/// as a follow-up.
/// </para>
/// </summary>
public partial class PublicReplicationApiContractTests
{
    [Test]
    public async Task Set_staged_crdt_write_couples_increment_and_replicates_across_clusters()
    {
        // The CRDT tree id is labelled so the fixture resolver routes it to
        // PnCounter merge mode on both sites; the sibling tree stays LWW.
        var counterTree = NextTreeId("crdt-pncounter-saga");
        var lwwTree = NextTreeId("lww-saga-sibling");
        var counterOnA = await CreateReplicatedTreeAsync(counterTree);
        var lwwOnA = await CreateReplicatedTreeAsync(lwwTree);
        var counterOnB = _fixture.TreeOnB(counterTree);
        var lwwOnB = _fixture.TreeOnB(lwwTree);

        var staged = await counterOnA.PnCounter("votes").StageIncrementAsync("site-a", 5);

        var outcome = await ClientA.BeginAtomicWrite($"crdtsaga-{Guid.NewGuid():N}")
            .ForTree(counterTree).Set(staged)
            .ForTree(lwwTree).Set("ballot", Bytes("cast"))
            .CommitAsync();

        Assert.That(outcome, Is.EqualTo(CrossTreeAtomicWriteOutcome.Committed));

        await PublicReplicationApiClusterFixture.WaitForConvergenceAsync(
            async () => await counterOnB.PnCounter("votes").ValueAsync() == 5
                && Str(await lwwOnB.GetAsync("ballot")) == "cast",
            $"the staged PnCounter increment and its sibling LWW write authored in one "
            + $"cross-tree atomic write on Site A must both converge on Site B for trees "
            + $"'{counterTree}' / '{lwwTree}'.");

        // The locally staged merged state is also readable on the authoring site.
        Assert.That(await counterOnA.PnCounter("votes").ValueAsync(), Is.EqualTo(5));
        Assert.That(Str(await lwwOnA.GetAsync("ballot")), Is.EqualTo("cast"));
    }

    [Test]
    public async Task Set_staged_crdt_writes_from_both_sites_converge_by_lww_not_typed_delta_union()
    {
        // Each site couples its own increment to the SAME counter key in an
        // independent cross-tree atomic write. Because cross-tree atomic writes
        // ride the prepared/terminal replication path - which ships the merged
        // value LWW and does not carry the typed delta to the receiver fold -
        // the two writes reconcile by last-writer-wins of their merged states,
        // NOT by the per-replica union (which would be 5 + 3 = 8). Both sites
        // still converge to the SAME value (replication is convergent), and that
        // value is one of the two single-writer merged states {5, 3}. A result
        // of 8 here would mean the prepared path had started folding typed
        // deltas - flip this test if that follow-up ships.
        var counterTree = NextTreeId("crdt-pncounter-bothsites");
        var lwwTree = NextTreeId("lww-bothsites-sibling");
        var counterOnA = await CreateReplicatedTreeAsync(counterTree);
        await CreateReplicatedTreeAsync(lwwTree);
        var counterOnB = _fixture.TreeOnB(counterTree);

        var stagedA = await counterOnA.PnCounter("total").StageIncrementAsync("site-a", 5);
        var stagedB = await counterOnB.PnCounter("total").StageIncrementAsync("site-b", 3);

        var outcomeA = await ClientA.BeginAtomicWrite($"bothA-{Guid.NewGuid():N}")
            .ForTree(counterTree).Set(stagedA)
            .ForTree(lwwTree).Set("a", Bytes("a"))
            .CommitAsync();
        var outcomeB = await ClientB.BeginAtomicWrite($"bothB-{Guid.NewGuid():N}")
            .ForTree(counterTree).Set(stagedB)
            .ForTree(lwwTree).Set("b", Bytes("b"))
            .CommitAsync();

        Assert.That(outcomeA, Is.EqualTo(CrossTreeAtomicWriteOutcome.Committed));
        Assert.That(outcomeB, Is.EqualTo(CrossTreeAtomicWriteOutcome.Committed));

        await PublicReplicationApiClusterFixture.WaitForConvergenceAsync(
            async () => await counterOnA.PnCounter("total").ValueAsync()
                == await counterOnB.PnCounter("total").ValueAsync(),
            $"concurrent staged increments coupled into cross-tree atomic writes on both "
            + $"sites must converge to the same LWW value on both sites for tree '{counterTree}'.");

        var finalA = await counterOnA.PnCounter("total").ValueAsync();
        var finalB = await counterOnB.PnCounter("total").ValueAsync();
        Assert.That(finalA, Is.EqualTo(finalB),
            "both sites must agree on the converged value.");
        Assert.That(finalA, Is.AnyOf(5L, 3L),
            "the prepared-saga path reconciles by LWW of the two merged states {5, 3}; "
            + "a union value of 8 would indicate typed-delta folding on the prepared path, "
            + "which this path does not yet provide.");
    }
}
