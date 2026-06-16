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
/// the two-phase prepared / terminal replication path. That path now carries the
/// staged typed delta and merge mode through to the receiver: a prepared CRDT-mode
/// entry folds its per-replica delta into the receiver's current visible state on
/// the saga's terminal commit (see
/// <see cref="Orleans.Lattice.Replication.Tests.PublicApiContract.PublicReplicationApiContractTests.Set_staged_crdt_writes_from_both_sites_converge_to_the_typed_delta_union"/>).
/// So two clusters writing the <em>same</em> CRDT key concurrently through staged
/// atomic writes converge by the per-replica typed-delta union - identical to the
/// live (non-atomic) accessor path - rather than by last-writer-wins of their
/// merged states. Value-only sagas (no staged CRDT delta) stay on the LWW path.
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
    public async Task Set_staged_crdt_writes_from_both_sites_converge_to_the_typed_delta_union()
    {
        // Each site couples its own increment to the SAME counter key in an
        // independent cross-tree atomic write. The prepared/terminal
        // replication path carries the staged typed delta and merge mode to
        // the receiver, which folds the per-replica delta into its current
        // visible state on the saga's terminal commit. So the two writes
        // converge by the per-replica UNION (5 + 3 = 8) on BOTH sites -
        // identical to the live (non-atomic) accessor path - rather than by
        // last-writer-wins of their merged states {5, 3}.
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
            async () => await counterOnA.PnCounter("total").ValueAsync() == 8
                && await counterOnB.PnCounter("total").ValueAsync() == 8,
            $"concurrent staged increments coupled into cross-tree atomic writes on both "
            + $"sites must converge to the typed-delta union (5 + 3 = 8) on both sites for "
            + $"tree '{counterTree}'.");

        var finalA = await counterOnA.PnCounter("total").ValueAsync();
        var finalB = await counterOnB.PnCounter("total").ValueAsync();
        Assert.That(finalA, Is.EqualTo(finalB),
            "both sites must agree on the converged value.");
        Assert.That(finalA, Is.EqualTo(8L),
            "the prepared-saga path folds the staged typed deltas on the receiver's "
            + "terminal commit, so two concurrent staged increments converge by the "
            + "per-replica union (5 + 3 = 8), identical to the live accessor path.");
    }

    [Test]
    public async Task Set_staged_orset_adds_from_both_sites_converge_to_the_membership_union_through_the_prepared_path()
    {
        // Flag / tag-index membership active-active convergence through the
        // ATOMIC (prepared) path, not just the eventual accessor path. Each
        // site stages an OR-Set add of a DIFFERENT element to the SAME set key
        // in an independent cross-tree atomic write. The prepared/terminal
        // replication path carries each staged typed delta to the receiver,
        // which folds it into its current visible state on the saga's terminal
        // commit. So both sites converge on the per-replica membership UNION
        // {alice, bob} - if the prepared path dropped the typed delta and
        // reconciled the merged-state values last-writer-wins, one site's
        // member would be lost.
        var setTree = NextTreeId("crdt-orset-bothsites");
        var lwwTree = NextTreeId("lww-orset-bothsites-sibling");
        var setOnA = await CreateReplicatedTreeAsync(setTree);
        await CreateReplicatedTreeAsync(lwwTree);
        var setOnB = _fixture.TreeOnB(setTree);

        var stagedA = await setOnA.OrSet("members").StageAddAsync(Bytes("alice"), replicaId: "site-a");
        var stagedB = await setOnB.OrSet("members").StageAddAsync(Bytes("bob"), replicaId: "site-b");

        var outcomeA = await ClientA.BeginAtomicWrite($"orsetA-{Guid.NewGuid():N}")
            .ForTree(setTree).Set(stagedA)
            .ForTree(lwwTree).Set("a", Bytes("a"))
            .CommitAsync();
        var outcomeB = await ClientB.BeginAtomicWrite($"orsetB-{Guid.NewGuid():N}")
            .ForTree(setTree).Set(stagedB)
            .ForTree(lwwTree).Set("b", Bytes("b"))
            .CommitAsync();

        Assert.That(outcomeA, Is.EqualTo(CrossTreeAtomicWriteOutcome.Committed));
        Assert.That(outcomeB, Is.EqualTo(CrossTreeAtomicWriteOutcome.Committed));

        await PublicReplicationApiClusterFixture.WaitForConvergenceAsync(
            async () =>
            {
                var onA = await setOnA.OrSet("members").GetAsync();
                var onB = await setOnB.OrSet("members").GetAsync();
                return onA.Contains(Bytes("alice")) && onA.Contains(Bytes("bob"))
                    && onB.Contains(Bytes("alice")) && onB.Contains(Bytes("bob"));
            },
            $"concurrent staged OR-Set adds coupled into cross-tree atomic writes on both "
            + $"sites must converge to the membership union {{alice, bob}} on both sites for "
            + $"tree '{setTree}/members'.");
    }
}
