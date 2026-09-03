using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication.Grains;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Coverage for the receiver's generic <see cref="LatticeMergeMode.OrMap"/> apply seam
/// and the point-apply contract guards around it.
/// <para>
/// OrMap is the one mode whose wire shape is generic over <c>(TKey, TValue)</c>, so the
/// receiver cannot statically pick a deserialiser and instead resolves the
/// host-registered <see cref="CrdtShape"/> by tree id. That gives it two distinct
/// inbound forms - a steady-state typed delta, and a bootstrap committed-projection row
/// carrying the full state with no delta - and only the first has ever been exercised.
/// The full-state form is what a cold peer receives when it joins an existing pair, so
/// a defect here is invisible in steady state and fatal at bootstrap.
/// </para>
/// </summary>
public partial class ReplicationApplierTests
{
    private static readonly CrdtShape OrMapShape = CrdtShape.ForOrMap<string, PnCounter>();

    private static (
        ReplicationApplier Applier,
        ILattice Lattice,
        IReplicationApplyGrain Apply,
        IReplicationHighWaterMarkGrain Hwm)
        CreateOrMapApplier(bool registerShape = true)
    {
        var factory = Substitute.For<IGrainFactory>();
        var apply = Substitute.For<IReplicationApplyGrain>();
        var hwm = Substitute.For<IReplicationHighWaterMarkGrain>();
        var lattice = Substitute.For<ILattice>();
        factory.GetGrain<IReplicationApplyGrain>(Tree).Returns(apply);
        factory.GetGrain<IReplicationHighWaterMarkGrain>(Tree).Returns(hwm);
        factory.GetGrain<ILattice>(Tree).Returns(lattice);
        hwm.GetAsync(Arg.Any<string>(), Arg.Any<CancellationToken>()).Returns(HybridLogicalClock.Zero);
        hwm.TryAdvanceAsync(Arg.Any<string>(), Arg.Any<HybridLogicalClock>(), Arg.Any<CancellationToken>())
            .Returns(true);
        hwm.GetVectorAsync(Arg.Any<CancellationToken>()).Returns(new VersionVector());
        lattice.GetWithVersionAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(new VersionedValue { Value = null, Version = HybridLogicalClock.Zero });
        lattice.SetIfVersionAsync(
            Arg.Any<string>(), Arg.Any<byte[]>(), Arg.Any<HybridLogicalClock>(), Arg.Any<CancellationToken>())
            .Returns(true);

        var registry = new CrdtShapeRegistry();
        if (registerShape)
        {
            registry.Register(Tree, CrdtShape.ForOrMap<string, PnCounter>());
        }

        var applier = new ReplicationApplier(
            factory,
            Monitor(),
            crdtShapes: registry,
            replicationContext: new AnyTreeLwwContext(
                new Dictionary<string, LatticeMergeMode> { [Tree] = LatticeMergeMode.OrMap }));
        return (applier, lattice, apply, hwm);
    }

    private static byte[] OrMapState(params (string Key, string Replica, long Increment)[] members)
    {
        var map = new OrMap<string, PnCounter>();
        foreach (var (key, replica, increment) in members)
        {
            var counter = new PnCounter();
            counter.Increment(replica, increment);
            map.Set(key, replica, counter);
        }

        return OrMapShape.SerializeState(map);
    }

    private static WalRecord OrMapEntry(string key, HybridLogicalClock ts) =>
        SetEntry(key, ts) with { Mode = LatticeMergeMode.OrMap, Value = null, Delta = null };

    // ---------------------------------------------------------------
    // Steady-state delta form.
    // ---------------------------------------------------------------

    [Test]
    public async Task ApplyAsync_forwards_an_ormap_delta_verbatim_through_the_crdt_delta_seam()
    {
        var (applier, lattice, apply, _) = CreateOrMapApplier();
        var delta = new byte[] { 7, 7, 7 };
        var ts = Hlc(10);

        var result = await applier.ApplyAsync(OrMapEntry("k", ts) with { Delta = delta });

        Assert.That(result.Applied, Is.True);
        await apply.Received(1).ApplyCrdtDeltaWithExpiryAsync(
            "k", LatticeMergeMode.OrMap, delta, Arg.Any<long>());
        await lattice.DidNotReceiveWithAnyArgs().GetWithVersionAsync(default!, default);
    }

    [Test]
    public void ApplyAsync_faults_an_ormap_entry_for_a_tree_with_no_registered_shape()
    {
        var (applier, _, _, _) = CreateOrMapApplier(registerShape: false);

        Assert.That(
            async () => await applier.ApplyAsync(OrMapEntry("k", Hlc(10)) with { Delta = new byte[] { 1 } }),
            Throws.InvalidOperationException.With.Message.Contains("AddOrMapShape"),
            "A misconfiguration must surface rather than silently drop the entry.");
    }

    // ---------------------------------------------------------------
    // Bootstrap committed-projection form: full state, no delta.
    // ---------------------------------------------------------------

    [Test]
    public async Task ApplyAsync_installs_an_ormap_bootstrap_row_verbatim_when_the_key_is_absent()
    {
        var (applier, lattice, apply, _) = CreateOrMapApplier();
        var state = OrMapState(("orders", "site-b", 3));

        var result = await applier.ApplyAsync(OrMapEntry("k", Hlc(10)) with { Value = state });

        Assert.That(result.Applied, Is.True);
        await lattice.Received(1).SetIfVersionAsync(
            "k", state, HybridLogicalClock.Zero, Arg.Any<CancellationToken>());
        await apply.DidNotReceiveWithAnyArgs().ApplyCrdtDeltaWithExpiryAsync(
            default!, default, default!, default);
    }

    [Test]
    public async Task ApplyAsync_folds_an_ormap_bootstrap_row_into_the_existing_state()
    {
        var (applier, lattice, _, _) = CreateOrMapApplier();
        var existing = OrMapState(("orders", "site-a", 5));
        var incoming = OrMapState(("shipments", "site-b", 2));
        lattice.GetWithVersionAsync("k", Arg.Any<CancellationToken>())
            .Returns(new VersionedValue { Value = existing, Version = Hlc(4) });
        byte[]? installed = null;
        lattice.SetIfVersionAsync("k", Arg.Any<byte[]>(), Arg.Any<HybridLogicalClock>(), Arg.Any<CancellationToken>())
            .Returns(call =>
            {
                installed = call.ArgAt<byte[]>(1);
                return Task.FromResult(true);
            });

        var result = await applier.ApplyAsync(OrMapEntry("k", Hlc(10)) with { Value = incoming });

        Assert.That(result.Applied, Is.True);
        Assert.That(installed, Is.Not.Null);
        var merged = (OrMap<string, PnCounter>)OrMapShape.DeserializeState(installed!);
        Assert.That(merged.Keys, Is.EquivalentTo(new[] { "orders", "shipments" }),
            "The bootstrap row must be folded into the receiver's state, not overwrite it.");
    }

    [Test]
    public async Task ApplyAsync_retries_an_ormap_bootstrap_fold_when_the_optimistic_install_loses()
    {
        var (applier, lattice, _, _) = CreateOrMapApplier();
        var attempts = 0;
        lattice.SetIfVersionAsync("k", Arg.Any<byte[]>(), Arg.Any<HybridLogicalClock>(), Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromResult(++attempts > 1));

        var result = await applier.ApplyAsync(
            OrMapEntry("k", Hlc(10)) with { Value = OrMapState(("orders", "site-b", 1)) });

        Assert.Multiple(() =>
        {
            Assert.That(result.Applied, Is.True);
            Assert.That(attempts, Is.EqualTo(2), "A lost CAS must re-read and retry rather than drop the row.");
        });
    }

    [Test]
    public void ApplyAsync_faults_an_ormap_bootstrap_fold_that_exhausts_its_cas_budget()
    {
        var (applier, lattice, _, _) = CreateOrMapApplier();
        lattice.SetIfVersionAsync("k", Arg.Any<byte[]>(), Arg.Any<HybridLogicalClock>(), Arg.Any<CancellationToken>())
            .Returns(false);

        Assert.That(
            async () => await applier.ApplyAsync(
                OrMapEntry("k", Hlc(10)) with { Value = OrMapState(("orders", "site-b", 1)) }),
            Throws.InvalidOperationException.With.Message.Contains("CAS budget exhausted"),
            "Sustained contention must surface as a fault, not as a silently dropped bootstrap row.");
    }

    [Test]
    public void ApplyAsync_rejects_an_ormap_entry_carrying_neither_a_delta_nor_a_full_state()
    {
        var (applier, _, _, _) = CreateOrMapApplier();

        Assert.That(
            async () => await applier.ApplyAsync(OrMapEntry("k", Hlc(10))),
            Throws.ArgumentException.With.Message.Contains("neither a"),
            "An entry with both absent is malformed and must be rejected, not merged as empty.");
    }

    // ---------------------------------------------------------------
    // Closed-shape full-state merge (the non-generic sibling path).
    // ---------------------------------------------------------------

    [Test]
    public void ApplyAsync_faults_a_typed_full_state_merge_that_exhausts_its_cas_budget()
    {
        var (applier, lattice, _, _) = CreateTypedCrdtApplier(LatticeMergeMode.PnCounter);
        lattice.SetIfVersionAsync(
            Arg.Any<string>(), Arg.Any<byte[]>(), Arg.Any<HybridLogicalClock>(), Arg.Any<CancellationToken>())
            .Returns(false);

        Assert.That(
            async () => await applier.ApplyAsync(
                SetEntry("k", Hlc(10)) with
                {
                    Mode = LatticeMergeMode.PnCounter,
                    Delta = null,
                    Value = EncodePnCounter(c => c.Increment("site-b", 4)),
                }),
            Throws.InvalidOperationException.With.Message.Contains("CAS budget exhausted"));
    }

    // ---------------------------------------------------------------
    // Point-apply contract guards.
    // ---------------------------------------------------------------

    [Test]
    public void ApplyAsync_rejects_a_range_delete_that_carries_atomic_batch_metadata()
    {
        var (applier, _, _, _) = CreateApplier();

        // The wire slot is additive on every WalRecord, so a producer defect can stamp
        // it onto a range delete. Accepting it would let a range delete carry an
        // atomic-batch promise the receiver has no way to fulfil.
        Assert.That(
            async () => await applier.ApplyAsync(RangeDeleteEntry("a", "z") with { AtomicBatchSize = 3 }),
            Throws.ArgumentException.With.Message.Contains("atomic-batch metadata"));
    }

    [Test]
    public void ApplyAsync_rejects_a_point_entry_whose_op_has_no_apply_rule()
    {
        var (applier, _, _, _) = CreateApplier();

        // A peer running a newer build can ship an op this receiver has no rule for.
        // Faulting is the documented behaviour (a future release dead-letters it);
        // silently treating it as a Set would apply an operation nobody defined.
        Assert.That(
            async () => await applier.ApplyAsync(SetEntry("k", Hlc(10)) with { Op = (MutationKind)99 }),
            Throws.InvalidOperationException.With.Message.Contains("Unsupported point-apply op"));
    }

    [Test]
    public async Task ApplyAsync_skips_the_lag_sample_for_an_entry_whose_source_clock_was_never_stamped()
    {
        var (applier, _, apply, _) = CreateApplier();

        // WallClockTicks of zero with a non-zero counter still clears the pinned floor,
        // so the entry genuinely applies - but "now - 0" would publish a multi-decade
        // lag value and corrupt the histogram, so no sample may be recorded.
        var ts = Hlc(0, 1);
        var result = await applier.ApplyAsync(SetEntry("k", ts));

        Assert.That(result.Applied, Is.True);
        await apply.Received(1).ApplySetAsync(
            "k", Arg.Any<byte[]>(), ts, RemoteCluster, null, Arg.Any<long>());
    }
}
