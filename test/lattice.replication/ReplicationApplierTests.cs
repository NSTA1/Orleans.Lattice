using Orleans.Lattice.BPlusTree.Grains;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication;
using Orleans.Lattice.Replication.Grains;

namespace Orleans.Lattice.Replication.Tests;

[TestFixture]
public partial class ReplicationApplierTests
{
    private const string Tree = "tree";
    private const string LocalCluster = "site-a";
    private const string RemoteCluster = "site-b";

    private static IOptionsMonitor<LatticeReplicationOptions> Monitor(string clusterId = LocalCluster)
    {
        var monitor = Substitute.For<IOptionsMonitor<LatticeReplicationOptions>>();
        var options = new LatticeReplicationOptions { ClusterId = clusterId };
        monitor.CurrentValue.Returns(options);
        monitor.Get(Arg.Any<string>()).Returns(options);
        return monitor;
    }

    private static (
        ReplicationApplier Applier,
        IGrainFactory Factory,
        IReplicationApplyGrain Apply,
        IReplicationHighWaterMarkGrain Hwm)
        CreateApplier(string treeId = Tree, string originClusterId = RemoteCluster)
    {
        var factory = Substitute.For<IGrainFactory>();
        var apply = Substitute.For<IReplicationApplyGrain>();
        var hwm = Substitute.For<IReplicationHighWaterMarkGrain>();
        factory.GetGrain<IReplicationApplyGrain>(treeId).Returns(apply);
        factory.GetGrain<IReplicationHighWaterMarkGrain>(treeId).Returns(hwm);
        hwm.GetAsync(Arg.Any<string>(), Arg.Any<CancellationToken>()).Returns(HybridLogicalClock.Zero);
        hwm.TryAdvanceAsync(Arg.Any<string>(), Arg.Any<HybridLogicalClock>(), Arg.Any<CancellationToken>())
            .Returns(true);
        hwm.GetVectorAsync(Arg.Any<CancellationToken>()).Returns(new VersionVector());
        var cache = new LocalVectorClockCache(factory);
        var applier = new ReplicationApplier(factory, Monitor(), cache);
        return (applier, factory, apply, hwm);
    }

    private static HybridLogicalClock Hlc(long ticks, int counter = 0) =>
        new() { WallClockTicks = ticks, Counter = counter };

    private static WalRecord SetEntry(string key, HybridLogicalClock ts, string origin = RemoteCluster) => new()
    {
        TreeId = Tree,
        Op = MutationKind.Set,
        Key = key,
        Value = new byte[] { 1 },
        Timestamp = ts,
        OriginClusterId = origin,
    };

    private static WalRecord DeleteEntry(string key, HybridLogicalClock ts, string origin = RemoteCluster) => new()
    {
        TreeId = Tree,
        Op = MutationKind.Delete,
        Key = key,
        Timestamp = ts,
        IsTombstone = true,
        OriginClusterId = origin,
    };

    private static WalRecord RangeDeleteEntry(string startInclusive, string endExclusive, string origin = RemoteCluster) => new()
    {
        TreeId = Tree,
        Op = MutationKind.DeleteRange,
        Key = startInclusive,
        EndExclusiveKey = endExclusive,
        Timestamp = HybridLogicalClock.Zero,
        IsTombstone = true,
        OriginClusterId = origin,
    };

    [Test]
    public async Task ApplyAsync_routes_set_through_apply_grain_with_source_hlc_and_origin()
    {
        var (applier, _, apply, hwm) = CreateApplier();
        var ts = Hlc(10, 1);

        var result = await applier.ApplyAsync(SetEntry("k", ts) with { ExpiresAtTicks = 99 });

        Assert.That(result.Applied, Is.True);
        await apply.Received(1).ApplySetAsync("k", Arg.Any<byte[]>(), ts, RemoteCluster, null, 99);
        await hwm.Received(1).TryAdvanceAsync(RemoteCluster, ts, Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task ApplyAsync_routes_delete_through_apply_grain_with_source_hlc_and_origin()
    {
        var (applier, _, apply, hwm) = CreateApplier();
        var ts = Hlc(20);

        var result = await applier.ApplyAsync(DeleteEntry("k", ts));

        Assert.That(result.Applied, Is.True);
        await apply.Received(1).ApplyDeleteAsync("k", ts, RemoteCluster, null);
        await hwm.Received(1).TryAdvanceAsync(RemoteCluster, ts, Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task ApplyAsync_routes_range_delete_with_origin_only()
    {
        var (applier, _, apply, hwm) = CreateApplier();

        var result = await applier.ApplyAsync(RangeDeleteEntry("a", "z"));

        Assert.That(result.Applied, Is.True);
        await apply.Received(1).ApplyDeleteRangeAsync("a", "z", HybridLogicalClock.Zero, RemoteCluster, null);

        // Range deletes carry HLC.Zero; the HWM is not advanced for them
        // because dedupe does not apply (range applies are naturally
        // idempotent at the leaf layer).
        await hwm.DidNotReceiveWithAnyArgs().TryAdvanceAsync(default!, default, default);
    }

    [Test]
    public async Task ApplyAsync_dedupes_when_entry_timestamp_equals_hwm()
    {
        var (applier, _, apply, hwm) = CreateApplier();
        hwm.GetAsync(Arg.Any<string>(), Arg.Any<CancellationToken>()).Returns(Hlc(10, 1));

        var result = await applier.ApplyAsync(SetEntry("k", Hlc(10, 1)));

        Assert.Multiple(() =>
        {
            Assert.That(result.Applied, Is.False);
            Assert.That(result.HighWaterMark, Is.EqualTo(Hlc(10, 1)));
        });
        await apply.DidNotReceiveWithAnyArgs().ApplySetAsync(default!, default!, default, default!, default, default);
        await hwm.DidNotReceiveWithAnyArgs().TryAdvanceAsync(default!, default, default);
    }

    [Test]
    public async Task ApplyAsync_dedupes_when_entry_timestamp_below_hwm()
    {
        var (applier, _, apply, hwm) = CreateApplier();
        hwm.GetAsync(Arg.Any<string>(), Arg.Any<CancellationToken>()).Returns(Hlc(50));

        var result = await applier.ApplyAsync(DeleteEntry("k", Hlc(20)));

        Assert.Multiple(() =>
        {
            Assert.That(result.Applied, Is.False);
            Assert.That(result.HighWaterMark, Is.EqualTo(Hlc(50)));
        });
        await apply.DidNotReceiveWithAnyArgs().ApplyDeleteAsync(default!, default, default!, default);
    }

    [Test]
    public async Task ApplyAsync_advances_hwm_after_successful_apply()
    {
        var (applier, _, _, hwm) = CreateApplier();
        var current = HybridLogicalClock.Zero;
        hwm.GetAsync(Arg.Any<string>(), Arg.Any<CancellationToken>()).Returns(_ => current);
        hwm.TryAdvanceAsync(Arg.Any<string>(), Arg.Any<HybridLogicalClock>(), Arg.Any<CancellationToken>())
            .Returns(call =>
            {
                var candidate = (HybridLogicalClock)call[1];
                if (candidate > current)
                {
                    current = candidate;
                    return Task.FromResult(true);
                }
                return Task.FromResult(false);
            });

        var result = await applier.ApplyAsync(SetEntry("k", Hlc(7)));

        Assert.That(result.HighWaterMark, Is.EqualTo(Hlc(7)));
    }

    [Test]
    public async Task ApplyAsync_skips_local_origin_entries_as_no_op()
    {
        var (applier, _, apply, hwm) = CreateApplier();

        var result = await applier.ApplyAsync(SetEntry("k", Hlc(5), origin: LocalCluster));

        Assert.That(result.Applied, Is.False);
        await apply.DidNotReceiveWithAnyArgs().ApplySetAsync(default!, default!, default, default!, default, default);
        await hwm.DidNotReceiveWithAnyArgs().TryAdvanceAsync(default!, default, default);
    }

    [Test]
    public void ApplyAsync_throws_when_tree_id_is_empty()
    {
        var (applier, _, _, _) = CreateApplier();
        var entry = SetEntry("k", Hlc(1)) with { TreeId = "" };

        Assert.That(async () => await applier.ApplyAsync(entry), Throws.ArgumentException);
    }

    [Test]
    public void ApplyAsync_throws_when_origin_cluster_id_is_null()
    {
        var (applier, _, _, _) = CreateApplier();
        var entry = SetEntry("k", Hlc(1)) with { OriginClusterId = null };

        Assert.That(async () => await applier.ApplyAsync(entry), Throws.ArgumentException);
    }

    [Test]
    public void ApplyAsync_throws_when_origin_cluster_id_is_empty()
    {
        var (applier, _, _, _) = CreateApplier();
        var entry = SetEntry("k", Hlc(1)) with { OriginClusterId = string.Empty };

        Assert.That(async () => await applier.ApplyAsync(entry), Throws.ArgumentException);
    }

    [Test]
    public void ApplyAsync_throws_when_set_entry_has_null_value()
    {
        var (applier, _, _, _) = CreateApplier();
        var entry = SetEntry("k", Hlc(1)) with { Value = null };

        Assert.That(async () => await applier.ApplyAsync(entry), Throws.ArgumentException);
    }

    [Test]
    public void ApplyAsync_throws_when_range_delete_has_null_end()
    {
        var (applier, _, _, _) = CreateApplier();
        var entry = RangeDeleteEntry("a", "z") with { EndExclusiveKey = null };

        Assert.That(async () => await applier.ApplyAsync(entry), Throws.ArgumentException);
    }

    [Test]
    public void ApplyAsync_observes_cancellation()
    {
        var (applier, _, _, _) = CreateApplier();
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        Assert.That(
            async () => await applier.ApplyAsync(SetEntry("k", Hlc(1)), cts.Token),
            Throws.InstanceOf<OperationCanceledException>());
    }

    [Test]
    public async Task ApplyAsync_resolves_options_with_entry_tree_id()
    {
        var factory = Substitute.For<IGrainFactory>();
        var apply = Substitute.For<IReplicationApplyGrain>();
        var hwm = Substitute.For<IReplicationHighWaterMarkGrain>();
        factory.GetGrain<IReplicationApplyGrain>(Arg.Any<string>()).Returns(apply);
        factory.GetGrain<IReplicationHighWaterMarkGrain>(Arg.Any<string>()).Returns(hwm);
        hwm.GetAsync(Arg.Any<string>(), Arg.Any<CancellationToken>()).Returns(HybridLogicalClock.Zero);
        hwm.TryAdvanceAsync(Arg.Any<string>(), Arg.Any<HybridLogicalClock>(), Arg.Any<CancellationToken>()).Returns(true);
        hwm.GetVectorAsync(Arg.Any<CancellationToken>()).Returns(new VersionVector());

        var monitor = Substitute.For<IOptionsMonitor<LatticeReplicationOptions>>();
        monitor.Get(Arg.Any<string>())
            .Returns(new LatticeReplicationOptions { ClusterId = LocalCluster });
        var applier = new ReplicationApplier(factory, monitor, new LocalVectorClockCache(factory));

        await applier.ApplyAsync(new WalRecord
        {
            TreeId = "alpha",
            Op = MutationKind.Set,
            Key = "k",
            Value = new byte[] { 1 },
            Timestamp = Hlc(1),
            OriginClusterId = RemoteCluster,
        });

        monitor.Received().Get("alpha");
    }

    [Test]
    public async Task ApplyAsync_returns_zero_high_water_mark_for_local_origin_no_op()
    {
        // Local-origin entries skip the HWM grain entirely (the row would
        // never carry state) - verify the seam reports Zero rather than
        // making a needless grain call.
        var (applier, _, _, hwm) = CreateApplier();

        var result = await applier.ApplyAsync(SetEntry("k", Hlc(5), origin: LocalCluster));

        Assert.Multiple(() =>
        {
            Assert.That(result.Applied, Is.False);
            Assert.That(result.HighWaterMark, Is.EqualTo(HybridLogicalClock.Zero));
        });
        await hwm.DidNotReceiveWithAnyArgs().GetAsync(default!, default);
    }

    [Test]
    public async Task ApplyAsync_returns_zero_high_water_mark_for_range_delete()
    {
        // Range deletes bypass per-origin HWM dedupe by design; the HWM
        // grain is not consulted, so the result reports Zero.
        var (applier, _, _, hwm) = CreateApplier();

        var result = await applier.ApplyAsync(RangeDeleteEntry("a", "z"));

        Assert.Multiple(() =>
        {
            Assert.That(result.Applied, Is.True);
            Assert.That(result.HighWaterMark, Is.EqualTo(HybridLogicalClock.Zero));
        });
        await hwm.DidNotReceiveWithAnyArgs().GetAsync(default!, default);
    }

    [Test]
    public async Task ApplyAsync_skips_redundant_get_after_successful_advance()
    {
        // Steady-state apply path: after TryAdvanceAsync returns true the
        // new HWM equals entry.Timestamp under single-threaded grain
        // semantics, so the applier must not issue a second GetAsync.
        var (applier, _, _, hwm) = CreateApplier();
        var ts = Hlc(42);

        await applier.ApplyAsync(SetEntry("k", ts));

        // Exactly one GetAsync (the pre-apply HWM check) and exactly one
        // TryAdvanceAsync (the post-apply advance). No redundant read.
        await hwm.Received(1).GetAsync(RemoteCluster, Arg.Any<CancellationToken>());
        await hwm.Received(1).TryAdvanceAsync(RemoteCluster, ts, Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task ApplyAsync_falls_back_to_get_when_advance_loses_race()
    {
        // Rare reentrant-race path: another applier raced ahead and
        // pushed the HWM past entry.Timestamp between our pre-apply
        // GetAsync and our TryAdvanceAsync. TryAdvanceAsync returns
        // false; the applier must fall back to a fresh GetAsync so the
        // returned HighWaterMark reflects the actual post-call state.
        var (applier, _, _, hwm) = CreateApplier();
        hwm.GetAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(HybridLogicalClock.Zero, Hlc(99));
        hwm.TryAdvanceAsync(Arg.Any<string>(), Arg.Any<HybridLogicalClock>(), Arg.Any<CancellationToken>())
            .Returns(false);

        var result = await applier.ApplyAsync(SetEntry("k", Hlc(42)));

        Assert.Multiple(() =>
        {
            Assert.That(result.Applied, Is.True);
            Assert.That(result.HighWaterMark, Is.EqualTo(Hlc(99)));
        });
        await hwm.Received(2).GetAsync(RemoteCluster, Arg.Any<CancellationToken>());
    }

    // ------------------------------------------------------------------
    // Typed CRDT mode dispatch (state-merge through ILattice)
    // ------------------------------------------------------------------

    private static (
        ReplicationApplier Applier,
        ILattice Lattice,
        IReplicationApplyGrain Apply,
        IReplicationHighWaterMarkGrain Hwm)
        CreateTypedCrdtApplier()
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
        lattice.GetWithVersionAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(new VersionedValue { Value = null, Version = HybridLogicalClock.Zero });
        lattice.SetIfVersionAsync(Arg.Any<string>(), Arg.Any<byte[]>(), Arg.Any<HybridLogicalClock>(), Arg.Any<CancellationToken>())
            .Returns(true);
        return (new ReplicationApplier(factory, Monitor(), new LocalVectorClockCache(factory)), lattice, apply, hwm);
    }

    private static byte[] EncodeOrSet(Action<OrSet>? configure = null)
    {
        var set = new OrSet();
        configure?.Invoke(set);
        return JsonLatticeSerializer<OrSet>.Default.Serialize(set);
    }

    private static byte[] EncodePnCounter(Action<PnCounter>? configure = null)
    {
        var counter = new PnCounter();
        configure?.Invoke(counter);
        return JsonLatticeSerializer<PnCounter>.Default.Serialize(counter);
    }

    private static byte[] EncodeVersionVector(Action<VersionVector>? configure = null)
    {
        var vector = new VersionVector();
        configure?.Invoke(vector);
        return JsonLatticeSerializer<VersionVector>.Default.Serialize(vector);
    }

    private static byte[] EncodeMvRegister(Action<MvRegister>? configure = null)
    {
        var register = new MvRegister();
        configure?.Invoke(register);
        return JsonLatticeSerializer<MvRegister>.Default.Serialize(register);
    }

    private static byte[] EncodeOrSetDelta(Action<List<OrSetDeltaDot>>? configure = null)
    {
        var adds = new List<OrSetDeltaDot>();
        configure?.Invoke(adds);
        return JsonLatticeSerializer<OrSetDelta>.Default.Serialize(new OrSetDelta
        {
            Adds = adds,
            Removes = Array.Empty<OrSetDeltaDot>(),
        });
    }

    private static byte[] EncodePnCounterDelta(Action<Dictionary<string, long>>? configureIncrements = null)
    {
        var incs = new Dictionary<string, long>(StringComparer.Ordinal);
        configureIncrements?.Invoke(incs);
        return JsonLatticeSerializer<PnCounterDelta>.Default.Serialize(new PnCounterDelta
        {
            Increments = incs,
            Decrements = new Dictionary<string, long>(StringComparer.Ordinal),
        });
    }

    private static byte[] EncodeVersionVectorDelta(Action<Dictionary<string, HybridLogicalClock>>? configure = null)
    {
        var entries = new Dictionary<string, HybridLogicalClock>(StringComparer.Ordinal);
        configure?.Invoke(entries);
        return JsonLatticeSerializer<VersionVectorDelta>.Default.Serialize(new VersionVectorDelta
        {
            Entries = entries,
        });
    }

    private static byte[] EncodeMvRegisterDelta(Action<List<MvRegisterEntry>, Dictionary<string, long>>? configure = null)
    {
        var entries = new List<MvRegisterEntry>();
        var ctx = new Dictionary<string, long>(StringComparer.Ordinal);
        configure?.Invoke(entries, ctx);
        return JsonLatticeSerializer<MvRegisterDelta>.Default.Serialize(new MvRegisterDelta
        {
            Entries = entries,
            Context = ctx,
        });
    }

    private static readonly byte[] OrSetMember = new byte[] { 0xab };

    [Test]
    public async Task ApplyAsync_dispatches_or_set_through_lattice_state_merge()
    {
        var (applier, lattice, apply, _) = CreateTypedCrdtApplier();
        var entry = SetEntry("k", Hlc(10)) with
        {
            Mode = LatticeMergeMode.OrSet,
            Value = EncodeOrSet(s => s.Add(OrSetMember, "site-b", 1)),
            Delta = EncodeOrSetDelta(a => a.Add(new OrSetDeltaDot { Element = OrSetMember, ReplicaId = "site-b", Counter = 1 })),
        };

        var result = await applier.ApplyAsync(entry);

        Assert.That(result.Applied, Is.True);
        await apply.DidNotReceiveWithAnyArgs().ApplySetAsync(default!, default!, default, default!, default, default);
        await lattice.Received(1).GetWithVersionAsync("k", Arg.Any<CancellationToken>());
        await lattice.Received(1).SetIfVersionAsync(
            "k",
            Arg.Is<byte[]>(b => JsonLatticeSerializer<OrSet>.Default.Deserialize(b).Contains(OrSetMember)),
            Arg.Any<HybridLogicalClock>(),
            Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task ApplyAsync_dispatches_pn_counter_through_lattice_state_merge()
    {
        var (applier, lattice, apply, _) = CreateTypedCrdtApplier();
        var entry = SetEntry("k", Hlc(11)) with
        {
            Mode = LatticeMergeMode.PnCounter,
            Value = EncodePnCounter(c => c.Increment("site-b", 5)),
            Delta = EncodePnCounterDelta(d => d["site-b"] = 5),
        };

        var result = await applier.ApplyAsync(entry);

        Assert.That(result.Applied, Is.True);
        await apply.DidNotReceiveWithAnyArgs().ApplySetAsync(default!, default!, default, default!, default, default);
        await lattice.Received(1).SetIfVersionAsync(
            "k",
            Arg.Is<byte[]>(b => JsonLatticeSerializer<PnCounter>.Default.Deserialize(b).Value == 5),
            Arg.Any<HybridLogicalClock>(),
            Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task ApplyAsync_dispatches_version_vector_through_lattice_state_merge()
    {
        var (applier, lattice, apply, _) = CreateTypedCrdtApplier();
        var remoteHlc = Hlc(42, 3);
        var entry = SetEntry("k", Hlc(12)) with
        {
            Mode = LatticeMergeMode.VersionVector,
            Value = EncodeVersionVector(v => v.Entries["site-b"] = remoteHlc),
            Delta = EncodeVersionVectorDelta(d => d["site-b"] = remoteHlc),
        };

        var result = await applier.ApplyAsync(entry);

        Assert.That(result.Applied, Is.True);
        await apply.DidNotReceiveWithAnyArgs().ApplySetAsync(default!, default!, default, default!, default, default);
        await lattice.Received(1).SetIfVersionAsync(
            "k",
            Arg.Is<byte[]>(b => JsonLatticeSerializer<VersionVector>.Default.Deserialize(b).GetClock("site-b") == remoteHlc),
            Arg.Any<HybridLogicalClock>(),
            Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task ApplyAsync_dispatches_mv_register_through_lattice_state_merge()
    {
        var (applier, lattice, apply, _) = CreateTypedCrdtApplier();
        var entry = SetEntry("k", Hlc(17)) with
        {
            Mode = LatticeMergeMode.MvRegister,
            Value = EncodeMvRegister(r => r.Set("site-b", new byte[] { 0xab })),
            Delta = EncodeMvRegisterDelta((entries, ctx) =>
            {
                entries.Add(new MvRegisterEntry
                {
                    ReplicaId = "site-b",
                    Counter = 1,
                    Value = new byte[] { 0xab },
                });
                ctx["site-b"] = 1;
            }),
        };

        var result = await applier.ApplyAsync(entry);

        Assert.That(result.Applied, Is.True);
        await apply.DidNotReceiveWithAnyArgs().ApplySetAsync(default!, default!, default, default!, default, default);
        await lattice.Received(1).SetIfVersionAsync(
            "k",
            Arg.Is<byte[]>(b =>
                JsonLatticeSerializer<MvRegister>.Default.Deserialize(b).Context.ContainsKey("site-b")),
            Arg.Any<HybridLogicalClock>(),
            Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task ApplyAsync_state_merge_advances_high_water_mark()
    {
        // Per-origin HWM is updated even for typed CRDT modes - re-delivery
        // of the same (origin, hlc) pair must be a no-op.
        var (applier, _, _, hwm) = CreateTypedCrdtApplier();
        var ts = Hlc(99, 1);
        var entry = SetEntry("k", ts) with
        {
            Mode = LatticeMergeMode.OrSet,
            Value = EncodeOrSet(),
            Delta = EncodeOrSetDelta(),
        };

        var result = await applier.ApplyAsync(entry);

        Assert.That(result.HighWaterMark, Is.EqualTo(ts));
        await hwm.Received(1).TryAdvanceAsync(RemoteCluster, ts, Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task ApplyAsync_state_merge_dedupes_when_entry_below_hwm()
    {
        var (applier, lattice, _, hwm) = CreateTypedCrdtApplier();
        hwm.GetAsync(Arg.Any<string>(), Arg.Any<CancellationToken>()).Returns(Hlc(50));
        var entry = SetEntry("k", Hlc(20)) with
        {
            Mode = LatticeMergeMode.OrSet,
            Value = EncodeOrSet(),
            Delta = EncodeOrSetDelta(),
        };

        var result = await applier.ApplyAsync(entry);

        Assert.That(result.Applied, Is.False);
        await lattice.DidNotReceiveWithAnyArgs().GetWithVersionAsync(default!, default);
        await lattice.DidNotReceiveWithAnyArgs().SetIfVersionAsync(default!, default!, default, default);
    }

    [Test]
    public void ApplyAsync_state_merge_throws_when_value_is_null()
    {
        var (applier, _, _, _) = CreateTypedCrdtApplier();
        var entry = SetEntry("k", Hlc(1)) with
        {
            Mode = LatticeMergeMode.OrSet,
            Value = null,
        };

        Assert.That(async () => await applier.ApplyAsync(entry), Throws.ArgumentException);
    }

    [Test]
    public async Task ApplyAsync_state_merge_retries_on_cas_failure()
    {
        var (applier, lattice, _, _) = CreateTypedCrdtApplier();
        // First two CAS attempts lose the race; third succeeds.
        lattice.SetIfVersionAsync(Arg.Any<string>(), Arg.Any<byte[]>(), Arg.Any<HybridLogicalClock>(), Arg.Any<CancellationToken>())
            .Returns(false, false, true);
        var entry = SetEntry("k", Hlc(7)) with
        {
            Mode = LatticeMergeMode.OrSet,
            Value = EncodeOrSet(s => s.Add(OrSetMember, "site-b", 1)),
            Delta = EncodeOrSetDelta(a => a.Add(new OrSetDeltaDot { Element = OrSetMember, ReplicaId = "site-b", Counter = 1 })),
        };

        var result = await applier.ApplyAsync(entry);

        Assert.That(result.Applied, Is.True);
        await lattice.Received(3).SetIfVersionAsync(
            "k",
            Arg.Any<byte[]>(),
            Arg.Any<HybridLogicalClock>(),
            Arg.Any<CancellationToken>());
    }

    [Test]
    public void ApplyAsync_throws_for_unrecognised_replication_mode()
    {
        var (applier, _, _, _) = CreateTypedCrdtApplier();
        var entry = SetEntry("k", Hlc(1)) with
        {
            Mode = (LatticeMergeMode)999,
            Value = new byte[] { 1 },
        };

        Assert.That(
            async () => await applier.ApplyAsync(entry),
            Throws.InstanceOf<InvalidOperationException>().With.Message.Contain("999"));
    }

    [Test]
    public async Task ApplyAsync_state_merge_does_not_invoke_apply_grain_for_set()
    {
        var (applier, _, apply, _) = CreateTypedCrdtApplier();
        var entry = SetEntry("k", Hlc(1)) with
        {
            Mode = LatticeMergeMode.OrSet,
            Value = EncodeOrSet(),
            Delta = EncodeOrSetDelta(),
        };

        await applier.ApplyAsync(entry);

        // Typed CRDT Set must bypass IReplicationApplyGrain.ApplySetAsync -
        // that path stamps the source HLC verbatim, which is wrong for
        // state-merge semantics where the persisted HLC is a fresh local
        // tick representing the merge point.
        await apply.DidNotReceiveWithAnyArgs().ApplySetAsync(default!, default!, default, default!, default, default);
    }

    [Test]
    public async Task ApplyAsync_records_apply_lag_for_set()
    {
        using var collector = new MeterCollector<double>(
            LatticeReplicationMetrics.MeterName,
            LatticeReplicationMetrics.ApplyLagName);
        var (applier, _, _, _) = CreateApplier();

        // Source HLC ~100ms in the past; lag must be > 0 and tagged by tree.
        var pastTicks = DateTime.UtcNow.Ticks - TimeSpan.FromMilliseconds(100).Ticks;
        await applier.ApplyAsync(SetEntry("k", Hlc(pastTicks)));

        Assert.That(collector.Measurements, Has.Count.EqualTo(1));
        var only = collector.Measurements.Single();
        Assert.That(only.Value, Is.GreaterThanOrEqualTo(0.0));
        Assert.That(only.Tags, Has.Some.Matches<KeyValuePair<string, object?>>(t =>
            t.Key == "tree" && (string?)t.Value == Tree));
    }

    [Test]
    public async Task ApplyAsync_records_apply_lag_for_delete()
    {
        using var collector = new MeterCollector<double>(
            LatticeReplicationMetrics.MeterName,
            LatticeReplicationMetrics.ApplyLagName);
        var (applier, _, _, _) = CreateApplier();

        var pastTicks = DateTime.UtcNow.Ticks - TimeSpan.FromMilliseconds(50).Ticks;
        await applier.ApplyAsync(DeleteEntry("k", Hlc(pastTicks)));

        Assert.That(collector.Measurements, Has.Count.EqualTo(1));
    }

    [Test]
    public async Task ApplyAsync_clamps_apply_lag_to_zero_for_future_source_hlc()
    {
        using var collector = new MeterCollector<double>(
            LatticeReplicationMetrics.MeterName,
            LatticeReplicationMetrics.ApplyLagName);
        var (applier, _, _, _) = CreateApplier();

        // Source HLC clearly in the future (a peer with faster wall clock).
        var futureTicks = DateTime.UtcNow.Ticks + TimeSpan.FromMinutes(1).Ticks;
        await applier.ApplyAsync(SetEntry("k", Hlc(futureTicks)));

        Assert.That(collector.Measurements, Has.Count.EqualTo(1));
        Assert.That(collector.Measurements.Single().Value, Is.EqualTo(0.0));
    }

    [Test]
    public async Task ApplyAsync_skips_apply_lag_for_range_delete()
    {
        using var collector = new MeterCollector<double>(
            LatticeReplicationMetrics.MeterName,
            LatticeReplicationMetrics.ApplyLagName);
        var (applier, _, _, _) = CreateApplier();

        await applier.ApplyAsync(RangeDeleteEntry("a", "z"));

        // Range deletes carry HybridLogicalClock.Zero by design and do
        // not contribute to the lag histogram.
        Assert.That(collector.Measurements, Is.Empty);
    }

    [Test]
    public async Task ApplyAsync_skips_apply_lag_when_source_hlc_is_zero()
    {
        using var collector = new MeterCollector<double>(
            LatticeReplicationMetrics.MeterName,
            LatticeReplicationMetrics.ApplyLagName);
        var (applier, _, _, _) = CreateApplier();

        await applier.ApplyAsync(SetEntry("k", HybridLogicalClock.Zero));

        Assert.That(collector.Measurements, Is.Empty);
    }

    [Test]
    public async Task ApplyAsync_skips_apply_lag_for_dedupe_path()
    {
        using var collector = new MeterCollector<double>(
            LatticeReplicationMetrics.MeterName,
            LatticeReplicationMetrics.ApplyLagName);
        var (applier, _, _, hwm) = CreateApplier();
        hwm.GetAsync(Arg.Any<string>(), Arg.Any<CancellationToken>()).Returns(Hlc(100));

        // Entry timestamp <= HWM so the apply path short-circuits before
        // RecordApplyLag is reached.
        await applier.ApplyAsync(SetEntry("k", Hlc(50)));

        Assert.That(collector.Measurements, Is.Empty);
    }

    // ------------------------------------------------------------------
    // apply.duration histogram (per-outcome instrumentation)
    // ------------------------------------------------------------------

    private static bool HasOutcome(IReadOnlyList<KeyValuePair<string, object?>> tags, string outcome) =>
        tags.Any(t => t.Key == LatticeReplicationMetrics.TagOutcome && (string?)t.Value == outcome);

    private static bool HasTree(IReadOnlyList<KeyValuePair<string, object?>> tags, string tree) =>
        tags.Any(t => t.Key == LatticeReplicationMetrics.TagTree && (string?)t.Value == tree);

    [Test]
    public async Task ApplyAsync_records_apply_duration_with_success_outcome_for_set()
    {
        using var collector = new MeterCollector<double>(
            LatticeReplicationMetrics.MeterName,
            LatticeReplicationMetrics.ApplyDurationName);
        var (applier, _, _, _) = CreateApplier();

        await applier.ApplyAsync(SetEntry("k", Hlc(10)));

        Assert.That(collector.Measurements, Has.Count.EqualTo(1));
        var only = collector.Measurements.Single();
        Assert.Multiple(() =>
        {
            Assert.That(only.Value, Is.GreaterThanOrEqualTo(0.0));
            Assert.That(HasTree(only.Tags, Tree), Is.True);
            Assert.That(HasOutcome(only.Tags, LatticeReplicationMetrics.OutcomeSuccess), Is.True);
        });
    }

    [Test]
    public async Task ApplyAsync_records_apply_duration_with_success_outcome_for_range_delete()
    {
        using var collector = new MeterCollector<double>(
            LatticeReplicationMetrics.MeterName,
            LatticeReplicationMetrics.ApplyDurationName);
        var (applier, _, _, _) = CreateApplier();

        await applier.ApplyAsync(RangeDeleteEntry("a", "z"));

        Assert.That(collector.Measurements, Has.Count.EqualTo(1));
        var only = collector.Measurements.Single();
        Assert.Multiple(() =>
        {
            Assert.That(only.Value, Is.GreaterThanOrEqualTo(0.0));
            Assert.That(HasTree(only.Tags, Tree), Is.True);
            Assert.That(HasOutcome(only.Tags, LatticeReplicationMetrics.OutcomeSuccess), Is.True);
        });
    }

    [Test]
    public async Task ApplyAsync_records_apply_duration_with_dedup_outcome_for_hwm_short_circuit()
    {
        using var collector = new MeterCollector<double>(
            LatticeReplicationMetrics.MeterName,
            LatticeReplicationMetrics.ApplyDurationName);
        var (applier, _, _, hwm) = CreateApplier();
        hwm.GetAsync(Arg.Any<string>(), Arg.Any<CancellationToken>()).Returns(Hlc(100));

        // Entry timestamp <= HWM so the apply path short-circuits before
        // merge with outcome=dedup.
        await applier.ApplyAsync(SetEntry("k", Hlc(50)));

        Assert.That(collector.Measurements, Has.Count.EqualTo(1));
        var only = collector.Measurements.Single();
        Assert.Multiple(() =>
        {
            Assert.That(only.Value, Is.GreaterThanOrEqualTo(0.0));
            Assert.That(HasTree(only.Tags, Tree), Is.True);
            Assert.That(HasOutcome(only.Tags, LatticeReplicationMetrics.OutcomeDedup), Is.True);
        });
    }

    [Test]
    public async Task ApplyAsync_records_apply_duration_with_dedup_outcome_for_local_origin()
    {
        // A local-origin entry must record outcome=dedup (the
        // defence-in-depth gate that prevents an entry from looping
        // back onto its authoring cluster).
        using var collector = new MeterCollector<double>(
            LatticeReplicationMetrics.MeterName,
            LatticeReplicationMetrics.ApplyDurationName);
        var (applier, _, _, _) = CreateApplier();

        await applier.ApplyAsync(SetEntry("k", Hlc(10), origin: LocalCluster));

        Assert.That(collector.Measurements, Has.Count.EqualTo(1));
        var only = collector.Measurements.Single();
        Assert.Multiple(() =>
        {
            Assert.That(only.Value, Is.GreaterThanOrEqualTo(0.0));
            Assert.That(HasTree(only.Tags, Tree), Is.True);
            Assert.That(HasOutcome(only.Tags, LatticeReplicationMetrics.OutcomeDedup), Is.True);
        });
    }

    [Test]
    public void ApplyAsync_records_apply_duration_with_failure_outcome_when_apply_throws()
    {
        // An unrecognised replication mode causes ApplyAsync to throw;
        // the finally block must record the duration with outcome=failure
        // before the exception unwinds.
        using var collector = new MeterCollector<double>(
            LatticeReplicationMetrics.MeterName,
            LatticeReplicationMetrics.ApplyDurationName);
        var (applier, _, _, _) = CreateTypedCrdtApplier();
        var entry = SetEntry("k", Hlc(1)) with
        {
            Mode = (LatticeMergeMode)999,
            Value = new byte[] { 1 },
        };

        Assert.That(
            async () => await applier.ApplyAsync(entry),
            Throws.InstanceOf<InvalidOperationException>());

        Assert.That(collector.Measurements, Has.Count.EqualTo(1));
        var only = collector.Measurements.Single();
        Assert.Multiple(() =>
        {
            Assert.That(only.Value, Is.GreaterThanOrEqualTo(0.0));
            Assert.That(HasTree(only.Tags, Tree), Is.True);
            Assert.That(HasOutcome(only.Tags, LatticeReplicationMetrics.OutcomeFailure), Is.True);
        });
    }

    [Test]
    public void ApplyAsync_does_not_record_apply_duration_when_tree_id_is_empty()
    {
        // RecordApplyDuration skips emission when treeId is empty - a
        // validation throw on the tree-id guard must not publish a
        // sample with an empty `tree` tag (which would be unusable for
        // per-tree alerting). Pins the guard in the helper.
        using var collector = new MeterCollector<double>(
            LatticeReplicationMetrics.MeterName,
            LatticeReplicationMetrics.ApplyDurationName);
        var (applier, _, _, _) = CreateApplier();
        var entry = SetEntry("k", Hlc(10)) with { TreeId = string.Empty };

        Assert.That(
            async () => await applier.ApplyAsync(entry),
            Throws.InstanceOf<ArgumentException>());

        Assert.That(collector.Measurements, Is.Empty);
    }

    [Test]
    public void ApplyAsync_records_apply_duration_with_failure_outcome_when_origin_is_empty()
    {
        // Entry has a non-empty TreeId so the histogram IS recorded
        // (the guard skip only fires for empty TreeId). The empty
        // OriginClusterId throws ArgumentException out of the body
        // and the finally records outcome=failure.
        using var collector = new MeterCollector<double>(
            LatticeReplicationMetrics.MeterName,
            LatticeReplicationMetrics.ApplyDurationName);
        var (applier, _, _, _) = CreateApplier();
        var entry = SetEntry("k", Hlc(10)) with { OriginClusterId = string.Empty };

        Assert.That(
            async () => await applier.ApplyAsync(entry),
            Throws.InstanceOf<ArgumentException>());

        Assert.That(collector.Measurements, Has.Count.EqualTo(1));
        var only = collector.Measurements.Single();
        Assert.Multiple(() =>
        {
            Assert.That(only.Value, Is.GreaterThanOrEqualTo(0.0));
            Assert.That(HasTree(only.Tags, Tree), Is.True);
            Assert.That(HasOutcome(only.Tags, LatticeReplicationMetrics.OutcomeFailure), Is.True);
        });
    }

    [Test]
    public void ApplyAsync_records_apply_duration_with_failure_outcome_when_token_is_pre_cancelled()
    {
        // ApplyAsync calls ThrowIfCancellationRequested before any
        // validation; OperationCanceledException unwinds through the
        // finally and must be classified as outcome=failure (graceful
        // shutdown traffic appears in the failure bucket per docs).
        using var collector = new MeterCollector<double>(
            LatticeReplicationMetrics.MeterName,
            LatticeReplicationMetrics.ApplyDurationName);
        var (applier, _, _, _) = CreateApplier();
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        Assert.That(
            async () => await applier.ApplyAsync(SetEntry("k", Hlc(10)), cts.Token),
            Throws.InstanceOf<OperationCanceledException>());

        Assert.That(collector.Measurements, Has.Count.EqualTo(1));
        var only = collector.Measurements.Single();
        Assert.Multiple(() =>
        {
            Assert.That(only.Value, Is.GreaterThanOrEqualTo(0.0));
            Assert.That(HasTree(only.Tags, Tree), Is.True);
            Assert.That(HasOutcome(only.Tags, LatticeReplicationMetrics.OutcomeFailure), Is.True);
        });
    }
}
