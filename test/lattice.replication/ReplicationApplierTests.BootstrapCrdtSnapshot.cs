using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using NSubstitute;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Regression tests for the bootstrap committed-projection apply path
/// (issue #924). A cross-cluster bootstrap snapshot compacts the sender's
/// WAL into committed state, so each committed CRDT row carries the full
/// state in <see cref="WalRecord.Value"/> with <see cref="WalRecord.Delta"/>
/// null - the steady-state typed-delta wire shape is unavailable because the
/// individual deltas were GC'd. The applier must fold such rows in via a
/// state-based CRDT merge instead of rejecting the missing delta; before the
/// fix the receiver threw
/// <c>WalRecord.Delta must be non-null ... during ApplyingSnapshot</c> and
/// the replicated projection never rebuilt (silent data loss on restart).
/// </summary>
public partial class ReplicationApplierTests
{
    private static byte[] EncodeOrFlag(Action<OrFlag>? configure = null)
    {
        var flag = new OrFlag();
        configure?.Invoke(flag);
        return JsonLatticeSerializer<OrFlag>.Default.Serialize(flag);
    }

    private static byte[] EncodeRwFlag(Action<RwFlag>? configure = null)
    {
        var flag = new RwFlag();
        configure?.Invoke(flag);
        return JsonLatticeSerializer<RwFlag>.Default.Serialize(flag);
    }

    private static byte[] EncodeRga(Action<Rga>? configure = null)
    {
        var rga = new Rga();
        configure?.Invoke(rga);
        return JsonLatticeSerializer<Rga>.Default.Serialize(rga);
    }

    private static WalRecord SnapshotEntry(string key, HybridLogicalClock ts, LatticeMergeMode mode, byte[] value) =>
        SetEntry(key, ts) with
        {
            Mode = mode,
            Value = value,
            // The committed-projection bootstrap row shape: full state in
            // Value, no typed Delta.
            Delta = null,
        };

    // ------------------------------------------------------------------
    // Full-state merge dispatch per CRDT mode (Delta == null, Value set)
    // ------------------------------------------------------------------

    [Test]
    public async Task ApplyAsync_bootstrap_or_set_full_state_installs_into_empty_receiver()
    {
        var (applier, lattice, apply, _) = CreateTypedCrdtApplier();
        var entry = SnapshotEntry("k", Hlc(10), LatticeMergeMode.OrSet,
            EncodeOrSet(s => s.Add(OrSetMember, "site-b", 1)));

        var result = await applier.ApplyAsync(entry);

        Assert.That(result.Applied, Is.True);
        // The receiver key is absent (default substitute returns Value=null),
        // so the full-state bytes are installed verbatim - no apply-grain
        // LWW write.
        await apply.DidNotReceiveWithAnyArgs().ApplySetAsync(default!, default!, default, default!, default, default);
        await lattice.Received(1).SetIfVersionAsync(
            "k",
            Arg.Is<byte[]>(b => JsonLatticeSerializer<OrSet>.Default.Deserialize(b).Contains(OrSetMember)),
            Arg.Any<HybridLogicalClock>(),
            Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task ApplyAsync_bootstrap_pn_counter_full_state_merges()
    {
        var (applier, lattice, _, _) = CreateTypedCrdtApplier();
        var entry = SnapshotEntry("k", Hlc(11), LatticeMergeMode.PnCounter,
            EncodePnCounter(c => c.Increment("site-b", 7)));

        var result = await applier.ApplyAsync(entry);

        Assert.That(result.Applied, Is.True);
        await lattice.Received(1).SetIfVersionAsync(
            "k",
            Arg.Is<byte[]>(b => JsonLatticeSerializer<PnCounter>.Default.Deserialize(b).Value == 7),
            Arg.Any<HybridLogicalClock>(),
            Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task ApplyAsync_bootstrap_version_vector_full_state_merges()
    {
        var (applier, lattice, _, _) = CreateTypedCrdtApplier();
        var remoteHlc = Hlc(42, 3);
        var entry = SnapshotEntry("k", Hlc(12), LatticeMergeMode.VersionVector,
            EncodeVersionVector(v => v.Entries["site-b"] = remoteHlc));

        var result = await applier.ApplyAsync(entry);

        Assert.That(result.Applied, Is.True);
        await lattice.Received(1).SetIfVersionAsync(
            "k",
            Arg.Is<byte[]>(b => JsonLatticeSerializer<VersionVector>.Default.Deserialize(b).GetClock("site-b") == remoteHlc),
            Arg.Any<HybridLogicalClock>(),
            Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task ApplyAsync_bootstrap_mv_register_full_state_merges()
    {
        var (applier, lattice, _, _) = CreateTypedCrdtApplier();
        var entry = SnapshotEntry("k", Hlc(13), LatticeMergeMode.MvRegister,
            EncodeMvRegister(r => r.Set("site-b", new byte[] { 0xab })));

        var result = await applier.ApplyAsync(entry);

        Assert.That(result.Applied, Is.True);
        await lattice.Received(1).SetIfVersionAsync(
            "k",
            Arg.Is<byte[]>(b => JsonLatticeSerializer<MvRegister>.Default.Deserialize(b).Context.ContainsKey("site-b")),
            Arg.Any<HybridLogicalClock>(),
            Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task ApplyAsync_bootstrap_or_flag_full_state_merges()
    {
        var (applier, lattice, _, _) = CreateTypedCrdtApplier();
        var entry = SnapshotEntry("k", Hlc(14), LatticeMergeMode.OrFlag,
            EncodeOrFlag(f => f.Enable("site-b", 1)));

        var result = await applier.ApplyAsync(entry);

        Assert.That(result.Applied, Is.True);
        await lattice.Received(1).SetIfVersionAsync(
            "k",
            Arg.Is<byte[]>(b => JsonLatticeSerializer<OrFlag>.Default.Deserialize(b).IsEnabled),
            Arg.Any<HybridLogicalClock>(),
            Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task ApplyAsync_bootstrap_rw_flag_full_state_merges()
    {
        var (applier, lattice, _, _) = CreateTypedCrdtApplier();
        var entry = SnapshotEntry("k", Hlc(15), LatticeMergeMode.RwFlag,
            EncodeRwFlag(f => f.Enable("site-b", 1)));

        var result = await applier.ApplyAsync(entry);

        Assert.That(result.Applied, Is.True);
        await lattice.Received(1).SetIfVersionAsync(
            "k",
            Arg.Is<byte[]>(b => JsonLatticeSerializer<RwFlag>.Default.Deserialize(b).IsEnabled),
            Arg.Any<HybridLogicalClock>(),
            Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task ApplyAsync_bootstrap_sequence_full_state_merges()
    {
        var (applier, lattice, _, _) = CreateTypedCrdtApplier();
        var entry = SnapshotEntry("k", Hlc(16), LatticeMergeMode.Sequence,
            EncodeRga(r => r.InsertAfter(default, "site-b", new byte[] { 0x01 })));

        var result = await applier.ApplyAsync(entry);

        Assert.That(result.Applied, Is.True);
        await lattice.Received(1).SetIfVersionAsync(
            "k",
            Arg.Is<byte[]>(b => JsonLatticeSerializer<Rga>.Default.Deserialize(b).ToList().Count == 1),
            Arg.Any<HybridLogicalClock>(),
            Arg.Any<CancellationToken>());
    }

    // ------------------------------------------------------------------
    // Lossless state-merge into a non-empty receiver
    // ------------------------------------------------------------------

    [Test]
    public async Task ApplyAsync_bootstrap_or_set_full_state_unions_with_existing_receiver_state()
    {
        // The receiver already holds a locally-added member; the bootstrap
        // full state carries a different member. A state-based CRDT merge
        // must yield the union - neither the local add nor the snapshot add
        // may be clobbered (a blind LWW overwrite would lose one).
        var (applier, lattice, _, _) = CreateTypedCrdtApplier();
        var localMember = new byte[] { 0x01 };
        var remoteMember = new byte[] { 0x02 };
        var existing = EncodeOrSet(s => s.Add(localMember, "site-a", 1));
        lattice.GetWithVersionAsync("k", Arg.Any<CancellationToken>())
            .Returns(new VersionedValue { Value = existing, Version = Hlc(5) });

        byte[]? written = null;
        lattice.SetIfVersionAsync("k", Arg.Any<byte[]>(), Arg.Any<HybridLogicalClock>(), Arg.Any<CancellationToken>())
            .Returns(ci => { written = ci.ArgAt<byte[]>(1); return true; });

        var entry = SnapshotEntry("k", Hlc(20), LatticeMergeMode.OrSet,
            EncodeOrSet(s => s.Add(remoteMember, "site-b", 1)));

        var result = await applier.ApplyAsync(entry);

        Assert.That(result.Applied, Is.True);
        Assert.That(written, Is.Not.Null);
        var merged = JsonLatticeSerializer<OrSet>.Default.Deserialize(written!);
        Assert.That(merged.Contains(localMember), Is.True, "local add must survive the bootstrap merge");
        Assert.That(merged.Contains(remoteMember), Is.True, "snapshot add must be applied");
    }

    [Test]
    public async Task ApplyAsync_bootstrap_full_state_advances_high_water_mark()
    {
        var (applier, _, _, hwm) = CreateTypedCrdtApplier();
        var ts = Hlc(77, 2);
        var entry = SnapshotEntry("k", ts, LatticeMergeMode.OrSet,
            EncodeOrSet(s => s.Add(OrSetMember, "site-b", 1)));

        var result = await applier.ApplyAsync(entry);

        Assert.That(result.HighWaterMark, Is.EqualTo(ts));
        await hwm.Received(1).TryAdvanceAsync(RemoteCluster, ts, Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task ApplyAsync_bootstrap_full_state_retries_on_cas_failure()
    {
        var (applier, lattice, _, _) = CreateTypedCrdtApplier();
        // A non-empty receiver forces the merge-and-write loop (not the
        // verbatim install fast-path), so each CAS attempt re-reads and
        // re-writes; the first two lose the race.
        lattice.GetWithVersionAsync("k", Arg.Any<CancellationToken>())
            .Returns(new VersionedValue { Value = EncodeOrSet(s => s.Add(new byte[] { 0x01 }, "site-a", 1)), Version = Hlc(5) });
        lattice.SetIfVersionAsync(Arg.Any<string>(), Arg.Any<byte[]>(), Arg.Any<HybridLogicalClock>(), Arg.Any<CancellationToken>())
            .Returns(false, false, true);

        var entry = SnapshotEntry("k", Hlc(30), LatticeMergeMode.OrSet,
            EncodeOrSet(s => s.Add(OrSetMember, "site-b", 1)));

        var result = await applier.ApplyAsync(entry);

        Assert.That(result.Applied, Is.True);
        await lattice.Received(3).SetIfVersionAsync(
            "k", Arg.Any<byte[]>(), Arg.Any<HybridLogicalClock>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public void ApplyAsync_bootstrap_full_state_throws_when_both_value_and_delta_null()
    {
        // A CRDT-mode Set with neither a Delta (steady-state) nor a Value
        // (bootstrap) is malformed and must fault rather than silently
        // installing empty state.
        var (applier, _, _, _) = CreateTypedCrdtApplier();
        var entry = SetEntry("k", Hlc(1)) with
        {
            Mode = LatticeMergeMode.PnCounter,
            Value = null,
            Delta = null,
        };

        Assert.That(async () => await applier.ApplyAsync(entry), Throws.ArgumentException);
    }
}
