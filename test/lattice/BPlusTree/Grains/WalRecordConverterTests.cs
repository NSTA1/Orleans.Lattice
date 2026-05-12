using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// the dormant seam - translation invariants between <see cref="LatticeMutation"/>
/// and <see cref="WalRecord"/>. Mirrors the field-by-field semantics
/// established by the existing <c>ReplicationMutationObserver</c>.
/// </summary>
[TestFixture]
public class WalRecordConverterTests
{
    [Test]
    public void ToReplogEntry_translates_Set_mutation_with_all_fields()
    {
        var txId = Guid.NewGuid();
        var mutation = new LatticeMutation
        {
            TreeId = "tree-A",
            Kind = MutationKind.Set,
            Key = "k",
            EndExclusiveKey = null,
            Value = new byte[] { 1, 2, 3 },
            Timestamp = HybridLogicalClock.Tick(HybridLogicalClock.Zero),
            IsTombstone = false,
            ExpiresAtTicks = 12345L,
            OriginClusterId = null,
            VectorClock = null,
            TransactionId = txId,
            AtomicBatchSize = 7,
            AtomicBatchIndex = 3,
            Category = MutationCategory.User,
            DeltaKind = "lww",
            DeltaPayload = new byte[] { 9, 9 },
        };

        var entry = WalRecordConverter.ToWalRecord(mutation, LatticeMergeMode.LwwRegister, "cluster-A");

        Assert.Multiple(() =>
        {
            Assert.That(entry.TreeId, Is.EqualTo("tree-A"));
            Assert.That(entry.Op, Is.EqualTo(MutationKind.Set));
            Assert.That(entry.Key, Is.EqualTo("k"));
            Assert.That(entry.EndExclusiveKey, Is.Null);
            Assert.That(entry.Value, Is.EqualTo(new byte[] { 1, 2, 3 }));
            Assert.That(entry.Timestamp, Is.EqualTo(mutation.Timestamp));
            Assert.That(entry.IsTombstone, Is.False);
            Assert.That(entry.ExpiresAtTicks, Is.EqualTo(12345L));
            Assert.That(entry.OriginClusterId, Is.EqualTo("cluster-A"));
            Assert.That(entry.Mode, Is.EqualTo(LatticeMergeMode.LwwRegister));
            Assert.That(entry.DeltaKind, Is.EqualTo("lww"));
            Assert.That(entry.DeltaPayload, Is.EqualTo(new byte[] { 9, 9 }));
            Assert.That(entry.TransactionId, Is.EqualTo(txId));
            Assert.That(entry.AtomicBatchSize, Is.EqualTo(7));
            Assert.That(entry.AtomicBatchIndex, Is.EqualTo(3));
        });
    }

    [Test]
    public void ToReplogEntry_preserves_existing_origin_over_supplied_default()
    {
        var mutation = new LatticeMutation
        {
            TreeId = "tree-A",
            Kind = MutationKind.Set,
            Key = "k",
            Value = Array.Empty<byte>(),
            Timestamp = HybridLogicalClock.Tick(HybridLogicalClock.Zero),
            OriginClusterId = "remote-origin",
        };

        var entry = WalRecordConverter.ToWalRecord(mutation, LatticeMergeMode.LwwRegister, "local-cluster");

        Assert.That(entry.OriginClusterId, Is.EqualTo("remote-origin"));
    }

    [Test]
    public void ToReplogEntry_translates_DeleteRange()
    {
        var mutation = new LatticeMutation
        {
            TreeId = "tree-A",
            Kind = MutationKind.DeleteRange,
            Key = "a",
            EndExclusiveKey = "z",
            IsTombstone = true,
            Timestamp = HybridLogicalClock.Zero,
        };

        var entry = WalRecordConverter.ToWalRecord(mutation, LatticeMergeMode.LwwRegister, "cluster-A");

        Assert.Multiple(() =>
        {
            Assert.That(entry.Op, Is.EqualTo(MutationKind.DeleteRange));
            Assert.That(entry.Key, Is.EqualTo("a"));
            Assert.That(entry.EndExclusiveKey, Is.EqualTo("z"));
            Assert.That(entry.IsTombstone, Is.True);
        });
    }

    [Test]
    public void ToReplogEntry_clones_VectorClock_defensively()
    {
        var vc = new VersionVector();
        vc.Tick("origin-A");

        var mutation = new LatticeMutation
        {
            TreeId = "tree-A",
            Kind = MutationKind.Set,
            Key = "k",
            Value = Array.Empty<byte>(),
            Timestamp = HybridLogicalClock.Tick(HybridLogicalClock.Zero),
            VectorClock = vc,
        };

        var entry = WalRecordConverter.ToWalRecord(mutation, LatticeMergeMode.LwwRegister, "cluster-A");

        Assert.That(entry.VectorClock, Is.Not.Null);
        Assert.That(entry.VectorClock, Is.Not.SameAs(vc), "must be a defensive clone");
        Assert.That(entry.DependencySummary, Is.SameAs(entry.VectorClock), "summary aliases the cloned frontier");
    }

    [Test]
    public void FromReplogEntry_reverses_a_Set_translation()
    {
        var original = new LatticeMutation
        {
            TreeId = "tree-A",
            Kind = MutationKind.Set,
            Key = "k",
            Value = new byte[] { 7 },
            Timestamp = HybridLogicalClock.Tick(HybridLogicalClock.Zero),
            ExpiresAtTicks = 999L,
            OriginClusterId = "origin-A",
            DeltaKind = "lww",
            DeltaPayload = new byte[] { 1 },
        };

        var entry = WalRecordConverter.ToWalRecord(original, LatticeMergeMode.LwwRegister, "cluster-A");
        var roundTripped = WalRecordConverter.FromWalRecord(entry);

        Assert.Multiple(() =>
        {
            Assert.That(roundTripped.TreeId, Is.EqualTo(original.TreeId));
            Assert.That(roundTripped.Kind, Is.EqualTo(original.Kind));
            Assert.That(roundTripped.Key, Is.EqualTo(original.Key));
            Assert.That(roundTripped.Value, Is.EqualTo(original.Value));
            Assert.That(roundTripped.Timestamp, Is.EqualTo(original.Timestamp));
            Assert.That(roundTripped.ExpiresAtTicks, Is.EqualTo(original.ExpiresAtTicks));
            Assert.That(roundTripped.OriginClusterId, Is.EqualTo(original.OriginClusterId));
            Assert.That(roundTripped.DeltaKind, Is.EqualTo(original.DeltaKind));
            Assert.That(roundTripped.DeltaPayload, Is.EqualTo(original.DeltaPayload));
            // TransactionId round-trips verbatim through the wire format
            // (the original here did not set one, so it defaults to Empty).
            Assert.That(roundTripped.TransactionId, Is.EqualTo(Guid.Empty));
            // Category is not on the replication wire today; defaults to User.
            Assert.That(roundTripped.Category, Is.EqualTo(MutationCategory.User));
        });
    }

    [Test]
    public void FromReplogEntry_reverses_a_Delete_translation()
    {
        var original = new LatticeMutation
        {
            TreeId = "tree-A",
            Kind = MutationKind.Delete,
            Key = "k",
            IsTombstone = true,
            Timestamp = HybridLogicalClock.Tick(HybridLogicalClock.Zero),
        };

        var entry = WalRecordConverter.ToWalRecord(original, LatticeMergeMode.LwwRegister, "cluster-A");
        var roundTripped = WalRecordConverter.FromWalRecord(entry);

        Assert.Multiple(() =>
        {
            Assert.That(roundTripped.Kind, Is.EqualTo(MutationKind.Delete));
            Assert.That(roundTripped.IsTombstone, Is.True);
            Assert.That(roundTripped.Key, Is.EqualTo("k"));
        });
    }

    [Test]
    public void ToReplogEntry_preserves_atomic_batch_metadata_round_trip()
    {
        // Producer-side atomic-batch stamping (LatticeAtomicBatchContext +
        // BPlusLeafGrain.MutationObserver) writes AtomicBatchSize/Index and
        // a shared TransactionId onto every per-key LatticeMutation in the
        // batch. The converter is on the WAL read-back path
        // (WalShardGrain.GetPageAsync) and the commit-log write path
        // (WalCommitLogWriter), so it must preserve all three slots
        // verbatim in both directions - otherwise the receiver-side
        // atomic-batch buffer gate (entry.AtomicBatchSize > 0) is unreachable
        // for any cross-cluster atomic batch.
        var txId = Guid.NewGuid();
        var mutation = new LatticeMutation
        {
            TreeId = "tree-A",
            Kind = MutationKind.Set,
            Key = "k",
            Value = new byte[] { 0xFF },
            Timestamp = HybridLogicalClock.Tick(HybridLogicalClock.Zero),
            TransactionId = txId,
            AtomicBatchSize = 5,
            AtomicBatchIndex = 2,
        };

        var entry = WalRecordConverter.ToWalRecord(mutation, LatticeMergeMode.LwwRegister, "cluster-A");
        var roundTripped = WalRecordConverter.FromWalRecord(entry);

        Assert.Multiple(() =>
        {
            Assert.That(entry.TransactionId, Is.EqualTo(txId), "forward: TransactionId");
            Assert.That(entry.AtomicBatchSize, Is.EqualTo(5), "forward: AtomicBatchSize");
            Assert.That(entry.AtomicBatchIndex, Is.EqualTo(2), "forward: AtomicBatchIndex");
            Assert.That(roundTripped.TransactionId, Is.EqualTo(txId), "reverse: TransactionId");
            Assert.That(roundTripped.AtomicBatchSize, Is.EqualTo(5), "reverse: AtomicBatchSize");
            Assert.That(roundTripped.AtomicBatchIndex, Is.EqualTo(2), "reverse: AtomicBatchIndex");
        });
    }

    [Test]
    public void ToReplogEntry_leaves_atomic_batch_metadata_at_defaults_for_non_atomic_writes()
    {
        // Non-atomic writes (the default SetAsync path) flow through the
        // same converter; their AtomicBatchSize/Index/TransactionId slots
        // must remain at their wire-compat defaults so the receiver-side
        // gate (AtomicBatchSize > 0) routes them through the point-apply
        // path rather than the buffered atomic-tx path.
        var mutation = new LatticeMutation
        {
            TreeId = "tree-A",
            Kind = MutationKind.Set,
            Key = "k",
            Value = Array.Empty<byte>(),
            Timestamp = HybridLogicalClock.Tick(HybridLogicalClock.Zero),
        };

        var entry = WalRecordConverter.ToWalRecord(mutation, LatticeMergeMode.LwwRegister, "cluster-A");
        var roundTripped = WalRecordConverter.FromWalRecord(entry);

        Assert.Multiple(() =>
        {
            Assert.That(entry.AtomicBatchSize, Is.Zero);
            Assert.That(entry.AtomicBatchIndex, Is.Zero);
            Assert.That(entry.TransactionId, Is.EqualTo(Guid.Empty));
            Assert.That(roundTripped.AtomicBatchSize, Is.Zero);
            Assert.That(roundTripped.AtomicBatchIndex, Is.Zero);
            Assert.That(roundTripped.TransactionId, Is.EqualTo(Guid.Empty));
        });
    }
}

[TestFixture]
public class WalRecordConverterShardIndexTests
{
    [Test]
    public void ToWalRecord_mirrors_ShardIndex_to_entry()
    {
        // Forward conversion: the ShardIndex slot stamped on the
        // foreground LatticeMutation must surface verbatim on the
        // resulting WalRecord so receivers (replication apply path,
        // operator tooling) see the originating chain shard.
        var mutation = new LatticeMutation
        {
            TreeId = "tree-A",
            Kind = MutationKind.Set,
            Key = "k",
            Value = new byte[] { 0xFF },
            Timestamp = HybridLogicalClock.Tick(HybridLogicalClock.Zero),
            ShardIndex = 4,
        };

        var entry = WalRecordConverter.ToWalRecord(mutation, LatticeMergeMode.LwwRegister, "cluster-A");

        Assert.That(entry.ShardIndex, Is.EqualTo(4));
    }

    [Test]
    public void FromWalRecord_mirrors_ShardIndex_back_to_mutation()
    {
        // Reverse conversion: when a WAL slice is read back at
        // activation-time replay, the persisted ShardIndex must
        // surface on the materialised LatticeMutation so the
        // apply-time filter on the leaf can compare it against the
        // leaf's own persisted shard index.
        var entry = new WalRecord
        {
            TreeId = "tree-A",
            Op = MutationKind.Set,
            Key = "k",
            Value = new byte[] { 0xFF },
            Timestamp = HybridLogicalClock.Tick(HybridLogicalClock.Zero),
            OriginClusterId = "cluster-A",
            ShardIndex = 6,
        };

        var roundTripped = WalRecordConverter.FromWalRecord(entry);

        Assert.That(roundTripped.ShardIndex, Is.EqualTo(6));
    }

    [Test]
    public void Conversion_preserves_ShardIndex_round_trip_through_both_directions()
    {
        var original = new LatticeMutation
        {
            TreeId = "tree-A",
            Kind = MutationKind.Set,
            Key = "k",
            Value = new byte[] { 7 },
            Timestamp = HybridLogicalClock.Tick(HybridLogicalClock.Zero),
            ShardIndex = 9,
        };

        var entry = WalRecordConverter.ToWalRecord(original, LatticeMergeMode.LwwRegister, "cluster-A");
        var roundTripped = WalRecordConverter.FromWalRecord(entry);

        Assert.Multiple(() =>
        {
            Assert.That(entry.ShardIndex, Is.EqualTo(9), "forward: ShardIndex mirrored to entry");
            Assert.That(roundTripped.ShardIndex, Is.EqualTo(9), "reverse: ShardIndex mirrored back to mutation");
        });
    }

    [Test]
    public void Conversion_leaves_ShardIndex_at_zero_for_unstamped_mutation()
    {
        // Wire-compat: a mutation with no explicit ShardIndex
        // (e.g. legacy producers, V1 single-shard tests) carries
        // the default zero value, and that must survive both
        // conversion directions so the receiver-side filter
        // observes the shard-0 default rather than a corrupted
        // value.
        var original = new LatticeMutation
        {
            TreeId = "tree-A",
            Kind = MutationKind.Set,
            Key = "k",
            Value = Array.Empty<byte>(),
            Timestamp = HybridLogicalClock.Tick(HybridLogicalClock.Zero),
        };

        var entry = WalRecordConverter.ToWalRecord(original, LatticeMergeMode.LwwRegister, "cluster-A");
        var roundTripped = WalRecordConverter.FromWalRecord(entry);

        Assert.Multiple(() =>
        {
            Assert.That(entry.ShardIndex, Is.Zero);
            Assert.That(roundTripped.ShardIndex, Is.Zero);
        });
    }
}

