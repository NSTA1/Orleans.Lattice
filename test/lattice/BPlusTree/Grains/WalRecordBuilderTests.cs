using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Unit tests for the internal <see cref="WalRecordBuilder"/> helper that
/// constructs a <see cref="WalRecord"/> directly from the leaf-side
/// commit context. Each helper captures the four ambient
/// <c>Lattice*Context</c> values; these tests pin "ambient X is mirrored
/// to slot Y" so a future re-shuffle of context names cannot silently
/// drop a slot.
/// </summary>
[TestFixture]
public class WalRecordBuilderTests
{
    private const string TreeId = "tree-x";
    private const int ShardIndex = 3;

    [SetUp]
    public void EnsureCleanContext()
    {
        // Sibling tests may have leaked context.
        RequestContext.Remove("ol.maint");
        RequestContext.Remove("ol.delta");
        RequestContext.Remove("ol.batch");
        RequestContext.Remove("ol.txid");
    }

    private static LwwValue<byte[]> StampedValue(byte[] value, string? origin = "site-a")
    {
        return LwwValue<byte[]>.CreateWithExpiry(value, HybridLogicalClock.Tick(HybridLogicalClock.Zero), 0L)
            with
            {
                OriginClusterId = origin,
                VectorClock = null,
            };
    }

    private static LwwValue<byte[]> StampedTombstone(string? origin = "site-a")
    {
        return LwwValue<byte[]>.Tombstone(HybridLogicalClock.Tick(HybridLogicalClock.Zero))
            with
            {
                OriginClusterId = origin,
                VectorClock = null,
            };
    }

    // --- ForSet ---

    [Test]
    public void ForSet_mirrors_committed_value_metadata_onto_record()
    {
        var committed = StampedValue(new byte[] { 1, 2, 3 });

        var entry = WalRecordBuilder.ForSet(TreeId, ShardIndex, "k", committed, isPrepared: false);

        Assert.Multiple(() =>
        {
            Assert.That(entry.TreeId, Is.EqualTo(TreeId));
            Assert.That(entry.Op, Is.EqualTo(MutationKind.Set));
            Assert.That(entry.Key, Is.EqualTo("k"));
            Assert.That(entry.Value, Is.EqualTo(committed.Value));
            Assert.That(entry.Timestamp, Is.EqualTo(committed.Timestamp));
            Assert.That(entry.IsTombstone, Is.False);
            Assert.That(entry.ExpiresAtTicks, Is.EqualTo(committed.ExpiresAtTicks));
            Assert.That(entry.OriginClusterId, Is.EqualTo(committed.OriginClusterId));
            Assert.That(entry.ShardIndex, Is.EqualTo(ShardIndex));
            Assert.That(entry.IsPrepared, Is.False);
        });
    }

    [Test]
    public void ForSet_with_prepared_true_stamps_IsPrepared_on_record()
    {
        var committed = StampedValue(new byte[] { 9 });

        var entry = WalRecordBuilder.ForSet(TreeId, ShardIndex, "k", committed, isPrepared: true);

        Assert.That(entry.IsPrepared, Is.True);
    }

    [Test]
    public void ForSet_carries_tombstone_value_as_null_when_committed_is_a_tombstone()
    {
        // CreateWithExpiry can produce a tombstone if the value is null;
        // ensure the builder zeroes the Value slot in that branch.
        var committed = StampedTombstone();

        var entry = WalRecordBuilder.ForSet(TreeId, ShardIndex, "k", committed, isPrepared: false);

        Assert.Multiple(() =>
        {
            Assert.That(entry.IsTombstone, Is.True);
            Assert.That(entry.Value, Is.Null);
        });
    }

    [Test]
    public void ForSet_captures_LatticeDeltaContext_into_Delta()
    {
        var committed = StampedValue(new byte[] { 1 });

        using (LatticeDeltaContext.With(new byte[] { 0xAA, 0xBB }))
        {
            var entry = WalRecordBuilder.ForSet(TreeId, ShardIndex, "k", committed, isPrepared: false);

            Assert.That(entry.Delta, Is.EqualTo(new byte[] { 0xAA, 0xBB }));
        }
    }

    [Test]
    public void ForSet_captures_LatticeAtomicBatchContext_into_AtomicBatch_slots()
    {
        var committed = StampedValue(new byte[] { 1 });

        using (LatticeAtomicBatchContext.With((Size: 4, Index: 2)))
        {
            var entry = WalRecordBuilder.ForSet(TreeId, ShardIndex, "k", committed, isPrepared: false);

            Assert.Multiple(() =>
            {
                Assert.That(entry.AtomicBatchSize, Is.EqualTo(4));
                Assert.That(entry.AtomicBatchIndex, Is.EqualTo(2));
            });
        }
    }

    [Test]
    public void ForSet_captures_LatticeTransactionContext_into_TransactionId()
    {
        var committed = StampedValue(new byte[] { 1 });
        var txId = Guid.NewGuid();
        LatticeTransactionContext.Set(txId);
        try
        {
            var entry = WalRecordBuilder.ForSet(TreeId, ShardIndex, "k", committed, isPrepared: false);
            Assert.That(entry.TransactionId, Is.EqualTo(txId));
        }
        finally
        {
            RequestContext.Remove("ol.txid");
        }
    }

    [Test]
    public void ForSet_captures_LatticeMaintenanceContext_into_Category()
    {
        var committed = StampedValue(new byte[] { 1 });

        using (LatticeMaintenanceContext.BeginScope())
        {
            var entry = WalRecordBuilder.ForSet(TreeId, ShardIndex, "k", committed, isPrepared: false);
            Assert.That(entry.Category, Is.EqualTo(MutationCategory.Maintenance));
        }
    }

    [Test]
    public void ForSet_with_no_ambient_context_emits_record_with_all_default_metadata()
    {
        var committed = StampedValue(new byte[] { 1 });

        var entry = WalRecordBuilder.ForSet(TreeId, ShardIndex, "k", committed, isPrepared: false);

        Assert.Multiple(() =>
        {
            Assert.That(entry.Delta, Is.Null);
            Assert.That(entry.AtomicBatchSize, Is.Zero);
            Assert.That(entry.AtomicBatchIndex, Is.Zero);
            Assert.That(entry.TransactionId, Is.EqualTo(Guid.Empty));
            Assert.That(entry.Category, Is.EqualTo(MutationCategory.User));
        });
    }

    // --- ForDelete ---

    [Test]
    public void ForDelete_emits_tombstone_record_with_mirrored_metadata()
    {
        var tombstone = StampedTombstone();

        var entry = WalRecordBuilder.ForDelete(TreeId, ShardIndex, "k", tombstone, isPrepared: false);

        Assert.Multiple(() =>
        {
            Assert.That(entry.TreeId, Is.EqualTo(TreeId));
            Assert.That(entry.Op, Is.EqualTo(MutationKind.Delete));
            Assert.That(entry.Key, Is.EqualTo("k"));
            Assert.That(entry.Value, Is.Null);
            Assert.That(entry.Timestamp, Is.EqualTo(tombstone.Timestamp));
            Assert.That(entry.IsTombstone, Is.True);
            Assert.That(entry.OriginClusterId, Is.EqualTo(tombstone.OriginClusterId));
            Assert.That(entry.ShardIndex, Is.EqualTo(ShardIndex));
            Assert.That(entry.IsPrepared, Is.False);
        });
    }

    [Test]
    public void ForDelete_with_prepared_true_stamps_IsPrepared_on_record()
    {
        var tombstone = StampedTombstone();

        var entry = WalRecordBuilder.ForDelete(TreeId, ShardIndex, "k", tombstone, isPrepared: true);

        Assert.That(entry.IsPrepared, Is.True);
    }

    [Test]
    public void ForDelete_captures_all_ambient_contexts()
    {
        var tombstone = StampedTombstone();
        var txId = Guid.NewGuid();
        LatticeTransactionContext.Set(txId);
        try
        {
            using (LatticeDeltaContext.With(new byte[] { 1 }))
            using (LatticeAtomicBatchContext.With((Size: 8, Index: 6)))
            using (LatticeMaintenanceContext.BeginScope())
            {
                var entry = WalRecordBuilder.ForDelete(TreeId, ShardIndex, "k", tombstone, isPrepared: false);

                Assert.Multiple(() =>
                {
                    Assert.That(entry.Delta, Is.EqualTo(new byte[] { 1 }));
                    Assert.That(entry.AtomicBatchSize, Is.EqualTo(8));
                    Assert.That(entry.AtomicBatchIndex, Is.EqualTo(6));
                    Assert.That(entry.TransactionId, Is.EqualTo(txId));
                    Assert.That(entry.Category, Is.EqualTo(MutationCategory.Maintenance));
                });
            }
        }
        finally
        {
            RequestContext.Remove("ol.txid");
        }
    }

    // --- ForDeleteRange ---

    [Test]
    public void ForDeleteRange_emits_range_tombstone_with_inclusive_start_and_exclusive_end()
    {
        var tombstone = StampedTombstone();

        var entry = WalRecordBuilder.ForDeleteRange(TreeId, ShardIndex, "a", "z", tombstone);

        Assert.Multiple(() =>
        {
            Assert.That(entry.TreeId, Is.EqualTo(TreeId));
            Assert.That(entry.Op, Is.EqualTo(MutationKind.DeleteRange));
            Assert.That(entry.Key, Is.EqualTo("a"));
            Assert.That(entry.EndExclusiveKey, Is.EqualTo("z"));
            Assert.That(entry.Timestamp, Is.EqualTo(tombstone.Timestamp));
            Assert.That(entry.IsTombstone, Is.True);
            Assert.That(entry.OriginClusterId, Is.EqualTo(tombstone.OriginClusterId));
            Assert.That(entry.ShardIndex, Is.EqualTo(ShardIndex));
        });
    }

    [Test]
    public void ForDeleteRange_captures_LatticeDeltaContext_LatticeTransactionContext_and_LatticeMaintenanceContext()
    {
        var tombstone = StampedTombstone();
        var txId = Guid.NewGuid();
        LatticeTransactionContext.Set(txId);
        try
        {
            using (LatticeDeltaContext.With(new byte[] { 0x42 }))
            using (LatticeMaintenanceContext.BeginScope())
            {
                var entry = WalRecordBuilder.ForDeleteRange(TreeId, ShardIndex, "a", "z", tombstone);

                Assert.Multiple(() =>
                {
                    Assert.That(entry.Delta, Is.EqualTo(new byte[] { 0x42 }));
                    Assert.That(entry.TransactionId, Is.EqualTo(txId));
                    Assert.That(entry.Category, Is.EqualTo(MutationCategory.Maintenance));
                });
            }
        }
        finally
        {
            RequestContext.Remove("ol.txid");
        }
    }
}
