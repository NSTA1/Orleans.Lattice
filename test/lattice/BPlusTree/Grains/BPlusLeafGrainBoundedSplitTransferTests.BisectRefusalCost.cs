using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Testing;
using System.Diagnostics.Metrics;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// The cost model documented on <see cref="LatticeMetrics.LeafBisectRefusals"/>,
/// pinned at the production seam (issue #2856).
/// <para>
/// What the split's <c>Cache.Keys</c> fallback costs is decided by whether a
/// snapshot frame is still attached when the bisect refuses, not by whether a
/// frame was ever detached. <c>NoSnapshotAttached</c> is reported exactly when
/// no frame is attached, so a refusal carrying a detach seam other than
/// <c>none</c> finds the leaf already fully resident - the cost was paid
/// earlier, by the named seam - and the fallback materialises nothing. A
/// refusal taken with a frame still attached is the opposite: the fallback
/// itself materialises the whole remainder of the leaf and detaches the frame.
/// Since issue #5607 the bisect walks the frame and resident rows together, so
/// a frame-attached refusal is reachable only through an unreadable frame key
/// or a row-accounting disagreement; the second arm pins that an emptied
/// hydrated block no longer produces one. An earlier revision of the instrument's documentation had those two the
/// wrong way round, sending operators to the split path to chase a cost paid on
/// the read path. Both arms drive the real <c>SplitAsync</c>, so the refusal
/// they observe is the one production records, and the materialisation they
/// measure is the one production performs.
/// </para>
/// </summary>
public sealed partial class BPlusLeafGrainBoundedSplitTransferTests
{
    private sealed record BisectRefusal(string Reason, string DetachSeam);

    private static MeterListener ListenForBisectRefusals(string treeId, List<BisectRefusal> sink)
        => MeterListening.StartForInstrument(
            LatticeMetrics.LeafBisectRefusals,
            l => l.SetMeasurementEventCallback<long>((_, _, tags, _) =>
            {
                string? tree = null, reason = null, seam = null;
                foreach (var tag in tags)
                {
                    if (tag.Key == LatticeMetrics.TagTree)
                    {
                        tree = tag.Value?.ToString();
                    }
                    else if (tag.Key == LatticeMetrics.TagReason)
                    {
                        reason = tag.Value?.ToString();
                    }
                    else if (tag.Key == LatticeMetrics.TagDetachSeam)
                    {
                        seam = tag.Value?.ToString();
                    }
                }

                // Filtered on a per-test tree id: other fixtures divide leaves
                // concurrently and would otherwise land in this sink.
                if (tree == treeId)
                {
                    lock (sink) sink.Add(new BisectRefusal(reason ?? string.Empty, seam ?? string.Empty));
                }
            }));

    private static string UniqueTreeId() => $"tree-bisect-cost-{Guid.NewGuid():N}";

    [Test]
    public async Task A_detach_seam_refusal_finds_the_leaf_resident_and_the_fallback_materialises_nothing()
    {
        const int rowCount = 2048;
        var treeId = UniqueTreeId();
        var (grain, _, recorder, _) = await RehydratedDividableLeafAsync(rowCount, treeId: treeId);
        var cache = grain.CacheForTest;

        // An unrelated whole-cache read consumes the frame before any division
        // is sought - the read-path forfeiture the detach seam attributes.
        _ = cache.Keys.Count();

        Assert.Multiple(() =>
        {
            Assert.That(cache.LastDetachSeam, Is.EqualTo(LeafSnapshotDetachSeam.KeysAccessor),
                "the arm must construct a leaf whose frame a named seam consumed");
            Assert.That(cache.HasPendingHydration, Is.False,
                "a recorded detach seam must mean no frame is attached - the invariant the cost model rests on");
            Assert.That(cache.HydratedRowCount, Is.EqualTo(rowCount),
                "the consuming read already paid for the whole leaf");
        });

        var rowsBefore = cache.SnapshotRowsMaterialised;
        var bytesBefore = cache.SnapshotBytesRead;
        var refusals = new List<BisectRefusal>();

        using (ListenForBisectRefusals(treeId, refusals))
        {
            var result = await InvokeSplit(grain);
            Assert.That(result, Is.Not.Null, "the division must still complete through the fallback");
        }

        Assert.Multiple(() =>
        {
            Assert.That(
                refusals,
                Is.EqualTo(new[] { new BisectRefusal("no_snapshot_attached", "keys_accessor") }),
                "the division must record exactly one refusal, attributed to the seam that consumed the frame");
            Assert.That(
                cache.SnapshotRowsMaterialised - rowsBefore,
                Is.Zero,
                "a detach-seam refusal finds every row already resident: the Cache.Keys fallback "
                + "materialises nothing, so the cost belongs to the seam, not to the split");
            Assert.That(
                cache.SnapshotBytesRead - bytesBefore,
                Is.Zero,
                "nor does the fallback read a byte of the snapshot");
            Assert.That(cache.LastDetachSeam, Is.EqualTo(LeafSnapshotDetachSeam.KeysAccessor),
                "the fallback had no frame to detach, so the attribution must be unchanged");
            Assert.That(recorder.Batches, Is.Not.Empty, "rows must actually have moved to the sibling");
        });
    }

    [Test]
    public async Task A_frame_attached_leaf_with_an_emptied_block_divides_without_a_refusal()
    {
        const int rowCount = 2048;
        var treeId = UniqueTreeId();
        var (grain, _, recorder, _) = await RehydratedDividableLeafAsync(rowCount, treeId: treeId);
        var cache = grain.CacheForTest;

        // Materialise block 0, then drop exactly the rows it brought in. Before
        // issue #5607 this was the construction of the frame-attached
        // NoKeySortsBelowPivot refusal, whose Cache.Keys fallback materialised
        // the whole leaf. The pivot is now the median of the frame and resident
        // rows together, so the emptied block simply contributes nothing.
        foreach (var _ in cache.EnumerateRange(Key(0), Key(1)))
        {
        }

        var block0Keys = new List<string>();
        foreach (var row in cache.EnumerateRange(Key(0), Key(32)))
        {
            block0Keys.Add(row.Key);
        }

        foreach (var key in block0Keys)
        {
            cache.Remove(key);
        }

        var remainingFrameRows = rowCount - block0Keys.Count;
        Assert.Multiple(() =>
        {
            Assert.That(block0Keys, Is.Not.Empty, "the arm must actually materialise block 0");
            Assert.That(cache.HasPendingHydration, Is.True, "the frame must still be attached");
            Assert.That(cache.LastDetachSeam, Is.EqualTo(LeafSnapshotDetachSeam.None));
            Assert.That(cache.HydratedRowCount, Is.Zero);
            Assert.That(
                cache.TryGetBisectingKeyWithoutHydrating(out _, out var reason) ? LeafBisectRefusalReason.None : reason,
                Is.EqualTo(LeafBisectRefusalReason.None),
                "the emptied block must not cost the leaf its bisect");
        });

        var refusals = new List<BisectRefusal>();

        using (ListenForBisectRefusals(treeId, refusals))
        {
            var result = await InvokeSplit(grain);
            Assert.That(result, Is.Not.Null, "the division must complete");
        }

        Assert.Multiple(() =>
        {
            Assert.That(refusals, Is.Empty, "the division must not record a refusal");
            Assert.That(
                recorder.AllTransferredKeys.Count(),
                Is.EqualTo(remainingFrameRows / 2),
                "the sibling must receive exactly the upper half of the rows that remain");
            Assert.That(cache.LastDetachSeam, Is.Not.EqualTo(LeafSnapshotDetachSeam.KeysAccessor),
                "no ordered-view fallback may have detached the frame");
            Assert.That(recorder.Batches, Is.Not.Empty, "rows must actually have moved to the sibling");
        });
    }
}
