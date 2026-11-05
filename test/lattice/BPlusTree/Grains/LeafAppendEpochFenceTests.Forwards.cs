using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Detectors for the fence's side of forward attribution and the forward exclusion
/// (#5837, F2', X1, X7): a stamp carries the forward chain of the call in flight and the
/// activation's persisted narrow tag, an overflowed chain appends unstamped, and the tag
/// is withdrawn on an ETag conflict.
/// </summary>
public sealed partial class LeafAppendEpochFenceTests
{
    private static readonly Guid ForwardingLeaf = new("f0000000-0000-0000-0000-0000000000f0");
    private static readonly Guid NarrowSibling = new("5b000000-0000-0000-0000-00000000005b");

    [Test]
    public async Task A_stamp_carries_the_forward_chain_of_its_call()
    {
        var h = new Harness();
        try
        {
            RequestContext.Set(WalForwardChain.ContextKey, WalForwardChain.Extend(null, ForwardingLeaf, Leaf));

            var claiming = await h.Fence.StampForAsync(h.Shard, partition: 0, CancellationToken.None);
            var claimed = await h.Fence.StampForAsync(h.Shard, partition: 0, CancellationToken.None);

            Assert.Multiple(() =>
            {
                Assert.That((claiming!.Value.Forwarder, claiming.Value.ForwarderTag), Is.EqualTo((ForwardingLeaf, Leaf)),
                    "the claiming append dropped its forward chain");
                Assert.That((claimed!.Value.Forwarder, claimed.Value.ForwarderTag), Is.EqualTo((ForwardingLeaf, Leaf)),
                    "a claimed append dropped its forward chain");
            });
        }
        finally
        {
            RequestContext.Clear();
        }
    }

    [Test]
    public async Task A_write_past_the_forward_bound_appends_unstamped()
    {
        // ReforwardChainOverflowStamped (C4).
        var h = new Harness();
        await h.Fence.StampForAsync(h.Shard, partition: 0, CancellationToken.None);
        try
        {
            RequestContext.Set(WalForwardChain.ContextKey, Array.Empty<Guid>());

            var stamp = await h.Fence.StampForAsync(h.Shard, partition: 0, CancellationToken.None);

            Assert.That(stamp, Is.Null, "a write whose forward chain overflowed was stamped");
        }
        finally
        {
            RequestContext.Clear();
        }
    }

    [Test]
    public async Task A_stamp_carries_the_persisted_narrow_until_an_etag_conflict_withdraws_it()
    {
        // ForwardExclusionByInMemorySpan (X1, X3, X7).
        var h = new Harness();
        var before = await h.Fence.StampForAsync(h.Shard, partition: 0, CancellationToken.None);

        h.Fence.NotePersistedNarrow(NarrowSibling);
        var persisted = await h.Fence.StampForAsync(h.Shard, partition: 0, CancellationToken.None);
        h.Fence.WithdrawNarrow();
        var withdrawn = await h.Fence.StampForAsync(h.Shard, partition: 0, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(before!.Value.ReaderTag, Is.EqualTo(Guid.Empty), "a stamp carried a narrow before it persisted");
            Assert.That(persisted!.Value.ReaderTag, Is.EqualTo(NarrowSibling), "a persisted narrow was not carried");
            Assert.That(withdrawn!.Value.ReaderTag, Is.EqualTo(Guid.Empty), "a withdrawn narrow was still carried");
        });
    }
}
