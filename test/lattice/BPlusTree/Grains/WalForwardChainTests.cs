using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Detectors for the forward chain a write carries before its leaf is linked (#5837, F2'):
/// every hop up to the bound is carried in order, a forward past the bound overflows the
/// chain and never truncates it (C4), and a forward's hop is scoped to its call.
/// </summary>
[TestFixture]
public sealed class WalForwardChainTests
{
    private static readonly Guid First = new("f1000000-0000-0000-0000-0000000000f1");
    private static readonly Guid Second = new("f2000000-0000-0000-0000-0000000000f2");
    private static readonly Guid Third = new("f3000000-0000-0000-0000-0000000000f3");
    private static readonly Guid Fourth = new("f4000000-0000-0000-0000-0000000000f4");
    private static readonly Guid TagA = new("a0000000-0000-0000-0000-0000000000a1");
    private static readonly Guid TagB = new("b0000000-0000-0000-0000-0000000000b1");
    private static readonly Guid TagC = new("c0000000-0000-0000-0000-0000000000c1");

    [TearDown]
    public void ClearContext() => RequestContext.Clear();

    [Test]
    public void A_stamp_carries_every_hop_of_its_chain_in_order()
    {
        // ReforwardOmitsImmediateForwarder and ReforwardOmitsIntermediateForwarder.
        var chain = WalForwardChain.Extend(WalForwardChain.Extend(WalForwardChain.Extend(null, First, TagA), Second, TagB), Third, TagC);
        var stamp = WalAppendStamp.ForLeaf(Guid.NewGuid(), 1);

        var applied = WalForwardChain.TryApply(ref stamp, chain);

        Assert.Multiple(() =>
        {
            Assert.That(applied, Is.True);
            Assert.That((stamp.Forwarder, stamp.ForwarderTag), Is.EqualTo((First, TagA)), "the first forwarder was dropped");
            Assert.That((stamp.Forwarder2, stamp.ForwarderTag2), Is.EqualTo((Second, TagB)), "an intermediate forwarder was dropped");
            Assert.That((stamp.Forwarder3, stamp.ForwarderTag3), Is.EqualTo((Third, TagC)), "the immediate forwarder was dropped");
        });
    }

    [Test]
    public void A_forward_past_the_bound_overflows_the_chain_and_is_never_stamped()
    {
        // ReforwardChainOverflowStamped (C4).
        var full = WalForwardChain.Extend(WalForwardChain.Extend(WalForwardChain.Extend(null, First, TagA), Second, TagB), Third, TagC);
        var overflowed = WalForwardChain.Extend(full, Fourth, TagA);
        var stillOverflowed = WalForwardChain.Extend(overflowed, First, TagB);
        var stamp = WalAppendStamp.ForLeaf(Guid.NewGuid(), 1);

        Assert.Multiple(() =>
        {
            Assert.That(full, Has.Length.EqualTo(WalAppendStamp.MaxForwardHops * 2));
            Assert.That(overflowed, Is.Empty, "a fourth hop truncated the chain instead of overflowing it");
            Assert.That(stillOverflowed, Is.Empty, "an overflowed chain was re-grown");
            Assert.That(WalForwardChain.TryApply(ref stamp, overflowed), Is.False, "an overflowed chain was stamped");
            Assert.That(stamp.Forwarder, Is.EqualTo(Guid.Empty));
        });
    }

    [Test]
    public void A_write_that_was_not_forwarded_carries_no_chain()
    {
        var stamp = WalAppendStamp.ForLeaf(Guid.NewGuid(), 1);

        Assert.Multiple(() =>
        {
            Assert.That(WalForwardChain.Current, Is.Null);
            Assert.That(WalForwardChain.TryApply(ref stamp, null), Is.True);
            Assert.That(stamp, Is.EqualTo(WalAppendStamp.ForLeaf(stamp.LeafId, 1)));
        });
    }

    [Test]
    public async Task A_forward_adds_its_hop_for_the_call_only()
    {
        Guid[]? inner = null;
        Guid[]? nested = null;

        await WalForwardChain.ForwardAsync(First, TagA, async () =>
        {
            inner = WalForwardChain.Current;
            await WalForwardChain.ForwardAsync(Second, TagB, () =>
            {
                nested = WalForwardChain.Current;
                return Task.CompletedTask;
            });
            Assert.That(WalForwardChain.Current, Is.EqualTo(inner), "a nested forward's hop outlived its call");
        });

        Assert.Multiple(() =>
        {
            Assert.That(inner, Is.EqualTo(new[] { First, TagA }));
            Assert.That(nested, Is.EqualTo(new[] { First, TagA, Second, TagB }), "a re-forward did not extend the chain it received");
            Assert.That(WalForwardChain.Current, Is.Null, "a forward's chain outlived its call");
        });
    }
}
