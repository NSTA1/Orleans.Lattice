using System.Text;
using Orleans.Lattice;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Unit tests for the <see cref="LatticeOptions.DigestCoalescingWindowMs"/>
/// opt-in. The window defers the
/// per-call <c>OnChildDigestPublishedAsync</c> hop behind a one-shot grain
/// timer so mutations arriving within the window share a single publish.
/// </summary>
public partial class BPlusLeafGrainTests
{
    [Test]
    public void DigestCoalescingWindowMs_default_is_five()
    {
        // The default carries the c2-xxviii measured sweet spot: a 5 ms
        // coalescing window. Operators opt OUT (set to 0) per-tree if a
        // consumer depends on the historical synchronous-publish shape.
        Assert.That(new LatticeOptions().DigestCoalescingWindowMs, Is.EqualTo(5));
        Assert.That(LatticeOptions.DefaultDigestCoalescingWindowMs, Is.EqualTo(5));
    }

    [Test]
    public async Task DigestCoalescingWindowMs_zero_preserves_synchronous_publish_shape()
    {
        // With the window at 0 the leaf still folds every mutation into the
        // running ProjectionHash and stamps it on persisted state on the
        // synchronous publish path - the pre-c2-xxviii regression target.
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state, options: new LatticeOptions
        {
            MaintainProjectionDigest = true,
            DigestCoalescingWindowMs = 0,
        });

        await grain.SetAsync("k1", Encoding.UTF8.GetBytes("v1"));
        await grain.SetAsync("k2", Encoding.UTF8.GetBytes("v2"));

        Assert.That(state.State.ProjectionHash, Is.Not.Null);
        Assert.That(state.State.ProjectionHash!.Length, Is.EqualTo(16));
        Assert.That(state.State.ProjectionHash.Any(b => b != 0), Is.True,
            "Synchronous-publish path must still drive the persisted XOR fold forward.");

        var digest = await grain.GetProjectionDigestAsync();
        Assert.That(digest.EntryCount, Is.EqualTo(2));
    }

    [Test]
    public async Task DigestCoalescingWindowMs_positive_does_not_change_persisted_hash()
    {
        // Coalescing only defers the cross-grain publish to the parent
        // internal node; the running ProjectionHash on persisted state must
        // still advance per-mutation so cold-reactivation replay produces a
        // byte-identical leaf. The grain factory in this unit-test harness
        // does not provide a running Orleans timer host - the timer
        // registration falls back to synchronous publish (see
        // PublishDigestUpwardAsync's catch block), so the test verifies the
        // hash-update invariant rather than the deferred-timer firing.
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state, options: new LatticeOptions
        {
            MaintainProjectionDigest = true,
            DigestCoalescingWindowMs = 5,
        });

        await grain.SetAsync("a", Encoding.UTF8.GetBytes("1"));
        await grain.SetAsync("b", Encoding.UTF8.GetBytes("2"));
        await grain.SetAsync("c", Encoding.UTF8.GetBytes("3"));

        Assert.That(state.State.ProjectionHash, Is.Not.Null);
        Assert.That(state.State.ProjectionHash!.Length, Is.EqualTo(16));
        Assert.That(state.State.ProjectionHash.Any(b => b != 0), Is.True,
            "Coalescing-enabled writes must still drive the persisted XOR fold forward.");

        var digest = await grain.GetProjectionDigestAsync();
        Assert.That(digest.EntryCount, Is.EqualTo(3));
    }

    [Test]
    public void DigestCoalescingWindowMs_round_trips_through_options()
    {
        // Pinning a positive value flows through to the configured option;
        // a guard against a future refactor that drops the property or
        // collapses it to the maintenance toggle.
        var opts = new LatticeOptions { DigestCoalescingWindowMs = 25 };
        Assert.That(opts.DigestCoalescingWindowMs, Is.EqualTo(25));
    }
}
