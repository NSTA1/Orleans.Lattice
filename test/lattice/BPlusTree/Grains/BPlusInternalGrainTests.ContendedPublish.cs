using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// A child digest publish that finds <c>_splitGate</c> held (issue #3523). The
/// holder can be waiting on that very child: AcceptSplit and the Initialize
/// seeding call SetParentAsync on a child, which queues behind the child's
/// current turn, and that turn can be the publish. Waiting for the gate closed
/// the cycle until the publish deadline faulted, and the fault took the
/// child's completed split with it. A contended publish is instead parked and
/// folded by the holder before it releases the gate.
/// </summary>
public partial class BPlusInternalGrainTests
{
    /// <summary>
    /// Holds the node's split gate: an AcceptSplit whose recovery forward is
    /// parked on the returned completion source.
    /// </summary>
    private static async Task<(BPlusInternalGrain Grain, FakePersistentState<InternalNodeState> State, TaskCompletionSource<SplitResult?> Forward, Task<SplitResult?> Accept)>
        HoldSplitGateAsync()
    {
        var (grain, state, factory) = CreateDigestGrain();
        await grain.InitializeAsync("fox", Child0, Child1, childrenAreLeaves: true);
        var siblingId = InterruptSplit(state);

        var forward = new TaskCompletionSource<SplitResult?>(TaskCreationOptions.RunContinuationsAsynchronously);
        var sibling = Substitute.For<IBPlusInternalGrain>();
        sibling.AcceptSplitAsync(Arg.Any<string>(), Arg.Any<GrainId>()).Returns(forward.Task);
        factory.GetGrain<IBPlusInternalGrain>(siblingId).Returns(sibling);

        var accept = grain.AcceptSplitAsync("zebra", Child3);
        await sibling.Received(1).AcceptSplitAsync("zebra", Child3);
        return (grain, state, forward, accept);
    }

    private static bool HasFolded(FakePersistentState<InternalNodeState> state, ChildDigestSnapshot snapshot) =>
        state.State.ChildDigests.TryGetValue(Child0, out var folded) && folded == snapshot;

    [Test]
    public async Task OnChildDigestPublished_parks_a_publish_that_finds_the_split_gate_held()
    {
        var (grain, state, forward, accept) = await HoldSplitGateAsync();
        var snapshot = DeferredSnapshot with { PublishSequence = 4 };

        var publish = grain.OnChildDigestPublishedAsync(Child0, snapshot);

        Assert.Multiple(() =>
        {
            Assert.That(publish.IsCompleted, Is.True,
                "a publish must not wait on a gate whose holder may be waiting on the publisher");
            Assert.That(HasFolded(state, snapshot), Is.False, "the fold belongs to the holder, under the gate");
        });

        forward.TrySetResult(null);
        var result = await accept.WaitAsync(TimeSpan.FromSeconds(10));

        Assert.Multiple(() =>
        {
            Assert.That(result, Is.Not.Null, "the holder's own division is unaffected");
            Assert.That(HasFolded(state, snapshot), Is.True, "the holder folds the parked publish before releasing");
        });
    }

    [Test]
    public async Task A_parked_publish_older_than_one_already_parked_for_the_child_is_dropped()
    {
        var (grain, state, forward, accept) = await HoldSplitGateAsync();
        var fresh = DeferredSnapshot with { PublishSequence = 5, EntryCount = 50 };

        await grain.OnChildDigestPublishedAsync(Child0, fresh);
        await grain.OnChildDigestPublishedAsync(Child0, DeferredSnapshot with { PublishSequence = 3, EntryCount = 30 });

        forward.TrySetResult(null);
        await accept.WaitAsync(TimeSpan.FromSeconds(10));

        Assert.That(HasFolded(state, fresh), Is.True);
    }

    [Test]
    public async Task A_parked_publish_newer_than_one_already_parked_for_the_child_replaces_it()
    {
        var (grain, state, forward, accept) = await HoldSplitGateAsync();
        var fresh = DeferredSnapshot with { PublishSequence = 5, EntryCount = 50 };

        await grain.OnChildDigestPublishedAsync(Child0, DeferredSnapshot with { PublishSequence = 3, EntryCount = 30 });
        await grain.OnChildDigestPublishedAsync(Child0, fresh);

        forward.TrySetResult(null);
        await accept.WaitAsync(TimeSpan.FromSeconds(10));

        Assert.That(HasFolded(state, fresh), Is.True);
    }

    [Test]
    public async Task A_parked_publish_whose_fold_faults_stays_parked_and_the_next_holder_folds_it()
    {
        var (grain, state, forward, accept) = await HoldSplitGateAsync();
        var snapshot = DeferredSnapshot with { PublishSequence = 9 };
        var faulted = false;
        var persisted = 0;
        state.OnWriteState = s =>
        {
            if (!s.ChildDigests.TryGetValue(Child0, out var d) || d.PublishSequence != 9)
            {
                return;
            }

            if (!faulted)
            {
                faulted = true;
                throw new IOException("storage unavailable");
            }

            persisted++;
        };

        await grain.OnChildDigestPublishedAsync(Child0, snapshot);
        forward.TrySetResult(null);
        var result = await accept.WaitAsync(TimeSpan.FromSeconds(10));

        Assert.Multiple(() =>
        {
            Assert.That(faulted, Is.True, "the fold under the holder's release was attempted");
            Assert.That(result, Is.Not.Null, "a fold fault must not take the holder's durable division with it");
        });

        // An unchanged parent makes SetParentAsync a gate holder that writes
        // nothing of its own, so any write now is the parked publish's fold.
        var before = persisted;
        await grain.SetParentAsync(state.State.ParentId);

        Assert.That(persisted, Is.GreaterThan(before), "the next holder re-drives the parked publish");
    }
}
