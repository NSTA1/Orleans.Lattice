using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Issue #5851 (O3c-1): an internal node does not republish upward a content its parent
/// already folded. Suppression needs the same parent, a fault-free last publish, equality
/// on every content field, and an unexpired <see cref="LatticeOptions.MaterialiserCheckpointInterval"/>;
/// a re-parent or re-link forgets it, and a pull always reads current state.
/// </summary>
public partial class BPlusInternalGrainTests
{
    private static readonly GrainId UnchangedPublishOtherParent = GrainId.Create("internal", "unchanged-publish-other-parent");

    /// <summary>Child0's row: the lowest key range, below Child1's.</summary>
    private static readonly ChildDigestSnapshot UnchangedChild0 = new()
    {
        Hash = Bytes16(0x5A), EntryCount = 3, CheckpointOffset = 7,
        LowKeyInclusive = "b", HighKeyExclusive = "m",
        LiveCount = 2, TombstoneCount = 1, SubtreeDepth = 1,
    };

    /// <summary>Child1's row: the highest key range, so its high bound is the node's.</summary>
    private static readonly ChildDigestSnapshot UnchangedChild1 = new()
    {
        Hash = Bytes16(0x11), EntryCount = 1, CheckpointOffset = 2,
        LowKeyInclusive = "m", HighKeyExclusive = "z",
        LiveCount = 1, TombstoneCount = 0, SubtreeDepth = 1,
    };

    /// <summary>
    /// A parented two-child node with both rows published once, and the parent's received
    /// calls cleared, so the next publish of either row unchanged is suppressible.
    /// </summary>
    private static async Task<(BPlusInternalGrain Grain, FakePersistentState<InternalNodeState> State, IGrainFactory Factory, IBPlusInternalGrain Parent)>
        CreateUnchangedPublishNodeAsync(TimeSpan? interval = null)
    {
        var (grain, state, factory) = CreateDigestGrain(new LatticeOptions
        {
            MaterialiserCheckpointInterval = interval ?? TimeSpan.FromHours(1),
        });
        await grain.InitializeAsync("m", Child0, Child1, childrenAreLeaves: true);
        state.State.ParentId = DigestParent;

        var parent = Substitute.For<IBPlusInternalGrain>();
        parent.OnChildDigestPublishedAsync(Arg.Any<GrainId>(), Arg.Any<ChildDigestSnapshot>())
            .Returns(Task.CompletedTask);
        factory.GetGrain<IBPlusInternalGrain>(DigestParent).Returns(parent);

        await grain.OnChildDigestPublishedAsync(Child1, UnchangedChild1 with { PublishSequence = 1 });
        await grain.OnChildDigestPublishedAsync(Child0, UnchangedChild0 with { PublishSequence = 1 });
        parent.ClearReceivedCalls();
        return (grain, state, factory, parent);
    }

    private static Task UpwardPublishes(IBPlusInternalGrain parent, int count) =>
        parent.Received(count).OnChildDigestPublishedAsync(Arg.Any<GrainId>(), Arg.Any<ChildDigestSnapshot>());

    [Test]
    public async Task An_unchanged_upward_republish_sends_nothing()
    {
        var (grain, _, _, parent) = await CreateUnchangedPublishNodeAsync();

        await grain.OnChildDigestPublishedAsync(Child0, UnchangedChild0 with { PublishSequence = 2 });

        await UpwardPublishes(parent, 0);
    }

    [Test]
    public async Task A_pull_after_a_suppressed_republish_reads_the_current_content()
    {
        // A catch-up bank: a child that does not hold the node's maximum checkpoint
        // advances below it, so the node's own content is unchanged and is not republished.
        var (grain, state, _, parent) = await CreateUnchangedPublishNodeAsync();
        await grain.OnChildDigestPublishedAsync(Child1, UnchangedChild1 with { CheckpointOffset = 5, PublishSequence = 2 });
        await UpwardPublishes(parent, 0);
        Assert.That(state.State.ChildDigests[Child1].CheckpointOffset, Is.EqualTo(5L),
            "the child's row is still folded; only the upward republish is suppressed");

        var pulled = await grain.GetChildDigestSnapshotAsync();

        Assert.Multiple(() =>
        {
            Assert.That(pulled.CheckpointOffset, Is.EqualTo(7L), "the node's max checkpoint is unchanged");
            Assert.That(pulled.EntryCount, Is.EqualTo(4L));
            Assert.That(pulled.PublishSequence, Is.GreaterThan(0L), "a pull is stamped, as before");
        });
    }

    [TestCase("hash")]
    [TestCase("entry-count")]
    [TestCase("checkpoint")]
    [TestCase("low-bound")]
    [TestCase("high-bound")]
    [TestCase("live-count")]
    [TestCase("tombstone-count")]
    [TestCase("depth")]
    public async Task A_change_in_any_content_field_publishes(string field)
    {
        var (grain, _, _, parent) = await CreateUnchangedPublishNodeAsync();

        var (child, snapshot) = field switch
        {
            "hash" => (Child0, UnchangedChild0 with { Hash = Bytes16(0x5B) }),
            "entry-count" => (Child0, UnchangedChild0 with { EntryCount = 4 }),
            "checkpoint" => (Child0, UnchangedChild0 with { CheckpointOffset = 8 }),
            "low-bound" => (Child0, UnchangedChild0 with { LowKeyInclusive = "a" }),
            "high-bound" => (Child1, UnchangedChild1 with { HighKeyExclusive = "zz" }),
            "live-count" => (Child0, UnchangedChild0 with { LiveCount = 3 }),
            "tombstone-count" => (Child0, UnchangedChild0 with { TombstoneCount = 2 }),
            "depth" => (Child0, UnchangedChild0 with { SubtreeDepth = 2 }),
            _ => throw new ArgumentOutOfRangeException(nameof(field)),
        };
        await grain.OnChildDigestPublishedAsync(child, snapshot with { PublishSequence = 2 });

        await UpwardPublishes(parent, 1);
    }

    [Test]
    public async Task A_change_in_fanout_publishes()
    {
        var (grain, _, _, parent) = await CreateUnchangedPublishNodeAsync();

        await grain.AcceptSplitAsync("t", Child2);

        await parent.Received().OnChildDigestPublishedAsync(
            Arg.Any<GrainId>(), Arg.Is<ChildDigestSnapshot>(s => s.ChildFanout == 3));
    }

    [Test]
    public async Task An_unchanged_republish_after_a_faulted_publish_is_sent()
    {
        var (grain, _, _, parent) = await CreateUnchangedPublishNodeAsync();
        parent.OnChildDigestPublishedAsync(Arg.Any<GrainId>(), Arg.Any<ChildDigestSnapshot>())
            .ThrowsAsync(new TimeoutException("parent parked"));
        try
        {
            await grain.OnChildDigestPublishedAsync(Child0, UnchangedChild0 with { EntryCount = 9, PublishSequence = 2 });
        }
        catch (TimeoutException)
        {
        }

        parent.ClearReceivedCalls();
        parent.OnChildDigestPublishedAsync(Arg.Any<GrainId>(), Arg.Any<ChildDigestSnapshot>())
            .Returns(Task.CompletedTask);

        await grain.OnChildDigestPublishedAsync(Child0, UnchangedChild0 with { EntryCount = 9, PublishSequence = 3 });

        await UpwardPublishes(parent, 1);
    }

    [Test]
    public async Task A_re_parented_node_publishes_unchanged_content_to_its_new_parent()
    {
        var (grain, _, factory, parent) = await CreateUnchangedPublishNodeAsync();
        var other = Substitute.For<IBPlusInternalGrain>();
        other.OnChildDigestPublishedAsync(Arg.Any<GrainId>(), Arg.Any<ChildDigestSnapshot>())
            .Returns(Task.CompletedTask);
        factory.GetGrain<IBPlusInternalGrain>(UnchangedPublishOtherParent).Returns(other);

        await grain.SetParentAsync(UnchangedPublishOtherParent);
        await grain.OnChildDigestPublishedAsync(Child0, UnchangedChild0 with { PublishSequence = 2 });

        await UpwardPublishes(other, 1);
        await UpwardPublishes(parent, 0);
    }

    [Test]
    public async Task A_re_link_to_the_same_parent_publishes_unchanged_content_again()
    {
        var (grain, _, _, parent) = await CreateUnchangedPublishNodeAsync();

        await grain.SetParentAsync(DigestParent);
        await grain.OnChildDigestPublishedAsync(Child0, UnchangedChild0 with { PublishSequence = 2 });

        await UpwardPublishes(parent, 1);
    }

    [Test]
    public async Task An_unchanged_republish_is_sent_once_the_interval_has_elapsed()
    {
        var (grain, _, _, parent) = await CreateUnchangedPublishNodeAsync(TimeSpan.FromMilliseconds(1));
        await Task.Delay(50);

        await grain.OnChildDigestPublishedAsync(Child0, UnchangedChild0 with { PublishSequence = 2 });

        await UpwardPublishes(parent, 1);
    }

    [Test]
    public async Task A_zero_interval_never_suppresses_an_unchanged_republish()
    {
        var (grain, _, _, parent) = await CreateUnchangedPublishNodeAsync(TimeSpan.Zero);

        await grain.OnChildDigestPublishedAsync(Child0, UnchangedChild0 with { PublishSequence = 2 });

        await UpwardPublishes(parent, 1);
    }
}
