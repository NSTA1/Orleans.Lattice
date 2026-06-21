using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;
using System.Text;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Regression coverage for the durable digest publish-sequence high-water mark.
/// The sequence stamped onto each <c>ChildDigestSnapshot</c> must stay strictly
/// monotonic across activations and silo relocation, even when a re-activated
/// leaf's wall clock is behind the sequence it last emitted - otherwise the
/// parent's staleness guard would permanently drop the re-activated leaf's
/// publishes. The fix persists the high-water mark in
/// <see cref="LeafNodeState.DigestPublishSequence"/> and seeds from the larger of
/// that value and the wall clock.
/// </summary>
public partial class BPlusLeafGrainTests
{
    [Test]
    public async Task DigestPublishSequence_persists_high_water_mark_across_publishes()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);

        await grain.SetAsync("k1", Encoding.UTF8.GetBytes("v1"));
        var first = await grain.GetChildDigestSnapshotAsync();
        await grain.SetAsync("k2", Encoding.UTF8.GetBytes("v2"));
        var second = await grain.GetChildDigestSnapshotAsync();

        Assert.That(second.PublishSequence, Is.GreaterThan(first.PublishSequence),
            "each publish must stamp a strictly increasing sequence");
        Assert.That(state.State.DigestPublishSequence, Is.EqualTo(second.PublishSequence),
            "the latest emitted sequence must be persisted as the durable high-water mark");
    }

    [Test]
    public async Task DigestPublishSequence_resumes_above_persisted_mark_when_wall_clock_is_behind()
    {
        // Simulate a re-activation that inherits a persisted high-water mark far
        // above the current wall clock (the cross-silo clock-skew scenario). The
        // fresh activation must resume strictly above the persisted mark rather
        // than seeding a lower value from its own wall clock.
        var skewedHighWater = long.MaxValue - 100;
        var state = new FakePersistentState<LeafNodeState>
        {
            State = { DigestPublishSequence = skewedHighWater },
        };
        var grain = CreateGrain(state);

        await grain.SetAsync("k1", Encoding.UTF8.GetBytes("v1"));
        var snapshot = await grain.GetChildDigestSnapshotAsync();

        Assert.That(snapshot.PublishSequence, Is.GreaterThan(skewedHighWater),
            "a re-activated leaf must resume above the sequence it last emitted, not below it");
        Assert.That(state.State.DigestPublishSequence, Is.GreaterThan(skewedHighWater),
            "the advanced sequence must be persisted back as the new high-water mark");
    }
}
