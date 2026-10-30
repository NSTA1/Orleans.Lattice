using NSubstitute;
using Orleans.Lattice.Replication.Grains;
using Orleans.Lattice.Replication.Tests.Fakes;

namespace Orleans.Lattice.Replication.Tests.Grains;

[TestFixture]
public sealed class ReplicationDecommissionedPeerRegistryGrainTests
{
    [Test]
    public async Task MarkDecommissionedAndGetPreviousAsync_persists_once_and_preserves_first_timestamp()
    {
        var state = new FakePersistentState<ReplicationDecommissionedPeerRegistryState>();
        var grain = new ReplicationDecommissionedPeerRegistryGrain(state);
        var first = DateTimeOffset.UnixEpoch;

        Assert.That(await grain.MarkDecommissionedAndGetPreviousAsync("peer", first), Is.False);
        Assert.That(await grain.MarkDecommissionedAndGetPreviousAsync("peer", first.AddDays(1)), Is.True);
        await grain.MarkDecommissionedAsync("peer", first.AddDays(2));

        Assert.That(state.WriteCount, Is.EqualTo(1));
        Assert.That(state.State.DecommissionedAtUtc["peer"], Is.EqualTo(first));
        await grain.ClearDecommissionedAsync("peer");
        Assert.That(await grain.MarkDecommissionedAndGetPreviousAsync("peer", first.AddDays(3)), Is.False);
    }

    [Test]
    public async Task MarkDecommissionedAndGetPreviousAsync_failed_write_rolls_back_and_retry_is_new()
    {
        var failure = new IOException("marker storage failed");
        var state = new FakePersistentState<ReplicationDecommissionedPeerRegistryState> { ThrowOnWrite = failure };
        var grain = new ReplicationDecommissionedPeerRegistryGrain(state);

        Assert.That(async () => await grain.MarkDecommissionedAndGetPreviousAsync("peer", DateTimeOffset.UnixEpoch),
            Throws.Exception.SameAs(failure));
        Assert.That(await grain.IsDecommissionedAsync("peer"), Is.False);
        Assert.That(await grain.MarkDecommissionedAndGetPreviousAsync("peer", DateTimeOffset.UnixEpoch), Is.False);
        Assert.That(state.WriteCount, Is.EqualTo(1));
    }

    [TestCase(null)]
    [TestCase("")]
    public void MarkDecommissionedAndGetPreviousAsync_rejects_empty_peer(string? peer)
    {
        var state = new FakePersistentState<ReplicationDecommissionedPeerRegistryState>();
        var grain = new ReplicationDecommissionedPeerRegistryGrain(state);
        Assert.That(async () => await grain.MarkDecommissionedAndGetPreviousAsync(peer!, DateTimeOffset.UnixEpoch),
            Throws.InstanceOf<ArgumentException>());
        Assert.That(state.WriteCount, Is.Zero);
    }

    [Test]
    public async Task MarkDecommissionedAndGetPreviousAsync_does_not_return_before_persistence()
    {
        var state = Substitute.For<IPersistentState<ReplicationDecommissionedPeerRegistryState>>();
        state.State.Returns(new ReplicationDecommissionedPeerRegistryState());
        var persisted = new TaskCompletionSource();
        state.WriteStateAsync().Returns(persisted.Task);
        var grain = new ReplicationDecommissionedPeerRegistryGrain(state);

        var mark = grain.MarkDecommissionedAndGetPreviousAsync("peer", DateTimeOffset.UnixEpoch);
        Assert.That(mark.IsCompleted, Is.False);
        persisted.SetResult();
        Assert.That(await mark, Is.False);
    }
}
