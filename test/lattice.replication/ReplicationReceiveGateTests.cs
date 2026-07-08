using NSubstitute;
using Orleans.Lattice.Replication.Grains;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Unit coverage for <see cref="ReplicationReceiveGate"/> (issue #1173), the
/// cached read-side view of the per-tree inbound receive fence consulted by the
/// apply hot path. Verifies the paused/unpaused answer and that the bounded
/// cache collapses repeated lookups into a single grain call.
/// </summary>
[TestFixture]
public class ReplicationReceiveGateTests
{
    private const string Tree = "orders";

    private static (ReplicationReceiveGate Gate, ITreeReceiveFenceGrain Fence) CreateGate()
    {
        var fence = Substitute.For<ITreeReceiveFenceGrain>();
        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<ITreeReceiveFenceGrain>(Arg.Any<string>()).Returns(fence);
        return (new ReplicationReceiveGate(factory), fence);
    }

    [Test]
    public async Task Reports_not_paused_when_the_fence_is_clear()
    {
        var (gate, fence) = CreateGate();
        fence.IsPausedAsync().Returns(Task.FromResult(false));

        Assert.That(await gate.IsReceivePausedAsync(Tree), Is.False);
    }

    [Test]
    public async Task Reports_paused_when_the_fence_is_engaged()
    {
        var (gate, fence) = CreateGate();
        fence.IsPausedAsync().Returns(Task.FromResult(true));

        Assert.That(await gate.IsReceivePausedAsync(Tree), Is.True);
    }

    [Test]
    public async Task Repeated_lookups_within_the_window_hit_the_cache_once()
    {
        var (gate, fence) = CreateGate();
        fence.IsPausedAsync().Returns(Task.FromResult(true));

        var first = await gate.IsReceivePausedAsync(Tree);

        // Change the underlying answer; a second immediate lookup must still
        // return the cached value and must not re-dial the grain.
        fence.IsPausedAsync().Returns(Task.FromResult(false));
        var second = await gate.IsReceivePausedAsync(Tree);

        Assert.That(first, Is.True);
        Assert.That(second, Is.True);
        await fence.Received(1).IsPausedAsync();
    }

    [Test]
    public void Rejects_null_or_empty_tree()
    {
        var (gate, _) = CreateGate();

        Assert.That(async () => await gate.IsReceivePausedAsync(null!),
            Throws.InstanceOf<ArgumentException>());
        Assert.That(async () => await gate.IsReceivePausedAsync(string.Empty),
            Throws.InstanceOf<ArgumentException>());
    }
}
