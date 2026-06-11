using System.Text;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

[TestFixture]
public class LatticeQueueGrainTests
{
    private const string QueueName = "work-items";

    private static byte[] Payload(string s) => Encoding.UTF8.GetBytes(s);

    private static async Task<(LatticeQueueGrain grain, SortedDictionary<string, byte[]> data, LatticeOptions options)> CreateGrainAsync(
        int? capacity = null,
        (Orleans.Lattice.BPlusTree.Grains.ISystemLattice store, SortedDictionary<string, byte[]> data)? backing = null)
    {
        var (store, data) = backing ?? FakeSystemLattice.Create();
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("queue", QueueName));
        var grainFactory = Substitute.For<IGrainFactory>();
        var options = new LatticeOptions { QueueCapacity = capacity };
        var monitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        monitor.Get(Arg.Any<string>()).Returns(options);
        var grain = new LatticeQueueGrain(context, grainFactory, monitor);
        await grain.InitializeForTestingAsync(QueueName, store, CancellationToken.None);
        return (grain, data, options);
    }

    [Test]
    public async Task EnqueueAsync_assigns_increasing_ids_starting_at_one()
    {
        var (grain, _, _) = await CreateGrainAsync();

        var id1 = await grain.EnqueueAsync(Payload("a"));
        var id2 = await grain.EnqueueAsync(Payload("b"));

        Assert.That(new[] { id1, id2 }, Is.EqualTo(new[] { 1L, 2L }));
    }

    [Test]
    public async Task EnqueueAsync_throws_on_null_value()
    {
        var (grain, _, _) = await CreateGrainAsync();
        Assert.That(async () => await grain.EnqueueAsync(null!), Throws.ArgumentNullException);
    }

    [Test]
    public async Task TryDequeueAsync_returns_head_in_fifo_order()
    {
        var (grain, _, _) = await CreateGrainAsync();
        await grain.EnqueueAsync(Payload("a"));
        await grain.EnqueueAsync(Payload("b"));

        var head = await grain.TryDequeueAsync();

        Assert.That(head, Is.Not.Null);
        Assert.That(head!.Value.EntryId, Is.EqualTo(1L));
        Assert.That(Encoding.UTF8.GetString(head.Value.Value), Is.EqualTo("a"));
    }

    [Test]
    public async Task TryDequeueAsync_returns_null_when_empty()
    {
        var (grain, _, _) = await CreateGrainAsync();
        Assert.That(await grain.TryDequeueAsync(), Is.Null);
    }

    [Test]
    public async Task PeekAsync_returns_head_without_removing()
    {
        var (grain, _, _) = await CreateGrainAsync();
        await grain.EnqueueAsync(Payload("a"));

        var head = await grain.PeekAsync();

        Assert.That(head, Is.Not.Null);
        Assert.That(head!.Value.EntryId, Is.EqualTo(1L));
        Assert.That(await grain.CountAsync(), Is.EqualTo(1));
    }

    [Test]
    public async Task PeekAsync_returns_null_when_empty()
    {
        var (grain, _, _) = await CreateGrainAsync();
        Assert.That(await grain.PeekAsync(), Is.Null);
    }

    [Test]
    public async Task CountAsync_reflects_current_size()
    {
        var (grain, _, _) = await CreateGrainAsync();
        Assert.That(await grain.CountAsync(), Is.EqualTo(0));
        await grain.EnqueueAsync(Payload("a"));
        Assert.That(await grain.CountAsync(), Is.EqualTo(1));
    }

    [Test]
    public async Task ListAsync_returns_entries_in_ascending_id_order()
    {
        var (grain, _, _) = await CreateGrainAsync();
        await grain.EnqueueAsync(Payload("a"));
        await grain.EnqueueAsync(Payload("b"));

        var entries = await grain.ListAsync();

        Assert.That(entries.Select(e => e.EntryId), Is.EqualTo(new[] { 1L, 2L }));
    }

    [Test]
    public async Task EnqueueAsync_evicts_oldest_when_capacity_reached()
    {
        var (grain, _, _) = await CreateGrainAsync(capacity: 2);

        var id1 = await grain.EnqueueAsync(Payload("a"));
        var id2 = await grain.EnqueueAsync(Payload("b"));
        var id3 = await grain.EnqueueAsync(Payload("c"));

        var entries = await grain.ListAsync();
        Assert.Multiple(() =>
        {
            Assert.That(entries, Has.Count.EqualTo(2));
            Assert.That(entries.Select(e => e.EntryId), Is.EqualTo(new[] { id2, id3 }));
            Assert.That(id1, Is.EqualTo(1L));
        });
    }

    [Test]
    public void EnqueueAsync_throws_before_activation()
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("queue", QueueName));
        var grain = new LatticeQueueGrain(context, Substitute.For<IGrainFactory>(), Substitute.For<IOptionsMonitor<LatticeOptions>>());

        Assert.That(async () => await grain.EnqueueAsync(Payload("a")), Throws.InvalidOperationException);
    }

    [Test]
    public async Task OnDeactivateAsync_flushes_pending_head_cursor()
    {
        var (grain, data, _) = await CreateGrainAsync();
        await grain.EnqueueAsync(Payload("a"));
        await grain.TryDequeueAsync(); // advances head once, below the flush interval
        Assert.That(data.ContainsKey(LatticeQueueCore.HeadCursorKey), Is.False);

        await grain.OnDeactivateAsync(default, CancellationToken.None);

        Assert.That(data.ContainsKey(LatticeQueueCore.HeadCursorKey), Is.True);
    }

    [Test]
    public void BackingTreeId_lives_under_the_queue_system_prefix()
    {
        Assert.That(LatticeQueueGrain.BackingTreeId("orders"), Is.EqualTo("_lattice_queue_orders"));
    }
}
