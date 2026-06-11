using System.Text.Json;
using NSubstitute;
using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Tests.BPlusTree;

[TestFixture]
public class LatticeQueueExtensionsTests
{
    private const string QueueName = "q";

    private static (IGrainFactory factory, ILatticeQueueGrain grain) Wire()
    {
        var grain = Substitute.For<ILatticeQueueGrain>();
        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<ILatticeQueueGrain>(QueueName, Arg.Any<string?>()).Returns(grain);
        return (factory, grain);
    }

    [Test]
    public void GetLatticeQueue_throws_on_null_factory()
    {
        IGrainFactory factory = null!;
        Assert.That(() => factory.GetLatticeQueue<string>(QueueName), Throws.ArgumentNullException);
    }

    [Test]
    public void GetLatticeQueue_throws_on_empty_name()
    {
        var (factory, _) = Wire();
        Assert.That(() => factory.GetLatticeQueue<string>(string.Empty), Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public void GetLatticeQueue_returns_a_facade_bound_to_the_named_grain()
    {
        var (factory, _) = Wire();

        var queue = factory.GetLatticeQueue<string>(QueueName);

        Assert.That(queue, Is.Not.Null);
        factory.Received(1).GetGrain<ILatticeQueueGrain>(QueueName, Arg.Any<string?>());
    }

    [Test]
    public async Task EnqueueAsync_serializes_the_value_through_the_serializer()
    {
        var (factory, grain) = Wire();
        byte[]? captured = null;
        grain.EnqueueAsync(Arg.Do<byte[]>(b => captured = b), Arg.Any<CancellationToken>()).Returns(1L);

        var queue = factory.GetLatticeQueue<string>(QueueName);
        await queue.EnqueueAsync("hello");

        Assert.That(captured, Is.Not.Null);
        Assert.That(JsonSerializer.Deserialize<string>(captured!), Is.EqualTo("hello"));
    }

    [Test]
    public async Task TryDequeueAsync_deserializes_the_returned_payload()
    {
        var (factory, grain) = Wire();
        var bytes = JsonSerializer.SerializeToUtf8Bytes("world");
        grain.TryDequeueAsync(Arg.Any<CancellationToken>())
            .Returns(new LatticeQueueByteEntry { EntryId = 5, Value = bytes });

        var queue = factory.GetLatticeQueue<string>(QueueName);
        var head = await queue.TryDequeueAsync();

        Assert.That(head, Is.Not.Null);
        Assert.That(head!.Value.EntryId, Is.EqualTo(5L));
        Assert.That(head.Value.Value, Is.EqualTo("world"));
    }

    [Test]
    public async Task TryDequeueAsync_returns_null_when_grain_returns_null()
    {
        var (factory, grain) = Wire();
        grain.TryDequeueAsync(Arg.Any<CancellationToken>()).Returns((LatticeQueueByteEntry?)null);

        var queue = factory.GetLatticeQueue<string>(QueueName);
        Assert.That(await queue.TryDequeueAsync(), Is.Null);
    }

    [Test]
    public async Task PeekAsync_deserializes_the_returned_payload()
    {
        var (factory, grain) = Wire();
        grain.PeekAsync(Arg.Any<CancellationToken>())
            .Returns(new LatticeQueueByteEntry { EntryId = 2, Value = JsonSerializer.SerializeToUtf8Bytes("peeked") });

        var queue = factory.GetLatticeQueue<string>(QueueName);
        var head = await queue.PeekAsync();

        Assert.That(head!.Value.Value, Is.EqualTo("peeked"));
    }

    [Test]
    public async Task ListAsync_deserializes_every_entry()
    {
        var (factory, grain) = Wire();
        IReadOnlyList<LatticeQueueByteEntry> grainEntries = new List<LatticeQueueByteEntry>
        {
            new() { EntryId = 1, Value = JsonSerializer.SerializeToUtf8Bytes("a") },
            new() { EntryId = 2, Value = JsonSerializer.SerializeToUtf8Bytes("b") },
        };
        grain.ListAsync(Arg.Any<CancellationToken>()).Returns(grainEntries);

        var queue = factory.GetLatticeQueue<string>(QueueName);
        var entries = await queue.ListAsync();

        Assert.That(entries.Select(e => e.Value), Is.EqualTo(new[] { "a", "b" }));
    }

    [Test]
    public async Task CountAsync_delegates_to_the_grain()
    {
        var (factory, grain) = Wire();
        grain.CountAsync(Arg.Any<CancellationToken>()).Returns(7);

        var queue = factory.GetLatticeQueue<string>(QueueName);
        Assert.That(await queue.CountAsync(), Is.EqualTo(7));
    }
}
