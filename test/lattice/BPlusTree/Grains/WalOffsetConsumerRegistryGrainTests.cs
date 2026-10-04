using NSubstitute;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Unit tests for <see cref="WalOffsetConsumerRegistryGrain"/>, the durable set
/// of offset-reading WAL consumers the GC asks for read positions (issue #4579).
/// </summary>
[TestFixture]
public class WalOffsetConsumerRegistryGrainTests
{
    private static readonly GrainId ShipperA = GrainId.Create("shipper", "tree/site-b");
    private static readonly GrainId ShipperB = GrainId.Create("shipper", "tree/site-c");

    private static (WalOffsetConsumerRegistryGrain Grain, FakePersistentState<WalOffsetConsumerRegistryState> State) CreateGrain()
    {
        var state = new FakePersistentState<WalOffsetConsumerRegistryState>();
        return (new WalOffsetConsumerRegistryGrain(Substitute.For<IGrainContext>(), state), state);
    }

    [Test]
    public async Task GetConsumersAsync_is_empty_for_a_log_no_consumer_reads()
    {
        var (grain, _) = CreateGrain();

        Assert.That(await grain.GetConsumersAsync(), Is.Empty);
    }

    [Test]
    public async Task RegisterAsync_persists_each_consumer_once()
    {
        var (grain, state) = CreateGrain();

        await grain.RegisterAsync(ShipperA);
        await grain.RegisterAsync(ShipperB);
        await grain.RegisterAsync(ShipperA);

        Assert.Multiple(async () =>
        {
            Assert.That(await grain.GetConsumersAsync(), Is.EqualTo(new[] { ShipperA, ShipperB }));
            Assert.That(state.State.Consumers, Is.EqualTo(new[] { ShipperA, ShipperB }));
            Assert.That(state.WriteCount, Is.EqualTo(2), "a repeat registration writes nothing");
        });
    }

    [Test]
    public async Task UnregisterAsync_removes_a_registered_consumer_and_ignores_an_unknown_one()
    {
        var (grain, state) = CreateGrain();
        await grain.RegisterAsync(ShipperA);

        await grain.UnregisterAsync(ShipperB);
        await grain.UnregisterAsync(ShipperA);

        Assert.Multiple(async () =>
        {
            Assert.That(await grain.GetConsumersAsync(), Is.Empty);
            Assert.That(state.WriteCount, Is.EqualTo(2));
        });
    }
}
