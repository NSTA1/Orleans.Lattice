using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Serialization;

namespace Orleans.Lattice.Tests.BPlusTree.State;

public class AliasRoutingMoveStateTests
{
    [Test]
    public void Durable_move_dictionary_round_trips_through_Orleans_serializer()
    {
        using var services = new ServiceCollection().AddSerializer().BuildServiceProvider();
        var serializer = services.GetRequiredService<Serializer<Dictionary<string, AliasRoutingMoveState>>>();
        var source = ShardMap.CreateDefault(LatticeConstants.DefaultVirtualShardCount, 2);
        var target = ShardMap.CreateDefault(LatticeConstants.DefaultVirtualShardCount, 3);
        var before = new TreeRegistryEntry { ShardMap = source };
        var move = new AliasRoutingMoveState
        {
            TreeId = "logical", Source = "logical", Destination = "target",
            SourceMap = source, DestinationMap = target, Before = before,
            After = before with { PhysicalTreeId = "target", ShardMap = target, AliasRoutingOperationId = "op" },
            OperationId = "op",
        };
        var decoded = serializer.Deserialize(serializer.SerializeToArray(new Dictionary<string, AliasRoutingMoveState>
        {
            [move.TreeId] = move,
        }))["logical"];
        Assert.Multiple(() =>
        {
            Assert.That(decoded.TreeId, Is.EqualTo(move.TreeId));
            Assert.That(decoded.Source, Is.EqualTo(move.Source));
            Assert.That(decoded.Destination, Is.EqualTo(move.Destination));
            Assert.That(decoded.SourceMap.Slots, Is.EqualTo(source.Slots));
            Assert.That(decoded.DestinationMap.Slots, Is.EqualTo(target.Slots));
            Assert.That(decoded.Before!.ShardMap!.Slots, Is.EqualTo(source.Slots));
            Assert.That(decoded.After.PhysicalTreeId, Is.EqualTo("target"));
            Assert.That(decoded.After.AliasRoutingOperationId, Is.EqualTo(decoded.OperationId));
        });
    }

    [Test]
    public void Redirect_backups_and_multiple_logical_fences_round_trip_through_Orleans_serializer()
    {
        using var services = new ServiceCollection().AddSerializer().BuildServiceProvider();
        var serializer = services.GetRequiredService<Serializer<ShardRootState>>();
        var redirect = new RetainedRedirectState
        {
            LogicalTreeId = "logical", OperationId = "previous", DestinationPhysicalTreeId = "copy",
        };
        var state = new ShardRootState
        {
            AdditionalRetainedRedirects = new() { ["logical"] = redirect },
            PreviousRetainedRedirects = new() { ["pending"] = redirect },
        };
        var decoded = serializer.Deserialize(serializer.SerializeToArray(state));
        foreach (var copy in new[] { decoded.AdditionalRetainedRedirects!["logical"], decoded.PreviousRetainedRedirects!["pending"] })
        {
            Assert.Multiple(() =>
            {
                Assert.That(copy.LogicalTreeId, Is.EqualTo(redirect.LogicalTreeId));
                Assert.That(copy.OperationId, Is.EqualTo(redirect.OperationId));
                Assert.That(copy.DestinationPhysicalTreeId, Is.EqualTo(redirect.DestinationPhysicalTreeId));
            });
        }
    }
}
