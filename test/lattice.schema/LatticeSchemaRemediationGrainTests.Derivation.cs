using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.Schema.Tests;

public partial class LatticeSchemaRemediationGrainTests
{
    [TestCase(false)]
    [TestCase(true)]
    public async Task StartVersionMigrationAsync_registers_derived_destination_before_writes_even_when_empty(bool empty)
    {
        var h = CreateGrainBytes(
            empty ? [] : [("key", Env(1, "{\"a\":1}"))],
            schemaRegistry: MigratingRegistry());
        var registered = false;
        h.Registry.RegisterAsync(Arg.Any<string>(), Arg.Any<TreeRegistryEntry>()).Returns(call =>
        {
            Assert.That(call.Arg<TreeRegistryEntry>().DerivedFrom, Is.EqualTo(TreeId));
            registered = true;
            return Task.CompletedTask;
        });
        h.Destination.SetAsync(Arg.Any<string>(), Arg.Any<byte[]>()).Returns(_ =>
        {
            Assert.That(registered, Is.True, "derivation must be durable before the first destination write");
            return Task.CompletedTask;
        });

        var report = await h.Grain.StartVersionMigrationAsync(MigSchemaId, 2);

        Assert.That(report.Succeeded, Is.True);
        await h.Registry.Received(1).RegisterAsync(
            report.DestinationTreeId!, Arg.Is<TreeRegistryEntry>(e => e.DerivedFrom == TreeId));
    }

    [TestCase(false)]
    [TestCase(true)]
    public async Task The_destination_inherits_the_source_topology_pins_and_overrides(bool migration)
    {
        // A destination registered with defaults routes by the default 64-shard map,
        // and the cutover carries that map onto the logical tree: a resharded or
        // pinned tree came back at the defaults (#4379). Both build modes share it.
        var h = migration
            ? CreateGrainBytes([("key", Env(1, "{\"a\":1}"))], schemaRegistry: MigratingRegistry())
            : CreateGrain([("key", "{\"a\":1}")]);
        var map = ShardMap.CreateDefault(256, 7);
        h.Source.GetRoutingAsync(true).Returns(new ValueTask<RoutingInfo>(new RoutingInfo(TreeId, map)));
        h.Registry.GetEntryAsync(TreeId).Returns(new TreeRegistryEntry
        {
            ShardCount = 7,
            NextShardIndex = 9,
            MaxLeafKeys = 4,
            MaxInternalChildren = 5,
            WalPartitions = 3,
            MaxCacheValueBytes = 8192,
            PublishEvents = true,
        });

        var report = migration
            ? await h.Grain.StartVersionMigrationAsync(MigSchemaId, 2)
            : await h.Grain.StartAsync(LatticeValueTransform.Passthrough(), JsonPolicy());

        Assert.That(report.Succeeded, Is.True);
        await h.Registry.Received(1).RegisterAsync(
            report.DestinationTreeId!,
            Arg.Is<TreeRegistryEntry>(e =>
                e.ShardMap != null && e.ShardMap.Slots.SequenceEqual(map.Slots)
                && e.NextShardIndex == 9
                && e.ShardCount == 7
                && e.MaxLeafKeys == 4
                && e.MaxInternalChildren == 5
                && e.WalPartitions == 3
                && e.MaxCacheValueBytes == 8192
                && e.PublishEvents == true
                && e.DerivedFrom == TreeId));
    }
}
