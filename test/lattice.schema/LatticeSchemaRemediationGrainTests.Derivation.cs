using NSubstitute;
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
}
