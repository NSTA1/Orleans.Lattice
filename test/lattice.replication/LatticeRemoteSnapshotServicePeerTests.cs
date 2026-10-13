using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Replication.Tests;

[TestFixture]
public sealed class LatticeRemoteSnapshotServicePeerTests
{
    private const string Tree = "t/acme/orders";

    private static LatticeRemoteSnapshotService Create(
        ISnapshotProvider provider, IReplicationTenantIsolationGate? gate = null) =>
        new(provider, new StubReplicationContext("source", LatticeMergeMode.LwwRegister),
            NullLogger<LatticeRemoteSnapshotService>.Instance, gate);

    [TestCase("metadata")]
    [TestCase("entries")]
    [TestCase("items")]
    public async Task Export_denied_peer_never_opens_or_enumerates_provider(string operation)
    {
        var provider = Substitute.For<ISnapshotProvider>();
        var gate = Substitute.For<IReplicationTenantIsolationGate>();
        gate.IsActive.Returns(true);
        gate.AuthorizeExportAsync(Tree, "outsider", Arg.Any<CancellationToken>())
            .Returns(ValueTask.FromResult(false));
        var service = Create(provider, gate);

        Assert.ThrowsAsync<UnauthorizedAccessException>(async () =>
        {
            if (operation == "metadata")
                await service.GetMetadataAsync(Tree, "source", HybridLogicalClock.Zero, "outsider", default);
            else if (operation == "entries")
                await foreach (var _ in service.RequestSnapshotAsync(Tree, "source", HybridLogicalClock.Zero, "outsider", default)) { }
            else
                await foreach (var _ in service.RequestSnapshotItemsAsync(Tree, "source", HybridLogicalClock.Zero, "outsider", default)) { }
        });

        await provider.DidNotReceiveWithAnyArgs().ExportAsync(default!, default, default);
        await gate.Received(1).AuthorizeExportAsync(Tree, "outsider", Arg.Any<CancellationToken>());
    }

    [TestCase("metadata")]
    [TestCase("entries")]
    [TestCase("items")]
    public async Task Export_legacy_overload_never_trusts_source_cluster_as_requester(string operation)
    {
        var provider = Substitute.For<ISnapshotProvider>();
        var gate = Substitute.For<IReplicationTenantIsolationGate>();
        gate.IsActive.Returns(true);
        var service = Create(provider, gate);

        Assert.ThrowsAsync<UnauthorizedAccessException>(async () =>
        {
            if (operation == "metadata")
                await service.GetMetadataAsync(Tree, "authorized-requester", HybridLogicalClock.Zero);
            else if (operation == "entries")
                await foreach (var _ in service.RequestSnapshotAsync(Tree, "authorized-requester", HybridLogicalClock.Zero)) { }
            else
                await foreach (var _ in service.RequestSnapshotItemsAsync(Tree, "authorized-requester", HybridLogicalClock.Zero)) { }
        });

        await provider.DidNotReceiveWithAnyArgs().ExportAsync(default!, default, default);
        await gate.DidNotReceiveWithAnyArgs().AuthorizeExportAsync(default!, default!, default);
    }

    [Test]
    public async Task Export_without_active_tenant_gate_denies_even_authenticated_peer()
    {
        var provider = Substitute.For<ISnapshotProvider>();
        Assert.ThrowsAsync<UnauthorizedAccessException>(async () =>
            await Create(provider).GetMetadataAsync(Tree, "source", HybridLogicalClock.Zero, "peer", default));
        await provider.DidNotReceiveWithAnyArgs().ExportAsync(default!, default, default);
    }

    [TestCase("metadata")]
    [TestCase("entries")]
    [TestCase("items")]
    public async Task Export_authorized_peer_can_receive_prepared_mutation_bytes(string operation)
    {
        var provider = Substitute.For<ISnapshotProvider>();
        var gate = Substitute.For<IReplicationTenantIsolationGate>();
        gate.IsActive.Returns(true);
        gate.AuthorizeExportAsync(Tree, "backfilling-peer", Arg.Any<CancellationToken>())
            .Returns(ValueTask.FromResult(true));
        provider.ExportAsync(Tree, Arg.Any<HybridLogicalClock>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(new SnapshotStream(Tree, HybridLogicalClock.Zero, new VersionVector(), PreparedEntries())));
        var service = Create(provider, gate);

        if (operation == "metadata")
            await service.GetMetadataAsync(Tree, "source", HybridLogicalClock.Zero, "backfilling-peer", default);
        else if (operation == "entries")
        {
            var entries = new List<SnapshotEntry>();
            await foreach (var entry in service.RequestSnapshotAsync(Tree, "source", HybridLogicalClock.Zero, "backfilling-peer", default))
                entries.Add(entry);
            Assert.That(entries.Single().Value, Is.EqualTo(new byte[] { 42 }));
        }
        else
        {
            var items = new List<RemoteSnapshotStreamItem>();
            await foreach (var item in service.RequestSnapshotItemsAsync(Tree, "source", HybridLogicalClock.Zero, "backfilling-peer", default))
                items.Add(item);
            Assert.That(items.Single().Entry.Value, Is.EqualTo(new byte[] { 42 }));
        }

        await gate.Received(1).AuthorizeExportAsync(Tree, "backfilling-peer", Arg.Any<CancellationToken>());
        await provider.Received(1).ExportAsync(Tree, Arg.Any<HybridLogicalClock>(), Arg.Any<CancellationToken>());
    }

    private static async IAsyncEnumerable<SnapshotEntry> PreparedEntries()
    {
        await Task.CompletedTask;
        yield return new SnapshotEntry { Key = "prepared", Value = [42], IsPrepared = true };
    }
}
