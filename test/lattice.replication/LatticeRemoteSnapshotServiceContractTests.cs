using Microsoft.Extensions.Logging.Abstractions;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Drives the inherited <see cref="RemoteSnapshotTransportContractTests"/>
/// suite against the real sender-side handler
/// <see cref="LatticeRemoteSnapshotService"/>. Establishes the
/// transport-agnostic loopback fixture: a host can route inbound
/// <see cref="IRemoteSnapshotTransport"/> RPCs into the shipped
/// handler and observe the cross-cluster bootstrap contract
/// (metadata-then-stream consistency, point-in-time view at
/// <see cref="RemoteSnapshotMetadata.AsOfHlc"/>, argument validation,
/// cancellation) end-to-end on the sender side without a concrete
/// transport binding.
/// </summary>
[TestFixture]
public class LatticeRemoteSnapshotServiceContractTests : RemoteSnapshotTransportContractTests
{
    /// <inheritdoc />
    protected override Task<TransportFixture> CreateTransportAsync()
    {
        var sender = new StubSenderSnapshotProvider();
        var service = new LatticeRemoteSnapshotService(
            sender,
            NullLogger<LatticeRemoteSnapshotService>.Instance);
        return Task.FromResult(new TransportFixture(service, sender, () => ValueTask.CompletedTask));
    }
}
