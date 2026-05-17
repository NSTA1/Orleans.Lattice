using System.Runtime.CompilerServices;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Exemplar in-test in-memory <see cref="IRemoteSnapshotTransport"/>
/// implementation used by
/// <see cref="InMemoryRemoteSnapshotTransportContractTests"/> to
/// exercise the contract suite. Not shipped from the product: the
/// canonical cross-cluster bindings ship in
/// <c>Orleans.Lattice.Replication.Grpc</c> alongside the sender-side
/// handler. Implementations are encouraged to reuse this loopback as
/// a reference for what "snapshot is a point-in-time view at the
/// metadata's <see cref="RemoteSnapshotMetadata.AsOfHlc"/>" implies in
/// practice.
/// </summary>
internal sealed class InMemoryRemoteSnapshotTransport : IRemoteSnapshotTransport
{
    private readonly ISnapshotProvider _senderProvider;

    public InMemoryRemoteSnapshotTransport(ISnapshotProvider senderProvider)
    {
        ArgumentNullException.ThrowIfNull(senderProvider);
        _senderProvider = senderProvider;
    }

    /// <inheritdoc />
    public async Task<RemoteSnapshotMetadata> GetMetadataAsync(
        string treeName,
        string sourceClusterId,
        HybridLogicalClock fromAsOfHlc,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(treeName);
        ArgumentException.ThrowIfNullOrWhiteSpace(sourceClusterId);
        cancellationToken.ThrowIfCancellationRequested();

        // The transport pins the sender's snapshot once and uses the
        // resulting stream's cut-point as the metadata. The stream is
        // captured here so the matching RequestSnapshotAsync call
        // observes the same point-in-time view; sender-side writes
        // committed between the two calls do not leak.
        var stream = await _senderProvider
            .ExportAsync(treeName, fromAsOfHlc, cancellationToken)
            .ConfigureAwait(false);

        return new RemoteSnapshotMetadata
        {
            TreeName = treeName,
            SourceClusterId = sourceClusterId,
            AsOfHlc = stream.AsOfHlc,
            CausalStableFrontier = stream.CausalStableFrontier,
        };
    }

    /// <inheritdoc />
    public async IAsyncEnumerable<SnapshotEntry> RequestSnapshotAsync(
        string treeName,
        string sourceClusterId,
        HybridLogicalClock fromAsOfHlc,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(treeName);
        ArgumentException.ThrowIfNullOrWhiteSpace(sourceClusterId);
        cancellationToken.ThrowIfCancellationRequested();

        var stream = await _senderProvider
            .ExportAsync(treeName, fromAsOfHlc, cancellationToken)
            .ConfigureAwait(false);

        await foreach (var entry in stream.Entries
            .WithCancellation(cancellationToken)
            .ConfigureAwait(false))
        {
            yield return entry;
        }
    }
}

/// <summary>
/// Drives the inherited <see cref="RemoteSnapshotTransportContractTests"/>
/// suite against the in-process <see cref="InMemoryRemoteSnapshotTransport"/>
/// exemplar. Proves the abstract contract suite is implementable and
/// catches regressions in the contract itself.
/// </summary>
[TestFixture]
public class InMemoryRemoteSnapshotTransportContractTests : RemoteSnapshotTransportContractTests
{
    /// <inheritdoc />
    protected override Task<TransportFixture> CreateTransportAsync()
    {
        var sender = new StubSenderSnapshotProvider();
        var transport = new InMemoryRemoteSnapshotTransport(sender);
        return Task.FromResult(new TransportFixture(transport, sender, () => ValueTask.CompletedTask));
    }
}
