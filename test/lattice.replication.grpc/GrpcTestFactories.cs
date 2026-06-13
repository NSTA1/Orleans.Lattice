using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Serialization;

namespace Orleans.Lattice.Replication.Grpc.Tests;

/// <summary>
/// Shared helpers for replication-grpc test fixtures. Builds the
/// canonical <see cref="IWalRecordEncoder"/> from a freshly-built
/// Orleans serializer service provider; the helper returns a real
/// <see cref="OrleansBinaryWalRecordEncoder"/> so the marshaller
/// framing-decode path exercised by these tests matches production.
/// </summary>
internal static class GrpcTestFactories
{
    /// <summary>
    /// Builds an <see cref="IWalRecordEncoder"/> backed by the
    /// canonical Orleans <see cref="Serializer{T}"/>. The underlying
    /// <see cref="ServiceProvider"/> is intentionally not disposed:
    /// the encoder retains a reference to the serializer for the
    /// lifetime of the surrounding test, and disposal is cheap to
    /// skip in test code where the process is torn down at suite end.
    /// </summary>
    public static IWalRecordEncoder CreateWalRecordEncoder()
    {
        var sp = new ServiceCollection().AddSerializer().BuildServiceProvider();
        return new OrleansBinaryWalRecordEncoder(sp.GetRequiredService<Serializer<WalRecord>>());
    }

    /// <summary>
    /// Builds a <see cref="LatticeReplicationGrpcMethod"/> wired with the
    /// canonical WAL-record encoder and the anti-entropy digest-probe
    /// request/response serializers resolved from a freshly-built Orleans
    /// serializer service provider.
    /// </summary>
    public static LatticeReplicationGrpcMethod CreateMethod(
        IReplicationBatchEncoder encoder,
        Serializer<ReplicationAck> ackSerializer)
    {
        var sp = new ServiceCollection().AddSerializer().BuildServiceProvider();
        return new LatticeReplicationGrpcMethod(
            encoder,
            CreateWalRecordEncoder(),
            ackSerializer,
            sp.GetRequiredService<Serializer<DigestProbeRequest>>(),
            sp.GetRequiredService<Serializer<DigestProbeResponse>>(),
            sp.GetRequiredService<Serializer<ContentManifestRequest>>(),
            sp.GetRequiredService<Serializer<ContentManifestResponse>>());
    }
}