using System.Collections.Concurrent;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Replication;

namespace Orleans.Lattice.Replication.Tests.PublicApiContract;

/// <summary>
/// In-process <see cref="IReplicationTransport"/> that delivers
/// <see cref="ReplicationBatch"/> payloads from one
/// <see cref="Orleans.TestingHost.TestCluster"/> to another inside the same
/// test process. Production-shaped end-to-end coverage for the public
/// replication API surface: every send still travels through the
/// canonical encoder, the canonical applier, and the per-origin
/// high-water-mark dedup path on the receiving cluster, but without
/// actually opening a socket.
/// <para>
/// Routing works via a static cluster-id -> <see cref="IServiceProvider"/>
/// map populated at silo startup (one entry per silo, last-writer-wins).
/// On <see cref="SendAsync(ReplicationBatch, CancellationToken)"/> the
/// transport looks up the destination cluster's services and decodes
/// the batch's pre-encoded entry segments through the destination's
/// <see cref="IWalRecordEncoder"/>; the resulting <see cref="WalRecord"/>
/// list is handed to <see cref="IReplicationApplier.ApplyBatchAsync"/>
/// and the returned <see cref="ReplicationAck"/> carries the
/// receiver-side high-water-mark so the sender's per-peer cursor
/// advances as it would over a real wire. An empty batch with no
/// <see cref="ReplicationBatch.EncodedEnvelope"/> is honoured as a
/// heartbeat without invoking the applier.
/// </para>
/// </summary>
internal sealed class LoopbackDeliveringTransport : IReplicationTransport
{
    private static readonly ConcurrentDictionary<string, IServiceProvider> ClusterServices = new();
    private static readonly ConcurrentQueue<ReplicationBatch> SentBatches = new();
    private static readonly ConcurrentQueue<DeliveryRecord> Deliveries = new();
    private static readonly ConcurrentQueue<FailureRecord> Failures = new();

    /// <summary>
    /// Registers the supplied <paramref name="services"/> as the destination
    /// container for <paramref name="clusterId"/>. Called from the fixture
    /// silo configurator as soon as each silo's DI graph is built;
    /// idempotent across multiple silos in the same cluster (last writer
    /// wins, but every silo in the cluster shares the same DI graph for
    /// the singletons the transport touches).
    /// </summary>
    public static void RegisterCluster(string clusterId, IServiceProvider services)
    {
        ArgumentNullException.ThrowIfNull(clusterId);
        ArgumentNullException.ThrowIfNull(services);
        ClusterServices[clusterId] = services;
    }

    /// <summary>
    /// Removes the cached <see cref="IServiceProvider"/> for the supplied
    /// <paramref name="clusterId"/>. Called by the fixture's tear-down so
    /// stale entries from a previous fixture run do not leak across tests
    /// when both clusters are torn down.
    /// </summary>
    public static void UnregisterCluster(string clusterId)
    {
        ArgumentNullException.ThrowIfNull(clusterId);
        ClusterServices.TryRemove(clusterId, out _);
    }

    /// <summary>
    /// Returns the registered <see cref="IServiceProvider"/> for the
    /// supplied <paramref name="clusterId"/>. Used by the contract
    /// suite's concern partials to resolve singletons from the
    /// destination silo's DI graph (for example
    /// <see cref="IChangeFeed"/>, <see cref="ISnapshotProvider"/>
    /// <see cref="IReplicationApplier"/>, <see cref="IReplicationBatchEncoder"/>
    /// <see cref="ILatticeWalGc"/>, <see cref="ILatticeBootstrapCoordinator"/>
    /// <see cref="IWalCursorRegistry"/>, <see cref="ILatticeReplicationDeadLetters"/>, 
    /// <see cref="ReplicationPeerStats"/>) without re-constructing
    /// them in the test code.
    /// </summary>
    /// <exception cref="InvalidOperationException">
    /// Thrown when no silo has registered itself under
    /// <paramref name="clusterId"/>; either the fixture has not been
    /// initialised yet or the silo has been torn down.
    /// </exception>
    public static IServiceProvider ServicesFor(string clusterId)
    {
        ArgumentNullException.ThrowIfNull(clusterId);
        if (!ClusterServices.TryGetValue(clusterId, out var services))
        {
            throw new InvalidOperationException(
                $"No silo registered for cluster id '{clusterId}'.");
        }

        return services;
    }

    /// <summary>Drops every recorded send and delivery so the next test starts clean.</summary>
    public static void Reset()
    {
        while (SentBatches.TryDequeue(out _)) { }
        while (Deliveries.TryDequeue(out _)) { }
        while (Failures.TryDequeue(out _)) { }
    }

    /// <summary>Every <see cref="ReplicationBatch"/> handed to <see cref="SendAsync"/>, in arrival order.</summary>
    public static IReadOnlyCollection<ReplicationBatch> Sent => SentBatches.ToArray();

    /// <summary>Every successful end-to-end delivery, in arrival order.</summary>
    public static IReadOnlyCollection<DeliveryRecord> DeliveredBatches => Deliveries.ToArray();

    /// <summary>Every send that failed routing or apply, in arrival order. Diagnostic only.</summary>
    public static IReadOnlyCollection<FailureRecord> RecordedFailures => Failures.ToArray();

    /// <summary>Cluster ids currently registered for cross-cluster delivery routing.</summary>
    public static IReadOnlyCollection<string> RegisteredClusterIds => ClusterServices.Keys.ToArray();

    /// <inheritdoc />
    public async Task<ReplicationAck> SendAsync(ReplicationBatch batch, CancellationToken cancellationToken)
    {
        if (string.IsNullOrEmpty(batch.TargetClusterId))
        {
            throw new ArgumentException(
                $"{nameof(ReplicationBatch)}.{nameof(ReplicationBatch.TargetClusterId)} must be non-empty.",
                nameof(batch));
        }

        if (string.IsNullOrEmpty(batch.TreeName))
        {
            throw new ArgumentException(
                $"{nameof(ReplicationBatch)}.{nameof(ReplicationBatch.TreeName)} must be non-empty.",
                nameof(batch));
        }

        if (string.IsNullOrEmpty(batch.OriginClusterId))
        {
            throw new ArgumentException(
                $"{nameof(ReplicationBatch)}.{nameof(ReplicationBatch.OriginClusterId)} must be non-empty.",
                nameof(batch));
        }

        SentBatches.Enqueue(batch);

        if (!ClusterServices.TryGetValue(batch.TargetClusterId, out var dest))
        {
            Failures.Enqueue(new FailureRecord(
                batch.TargetClusterId, batch.TreeName, batch.OriginClusterId,
                "destination-cluster-not-registered", null));
            return new ReplicationAck { Accepted = false, HighestAppliedHlc = default };
        }

        // Empty payload AND no framing-only encoded envelope: this is
        // the heartbeat / no-op tick shape. Accept without invoking
        // the applier so the wire-shape contract for empty batches
        // is honoured end-to-end. Production today never sends this
        // shape (the shipper only constructs a batch when it has
        // segments to ship), but the cheap guard documents the
        // contract and keeps the routing-failure path narrow.
        if (batch.Payload.IsEmpty && batch.EncodedEnvelope is null)
        {
            return new ReplicationAck { Accepted = true, HighestAppliedHlc = default };
        }

        // Steady-state ship path: the shipper writes only
        // EncodedEnvelope (Payload is always empty, Envelope is always
        // null on the framing-only ship path). Decode each pre-encoded
        // entry segment via the destination's canonical
        // IWalRecordEncoder so the apply path sees the same WalRecord
        // shape it would have got from a typed envelope decode.
        try
        {
            var encoded = batch.EncodedEnvelope!.Value;
            var applier = dest.GetRequiredService<IReplicationApplier>();
            var walEncoder = dest.GetRequiredService<IWalRecordEncoder>();

            var segments = encoded.EncodedEntries.Span;
            var decoded = new WalRecord[segments.Length];
            for (var i = 0; i < segments.Length; i++)
            {
                // Re-stamp TreeId from the surrounding batch context
                // and Mode from the framing header: the producer's
                // Encode strips both slots, so the 2-arg Decode would
                // yield TreeId == "" / Mode == LwwRegister.
                decoded[i] = walEncoder.Decode(segments[i].AsSpan(), batch.TreeName, encoded.Header.Mode);
            }

            var result = await applier.ApplyBatchAsync(decoded, cancellationToken).ConfigureAwait(false);
            Deliveries.Enqueue(new DeliveryRecord(
                batch.TargetClusterId,
                batch.TreeName,
                batch.OriginClusterId,
                decoded.Length,
                result));

            return new ReplicationAck
            {
                Accepted = true,
                HighestAppliedHlc = result.HighWaterMark,
            };
        }
        catch (Exception ex)
        {
            Failures.Enqueue(new FailureRecord(
                batch.TargetClusterId, batch.TreeName, batch.OriginClusterId,
                ex.GetType().FullName ?? "<unknown>", ex.ToString()));
            throw;
        }
    }

    /// <summary>
    /// One captured end-to-end delivery: routing metadata, decoded entry
    /// count, and the receiver-side <see cref="ApplyResult"/>.
    /// </summary>
    public readonly record struct DeliveryRecord(
        string TargetClusterId,
        string TreeName,
        string OriginClusterId,
        int EntryCount,
        ApplyResult Result);

    /// <summary>
    /// One captured send-side failure: routing metadata plus the failure
    /// kind ("destination-cluster-not-registered" or an exception type)
    /// and the optional exception text. Diagnostic-only.
    /// </summary>
    public readonly record struct FailureRecord(
        string TargetClusterId,
        string TreeName,
        string OriginClusterId,
        string Kind,
        string? Detail);
}
