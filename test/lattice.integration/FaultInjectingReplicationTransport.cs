using System.Collections.Concurrent;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Replication;

namespace Orleans.Lattice.Integration.Tests;

/// <summary>
/// Production-shaped, fault-injecting in-process <see cref="IReplicationTransport"/>
/// used by the durable active-active integration suite. Modeled directly on
/// <c>test/lattice.replication/PublicApiContract/LoopbackDeliveringTransport.cs</c>:
/// every send still travels through the destination cluster's canonical
/// <see cref="IWalRecordEncoder"/> and <see cref="IReplicationApplier"/>, and
/// the returned <see cref="ReplicationAck"/> carries the receiver-side
/// high-water-mark, exactly as a real wire transport would. Routing is a
/// static cluster-id -> <see cref="IServiceProvider"/> map populated by a
/// per-silo hosted service (<c>ClusterServiceProviderRegistrar</c> in
/// the fixture); this static side-channel is the only place fixture identity
/// crosses into the type-instantiated <c>ISiloConfigurator</c>, and it is
/// fully cleaned up on cluster teardown / restart.
/// <para>
/// On top of the base loopback behavior, this transport supports fault
/// injection needed by the eight durable active-active scenarios:
/// </para>
/// <list type="bullet">
///   <item>Directed <see cref="Partition"/> / <see cref="Heal"/> / <see cref="HealAll"/>
///   between an (origin, target) cluster-id pair - a partitioned send is
///   dropped before the destination is even resolved, exactly like a
///   severed network link.</item>
///   <item>A one-shot <see cref="ScheduleRejectAfterApplyOnce"/> per tree: the
///   destination applies the batch normally (so receiver state converges),
///   but the ack reports <c>Accepted = false</c> exactly once - modeling a
///   lost ack that forces the sender to retry (and the receiver to observe a
///   harmless duplicate apply).</item>
///   <item>A one-shot <see cref="ScheduleGateBeforeApplyOnce"/> per tree: the
///   next send resolves and decodes against the destination's current
///   service provider, signals <see cref="OneShotGate.Entered"/>, and then
///   blocks at the apply boundary until the test calls
///   <see cref="OneShotGate.Release"/>. This lets a test cold-restart the
///   receiver while a delivery is already bound to its old DI graph.</item>
///   <item>Per-tree delivery records and counts (<see cref="DeliveryCount"/>,
///   <see cref="DeliveriesFor"/>) so a test can assert on exactly-once
///   convergence despite an injected duplicate.</item>
///   <item><see cref="ResetTransientFaults"/> clears every partition and
///   releases/clears every pending one-shot (gate or reject) without
///   touching delivery history or the cluster registry - the shape a
///   <c>[TearDown]</c> needs to normalize state between tests without
///   stopping either site.</item>
/// </list>
/// </summary>
internal sealed class FaultInjectingReplicationTransport : IReplicationTransport
{
    private static readonly ConcurrentDictionary<string, IServiceProvider> ClusterServices =
        new(StringComparer.Ordinal);

    private static readonly ConcurrentDictionary<(string Origin, string Target), byte> Partitions = new();

    private static readonly ConcurrentDictionary<string, byte> PendingRejectAfterApplyByTree =
        new(StringComparer.Ordinal);

    private static readonly ConcurrentDictionary<string, byte> PendingRejectAndPartitionAfterApplyByTree =
        new(StringComparer.Ordinal);

    private static readonly ConcurrentDictionary<string, OneShotGate> PendingGateByTree =
        new(StringComparer.Ordinal);

    private static readonly ConcurrentDictionary<string, int> DeliveryCountByTree =
        new(StringComparer.Ordinal);

    private static readonly ConcurrentDictionary<string, int> AcceptedAckCountByTree =
        new(StringComparer.Ordinal);

    private static readonly ConcurrentDictionary<string, ConcurrentQueue<DeliveryRecord>> DeliveriesByTree =
        new(StringComparer.Ordinal);

    private static readonly ConcurrentQueue<DroppedSendRecord> DroppedSends = new();

    /// <summary>
    /// Registers <paramref name="services"/> as the current destination
    /// container for <paramref name="clusterId"/>. Called from a per-silo
    /// hosted service as soon as that silo's DI graph is built; a cold
    /// restart re-registers the same cluster id against the new silo's
    /// service provider, so an in-flight gated send that resolves the
    /// destination only after release picks up the post-restart graph.
    /// </summary>
    public static void RegisterCluster(string clusterId, IServiceProvider services)
    {
        ArgumentNullException.ThrowIfNull(clusterId);
        ArgumentNullException.ThrowIfNull(services);
        ClusterServices[clusterId] = services;
    }

    /// <summary>
    /// Removes the cached <see cref="IServiceProvider"/> for
    /// <paramref name="clusterId"/>. Called before a site is stopped so a
    /// stale, disposed provider is never handed to a concurrent send.
    /// </summary>
    public static void UnregisterCluster(string clusterId)
    {
        ArgumentNullException.ThrowIfNull(clusterId);
        ClusterServices.TryRemove(clusterId, out _);
    }

    /// <summary>Returns the currently-registered <see cref="IServiceProvider"/> for <paramref name="clusterId"/>.</summary>
    /// <exception cref="InvalidOperationException">No silo is currently registered under <paramref name="clusterId"/>.</exception>
    public static IServiceProvider ServicesFor(string clusterId)
    {
        ArgumentNullException.ThrowIfNull(clusterId);
        if (!ClusterServices.TryGetValue(clusterId, out var services))
        {
            throw new InvalidOperationException($"No silo registered for cluster id '{clusterId}'.");
        }

        return services;
    }

    /// <summary>Drops every delivered-batch record and per-tree count. Registry, partitions, and pending one-shots are untouched.</summary>
    public static void ResetDeliveryHistory()
    {
        DeliveryCountByTree.Clear();
        AcceptedAckCountByTree.Clear();
        DeliveriesByTree.Clear();
        while (DroppedSends.TryDequeue(out _)) { }
    }

    /// <summary>
    /// Clears every directed partition and releases/clears every pending
    /// one-shot gate or reject-after-apply flag, without touching the
    /// cluster registry or delivery history. This is the exact reset a
    /// <c>[TearDown]</c> needs: it normalizes transient fault state between
    /// tests without stopping either site.
    /// </summary>
    public static void ResetTransientFaults()
    {
        Partitions.Clear();
        PendingRejectAfterApplyByTree.Clear();
        PendingRejectAndPartitionAfterApplyByTree.Clear();

        foreach (var treeName in PendingGateByTree.Keys.ToArray())
        {
            if (PendingGateByTree.TryRemove(treeName, out var gate))
            {
                gate.Release();
            }
        }
    }

    /// <summary>Drops every send from <paramref name="origin"/> to <paramref name="target"/> until healed.</summary>
    public static void Partition(string origin, string target)
    {
        ArgumentNullException.ThrowIfNull(origin);
        ArgumentNullException.ThrowIfNull(target);
        Partitions[(origin, target)] = 1;
    }

    /// <summary>Restores delivery from <paramref name="origin"/> to <paramref name="target"/>.</summary>
    public static void Heal(string origin, string target)
    {
        ArgumentNullException.ThrowIfNull(origin);
        ArgumentNullException.ThrowIfNull(target);
        Partitions.TryRemove((origin, target), out _);
    }

    /// <summary>Restores delivery in every direction between every currently-registered cluster pair.</summary>
    public static void HealAll() => Partitions.Clear();

    /// <summary>
    /// Arms a one-shot fault: the next send for <paramref name="treeName"/>
    /// is applied normally (the receiver's state converges) but the
    /// returned ack reports <c>Accepted = false</c>, exactly once - modeling
    /// a lost ack that the sender must retry.
    /// </summary>
    public static void ScheduleRejectAfterApplyOnce(string treeName)
    {
        ArgumentNullException.ThrowIfNull(treeName);
        PendingRejectAfterApplyByTree[treeName] = 1;
    }

    /// <summary>
    /// Arms a one-shot lost acknowledgement and immediately partitions the
    /// exact directed sender-to-receiver edge after the receiver applies the
    /// batch. The partition prevents the sender from resolving the uncertain
    /// delivery with a retry before the test can restart it.
    /// </summary>
    public static void ScheduleRejectAndPartitionAfterApplyOnce(string treeName)
    {
        ArgumentNullException.ThrowIfNull(treeName);
        PendingRejectAndPartitionAfterApplyByTree[treeName] = 1;
    }

    /// <summary>
    /// Arms a one-shot gate: the next send for <paramref name="treeName"/>
    /// resolves and decodes against the destination's current service
    /// provider, signals <see cref="OneShotGate.Entered"/>, and then blocks
    /// at the apply boundary until the returned gate's
    /// <see cref="OneShotGate.Release"/> is called. Returns the gate so the
    /// caller can await entry and later release it.
    /// </summary>
    public static OneShotGate ScheduleGateBeforeApplyOnce(string treeName)
    {
        ArgumentNullException.ThrowIfNull(treeName);
        var gate = new OneShotGate();
        PendingGateByTree[treeName] = gate;
        return gate;
    }

    /// <summary>The number of times a send for <paramref name="treeName"/> reached and completed an apply call.</summary>
    public static int DeliveryCount(string treeName)
    {
        ArgumentNullException.ThrowIfNull(treeName);
        return DeliveryCountByTree.TryGetValue(treeName, out var count) ? count : 0;
    }

    /// <summary>The number of successful acknowledgements returned for <paramref name="treeName"/>.</summary>
    public static int AcceptedAckCount(string treeName)
    {
        ArgumentNullException.ThrowIfNull(treeName);
        return AcceptedAckCountByTree.TryGetValue(treeName, out var count) ? count : 0;
    }

    /// <summary>Every completed delivery for <paramref name="treeName"/>, in arrival order.</summary>
    public static IReadOnlyList<DeliveryRecord> DeliveriesFor(string treeName)
    {
        ArgumentNullException.ThrowIfNull(treeName);
        return DeliveriesByTree.TryGetValue(treeName, out var queue) ? queue.ToArray() : Array.Empty<DeliveryRecord>();
    }

    /// <summary>Every send dropped by a directed partition, in arrival order. Diagnostic only.</summary>
    public static IReadOnlyList<DroppedSendRecord> Dropped => DroppedSends.ToArray();

    /// <inheritdoc />
    public async Task<ReplicationAck> SendAsync(ReplicationBatch batch, CancellationToken cancellationToken)
    {
        if (string.IsNullOrEmpty(batch.TargetClusterId))
        {
            throw new ArgumentException(
                $"{nameof(ReplicationBatch)}.{nameof(ReplicationBatch.TargetClusterId)} must be non-empty.", nameof(batch));
        }

        if (string.IsNullOrEmpty(batch.TreeName))
        {
            throw new ArgumentException(
                $"{nameof(ReplicationBatch)}.{nameof(ReplicationBatch.TreeName)} must be non-empty.", nameof(batch));
        }

        if (string.IsNullOrEmpty(batch.OriginClusterId))
        {
            throw new ArgumentException(
                $"{nameof(ReplicationBatch)}.{nameof(ReplicationBatch.OriginClusterId)} must be non-empty.", nameof(batch));
        }

        if (Partitions.ContainsKey((batch.OriginClusterId, batch.TargetClusterId)))
        {
            DroppedSends.Enqueue(new DroppedSendRecord(batch.OriginClusterId, batch.TargetClusterId, batch.TreeName));
            return new ReplicationAck { Accepted = false, HighestAppliedHlc = default };
        }

        if (batch.Payload.IsEmpty && batch.EncodedEnvelope is null)
        {
            // Heartbeat / no-op tick shape: accept without invoking the applier.
            return new ReplicationAck { Accepted = true, HighestAppliedHlc = default };
        }

        var dest = ServicesFor(batch.TargetClusterId);

        var encoded = batch.EncodedEnvelope!.Value;
        var applier = dest.GetRequiredService<IReplicationApplier>();
        var walEncoder = dest.GetRequiredService<IWalRecordEncoder>();

        var segments = encoded.EncodedEntries.Span;
        var decoded = new WalRecord[segments.Length];
        for (var i = 0; i < segments.Length; i++)
        {
            // Re-stamp TreeId from the surrounding batch context and Mode
            // from the framing header, mirroring LoopbackDeliveringTransport.
            decoded[i] = walEncoder.Decode(segments[i].AsSpan(), batch.TreeName, encoded.Header.Mode);
        }

        // Resolve the receiver-side apply path first, then stop at its boundary.
        // A receiver restart while this gate is held invalidates the captured
        // destination graph and forces the sender's production retry path.
        if (PendingGateByTree.TryRemove(batch.TreeName, out var gate))
        {
            await gate.WaitForReleaseAsync(cancellationToken).ConfigureAwait(false);
            if (!ReferenceEquals(dest, ServicesFor(batch.TargetClusterId)))
            {
                // The receiver was replaced while this send was parked at its
                // apply boundary. Treat the stale in-flight attempt as
                // unacknowledged so the production shipper retries against
                // the replacement receiver.
                return new ReplicationAck { Accepted = false, HighestAppliedHlc = default };
            }
        }

        // Marshal off the sending grain's activation turn. Running the
        // applier inline would deadlock the destination's own grain-call
        // chain while still pinned to the sender's scheduler; Task.Run
        // reproduces the decoupling a real wire transport gives for free.
        var result = await Task.Run(
            () => applier.ApplyBatchAsync(decoded, cancellationToken),
            cancellationToken).ConfigureAwait(false);

        DeliveryCountByTree.AddOrUpdate(batch.TreeName, 1, static (_, count) => count + 1);
        DeliveriesByTree.GetOrAdd(batch.TreeName, static _ => new ConcurrentQueue<DeliveryRecord>())
            .Enqueue(new DeliveryRecord(batch.TargetClusterId, batch.TreeName, batch.OriginClusterId, decoded.Length, result));

        if (PendingRejectAfterApplyByTree.TryRemove(batch.TreeName, out _))
        {
            // The apply above already ran (receiver state converged); report
            // failure anyway so the sender retries - a one-shot lost ack.
            return new ReplicationAck { Accepted = false, HighestAppliedHlc = result.HighWaterMark };
        }

        if (PendingRejectAndPartitionAfterApplyByTree.TryRemove(batch.TreeName, out _))
        {
            Partition(batch.OriginClusterId, batch.TargetClusterId);
            return new ReplicationAck { Accepted = false, HighestAppliedHlc = result.HighWaterMark };
        }

        AcceptedAckCountByTree.AddOrUpdate(batch.TreeName, 1, static (_, count) => count + 1);
        return new ReplicationAck
        {
            // DURABLE RECEIVE FENCE: a deferred result means the tree's
            // inbound receive fence deferred the apply, so the sender must
            // retry rather than advance its cursor.
            Accepted = !result.Deferred,
            HighestAppliedHlc = result.HighWaterMark,
        };
    }

    /// <summary>
    /// A single-use signal gate: <see cref="WaitForReleaseAsync"/> marks
    /// <see cref="Entered"/> complete and then blocks until
    /// <see cref="Release"/> is called or the awaited send's
    /// <see cref="CancellationToken"/> fires, so a caller can never hang the
    /// transport indefinitely on a cancelled or aborted send.
    /// </summary>
    public sealed class OneShotGate
    {
        private readonly TaskCompletionSource _entered =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        private readonly TaskCompletionSource _release =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>Completes once the gated send has signaled entry and begun waiting for release.</summary>
        public Task Entered => _entered.Task;

        /// <summary>Releases the gate, letting the gated send proceed. Idempotent.</summary>
        public void Release() => _release.TrySetResult();

        internal async Task WaitForReleaseAsync(CancellationToken cancellationToken)
        {
            _entered.TrySetResult();
            using var registration = cancellationToken.Register(
                static state => ((TaskCompletionSource)state!).TrySetCanceled(),
                _release);
            await _release.Task.ConfigureAwait(false);
        }
    }

    /// <summary>One completed end-to-end delivery: routing metadata, decoded entry count, and the receiver-side <see cref="ApplyResult"/>.</summary>
    public readonly record struct DeliveryRecord(
        string TargetClusterId, string TreeName, string OriginClusterId, int EntryCount, ApplyResult Result);

    /// <summary>One send dropped by a directed partition. Diagnostic only.</summary>
    public readonly record struct DroppedSendRecord(string OriginClusterId, string TargetClusterId, string TreeName);
}
