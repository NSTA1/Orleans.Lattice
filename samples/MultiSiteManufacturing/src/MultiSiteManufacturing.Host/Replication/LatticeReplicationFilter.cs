using Orleans.Lattice;
using Orleans.Runtime;

namespace MultiSiteManufacturing.Host.Replication;

/// <summary>
/// Outgoing grain call filter that observes every call to
/// <see cref="ILattice"/> methods that mutate a tree
/// (<c>SetAsync</c>, <c>DeleteAsync</c>) and appends the write to the
/// local replog when the tree is opted in to replication.
/// </summary>
/// <remarks>
/// <para>
/// Runs on the silo that originates the call (not the one hosting
/// the grain activation), so it naturally batches with the user's
/// in-process transactional workflow — if a caller issues multiple
/// lattice writes as part of one operation, each one is recorded as
/// a separate replog entry.
/// </para>
/// <para>
/// <b>Loop break.</b> Inbound replication replay sets
/// <c>RequestContext["lattice.replay"]</c>. This filter short-circuits
/// when the flag is present, so an apply-from-peer never re-enters
/// the local replog and can't ship back to the source cluster.
/// </para>
/// <para>
/// <b>FUTURE seam.</b> When <c>Orleans.Lattice</c> ships a native
/// change feed, this filter is replaced by a library-level event
/// subscription. The rest of the replication pipeline
/// (<c>ReplicatorGrain</c>, inbound endpoint, janitor) is unaffected
/// — they consume a stream of changes regardless of how that stream
/// is produced.
/// </para>
/// </remarks>
internal sealed class LatticeReplicationFilter(
    ReplicationTopology topology,
    ReplicationLogWriter writer) : IOutgoingGrainCallFilter
{
    public async Task Invoke(IOutgoingGrainCallContext context)
    {
        // IMPORTANT: Capture arguments BEFORE awaiting Invoke().
        // Orleans codegen releases the argument slots on the
        // invokable as soon as the wire message is dispatched —
        // reference-type args read back as null after the await.
        // Struct args (e.g. CancellationToken) are unaffected.
        string? capturedKey = null;
        string? capturedTree = null;
        string? capturedMethod = null;

        if (topology.IsEnabled
            && context.InterfaceMethod?.DeclaringType == typeof(ILattice)
            && context.InterfaceMethod.Name is "SetAsync" or "DeleteAsync"
            && RequestContext.Get(ReplicationConstants.ReplayRequestContextKey) is null)
        {
            capturedMethod = context.InterfaceMethod.Name;
            capturedTree = context.TargetId.Key.ToString();
            var req = context.Request;
            if (req is not null && req.GetArgumentCount() > 0 && req.GetArgument(0) is string s)
            {
                capturedKey = s;
            }
        }

        // Let the underlying call run first. We only log a
        // successful mutation — failed writes must not produce a
        // replog entry (that would ship an apparent write the
        // local cluster never actually performed).
        await context.Invoke().ConfigureAwait(false);

        if (capturedMethod is null || capturedTree is null || capturedKey is null)
        {
            return;
        }

        var methodName = capturedMethod;
        var treeName = capturedTree;
        var originalKey = capturedKey;

        if (string.IsNullOrEmpty(treeName) || treeName.StartsWith(ReplicationConstants.ReplogTreePrefix, StringComparison.Ordinal))
        {
            // Don't replicate the replog itself.
            return;
        }

        if (!topology.IsKeyReplicated(treeName, originalKey))
        {
            // Either the tree isn't opted in at all, or the tree has
            // a per-key filter that this key doesn't pass. The only
            // shipped per-key filter is the mfg-part-crdt split:
            // labels replicate, operator register does not. See
            // ReplicationTopology.IsKeyReplicated remarks.
            return;
        }

        var op = methodName == "SetAsync" ? ReplicationOp.Set : ReplicationOp.Delete;

        // Fire-and-forget replog append — we never want to slow the
        // caller on storage latency. Failures are logged inside the
        // writer and compensated for by the anti-entropy sweep.
        _ = writer.AppendAsync(treeName, originalKey, op, CancellationToken.None);
    }
}
