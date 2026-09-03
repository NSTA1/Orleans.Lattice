using System.Reflection;
using Orleans.Lattice.Testing;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Concrete guard for the core <c>Orleans.Lattice</c> assembly: every
/// <c>[Immutable]</c> type reachable from one of its grain interfaces while
/// carrying a mutable <see cref="byte"/>[] or collection must be a reviewed,
/// read-only-on-receipt payload, and no CRDT state may be shared across a
/// skipped same-silo copy.
/// <para>
/// This is the enforcement half of the <c>[Immutable]</c> sweep deferred by the
/// CRDT primitive aliasing audit. That sweep concluded the risk is bounded
/// because CRDT payloads cross grain boundaries as opaque <see cref="byte"/>[]
/// and are decoded into a fresh object graph before being folded - never shared
/// and folded in place. That conclusion is a reachability argument about today's
/// signatures, so it is pinned here rather than left as prose: a future grain
/// method that puts a typed CRDT (or any other in-place-folded payload) on the
/// boundary fails this fixture until it is deliberately reviewed.
/// </para>
/// </summary>
[TestFixture]
public sealed class ImmutableGrainBoundaryContractTests : ImmutableGrainBoundaryContractTestsBase
{
    /// <summary>
    /// An opaque caller-authored buffer: the tree stores and returns the bytes
    /// without inspecting them, and a new value replaces the reference wholesale
    /// rather than being written into. Nothing on the receiving side indexes into
    /// the array, so a shared instance is only ever read.
    /// </summary>
    private const string OpaquePayload =
        "Opaque caller-authored value/delta bytes; stored and forwarded verbatim, replaced by reference on write, never written into.";

    /// <summary>
    /// A content digest computed by the sender and only ever compared by the
    /// receiver.
    /// </summary>
    private const string ContentDigest =
        "Content digest computed by the sender; the receiver only compares it for equality.";

    /// <summary>
    /// A projection built fresh for one call and handed to a caller that reads
    /// it. The producing grain keeps no reference to the collection after the
    /// call returns, so a skipped copy shares a graph nobody subsequently writes.
    /// </summary>
    private const string ReadModel =
        "Per-call read model projected out of grain state; the producer retains no reference and the consumer only enumerates it.";

    /// <summary>
    /// A mutable <c>VersionVector</c> reached through an <c>[Immutable]</c>
    /// carrier, made safe by a defensive copy on both the ingress seam
    /// (<c>LatticeVectorClockContext</c>'s setter) and the egress seam
    /// (<c>LwwEntry</c>'s <c>LwwValue</c> constructor).
    /// </summary>
    private const string CopiedAtBothSeams =
        "Mutable VersionVector inside an [Immutable] carrier, but copied at both the ingress seam "
        + "(LatticeVectorClockContext setter) and the egress seam (LwwEntry), so no shared instance "
        + "ever becomes durable state or escapes to a caller.";

    /// <inheritdoc />
    protected override Assembly PackageAssembly => typeof(LatticeWriteFencedException).Assembly;

    /// <inheritdoc />
    protected override IReadOnlyDictionary<string, string> AcknowledgedReadOnlyPayloads =>
        new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["Orleans.Lattice.AtomicActionEntry"] = OpaquePayload,
            ["Orleans.Lattice.BPlusTree.ApplyCrdtDeltaItem"] = OpaquePayload,
            ["Orleans.Lattice.BPlusTree.ApplyMergeItem"] = OpaquePayload,
            ["Orleans.Lattice.BPlusTree.Grains.LatticeQueueByteEntry"] = OpaquePayload,
            ["Orleans.Lattice.BPlusTree.Grains.WalShardShippingEntry"] = OpaquePayload,
            ["Orleans.Lattice.BPlusTree.LwwEntry"] = OpaquePayload,
            ["Orleans.Lattice.BPlusTree.PendingMutationSnapshot"] = OpaquePayload,
            ["Orleans.Lattice.BPlusTree.State.LeafBaselinePendingEntry"] = OpaquePayload,
            ["Orleans.Lattice.EntryRevision"] = OpaquePayload,
            ["Orleans.Lattice.LatticeMutation"] = OpaquePayload,
            ["Orleans.Lattice.Views.RuntimeViewRegistration"] = OpaquePayload,
            ["Orleans.Lattice.WalRecord"] = OpaquePayload,

            ["Orleans.Lattice.BPlusTree.ChildDigestSnapshot"] = ContentDigest,
            ["Orleans.Lattice.LeafProjectionDigest"] = ContentDigest,
            ["Orleans.Lattice.ViewDigest"] = ContentDigest,

            ["Orleans.Lattice.BPlusTree.CrossTreeReceiverDecision"] = ReadModel,
            ["Orleans.Lattice.BPlusTree.CrossTreeReceiverTerminal"] = ReadModel,
            ["Orleans.Lattice.BPlusTree.CrossTreeReceiverTreeFinalize"] = ReadModel,
            ["Orleans.Lattice.BPlusTree.DirtyLeavesSnapshot"] = ReadModel,
            ["Orleans.Lattice.BPlusTree.Grains.WalShardPage"] = ReadModel,
            ["Orleans.Lattice.BPlusTree.Grains.WalShardShippingPage"] = ReadModel,
            ["Orleans.Lattice.BPlusTree.RoutingTableSnapshot"] = ReadModel,
            ["Orleans.Lattice.BPlusTree.ShardCountResult"] = ReadModel,
            ["Orleans.Lattice.BPlusTree.ShardCountWithMovedAwayPage"] = ReadModel,
            ["Orleans.Lattice.BPlusTree.ShardTopologyNode"] = ReadModel,
            ["Orleans.Lattice.BPlusTree.SnapshotBaselineCaptureResult"] = ReadModel,
            ["Orleans.Lattice.BPlusTree.TerminalTallyResult"] = ReadModel,
            ["Orleans.Lattice.BPlusTree.TxRegistrySnapshot"] = ReadModel,
            ["Orleans.Lattice.ConditionalSetManyResult"] = ReadModel,
            ["Orleans.Lattice.EntryHistoryPage"] = ReadModel,
            ["Orleans.Lattice.LatticeSnapshotCoordinate"] = ReadModel,
            ["Orleans.Lattice.RangeDeleteResult"] = ReadModel,

            ["Orleans.Lattice.LatticePredicateNode"] =
                "Caller-authored predicate tree; the grain walks it to evaluate a match and never writes into it.",
        };

    /// <inheritdoc />
    protected override IReadOnlyDictionary<string, string> TrackedCrdtCarrierExemptions =>
        new Dictionary<string, string>(StringComparer.Ordinal)
        {
            // These carriers hold a VersionVector, which is a mutable CRDT, so an
            // elided same-silo copy shares one instance. They are safe because the
            // two seams where that instance could become durable state, or escape
            // to a caller, both take a defensive copy:
            //
            //   ingress - LatticeVectorClockContext's setter clones, so the frontier
            //             stamped onto every LwwValue<T>.VectorClock in a scope is
            //             platform-owned rather than the (possibly co-located
            //             sender's) instance that arrived in the carrier;
            //   egress  - LwwEntry's LwwValue constructor clones, so a caller reading
            //             an entry never receives a live handle on stored state.
            //
            // Both are `?.Clone()`, and a purely local write leaves the frontier null,
            // so the dominant path allocates nothing. Fixed under issue 1725.
            ["ApplyCrdtDeltaItem.SourceVectorClock"] = CopiedAtBothSeams,
            ["ApplyMergeItem.SourceVectorClock"] = CopiedAtBothSeams,
            ["EntryRevision.VectorClock"] = CopiedAtBothSeams,
            ["LatticeMutation.VectorClock"] = CopiedAtBothSeams,
            ["LwwEntry.VectorClock"] = CopiedAtBothSeams,
            ["PendingMutationSnapshot.VectorClock"] = CopiedAtBothSeams,
            ["WalRecord.DependencySummary"] = CopiedAtBothSeams,
            ["WalRecord.VectorClock"] = CopiedAtBothSeams,
        };
}
