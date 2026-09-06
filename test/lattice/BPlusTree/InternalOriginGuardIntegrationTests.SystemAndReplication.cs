using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Regression coverage for the internal-origin assertion on the two internal
/// grain interfaces <c>LatticeGrain</c> implements beside the public
/// <see cref="ILattice"/> facade, and on the receiver-side cross-tree decision
/// coordinator.
/// <para>
/// The physical shard and leaf grains have carried the guard since issue #1103,
/// but <c>ISystemLattice</c>, <c>IReplicationApplyGrain</c>, and
/// <c>ILatticeCrossTreeReceiverGrain.NotifyTerminalAsync</c> did not, even though
/// all three are reachable by a direct external Orleans grain call. Orleans binds
/// a grain interface by its stable <c>[Alias]</c> string rather than by CLR
/// identity, so declaring these interfaces <c>internal</c> does not stop an
/// external client from binding a structurally matching interface to the same
/// grain. That is exactly why the shard and leaf grains guard rather than relying
/// on their own <c>internal</c> accessibility.
/// </para>
/// <para>
/// The impact of each gap differs, which is why all three are covered here:
/// <c>ISystemLattice</c> addresses the reserved system-tree namespace that the
/// public facade refuses (it holds the auth policy tree and the replication WAL),
/// <c>IReplicationApplyGrain</c> installs mutations while preserving an
/// attacker-chosen HLC and origin cluster, and the cross-tree receiver's
/// <c>NotifyTerminalAsync</c> carries the commit/abort verdict for a whole
/// cross-tree batch.
/// </para>
/// </summary>
public sealed partial class InternalOriginGuardIntegrationTests
{
    private const string SystemTreeId = "_lattice_origin_guard";

    private static HybridLogicalClock Hlc(long ticks) =>
        new() { WallClockTicks = ticks, Counter = 0 };

    // ISystemLattice: the internal read/write/delete/enumerate surface over the
    // reserved system-tree namespace. A direct external call is client-sourced,
    // so the capability-stripping filter stamps no internal-origin marker and the
    // guard must refuse it before the system-tree boundary is even entered.

    [Test]
    public void SystemLattice_SetAsync_direct_external_call_is_refused()
    {
        var sys = _cluster.GrainFactory.GetGrain<ISystemLattice>(SystemTreeId);

        Assert.ThrowsAsync<LatticeAuthorizationDeniedException>(
            async () => await sys.SetAsync("k", Val("k")));
    }

    [Test]
    public void SystemLattice_GetAsync_direct_external_call_is_refused()
    {
        var sys = _cluster.GrainFactory.GetGrain<ISystemLattice>(SystemTreeId);

        Assert.ThrowsAsync<LatticeAuthorizationDeniedException>(
            async () => await sys.GetAsync("k"));
    }

    [Test]
    public void SystemLattice_DeleteAsync_direct_external_call_is_refused()
    {
        var sys = _cluster.GrainFactory.GetGrain<ISystemLattice>(SystemTreeId);

        Assert.ThrowsAsync<LatticeAuthorizationDeniedException>(
            async () => await sys.DeleteAsync("k"));
    }

    [Test]
    public void SystemLattice_ExistsAsync_direct_external_call_is_refused()
    {
        var sys = _cluster.GrainFactory.GetGrain<ISystemLattice>(SystemTreeId);

        Assert.ThrowsAsync<LatticeAuthorizationDeniedException>(
            async () => await sys.ExistsAsync("k"));
    }

    // The two streaming members return IAsyncEnumerable by delegating to a private
    // iterator. The guard is asserted on the call itself rather than inside the
    // iterator body, so it cannot be skipped by a caller that never enumerates.

    [Test]
    public void SystemLattice_KeysAsync_direct_external_call_is_refused()
    {
        var sys = _cluster.GrainFactory.GetGrain<ISystemLattice>(SystemTreeId);

        Assert.ThrowsAsync<LatticeAuthorizationDeniedException>(async () =>
        {
            await foreach (var _ in sys.KeysAsync())
            {
                // The guard refuses before the first element is produced.
            }
        });
    }

    [Test]
    public void SystemLattice_EntriesAsync_direct_external_call_is_refused()
    {
        var sys = _cluster.GrainFactory.GetGrain<ISystemLattice>(SystemTreeId);

        Assert.ThrowsAsync<LatticeAuthorizationDeniedException>(async () =>
        {
            await foreach (var _ in sys.EntriesAsync())
            {
                // The guard refuses before the first element is produced.
            }
        });
    }

    // IReplicationApplyGrain: installs a remote mutation while preserving the
    // authoring cluster's HLC and origin verbatim, so it deliberately bypasses
    // both the access gate and local conflict resolution. Reaching it externally
    // would let a client forge replicated writes attributed to another cluster.

    [Test]
    public void ReplicationApply_ApplySetAsync_direct_external_call_is_refused()
    {
        var apply = _cluster.GrainFactory.GetGrain<IReplicationApplyGrain>("origin-guard-apply-set");

        Assert.ThrowsAsync<LatticeAuthorizationDeniedException>(
            async () => await apply.ApplySetAsync("k", Val("k"), Hlc(1), "forged-cluster", null, 0));
    }

    [Test]
    public void ReplicationApply_ApplyCrdtDeltaWithExpiryAsync_direct_external_call_is_refused()
    {
        var apply = _cluster.GrainFactory.GetGrain<IReplicationApplyGrain>("origin-guard-apply-crdt");

        Assert.ThrowsAsync<LatticeAuthorizationDeniedException>(
            async () => await apply.ApplyCrdtDeltaWithExpiryAsync(
                "k", LatticeMergeMode.LwwRegister, Val("k"), 0));
    }

    // The receiver-side cross-tree coordinator. NotifyTerminalAsync carries a
    // caller-supplied commit/abort verdict and, on the first terminal, freezes the
    // wait set, origin cluster, and operation id. An unguarded external call could
    // therefore both poison a barrier that has not started and force a premature
    // verdict on one in flight, breaking the all-or-nothing cross-tree visibility
    // this coordinator exists to enforce.

    [Test]
    public void CrossTreeReceiver_NotifyTerminalAsync_direct_external_call_is_refused()
    {
        var key = LatticeCrossTreeReceiverGrain.ComputeKey("forged-cluster", "op-origin-guard");
        var receiver = _cluster.GrainFactory.GetGrain<ILatticeCrossTreeReceiverGrain>(key);
        var terminal = new CrossTreeReceiverTerminal
        {
            OriginClusterId = "forged-cluster",
            OperationId = "op-origin-guard",
            TreeId = "origin-guard-cross-tree",
            TransactionId = Guid.NewGuid(),
            Committed = false,
            WaitSet = ["origin-guard-cross-tree"],
            ObservedSourceShards = [0],
            TerminalHlc = Hlc(1),
        };

        Assert.ThrowsAsync<LatticeAuthorizationDeniedException>(
            async () => await receiver.NotifyTerminalAsync(terminal));
    }
}
