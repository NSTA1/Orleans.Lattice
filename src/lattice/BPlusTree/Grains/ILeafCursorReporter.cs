using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Internal silo-scoped seam by which a <see cref="BPlusLeafGrain"/> reports
/// the highest <see cref="HybridLogicalClock"/> its materialiser projection
/// has durably applied so the per-shard write-ahead-log GC can pin its
/// trim point under the slowest local consumer.
/// <para>
/// The core <c>Orleans.Lattice</c> assembly intentionally does not depend
/// on <c>Orleans.Lattice.Replication</c> (the dependency direction is the
/// other way around). This interface is the bridge: the replication
/// package registers an adapter that forwards to its
/// <c>IWalCursorRegistry</c>, so a host that has not added
/// replication leaves the registration absent and the leaf grain becomes
/// a no-op on the cursor-report path. The implementation is resolved
/// from <see cref="IGrainContext.ActivationServices"/> via
/// <see cref="Microsoft.Extensions.DependencyInjection.ServiceProviderServiceExtensions.GetService{T}"/>
/// - a missing registration returns <see langword="null"/> and is
/// expected.
/// </para>
/// </summary>
internal interface ILeafCursorReporter
{
    /// <summary>
    /// Reserved consumer-id prefix for leaf-as-materialiser cursors.
    /// The full consumer id has the form
    /// <c>{MaterialiserConsumerIdPrefix}{treeName}_{leafGrainId}</c>;
    /// the <see cref="UnregisterTreeAsync"/> bulk path uses
    /// <c>{MaterialiserConsumerIdPrefix}{treeName}_</c> as a filter so
    /// only leaf-materialiser cursors are removed and any peer or
    /// custom consumer that happens to be registered against the tree
    /// is left alone.
    /// </summary>
    public const string MaterialiserConsumerIdPrefix = "_lattice_materialiser_";

    /// <summary>
    /// Reports the leaf's highest applied HLC for <paramref name="treeName"/>
    /// under the stable <paramref name="consumerId"/>. Implementations
    /// must be idempotent and monotonic - a report whose
    /// <paramref name="cursor"/> is below a previously-reported cursor for
    /// the same <c>(treeName, consumerId)</c> pair is silently coalesced
    /// rather than rolling backwards. The leaf grain only invokes this
    /// helper when <paramref name="cursor"/> is strictly greater than
    /// <see cref="HybridLogicalClock.Zero"/>.
    /// </summary>
    Task ReportAsync(
        string treeName,
        string consumerId,
        HybridLogicalClock cursor,
        CancellationToken cancellationToken);

    /// <summary>
    /// Removes <paramref name="consumerId"/>'s registration for
    /// <paramref name="treeName"/>. Idempotent: unregistering a consumer
    /// that has not been registered (or was already unregistered) is a
    /// no-op. Reserved for terminal lifecycle events (tree deletion,
    /// leaf eviction during a purge) - routine deactivation must not
    /// deregister, otherwise the WAL GC could trim entries the next
    /// activation needs to replay.
    /// </summary>
    Task UnregisterAsync(
        string treeName,
        string consumerId,
        CancellationToken cancellationToken);

    /// <summary>
    /// Bulk-removes every leaf-materialiser cursor registered for
    /// <paramref name="treeName"/> (every consumer id matching
    /// <see cref="MaterialiserConsumerIdPrefix"/><c>{treeName}_*</c>).
    /// Called from terminal tree-lifecycle events (tree deletion /
    /// purge) so the per-shard WAL GC is no longer pinned by stale
    /// leaf cursors after the tree's data has been removed. Idempotent:
    /// a tree with no registered materiliser cursors is a no-op. Peer
    /// or custom consumers registered against the tree under non-
    /// materialiser consumer ids are left alone.
    /// </summary>
    /// <param name="treeName">Logical tree id whose leaf-materialiser cursors are being cleared. Must not be <see langword="null"/> or whitespace.</param>
    /// <param name="cancellationToken">Cancellation token observed before any state mutation.</param>
    Task UnregisterTreeAsync(
        string treeName,
        CancellationToken cancellationToken);
}