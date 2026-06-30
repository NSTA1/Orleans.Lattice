using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Internal silo-scoped seam by which a <see cref="BPlusLeafGrain"/> reports
/// the highest <see cref="HybridLogicalClock"/> its materialiser projection
/// has durably applied so the per-shard write-ahead-log GC can pin its
/// trim point under the slowest local consumer.
/// <para>
/// The core <c>Orleans.Lattice</c> assembly registers a lightweight
/// <see cref="InMemoryLeafCursorReporter"/> by default (via
/// <c>AddLattice</c>), so every host reports leaf cursors into the
/// always-on in-memory <c>IWalCursorRegistry</c> and the WAL saturation
/// sampler's materialiser drain-lag back-pressure is live for every write
/// workload out of the box. That default does only the cheap in-memory
/// work and treats every durable-pin method below as a no-op. The durable
/// cross-restart trim-floor backstop (the sharded cluster-wide
/// <see cref="IWalMaterialiserPinGrain"/> store) is the opt-in layer:
/// <c>AddWalCursorRegistry</c> (called directly, or transitively by the
/// WAL GC / views / replication / Azure-table storage packages) replaces
/// the lightweight default with the durable-pin-aware
/// <see cref="LeafCursorReporter"/>. The implementation is resolved from
/// <see cref="IGrainContext.ActivationServices"/> via
/// <see cref="Microsoft.Extensions.DependencyInjection.ServiceProviderServiceExtensions.GetService{T}"/>;
/// a host that has somehow not registered any reporter returns
/// <see langword="null"/> and the leaf grain becomes a no-op on the
/// cursor-report path.
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

    /// <summary>
    /// Publishes the leaf's durable checkpoint <paramref name="frontier"/> to
    /// the cluster-wide durable pin store
    /// (<see cref="IWalMaterialiserPinGrain"/>) so the WAL GC's trim floor
    /// survives a full silo/cluster restart that wipes the process-local
    /// <see cref="IWalCursorRegistry"/>.
    /// <para>
    /// Deliberately fire-and-forget and coalesced: the call returns
    /// immediately without awaiting the durable write so the leaf's
    /// foreground/checkpoint path takes on no synchronous storage latency.
    /// A durable pin that lags the leaf's true frontier is always GC-safe
    /// (it only retains more WAL), so the implementation debounces writes
    /// and swallows transient failures. The leaf calls this on activation
    /// (seeding a <see cref="HybridLogicalClock.Zero"/> "block" pin for a
    /// leaf that has never checkpointed) and after each checkpoint flush.
    /// Implementations with no durable backing (no grain factory, pre-WAL
    /// hosts) treat this as a no-op.
    /// </para>
    /// </summary>
    /// <param name="treeName">Logical tree id whose leaf is reporting.</param>
    /// <param name="consumerId">Stable leaf-materialiser consumer id (the same id reported to the in-memory registry).</param>
    /// <param name="frontier">The leaf's durable checkpoint frontier; <see cref="HybridLogicalClock.Zero"/> seeds a never-checkpointed block pin.</param>
    void NoteDurableMaterialiserFrontier(
        string treeName,
        string consumerId,
        HybridLogicalClock frontier);

    /// <summary>
    /// Durably seeds a leaf-materialiser <paramref name="frontier"/> pin
    /// (<see cref="IWalMaterialiserPinGrain"/>) and <b>awaits</b> the write,
    /// unlike the fire-and-forget <see cref="NoteDurableMaterialiserFrontier"/>.
    /// A leaf calls this at <i>birth</i> - the moment it first acquires a tree
    /// id and becomes capable of holding data (a split sibling's
    /// <c>InitializeSiblingAsync</c> or a root/bulk-load leaf's
    /// <c>SetTreeIdAsync</c>) - to plant a
    /// <see cref="HybridLogicalClock.Zero"/> "block" pin <em>before</em> any
    /// inherited or routed write becomes reachable in the WAL. Awaiting closes
    /// the window the fire-and-forget mirror leaves open: a forward trim driver
    /// (replication shipper, materialised view, or the wall-clock TTL ceiling)
    /// could otherwise advance the WAL GC floor past the new leaf's
    /// un-materialised frontier before the debounced durable write lands,
    /// trimming committed-but-not-yet-checkpointed data the leaf still needs to
    /// replay. The monotonic-max merge in the pin store makes a Zero seed a
    /// no-op once the leaf has reported a real frontier, so the call is
    /// idempotent and safe to re-issue on a recovery-path re-call. Transient
    /// durable-write failures are swallowed (logged) so the foreground
    /// birth/create path is never blocked; a missed seed only narrows the
    /// protection window and the next checkpoint flush re-seeds. Implementations
    /// with no durable backing (no grain factory, pre-WAL hosts) treat this as a
    /// no-op.
    /// </summary>
    /// <param name="treeName">Logical tree id whose leaf is being seeded.</param>
    /// <param name="consumerId">Stable leaf-materialiser consumer id (the same id later reported to the in-memory registry).</param>
    /// <param name="frontier">The block frontier to seed; <see cref="HybridLogicalClock.Zero"/> for a leaf whose entire data range is still un-materialised.</param>
    /// <param name="cancellationToken">Cancellation token observed before the durable write.</param>
    Task SeedDurableMaterialiserBlockAsync(
        string treeName,
        string consumerId,
        HybridLogicalClock frontier,
        CancellationToken cancellationToken);

    /// <summary>
    /// Batched form of <see cref="SeedDurableMaterialiserBlockAsync"/>: durably
    /// seeds every pin in <paramref name="reports"/> for
    /// <paramref name="treeName"/> and <b>awaits</b> the writes. A leaf at birth
    /// calls this once with one report per WAL partition instead of issuing one
    /// awaited seed per partition; the implementation groups the reports by their
    /// routed durable-pin shard and issues a single batched durable write per
    /// distinct shard concurrently, collapsing what was
    /// <c>O(partitions)</c> serialized round-trips through one hot grain into
    /// at most <c>O(shards)</c> concurrent batched writes. Same idempotency,
    /// crash-safety, and failure-swallowing contract as the single-pin seed:
    /// the block pins must be durable before the caller lets the new leaf's data
    /// become reachable in the WAL, a Zero seed is a no-op once a real frontier
    /// has landed, and transient durable-write failures are swallowed so the
    /// foreground birth path is never blocked. A no-op when the host has no
    /// durable backing.
    /// </summary>
    /// <param name="treeName">Logical tree id whose leaf is being seeded.</param>
    /// <param name="reports">One block pin per WAL partition; each report's consumer id must not be <see langword="null"/> or whitespace.</param>
    /// <param name="cancellationToken">Cancellation token observed before the durable writes.</param>
    Task SeedDurableMaterialiserBlockManyAsync(
        string treeName,
        IReadOnlyList<MaterialiserPinReport> reports,
        CancellationToken cancellationToken);
}