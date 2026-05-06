using System.Text;
using MultiSiteManufacturing.Host.Domain;
using MultiSiteManufacturing.Host.Federation;
using Orleans.Lattice;

namespace MultiSiteManufacturing.Host.Lattice;

/// <summary>
/// CRDT-typed state for a part, backed by two <see cref="ILattice"/>
/// B+ trees — one per CRDT type:
/// <list type="bullet">
///   <item>
///     <b>Current operator</b> — a last-writer-wins register stored in
///     the <c>mfg-part-operator</c> tree, one key per serial. Concurrent
///     writes from silo A and silo B resolve to whichever write landed
///     with the higher
///     <see cref="Orleans.Lattice.Primitives.HybridLogicalClock"/> tick.
///     Cluster-local: the operator tree is intentionally not opted into
///     replication (LWW across clusters with disjoint HLCs is meaningless).
///   </item>
///   <item>
///     <b>Process labels</b> — an observed-remove (OR) set CRDT stored in
///     the <c>mfg-part-labels</c> tree, one OrSet per serial, accessed
///     through the package's
///     <see cref="OrSetAccessor"/>. Cross-cluster replicated as
///     <see cref="Orleans.Lattice.Replication.ReplicationMode.OrSet"/> so
///     the package ships typed CRDT deltas (add / remove / merge) rather
///     than raw byte writes.
///   </item>
/// </list>
/// This is the only surface in the sample where state is written
/// <i>without</i> routing through the fact log — the CRDT convergence
/// story is independent of the arrival-order-vs-HLC-fold story told by
/// the baseline and lattice fact backends.
///
/// <para>
/// <b>Partition awareness.</b> When
/// <see cref="IPartitionChaosGrain"/> reports the simulated inter-silo
/// partition is active, every write from this silo is redirected to a
/// silo-local shadow key (<c>shadow/{siloId}/{serial}</c>) instead of
/// the shared key (<c>{serial}</c>) — in both trees. Reads always merge
/// (shared ∪ local-silo-shadow), so divergence is observable in the UI:
/// during partition silo A sees only its own recent writes, silo B sees
/// only its own. On heal,
/// <see cref="PartitionHealHostedService"/> invokes
/// <see cref="HealLocalShadowAsync"/>, which promotes every shadow
/// entry into its shared counterpart and deletes the shadow — for
/// labels via the OrSet's natural merge, for the operator via a plain
/// shared-key Set (LWW resolution by the tree's HLC).
/// </para>
/// </summary>
public sealed class PartCrdtStore(IGrainFactory grainFactory, SiloIdentity silo)
{
    /// <summary>Lattice tree id that holds every part's current-operator LWW register.</summary>
    public const string OperatorTreeId = "mfg-part-operator";

    /// <summary>Lattice tree id that holds every part's process-label OR-Set.</summary>
    public const string LabelsTreeId = "mfg-part-labels";

    // Real serials are "HPT-…" (uppercase ASCII ≥ 'H'). "shadow/" begins
    // with lowercase 's' (0x73) so shadow keys sort strictly above every
    // serial-prefixed key and never collide with a serial-scoped range
    // scan.
    private const string ShadowPrefix = "shadow/";

    private ILattice OperatorTree => grainFactory.GetGrain<ILattice>(OperatorTreeId);

    private ILattice LabelsTree => grainFactory.GetGrain<ILattice>(LabelsTreeId);

    private IPartitionChaosGrain Partition =>
        grainFactory.GetGrain<IPartitionChaosGrain>(IPartitionChaosGrain.SingletonKey);

    // Globally-unique replica id for OR-Set causal dots. Combining the
    // cluster name with the silo id ensures dots from "us:a" never
    // collide with dots from "eu:a" once the OrSet ships through
    // package replication.
    private string ReplicaId => silo.ClusterName + ":" + silo.Id;

    /// <summary>
    /// Raised after every successful CRDT mutation that may have
    /// changed what <see cref="GetOperatorAsync"/> or
    /// <see cref="GetLabelsAsync"/> would return for the carried
    /// serial. Subscribed by <c>DashboardBroadcaster</c> so the
    /// part-detail page's CRDT card refreshes live without the user
    /// reloading the browser. Also raised on the receiver side of
    /// cross-cluster <c>mfg-part-labels</c> applies (see
    /// <c>BaselineReplicationApplier</c>) so OR-Set deltas arriving
    /// from a peer cluster light up the local UI immediately.
    ///
    /// <para>
    /// Handler exceptions are swallowed - a single bad subscriber
    /// must never propagate back into the mutation path or the
    /// receiver-side apply pipeline (which would surface as a 500
    /// on the inbound gRPC <c>Push</c> RPC and stall replication).
    /// Mirrors <see cref="FederationRouter.RaiseFactReplicated"/>'s
    /// fault-isolation contract.
    /// </para>
    ///
    /// <para>
    /// <b>Coverage scope.</b> The event reaches every Blazor circuit
    /// pinned to the same silo as the mutation, plus every circuit on
    /// the peer cluster (because OR-Set label deltas replicate). It
    /// does <b>not</b> currently reach circuits pinned to a different
    /// silo within the same cluster - that would require a second
    /// cluster-wide Orleans stream typed for CRDT notices, and is a
    /// deferred enhancement. The sample's main demo case (one browser
    /// per cluster, sticky cookie) is fully covered.
    /// </para>
    /// </summary>
    public event Action<PartSerialNumber>? PartChanged;

    /// <summary>
    /// Fires <see cref="PartChanged"/> for <paramref name="serial"/>,
    /// swallowing any handler exceptions. Exposed as <c>internal</c>
    /// so <c>BaselineReplicationApplier</c> can raise it from the
    /// cross-cluster receive path - the OR-Set merge that fans the
    /// label delta into the local labels tree happens inside the
    /// package's apply pipeline (which we don't decorate at the
    /// per-key level), so the store's mutation methods aren't on
    /// that path and we have to fan out explicitly.
    /// </summary>
    internal void RaisePartChanged(PartSerialNumber serial)
    {
        var handler = PartChanged;
        if (handler is null)
        {
            return;
        }
        try
        {
            handler(serial);
        }
        catch
        {
            // Subscribers own their own error reporting; the store
            // must not leak handler faults back into the caller.
        }
    }

    /// <summary>
    /// Assigns <paramref name="op"/> as the current operator for
    /// <paramref name="serial"/>. Under partition the write goes to
    /// this silo's shadow key in the operator tree; otherwise directly
    /// to the shared key. Concurrent calls from multiple silos race —
    /// whichever lands last (by the lattice tree's internal HLC) wins.
    /// </summary>
    public async Task AssignOperatorAsync(PartSerialNumber serial, OperatorId op, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(serial.Value);
        ArgumentNullException.ThrowIfNull(op.Value);
        var key = await Partition.IsPartitionedAsync().ConfigureAwait(false)
            ? ShadowKey(silo.Id, serial)
            : SharedKey(serial);
        await OperatorTree.SetAsync(key, op, cancellationToken).ConfigureAwait(false);
        RaisePartChanged(serial);
    }

    /// <summary>
    /// Returns the current operator for <paramref name="serial"/>, or
    /// <c>null</c> if no operator has ever been assigned. Always merges
    /// (shared, local-silo shadow) — during partition this surfaces
    /// the silo's own recent writes; after heal the shared key alone
    /// is authoritative.
    /// </summary>
    public async Task<OperatorId?> GetOperatorAsync(PartSerialNumber serial, CancellationToken cancellationToken = default)
    {
        var shadow = await OperatorTree.GetAsync<OperatorId>(ShadowKey(silo.Id, serial), cancellationToken).ConfigureAwait(false);
        if (!string.IsNullOrEmpty(shadow.Value))
        {
            return shadow;
        }
        var shared = await OperatorTree.GetAsync<OperatorId>(SharedKey(serial), cancellationToken).ConfigureAwait(false);
        return string.IsNullOrEmpty(shared.Value) ? null : shared;
    }

    /// <summary>
    /// Adds <paramref name="label"/> to the part's process-label OR-Set
    /// via <see cref="OrSetAccessor.AddAsync"/>. Idempotent at the set
    /// level — re-adding the same label produces a fresh causal dot but
    /// the live element-set membership is unchanged. Throws
    /// <see cref="ArgumentException"/> if the label is empty or contains
    /// <c>'/'</c> (reserved as the sub-key separator in the shadow key
    /// scheme). Under partition the write goes to this silo's shadow
    /// key in the labels tree.
    /// </summary>
    public async Task AddLabelAsync(PartSerialNumber serial, string label, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(label);
        if (label.Contains('/'))
        {
            throw new ArgumentException("Process labels may not contain '/'.", nameof(label));
        }
        var key = await Partition.IsPartitionedAsync().ConfigureAwait(false)
            ? ShadowKey(silo.Id, serial)
            : SharedKey(serial);
        var bytes = Encoding.UTF8.GetBytes(label);
        await LabelsTree.OrSet(key).AddAsync(bytes, ReplicaId, cancellationToken).ConfigureAwait(false);
        RaisePartChanged(serial);
    }

    /// <summary>
    /// Returns the current process-label set for <paramref name="serial"/>
    /// in lexicographic order. Union of (shared OR-Set, this silo's
    /// shadow OR-Set) — so a silo always sees its own partition-era
    /// writes even before the heal promotes them.
    /// </summary>
    public async Task<IReadOnlyList<string>> GetLabelsAsync(PartSerialNumber serial, CancellationToken cancellationToken = default)
    {
        var labels = new SortedSet<string>(StringComparer.Ordinal);

        var shared = await LabelsTree.OrSet(SharedKey(serial)).GetAsync(cancellationToken).ConfigureAwait(false);
        foreach (var element in shared.Elements())
        {
            labels.Add(Encoding.UTF8.GetString(element));
        }

        var shadow = await LabelsTree.OrSet(ShadowKey(silo.Id, serial)).GetAsync(cancellationToken).ConfigureAwait(false);
        foreach (var element in shadow.Elements())
        {
            labels.Add(Encoding.UTF8.GetString(element));
        }

        return [.. labels];
    }

    /// <summary>
    /// Drains every shadow entry written by this silo during a partition
    /// into the shared CRDT state and deletes the shadow keys. Called
    /// by <see cref="PartitionHealHostedService"/> on the <c>true→false</c>
    /// transition of the partition flag. Safe to call when no shadow
    /// entries exist (returns 0). Returns the total number of entries
    /// promoted across both trees.
    ///
    /// <para>
    /// Operator entries promote via a plain
    /// <see cref="ILattice.SetAsync(string, byte[], CancellationToken)"/>
    /// of the shadow value to the shared key — LWW resolution by the
    /// tree's HLC. Label entries promote via
    /// <see cref="OrSetAccessor.MergeAsync"/> of the shadow OR-Set into
    /// the shared OR-Set — preserving every causal dot from both sides
    /// and yielding deterministic union semantics.
    /// </para>
    /// </summary>
    public async Task<int> HealLocalShadowAsync(CancellationToken cancellationToken = default)
    {
        var (start, end) = ShadowRange(silo.Id);
        var promoted = 0;
        // Track every distinct serial we touched so we can raise
        // PartChanged exactly once per serial at the end - regardless
        // of whether it was the operator tree, the labels tree, or
        // both that promoted entries for that serial.
        var touchedSerials = new HashSet<string>(StringComparer.Ordinal);

        // ---- Operator tree: byte-for-byte promote shadow → shared. ----
        // Materialise the shadow keyspace first — we're going to mutate
        // it (Set + Delete) during iteration, and resilient scans don't
        // guarantee stability under concurrent writes.
        var operatorEntries = new List<KeyValuePair<string, byte[]>>();
        await foreach (var entry in OperatorTree.ScanEntriesAsync(start, end, cancellationToken: cancellationToken).ConfigureAwait(false))
        {
            operatorEntries.Add(entry);
        }

        foreach (var entry in operatorEntries)
        {
            var shared = PromoteShadowKey(entry.Key);
            if (shared is null)
            {
                continue;
            }
            await OperatorTree.SetAsync(shared, entry.Value, cancellationToken).ConfigureAwait(false);
            await OperatorTree.DeleteAsync(entry.Key, cancellationToken).ConfigureAwait(false);
            promoted++;
            touchedSerials.Add(shared);
        }

        // ---- Labels tree: OR-Set merge for every shadow key. ----
        var labelShadowKeys = new List<string>();
        await foreach (var key in LabelsTree.ScanKeysAsync(start, end, cancellationToken: cancellationToken).ConfigureAwait(false))
        {
            labelShadowKeys.Add(key);
        }
        foreach (var shadowKey in labelShadowKeys)
        {
            var sharedKey = PromoteShadowKey(shadowKey);
            if (sharedKey is null)
            {
                continue;
            }
            var shadowSet = await LabelsTree.OrSet(shadowKey).GetAsync(cancellationToken).ConfigureAwait(false);
            await LabelsTree.OrSet(sharedKey).MergeAsync(shadowSet, cancellationToken).ConfigureAwait(false);
            await LabelsTree.DeleteAsync(shadowKey, cancellationToken).ConfigureAwait(false);
            promoted++;
            touchedSerials.Add(sharedKey);
        }

        foreach (var serial in touchedSerials)
        {
            RaisePartChanged(new PartSerialNumber(serial));
        }

        return promoted;
    }

    // ---------- key helpers ----------

    private static string SharedKey(PartSerialNumber serial) => serial.Value;

    private static string ShadowKey(string siloId, PartSerialNumber serial) =>
        ShadowPrefix + siloId + "/" + serial.Value;

    private static (string Start, string EndExclusive) ShadowRange(string siloId)
    {
        // shadow/{siloId}/… — end-exclusive uses '0' (0x30) which sorts
        // just above '/' (0x2F).
        var prefix = ShadowPrefix + siloId + "/";
        var end = ShadowPrefix + siloId + "0";
        return (prefix, end);
    }

    /// <summary>
    /// Strips the <c>shadow/{siloId}/</c> prefix from a shadow key to
    /// produce its shared-prefix counterpart. Returns <c>null</c> if
    /// the input does not start with <c>shadow/{siloId}/</c>.
    /// </summary>
    private string? PromoteShadowKey(string shadowKey)
    {
        var expected = ShadowPrefix + silo.Id + "/";
        return shadowKey.StartsWith(expected, StringComparison.Ordinal)
            ? shadowKey[expected.Length..]
            : null;
    }
}

