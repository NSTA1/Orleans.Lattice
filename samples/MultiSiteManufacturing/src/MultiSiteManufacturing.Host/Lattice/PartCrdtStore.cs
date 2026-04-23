using MultiSiteManufacturing.Host.Domain;
using MultiSiteManufacturing.Host.Federation;
using Orleans.Lattice;

namespace MultiSiteManufacturing.Host.Lattice;

/// <summary>
/// CRDT-typed state for a part, backed directly by an
/// <see cref="ILattice"/> B+ tree (plan §M12b). Two sub-kinds are
/// exposed:
/// <list type="bullet">
///   <item>
///     <b>Current operator</b> — a last-writer-wins register. Stored
///     under the single key <c>{serial}/operator</c>. Concurrent writes
///     from silo A and silo B resolve to whichever write landed with
///     the higher <see cref="Orleans.Lattice.Primitives.HybridLogicalClock"/>
///     tick inside the lattice tree.
///   </item>
///   <item>
///     <b>Process labels</b> — a grow-only set (G-Set), one key per
///     label under the prefix <c>{serial}/labels/</c>. Union over that
///     prefix gives commutative, associative, idempotent merge — the
///     textbook G-Set CRDT.
///   </item>
/// </list>
/// This is the only surface in the sample where state is written
/// <i>without</i> routing through the fact log — the CRDT convergence
/// story is independent of the arrival-order-vs-HLC-fold story told by
/// the baseline and lattice fact backends.
///
/// <para>
/// <b>Partition awareness (plan §M12c).</b> When
/// <see cref="IPartitionChaosGrain"/> reports the simulated inter-silo
/// partition is active, every write from this silo is redirected to a
/// silo-local shadow prefix (<c>shadow/{siloId}/…</c>) instead of the
/// shared prefix. Reads always merge (shared ∪ local-silo-shadow), so
/// divergence is observable in the UI: during partition silo A sees
/// only its own recent writes, silo B sees only its own. On heal,
/// <see cref="PartitionHealHostedService"/> invokes
/// <see cref="HealLocalShadowAsync"/>, which promotes every shadow
/// entry into the shared prefix and deletes the shadow — LWW and G-Set
/// merge happen naturally via the tree's HLC and union semantics.
/// </para>
/// </summary>
public sealed class PartCrdtStore(IGrainFactory grainFactory, SiloIdentity silo)
{
    /// <summary>Lattice tree id that holds every part's CRDT state.</summary>
    public const string TreeId = "mfg-part-crdt";

    private const string OperatorSuffix = "/operator";
    private const string LabelsInfix = "/labels/";

    // Real serials are "HPT-…" (uppercase ASCII ≥ 'H'). "shadow/" begins
    // with lowercase 's' (0x73) so shadow keys sort strictly above every
    // serial-prefixed key and never collide with a serial-scoped range
    // scan.
    private const string ShadowPrefix = "shadow/";

    private static readonly byte[] LabelMarker = [1];

    private ILattice Tree => grainFactory.GetGrain<ILattice>(TreeId);

    private IPartitionChaosGrain Partition =>
        grainFactory.GetGrain<IPartitionChaosGrain>(IPartitionChaosGrain.SingletonKey);

    /// <summary>
    /// Assigns <paramref name="op"/> as the current operator for
    /// <paramref name="serial"/>. Under partition the write goes to
    /// this silo's shadow prefix; otherwise directly to the shared LWW
    /// key. Concurrent calls from multiple silos race — whichever
    /// lands last (by the lattice tree's internal HLC) wins.
    /// </summary>
    public async Task AssignOperatorAsync(PartSerialNumber serial, OperatorId op, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(serial.Value);
        ArgumentNullException.ThrowIfNull(op.Value);
        var key = await Partition.IsPartitionedAsync().ConfigureAwait(false)
            ? ShadowOperatorKey(silo.Id, serial)
            : SharedOperatorKey(serial);
        await Tree.SetAsync(key, op, cancellationToken).ConfigureAwait(false);
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
        var shadow = await Tree.GetAsync<OperatorId>(ShadowOperatorKey(silo.Id, serial), cancellationToken).ConfigureAwait(false);
        if (!string.IsNullOrEmpty(shadow.Value))
        {
            return shadow;
        }
        var shared = await Tree.GetAsync<OperatorId>(SharedOperatorKey(serial), cancellationToken).ConfigureAwait(false);
        return string.IsNullOrEmpty(shared.Value) ? null : shared;
    }

    /// <summary>
    /// Adds <paramref name="label"/> to the part's process-label G-Set.
    /// Idempotent: re-adding an existing label is a no-op. Throws
    /// <see cref="ArgumentException"/> if the label is empty or contains
    /// <c>'/'</c> (reserved as the sub-key separator). Under partition
    /// the write goes to this silo's shadow prefix.
    /// </summary>
    public async Task AddLabelAsync(PartSerialNumber serial, string label, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(label);
        if (label.Contains('/'))
        {
            throw new ArgumentException("Process labels may not contain '/'.", nameof(label));
        }
        var key = await Partition.IsPartitionedAsync().ConfigureAwait(false)
            ? ShadowLabelKey(silo.Id, serial, label)
            : SharedLabelKey(serial, label);
        await Tree.SetAsync(key, LabelMarker, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Returns the current process-label set for <paramref name="serial"/>
    /// in lexicographic order. Union of (shared prefix, this silo's
    /// shadow prefix) — so a silo always sees its own partition-era
    /// writes even before the heal promotes them.
    /// </summary>
    public async Task<IReadOnlyList<string>> GetLabelsAsync(PartSerialNumber serial, CancellationToken cancellationToken = default)
    {
        var labels = new SortedSet<string>(StringComparer.Ordinal);

        var (sharedStart, sharedEnd) = SharedLabelRange(serial);
        await foreach (var key in Tree.ScanKeysAsync(sharedStart, sharedEnd, cancellationToken: cancellationToken).ConfigureAwait(false))
        {
            labels.Add(key[sharedStart.Length..]);
        }

        var (shadowStart, shadowEnd) = ShadowLabelRange(silo.Id, serial);
        await foreach (var key in Tree.ScanKeysAsync(shadowStart, shadowEnd, cancellationToken: cancellationToken).ConfigureAwait(false))
        {
            labels.Add(key[shadowStart.Length..]);
        }

        return [.. labels];
    }

    /// <summary>
    /// Drains every shadow entry written by this silo during a partition
    /// into the shared CRDT prefix and deletes the shadow keys. Called
    /// by <see cref="PartitionHealHostedService"/> on the <c>true→false</c>
    /// transition of the partition flag. Safe to call when no shadow
    /// entries exist (returns 0). Returns the number of entries
    /// promoted.
    /// </summary>
    public async Task<int> HealLocalShadowAsync(CancellationToken cancellationToken = default)
    {
        var (start, end) = ShadowRange(silo.Id);
        var promoted = 0;

        // Materialise the shadow keyspace first — we're going to mutate
        // it (Set + Delete) during iteration, and resilient scans don't
        // guarantee stability under concurrent writes.
        var entries = new List<KeyValuePair<string, byte[]>>();
        await foreach (var entry in Tree.ScanEntriesAsync(start, end, cancellationToken: cancellationToken).ConfigureAwait(false))
        {
            entries.Add(entry);
        }

        foreach (var entry in entries)
        {
            var shared = PromoteShadowKey(entry.Key);
            if (shared is null)
            {
                continue;
            }
            await Tree.SetAsync(shared, entry.Value, cancellationToken).ConfigureAwait(false);
            await Tree.DeleteAsync(entry.Key, cancellationToken).ConfigureAwait(false);
            promoted++;
        }

        return promoted;
    }

    // ---------- key helpers ----------

    private static string SharedOperatorKey(PartSerialNumber serial) =>
        serial.Value + OperatorSuffix;

    private static string SharedLabelKey(PartSerialNumber serial, string label) =>
        serial.Value + LabelsInfix + label;

    private static string ShadowOperatorKey(string siloId, PartSerialNumber serial) =>
        ShadowPrefix + siloId + "/" + serial.Value + OperatorSuffix;

    private static string ShadowLabelKey(string siloId, PartSerialNumber serial, string label) =>
        ShadowPrefix + siloId + "/" + serial.Value + LabelsInfix + label;

    private static (string Start, string EndExclusive) SharedLabelRange(PartSerialNumber serial)
    {
        // '/' = 0x2F, '0' = 0x30 — so {serial}/labels0 is strictly greater
        // than every key of the form {serial}/labels/<x>.
        var prefix = serial.Value + LabelsInfix;
        var end = serial.Value + "/labels0";
        return (prefix, end);
    }

    private static (string Start, string EndExclusive) ShadowLabelRange(string siloId, PartSerialNumber serial)
    {
        var prefix = ShadowPrefix + siloId + "/" + serial.Value + LabelsInfix;
        var end = ShadowPrefix + siloId + "/" + serial.Value + "/labels0";
        return (prefix, end);
    }

    private static (string Start, string EndExclusive) ShadowRange(string siloId)
    {
        // shadow/{siloId}/… — end-exclusive uses '0' (0x30) which sorts
        // just above '/' (0x2F), matching the same trick as the label
        // range above.
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

