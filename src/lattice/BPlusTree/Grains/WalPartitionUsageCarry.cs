using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Per-activation memory of each WAL partition's last measured byte figures,
/// so a partition whose retained-byte scan was refused or failed reports its
/// previous figure instead of nothing (issue #6017). Held by
/// <see cref="LatticeWalUsageGrain"/> and <see cref="LatticeStorageUsageGrain"/>;
/// in memory only and never persisted, so a reactivation starts empty.
/// <para>
/// <b>Why carry rather than flag.</b> A partition that does not answer used to
/// contribute nothing and mark the whole report partial. Several consumers read
/// a partial report's lower bound as the real figure - write admission,
/// cross-tree admission, tenant metering and the scaling pressure signal - so a
/// refused scan, which says nothing about the bytes on disk, silently lowered
/// what they saw. The bytes a partition held a moment ago are a far better
/// estimate than zero.
/// </para>
/// <para>
/// <b>Bounded age.</b> A carried figure is used for at most
/// <see cref="MaxCarryAge"/> after it was measured. Past that the partition is
/// reported unavailable again and the report is partial, as before: a figure
/// that old is no longer an estimate an operator would accept for a quota or
/// a capacity decision.
/// </para>
/// <para>
/// <b>Identity.</b> A carried figure is only valid for the WAL it was measured
/// on. The tree identity is the physical tree id plus the registry lineage,
/// which a purge and recreate (and any content swap) replaces; the partition
/// identity is the WAL placement pin version plus the provider key backing that
/// partition, which a placement move changes. Any change discards the carried
/// figure, so a recreated tree never inherits its predecessor's bytes and a
/// moved partition never reports its old backend's bytes.
/// </para>
/// </summary>
internal sealed class WalPartitionUsageCarry
{
    /// <summary>
    /// The oldest a carried partition figure may be and still be reported.
    /// Five minutes is twenty polls at the default 15 second WAL-usage poll
    /// interval, enough to ride out a burst of refused scans on a busy silo,
    /// and short enough that a quota decision or a capacity reading is never
    /// made on a figure that predates a sustained write burst by more than a
    /// few minutes.
    /// </summary>
    internal static readonly TimeSpan MaxCarryAge = TimeSpan.FromMinutes(5);

    private readonly Dictionary<int, Entry> _last = new();
    private string? _treeIdentity;

    /// <summary>
    /// Starts a sample for a tree identity. Discards every carried figure when
    /// the identity changed since the last sample, and any partition at or
    /// past <paramref name="partitionCount"/>.
    /// </summary>
    /// <param name="treeIdentity">The identity from <see cref="TreeIdentity"/>.</param>
    /// <param name="partitionCount">The tree's WAL partition count.</param>
    internal void Begin(string treeIdentity, int partitionCount)
    {
        if (!string.Equals(_treeIdentity, treeIdentity, StringComparison.Ordinal))
        {
            _last.Clear();
            _treeIdentity = treeIdentity;
        }

        if (_last.Count == 0)
        {
            return;
        }

        foreach (var partition in _last.Keys.Where(p => p >= partitionCount).ToList())
        {
            _last.Remove(partition);
        }
    }

    /// <summary>
    /// Resolves one partition's figures for this sample. A measured retained
    /// figure (zero or more) is recorded and returned as is. An unavailable one
    /// (<c>-1</c>) is replaced by the partition's last measured figures when
    /// they were measured under the same <paramref name="partitionIdentity"/>
    /// no more than <see cref="MaxCarryAge"/> ago; otherwise it stays
    /// <c>-1</c>.
    /// </summary>
    /// <param name="partition">The WAL partition index.</param>
    /// <param name="partitionIdentity">The identity from <see cref="PartitionIdentity"/>.</param>
    /// <param name="retained">The measured retained bytes, or <c>-1</c>.</param>
    /// <param name="physical">The measured physical bytes, or <c>-1</c>.</param>
    /// <param name="now">The sample time.</param>
    /// <returns>
    /// The figures to report, and the time the carried figure was measured when
    /// one was used.
    /// </returns>
    internal (long Retained, long Physical, DateTimeOffset? CarriedFrom) Resolve(
        int partition, string partitionIdentity, long retained, long physical, DateTimeOffset now)
    {
        if (retained >= 0)
        {
            _last[partition] = new Entry(partitionIdentity, retained, physical >= 0 ? physical : retained, now);
            return (retained, physical, null);
        }

        if (_last.TryGetValue(partition, out var previous))
        {
            if (string.Equals(previous.PartitionIdentity, partitionIdentity, StringComparison.Ordinal)
                && now - previous.MeasuredAt <= MaxCarryAge)
            {
                return (previous.Retained, physical >= 0 ? physical : previous.Physical, previous.MeasuredAt);
            }

            if (!string.Equals(previous.PartitionIdentity, partitionIdentity, StringComparison.Ordinal))
            {
                _last.Remove(partition);
            }
        }

        return (-1L, physical, null);
    }

    /// <summary>
    /// The tree half of the carry identity: the physical tree id and the
    /// registry lineage, which a purge and recreate replaces.
    /// </summary>
    /// <param name="physicalTreeId">The physical tree id whose WAL is measured.</param>
    /// <param name="entry">The tree's registry entry, when one was read.</param>
    internal static string TreeIdentity(string physicalTreeId, TreeRegistryEntry? entry)
        => $"{physicalTreeId}|{entry?.Lineage?.ToString("N") ?? "-"}";

    /// <summary>
    /// The partition half of the carry identity: the placement pin version and
    /// the provider key backing <paramref name="partition"/>, which a placement
    /// move changes. A pin that could not be read yields an identity no
    /// readable pin matches, so nothing is carried across it.
    /// </summary>
    /// <param name="pin">The physical tree's WAL placement pin, or <see langword="null"/> when unread.</param>
    /// <param name="partition">The WAL partition index.</param>
    internal static string PartitionIdentity(WalPlacementPin? pin, int partition)
        => pin is null ? "?" : $"{pin.Version}|{pin.ResolveKey(partition)}";

    /// <summary>
    /// Reads the WAL placement pin of the physical tree being measured. The
    /// logical entry already carries it when the tree is not aliased, so only an
    /// aliased tree costs a registry read. Returns <see langword="null"/> when
    /// the pin cannot be read, which disables carrying for this sample.
    /// </summary>
    /// <param name="optionsResolver">The options resolver.</param>
    /// <param name="treeId">The addressed tree id.</param>
    /// <param name="physicalTreeId">The physical tree id whose WAL is measured.</param>
    /// <param name="entry">The addressed tree's registry entry, when one was read.</param>
    internal static async Task<WalPlacementPin?> ReadPinAsync(
        LatticeOptionsResolver optionsResolver, string treeId, string physicalTreeId, TreeRegistryEntry? entry)
    {
        if (string.Equals(treeId, physicalTreeId, StringComparison.Ordinal))
        {
            return entry?.WalPlacement ?? WalPlacementPin.Create();
        }

        try
        {
            return await optionsResolver.GetWalPlacementSnapshotAsync(physicalTreeId);
        }
        catch (Exception)
        {
            return null;
        }
    }

    private readonly record struct Entry(string PartitionIdentity, long Retained, long Physical, DateTimeOffset MeasuredAt);
}
