using Microsoft.Extensions.Logging.Abstractions;
using Orleans.Lattice.Replication;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Shared builders for the runtime replication-configuration snapshot tests:
/// per-tree <see cref="LatticeReplicationConfigEntry"/> factories and a warmed
/// <see cref="CompiledReplicationConfigSnapshotMaintainer"/> over an in-memory
/// entry set, so the dynamic-seam tests can drive a known snapshot with no
/// cluster.
/// </summary>
internal static class ReplicationConfigSnapshotTestHelpers
{
    public static LatticeReplicationConfigEntry Enabled(LatticeMergeMode mode)
    {
        var entry = new LatticeReplicationConfigEntry();
        entry.Enable("site-a", 1);
        entry.SetMode("site-a", mode);
        return entry;
    }

    public static LatticeReplicationConfigEntry DisabledWithMode(LatticeMergeMode mode)
    {
        var entry = new LatticeReplicationConfigEntry();
        entry.SetMode("site-a", mode);
        return entry;
    }

    public static LatticeReplicationConfigEntry AmbiguousEnabled()
    {
        var a = new LatticeReplicationConfigEntry();
        a.Enable("site-a", 1);
        a.SetMode("site-a", LatticeMergeMode.LwwRegister);

        var b = new LatticeReplicationConfigEntry();
        b.Enable("site-b", 1);
        b.SetMode("site-b", LatticeMergeMode.OrSet);

        a.MergeFrom(b);
        return a;
    }

    public static async Task<CompiledReplicationConfigSnapshotMaintainer> WarmMaintainerAsync(
        IReadOnlyDictionary<string, LatticeReplicationConfigEntry> entries)
    {
        var store = new StaticConfigStore(entries);
        var maintainer = new CompiledReplicationConfigSnapshotMaintainer(
            store, NullLogger<CompiledReplicationConfigSnapshotMaintainer>.Instance);
        await maintainer.EnsureWarmAsync();
        return maintainer;
    }

    private sealed class StaticConfigStore(IReadOnlyDictionary<string, LatticeReplicationConfigEntry> entries)
        : ILatticeReplicationConfigStore
    {
        public Task<IReadOnlyDictionary<string, LatticeReplicationConfigEntry>> ReadEntriesAsync(
            CancellationToken cancellationToken = default) => Task.FromResult(entries);

        public Task<LatticeReplicationConfigEntry?> ReadEntryAsync(
            string treeId,
            CancellationToken cancellationToken = default) =>
            Task.FromResult(entries.TryGetValue(treeId, out var entry) ? entry : null);

        public Task WriteEntryAsync(
            string treeId,
            string replicaId,
            LatticeReplicationConfigEntry entry,
            CancellationToken cancellationToken = default) =>
            throw new NotSupportedException("The static config store is read-only.");
    }
}
