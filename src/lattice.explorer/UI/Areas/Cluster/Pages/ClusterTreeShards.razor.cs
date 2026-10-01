using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Api.TreeAdmin;
using Orleans.Lattice.Explorer.UI.Design.Tokens;
using Orleans.Lattice.Explorer.UI.Design.Components;

namespace Orleans.Lattice.Explorer.UI.Areas.Cluster.Pages;

/// <summary>
/// A tree's shards tab: the live shard map and the registry's persisted map, then
/// one row per physical shard joining the virtual slots routed to it, its
/// diagnostics and its hotness. Counting tombstones is a deep, expensive read and
/// asks for the tree's name first. This replaces the old Topology plugin's graph.
/// </summary>
public partial class ClusterTreeShards : IDisposable
{
    private const string Unknown = "-";

    private readonly ComponentLifetime _lifetime = new();
    private ClusterLoad<ShardMapInspection> _map = ClusterLoad<ShardMapInspection>.Loading;
    private ClusterLoad<TreeShardMapView> _registry = ClusterLoad<TreeShardMapView>.Loading;
    private ClusterLoad<TreeAdminDiagnosticReport> _diagnostics = ClusterLoad<TreeAdminDiagnosticReport>.Loading;
    private ClusterLoad<TreeHotnessReport> _hotness = ClusterLoad<TreeHotnessReport>.Loading;
    private IReadOnlyList<ClusterShardRow> _rows = [];
    private bool _loading;
    private bool _confirmDeep;

    /// <summary>The logical tree id.</summary>
    [Parameter, EditorRequired]
    public string TreeId { get; set; } = string.Empty;

    /// <summary>What the caller may do to the tree.</summary>
    [Parameter, EditorRequired]
    public LatticeTreeAdminCapabilities Capabilities { get; set; } = default!;

    [Inject]
    private ClusterFacades Facades { get; set; } = default!;

    /// <inheritdoc />
    public void Dispose()
    {
        _lifetime.Leave();
    }

    /// <inheritdoc />
    protected override Task OnInitializedAsync() =>
        Capabilities.CanViewDiagnostics ? LoadAsync(deep: false) : Task.CompletedTask;

    private async Task LoadAsync(bool deep)
    {
        _loading = true;
        var admin = Facades.RequireTreeAdmin();
        var token = _lifetime.Token;
        var map = ClusterLoad<ShardMapInspection>.RunAsync(ct => admin.InspectShardMapAsync(TreeId, ct), token);
        var registry = ClusterLoad<TreeShardMapView>.RunAsync(ct => admin.GetShardMapAsync(TreeId, ct), token);
        var diagnostics = ClusterLoad<TreeAdminDiagnosticReport>.RunAsync(ct => admin.GetDiagnosticsAsync(TreeId, deep, ct), token);
        var hotness = ClusterLoad<TreeHotnessReport>.RunAsync(ct => admin.GetShardHotnessAsync(TreeId, ct), token);

        _map = await map;
        _registry = await registry;
        _diagnostics = await diagnostics;
        _hotness = await hotness;
        _rows = Join(_map.Value, _diagnostics.Value, _hotness.Value);
        _loading = false;
    }

    /// <summary>Joins the three per-shard reads on the physical shard index.</summary>
    /// <param name="map">The live shard map, or <see langword="null"/>.</param>
    /// <param name="diagnostics">The diagnostics, or <see langword="null"/>.</param>
    /// <param name="hotness">The hotness sample, or <see langword="null"/>.</param>
    /// <returns>
    /// One row per physical shard the live shard map routes to, ordered by index.
    /// A shard a shrink retired keeps its shard root as a routing tombstone and
    /// can still be named by a cached diagnostics or hotness read, so those reads
    /// only fill in rows; they never add one. Without the map, the rows are the
    /// shards the diagnostics and hotness reports name, which the cluster
    /// enumerates from the same map.
    /// </returns>
    internal static IReadOnlyList<ClusterShardRow> Join(ShardMapInspection? map, TreeAdminDiagnosticReport? diagnostics, TreeHotnessReport? hotness)
    {
        var routed = map is { PhysicalShardIndices.IsDefaultOrEmpty: false } ? map.PhysicalShardIndices : default;
        var slots = SlotsByShard(map);
        var shards = diagnostics?.Shards.ToDictionary(shard => shard.ShardIndex);
        var heat = hotness?.Shards.ToDictionary(shard => shard.ShardIndex);

        var indices = new SortedSet<int>();
        if (!routed.IsDefaultOrEmpty)
        {
            indices.UnionWith(routed);
        }
        else
        {
            indices.UnionWith(shards?.Keys ?? Enumerable.Empty<int>());
            indices.UnionWith(heat?.Keys ?? Enumerable.Empty<int>());
        }

        return
        [
            .. indices.Select(index =>
            {
                ShardDiagnosticSnapshot? shard = shards is not null && shards.TryGetValue(index, out var s) ? s : null;
                ShardHotnessSnapshot? hot = heat is not null && heat.TryGetValue(index, out var h) ? h : null;
                return new ClusterShardRow(
                    index,
                    slots is not null && slots.TryGetValue(index, out var owned) ? owned : null,
                    shard?.Depth,
                    shard?.LiveKeys,
                    diagnostics is { Deep: true } ? shard?.Tombstones : null,
                    hot?.Reads ?? shard?.Reads,
                    hot?.Writes ?? shard?.Writes,
                    hot?.OpsPerSecond ?? shard?.OpsPerSecond,
                    shard?.SplitInProgress ?? false,
                    shard?.BulkOperationPending ?? false);
            }),
        ];
    }

    /// <summary>
    /// The virtual slots the live map routes to each shard, from the inspection's
    /// per-shard counts. <see langword="null"/> when there is no map, or the
    /// cluster predates the counts: the distinct indices alone cannot say how
    /// many slots each shard owns.
    /// </summary>
    /// <param name="map">The live shard map, or <see langword="null"/>.</param>
    /// <returns>The slot count by shard index, or <see langword="null"/>.</returns>
    private static Dictionary<int, int>? SlotsByShard(ShardMapInspection? map)
    {
        if (map is null
            || map.PhysicalShardIndices.IsDefaultOrEmpty
            || map.SlotCounts.IsDefaultOrEmpty
            || map.SlotCounts.Length != map.PhysicalShardIndices.Length)
        {
            return null;
        }

        var slots = new Dictionary<int, int>(map.PhysicalShardIndices.Length);
        for (var i = 0; i < map.PhysicalShardIndices.Length; i++)
        {
            slots[map.PhysicalShardIndices[i]] = map.SlotCounts[i];
        }

        return slots;
    }

    private static string Number(long? value) => value is { } number ? ClusterFormat.Count(number) : Unknown;

    private static string Number(int? value) => value is { } number ? ClusterFormat.Count(number) : Unknown;

    private static LtStateRole StateOf(ClusterShardRow row) =>
        row.Splitting || row.BulkPending ? LtStateRole.Lagging : LtStateRole.Healthy;

    private static string StateText(ClusterShardRow row) => row switch
    {
        { Splitting: true } => "Splitting",
        { BulkPending: true } => "Bulk load pending",
        _ => "Steady",
    };
}
