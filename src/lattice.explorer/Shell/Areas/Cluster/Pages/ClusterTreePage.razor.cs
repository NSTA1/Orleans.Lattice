using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Api.TreeAdmin;
using Orleans.Lattice.Explorer.Shell.Navigation;
using Orleans.Lattice.Explorer.Shell.Navigation.Address;

namespace Orleans.Lattice.Explorer.Shell.Areas.Cluster.Pages;

/// <summary>
/// <c>/cluster/trees/{tree-path}</c>: one tree's administration. It probes what
/// the caller may do, and a tree the cluster does not know is not found. Its tabs
/// are the tree's summary, configuration, shards, storage and lifecycle; its
/// operations bar links the resumable reshard, resize and snapshot pages, the
/// admin tools, WAL placement and orphaned leaves.
/// </summary>
public partial class ClusterTreePage : IDisposable
{
    private readonly CancellationTokenSource _lifetime = new();
    private ClusterLoad<LatticeTreeAdminCapabilities> _access = ClusterLoad<LatticeTreeAdminCapabilities>.Loading;
    private TreeConfigurationReport? _config;
    private string? _tab = "summary";

    /// <summary>The logical tree id.</summary>
    [Parameter, EditorRequired]
    public string TreeId { get; set; } = string.Empty;

    [Inject]
    private ClusterFacades Facades { get; set; } = default!;

    [Inject]
    private ExplorerNavigator Navigator { get; set; } = default!;

    [Inject]
    private NavigationManager Navigation { get; set; } = default!;

    private string Lede => _config is { } config
        ? string.Join(" - ", new[]
        {
            ClusterTreeName.Parse(TreeId).Ownership is { } owners ? "Owned by " + owners : null,
            config.ShardCount is { } shards ? ClusterFormat.Plural(shards, "shard") : null,
            config.MaxLeafKeys is { } leaf ? $"{ClusterFormat.Count(leaf)} keys per leaf" : null,
        }.Where(part => part is not null))
        : string.Empty;

    /// <inheritdoc />
    public void Dispose()
    {
        _lifetime.Cancel();
        _lifetime.Dispose();
    }

    /// <inheritdoc />
    protected override async Task OnInitializedAsync()
    {
        var token = _lifetime.Token;
        _access = await ClusterLoad<LatticeTreeAdminCapabilities>.RunAsync(
            ct => ClusterTreeAccess.ProbeAsync(Facades.RequireTreeAdmin(), TreeId, ct),
            token);

        if (_access.Value is { CanViewDiagnostics: true } or { CanAdministerTree: true })
        {
            var config = await ClusterLoad<TreeConfigurationReport>.RunAsync(
                ct => Facades.RequireTreeAdmin().GetTreeConfigAsync(TreeId, ct),
                token);
            if (config.Value is { Exists: false })
            {
                Navigation.NotFound();
                return;
            }

            _config = config.Value;
        }
    }

    private string Href(ExplorerAddress address) => Navigator.Canonicalize(address).ToHref();
}
