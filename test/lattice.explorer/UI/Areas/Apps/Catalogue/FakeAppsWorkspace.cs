using System.Collections.Immutable;
using Orleans.Lattice.Api.Apps;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Apps.Catalogue;

/// <summary>A scripted <see cref="ILatticeAppWorkspace"/>: the caller's apps and their icons.</summary>
internal sealed class FakeAppsWorkspace : ILatticeAppWorkspace
{
    /// <summary>The caller's apps.</summary>
    public List<WorkspaceAppSummary> Apps { get; } = [];

    /// <summary>Icons by slug.</summary>
    public Dictionary<string, AppIconAsset> Icons { get; } = new(StringComparer.Ordinal);

    /// <summary>When set, listing throws it (a caller who is not signed in).</summary>
    public Exception? Failure { get; set; }

    /// <summary>How many times the caller's apps were listed.</summary>
    public int ListCalls { get; private set; }

    /// <summary>When set, listing waits for it.</summary>
    public TaskCompletionSource? ListGate { get; set; }

    /// <inheritdoc />
    public async Task<ImmutableArray<WorkspaceAppSummary>> ListMyAppsAsync(CancellationToken cancellationToken = default)
    {
        ListCalls++;
        if (ListGate is { } gate)
        {
            await gate.Task;
        }

        return Failure is { } failure ? throw failure : [.. Apps];
    }

    /// <inheritdoc />
    public Task<WorkspaceAppDescriptor?> DescribeMyAppAsync(string appSlug, CancellationToken cancellationToken = default) =>
        Task.FromResult<WorkspaceAppDescriptor?>(null);

    /// <inheritdoc />
    public Task<AppIconAsset?> GetIconAsync(string appSlug, CancellationToken cancellationToken = default) =>
        Task.FromResult(Icons.GetValueOrDefault(appSlug));

    /// <inheritdoc />
    public Task<AppUiAsset?> GetUiAssetAsync(string appSlug, string path, CancellationToken cancellationToken = default) =>
        Task.FromResult<AppUiAsset?>(null);
}
