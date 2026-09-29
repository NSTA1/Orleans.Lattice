using System.Collections.Immutable;
using Orleans.Lattice.Api.Apps;

namespace Orleans.Lattice.Explorer.Tests.UI.Framing;

/// <summary>
/// A scriptable <see cref="ILatticeAppWorkspace"/>: one caller's view of their apps, the
/// descriptions a role holder sees, and the installed version's UI assets. Every call is
/// counted so a test can prove what was, and was not, fetched.
/// </summary>
internal sealed class FakeAppWorkspace : ILatticeAppWorkspace
{
    /// <summary>The caller's apps, as <see cref="ListMyAppsAsync"/> reports them.</summary>
    public List<WorkspaceAppSummary> Apps { get; } = [];

    /// <summary>The descriptions <see cref="DescribeMyAppAsync"/> returns, by slug.</summary>
    public Dictionary<string, WorkspaceAppDescriptor> Descriptions { get; } = new(StringComparer.Ordinal);

    /// <summary>The assets <see cref="GetUiAssetAsync"/> returns, by path.</summary>
    public Dictionary<string, AppUiAsset> Assets { get; } = new(StringComparer.Ordinal);

    /// <summary>When set, every call throws it.</summary>
    public Exception? Throw { get; set; }

    /// <summary>The number of <see cref="ListMyAppsAsync"/> calls.</summary>
    public int ListCalls { get; private set; }

    /// <summary>The number of <see cref="DescribeMyAppAsync"/> calls.</summary>
    public int DescribeCalls { get; private set; }

    /// <summary>The paths <see cref="GetUiAssetAsync"/> was asked for, in order.</summary>
    public List<string> AssetRequests { get; } = [];

    /// <summary>Grants the caller the app described by <paramref name="descriptor"/>.</summary>
    /// <param name="descriptor">The description.</param>
    /// <param name="assets">The UI assets the workspace serves.</param>
    /// <returns>This workspace.</returns>
    public FakeAppWorkspace Grant(WorkspaceAppDescriptor descriptor, IEnumerable<AppUiAsset>? assets = null)
    {
        Apps.RemoveAll(app => app.Slug == descriptor.Slug);
        Apps.Add(new WorkspaceAppSummary
        {
            Slug = descriptor.Slug,
            Version = descriptor.Version,
            InstallRevision = descriptor.InstallRevision,
            Presentation = descriptor.Presentation,
            HasUi = descriptor.Ui is not null,
            Roles = ["viewer"],
        });
        Descriptions[descriptor.Slug] = descriptor;
        foreach (var asset in assets ?? [])
        {
            Assets[asset.Path] = asset;
        }

        return this;
    }

    /// <summary>Revokes every grant: the caller sees no app.</summary>
    public void RevokeAll()
    {
        Apps.Clear();
        Descriptions.Clear();
    }

    /// <inheritdoc />
    public Task<ImmutableArray<WorkspaceAppSummary>> ListMyAppsAsync(CancellationToken cancellationToken = default)
    {
        ListCalls++;
        ThrowIfScripted();
        return Task.FromResult(Apps.ToImmutableArray());
    }

    /// <inheritdoc />
    public Task<WorkspaceAppDescriptor?> DescribeMyAppAsync(string appSlug, CancellationToken cancellationToken = default)
    {
        DescribeCalls++;
        ThrowIfScripted();
        return Task.FromResult(Descriptions.GetValueOrDefault(appSlug));
    }

    /// <inheritdoc />
    public Task<AppIconAsset?> GetIconAsync(string appSlug, CancellationToken cancellationToken = default) =>
        Task.FromResult<AppIconAsset?>(null);

    /// <inheritdoc />
    public Task<AppUiAsset?> GetUiAssetAsync(string appSlug, string path, CancellationToken cancellationToken = default)
    {
        AssetRequests.Add(path);
        ThrowIfScripted();
        return Task.FromResult(Descriptions.ContainsKey(appSlug) ? Assets.GetValueOrDefault(path) : null);
    }

    private void ThrowIfScripted()
    {
        if (Throw is not null)
        {
            throw Throw;
        }
    }
}
