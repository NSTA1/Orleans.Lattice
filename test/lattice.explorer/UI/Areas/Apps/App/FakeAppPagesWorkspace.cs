using System.Collections.Immutable;
using Orleans.Lattice.Api.Apps;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Apps.App;

/// <summary>
/// A scriptable <see cref="ILatticeAppWorkspace"/> for the app pages: the caller's apps as
/// <see cref="ListMyAppsAsync"/> reports them, the sanitised descriptions a role holder
/// sees, and verified icons. A slug it does not hold answers exactly as the real facade
/// does for a caller without a role: no list entry, and <see langword="null"/>.
/// </summary>
internal sealed class FakeAppPagesWorkspace : ILatticeAppWorkspace
{
    /// <summary>The caller's apps, in slug order.</summary>
    public List<WorkspaceAppSummary> Apps { get; } = [];

    /// <summary>The descriptions a role holder reads, by slug.</summary>
    public Dictionary<string, WorkspaceAppDescriptor> Descriptions { get; } = new(StringComparer.Ordinal);

    /// <summary>The verified icons, by slug.</summary>
    public Dictionary<string, AppIconAsset> Icons { get; } = new(StringComparer.Ordinal);

    /// <summary>When set, every call throws it.</summary>
    public Exception? Throw { get; set; }

    /// <summary>When set, <see cref="DescribeMyAppAsync"/> waits for it, so a test can hold the page in its loading state.</summary>
    public TaskCompletionSource? Gate { get; set; }

    /// <summary>The slugs <see cref="DescribeMyAppAsync"/> was asked for, in order.</summary>
    public List<string> Described { get; } = [];

    /// <summary>The number of <see cref="GetIconAsync"/> calls.</summary>
    public int IconCalls { get; private set; }

    /// <summary>Grants the caller <paramref name="descriptor"/> with <paramref name="roles"/>.</summary>
    /// <param name="descriptor">The description a role holder sees.</param>
    /// <param name="roles">The caller's role names; defaults to the descriptor's roles.</param>
    /// <returns>This workspace.</returns>
    public FakeAppPagesWorkspace Grant(WorkspaceAppDescriptor descriptor, params string[] roles)
    {
        Apps.RemoveAll(app => app.Slug == descriptor.Slug);
        Apps.Add(new WorkspaceAppSummary
        {
            Slug = descriptor.Slug,
            Version = descriptor.Version,
            InstallRevision = descriptor.InstallRevision,
            Presentation = descriptor.Presentation,
            HasUi = descriptor.Ui is not null,
            Roles = roles.Length == 0 ? [.. descriptor.Roles.Select(role => role.Name)] : [.. roles],
        });
        Descriptions[descriptor.Slug] = descriptor;
        return this;
    }

    /// <inheritdoc />
    public Task<ImmutableArray<WorkspaceAppSummary>> ListMyAppsAsync(CancellationToken cancellationToken = default)
    {
        ThrowIfScripted();
        return Task.FromResult(Apps.OrderBy(app => app.Slug, StringComparer.Ordinal).ToImmutableArray());
    }

    /// <inheritdoc />
    public async Task<WorkspaceAppDescriptor?> DescribeMyAppAsync(string appSlug, CancellationToken cancellationToken = default)
    {
        Described.Add(appSlug);
        if (Gate is { } gate)
        {
            await gate.Task.WaitAsync(cancellationToken);
        }

        ThrowIfScripted();
        return Descriptions.GetValueOrDefault(appSlug);
    }

    /// <inheritdoc />
    public Task<AppIconAsset?> GetIconAsync(string appSlug, CancellationToken cancellationToken = default)
    {
        IconCalls++;
        ThrowIfScripted();
        return Task.FromResult(Descriptions.ContainsKey(appSlug) ? Icons.GetValueOrDefault(appSlug) : null);
    }

    /// <inheritdoc />
    public Task<AppUiAsset?> GetUiAssetAsync(string appSlug, string path, CancellationToken cancellationToken = default) =>
        throw new NotSupportedException("The app pages never fetch UI assets; the frame's loader does.");

    private void ThrowIfScripted()
    {
        if (Throw is not null)
        {
            throw Throw;
        }
    }
}
