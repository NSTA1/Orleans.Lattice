using System.Collections.Immutable;
using System.Text;
using Orleans.Lattice.Api.Apps;
using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Explorer.Tests.Shell.Areas.Apps.Catalogue;

/// <summary>Sources, apps and descriptions the Apps catalogue tests are built from.</summary>
internal static class AppsTestData
{
    /// <summary>The in-image source: static, enumerable, no search.</summary>
    public static AppSourceSummary InImage { get; } = new()
    {
        Key = "in-image",
        DisplayName = "In-image apps",
        Kind = AppSourceSummaryKind.Static,
        Capabilities = AppSourceSummaryCapabilities.Enumerate,
    };

    /// <summary>A dynamic source that searches, offers several versions and acquires before describing.</summary>
    public static AppSourceSummary Feed { get; } = new()
    {
        Key = "nuget-contoso",
        DisplayName = "Contoso feed",
        Kind = AppSourceSummaryKind.Dynamic,
        Capabilities = AppSourceSummaryCapabilities.Enumerate | AppSourceSummaryCapabilities.Search
            | AppSourceSummaryCapabilities.MultipleVersions | AppSourceSummaryCapabilities.RequiresAcquisition,
    };

    /// <summary>A second dynamic source that searches but needs no acquisition.</summary>
    public static AppSourceSummary Blob { get; } = new()
    {
        Key = "blob-ops",
        DisplayName = "Ops blob store",
        Kind = AppSourceSummaryKind.Dynamic,
        Capabilities = AppSourceSummaryCapabilities.Enumerate | AppSourceSummaryCapabilities.Search,
    };

    /// <summary>A 64-character lower-case hex digest.</summary>
    public static string Digest(char fill = 'a') => new(fill, 64);

    /// <summary>A small SVG icon.</summary>
    public static AppIconAsset Icon { get; } = new()
    {
        Bytes = Encoding.UTF8.GetBytes("<svg xmlns=\"http://www.w3.org/2000/svg\"/>"),
        MediaType = "image/svg+xml",
        Sha256 = Digest('b'),
    };

    /// <summary>
    /// A task-board app: two roles over its own tree, a UI asking for three bridge
    /// grants, optionally a cross-app scope and an adopted tree.
    /// </summary>
    public static AppDescriptor TaskBoard(
        string version = "1.0.0",
        string source = "in-image",
        bool crossApp = false,
        bool adopted = false,
        bool withIcon = false,
        AppPresentationDescriptor? presentation = null,
        ImmutableArray<AppUiBridgeGrantDescriptor>? bridge = null,
        LatticeOperation editorOperations = LatticeOperation.Read | LatticeOperation.Write | LatticeOperation.Delete)
    {
        var viewerScopes = ImmutableArray.Create(new AppRoleScope { Tree = "tasks" });
        if (crossApp)
        {
            viewerScopes = viewerScopes.Add(new AppRoleScope { Tree = "contacts", App = "crm", Kind = LatticeScopeKind.Prefix, KeyOrPrefix = "eu-" });
        }

        var trees = ImmutableArray.Create(new AppTreeDescriptor { Name = "tasks", Rebuildable = false, SoftDeleteDuration = TimeSpan.FromDays(7) });
        if (adopted)
        {
            trees = trees.Add(new AppTreeDescriptor { Name = "archive", AdoptedTreeId = "legacy-archive" });
        }

        return new AppDescriptor
        {
            Slug = "task-board",
            Version = version,
            SourceKey = source,
            Provenance = new AppProvenanceDescriptor { Source = source, Publisher = "Contoso", Reference = "pkg:task-board" },
            State = AppLifecycleState.NotInstalled,
            Trees = trees,
            Roles =
            [
                new AppRoleDescriptor { Name = "viewer", Operations = LatticeOperation.Read | LatticeOperation.RangeRead, Scopes = viewerScopes },
                new AppRoleDescriptor { Name = "editor", Operations = editorOperations, Scopes = [new AppRoleScope { Tree = "tasks" }] },
            ],
            McpTools = [new AppMcpToolDescriptor { Name = "list_tasks", Description = "Lists tasks", Role = "viewer" }],
            Subscriptions = crossApp ? [new AppSubscriptionDescriptor { Name = "contacts-feed", Tree = "contacts", App = "crm" }] : [],
            Replication = [new AppReplicationDescriptor { Tree = "tasks", MergeMode = LatticeMergeMode.LwwRegister }],
            Schema = [new AppSchemaDescriptor { Tree = "tasks", Family = "task", Version = 2, StrictIngest = true }],
            Presentation = presentation ?? new AppPresentationDescriptor
            {
                DisplayName = "Task board",
                Summary = "Kanban board over a single app tree",
                Description = "Track work.\nMove cards between columns.",
                Categories = ["productivity"],
                Icon = withIcon ? new AppIconDescriptor { Path = "icon.svg", Sha256 = Digest('b') } : null,
            },
            Ui = new AppUiDescriptor
            {
                Entry = "index.html",
                BundleDigest = Digest('c'),
                Assets = [new AppUiAssetDescriptor { Path = "index.html", MediaType = "text/html", Sha256 = Digest('d') }],
                Bridge = bridge ??
                [
                    new AppUiBridgeGrantDescriptor { Operation = "data.read" },
                    new AppUiBridgeGrantDescriptor { Operation = "data.write", Tree = "tasks" },
                    new AppUiBridgeGrantDescriptor { Operation = "context.user" },
                ],
                MinProtocol = 1,
            },
        };
    }

    /// <summary>A catalogue row for <paramref name="app"/> as <paramref name="source"/> offers it.</summary>
    public static AvailableAppSummary Offer(
        AppDescriptor app,
        string source,
        string? installedVersion = null,
        AppLifecycleState? state = null) => new()
    {
        SourceKey = source,
        Slug = app.Slug,
        NewestVersion = app.Version,
        AvailableVersions = [app.Version],
        Presentation = app.Presentation,
        HasUi = app.Ui is not null,
        InstalledVersion = installedVersion,
        InstalledState = state,
    };

    /// <summary>A plain catalogue row.</summary>
    public static AvailableAppSummary Offer(string slug, string source, string version = "1.0.0", string? installedVersion = null, AppLifecycleState? state = null, string? name = null) => new()
    {
        SourceKey = source,
        Slug = slug,
        NewestVersion = version,
        AvailableVersions = [version],
        Presentation = name is null ? null : new AppPresentationDescriptor { DisplayName = name, Summary = name + " summary" },
        InstalledVersion = installedVersion,
        InstalledState = state,
    };

    /// <summary>One of the caller's apps.</summary>
    public static WorkspaceAppSummary Mine(string slug, bool hasUi = true, string? name = null, params string[] roles) => new()
    {
        Slug = slug,
        Version = "1.0.0",
        InstallRevision = 1,
        HasUi = hasUi,
        Presentation = name is null ? null : new AppPresentationDescriptor { DisplayName = name },
        Roles = roles.Length == 0 ? ["viewer"] : [.. roles],
    };
}
