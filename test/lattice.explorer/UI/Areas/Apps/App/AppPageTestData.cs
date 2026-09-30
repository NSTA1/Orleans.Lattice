using System.Collections.Immutable;
using Orleans.Lattice;
using Orleans.Lattice.Api.Apps;
using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Apps.App;

/// <summary>
/// The apps the app-page tests describe: a CRM with a UI, trees (one adopted), roles, tools,
/// subscriptions and replication intent; its administrative twin; and consents that do and
/// do not cover it.
/// </summary>
internal static class AppPageTestData
{
    /// <summary>The CRM app's slug.</summary>
    public const string Slug = "crm";

    /// <summary>The operator-declared legacy tree the CRM adopts; it must never appear outside consent.</summary>
    public const string AdoptedTreeId = "legacy-orders-2019";

    /// <summary>A presentation text an app might use to smuggle markup; it must render literally.</summary>
    public const string Hostile = "<script>alert('x')</script><b onmouseover=\"steal()\">bold</b>";

    /// <summary>The CRM's presentation.</summary>
    public static AppPresentationDescriptor Presentation(string? description = null, string? documentation = "https://example.test/crm") => new()
    {
        DisplayName = "CRM",
        Summary = "Accounts, orders and contact history",
        Description = description ?? "Keeps accounts and orders.\nOne line per fact.",
        Icon = new AppIconDescriptor { Path = "icon.svg", Sha256 = new string('a', 64) },
        Categories = ["sales", "records"],
        DocumentationUrl = documentation,
        PublisherDisplayName = "Contoso",
    };

    /// <summary>The CRM's UI declaration.</summary>
    public static AppUiDescriptor Ui() => new()
    {
        Entry = "index.html",
        BundleDigest = new string('b', 64),
        Bridge =
        [
            new AppUiBridgeGrantDescriptor { Operation = "data.read" },
            new AppUiBridgeGrantDescriptor { Operation = "data.write", Tree = "orders" },
            new AppUiBridgeGrantDescriptor { Operation = "nav.sync" },
        ],
        MinProtocol = 1,
    };

    /// <summary>The viewer role.</summary>
    public static AppRoleDescriptor Viewer { get; } = new()
    {
        Name = "viewer",
        Operations = LatticeOperation.Read | LatticeOperation.RangeRead,
        Scopes = [new AppRoleScope { Tree = "orders", Kind = LatticeScopeKind.Prefix, KeyOrPrefix = "eu/" }],
    };

    /// <summary>The editor role, which also reads another app's tree.</summary>
    public static AppRoleDescriptor Editor { get; } = new()
    {
        Name = "editor",
        Operations = LatticeOperation.Read | LatticeOperation.Write | LatticeOperation.Delete,
        Scopes =
        [
            new AppRoleScope { Tree = "orders" },
            new AppRoleScope { Tree = "invoices", App = "billing" },
        ],
    };

    /// <summary>The role holder's sanitised description of the CRM.</summary>
    /// <param name="ui">Whether the installed version ships a UI.</param>
    /// <param name="presentation">The presentation, or the default.</param>
    /// <param name="roles">The caller's roles; the viewer by default.</param>
    public static WorkspaceAppDescriptor Workspace(bool ui = true, AppPresentationDescriptor? presentation = null, params AppRoleDescriptor[] roles) => new()
    {
        Slug = Slug,
        Version = "2.1.0",
        InstallRevision = 7,
        SourceKey = "in-image",
        State = AppLifecycleState.Enabled,
        Presentation = presentation ?? Presentation(),
        Trees =
        [
            new WorkspaceTreeDescriptor { Name = "orders", ShardCount = 4, SoftDeleteDuration = TimeSpan.FromDays(30), MaxLeafKeys = 128 },
            new WorkspaceTreeDescriptor { Name = "legacy", Adopted = true, Rebuildable = true },
        ],
        Roles = roles.Length == 0 ? [Viewer] : [.. roles],
        McpTools = [new AppMcpToolDescriptor { Name = "search", Description = "Finds accounts by name.", Role = "viewer" }],
        Subscriptions = [new AppSubscriptionDescriptor { Name = "invoices-feed", Tree = "invoices", App = "billing", KeyPrefix = "2026/" }],
        Replication = [new AppReplicationDescriptor { Tree = "orders", MergeMode = LatticeMergeMode.LwwRegister }],
        Ui = ui ? Ui() : null,
    };

    /// <summary>The administrative description of the CRM.</summary>
    /// <param name="ui">Whether the installed version ships a UI.</param>
    /// <param name="state">The lifecycle state.</param>
    public static AppDescriptor Admin(bool ui = true, AppLifecycleState state = AppLifecycleState.Enabled) => new()
    {
        Slug = Slug,
        Version = "2.1.0",
        SourceKey = "in-image",
        State = state,
        Provenance = new AppProvenanceDescriptor { Source = "in-image", Publisher = "Contoso", Reference = "Contoso.Crm/2.1.0" },
        Presentation = Presentation(),
        Trees =
        [
            new AppTreeDescriptor { Name = "orders", ShardCount = 4, SoftDeleteDuration = TimeSpan.FromDays(30) },
            new AppTreeDescriptor { Name = "legacy", AdoptedTreeId = AdoptedTreeId },
        ],
        Roles = [Viewer, Editor],
        RoleBindings =
        [
            new AppRoleBindingDescriptor { RoleName = "viewer", GroupId = "grp-crm-viewers" },
            new AppRoleBindingDescriptor { RoleName = "editor", GroupId = "grp-crm-editors" },
        ],
        McpTools = [new AppMcpToolDescriptor { Name = "search", Description = "Finds accounts by name.", Role = "viewer" }],
        Subscriptions = [new AppSubscriptionDescriptor { Name = "invoices-feed", Tree = "invoices", App = "billing", KeyPrefix = "2026/" }],
        Replication = [new AppReplicationDescriptor { Tree = "orders", MergeMode = LatticeMergeMode.LwwRegister }],
        Ui = ui ? Ui() : null,
    };

    /// <summary>A consent that covers the administrative description in full.</summary>
    public static AppConsentReport CoveringConsent() => new()
    {
        Slug = Slug,
        Version = "2.1.0",
        Ceiling = new AppCapabilityCeilingDescriptor
        {
            AllowedOperations = LatticeOperation.Read | LatticeOperation.RangeRead | LatticeOperation.Write | LatticeOperation.Delete,
            ApprovedExceptionScopes =
            [
                new AppExceptionScope { App = "billing", Tree = "invoices" },
                new AppExceptionScope { AdoptedTreeId = AdoptedTreeId, Kind = LatticeScopeKind.Prefix, KeyOrPrefix = "eu/" },
            ],
        },
        BridgeGrants =
        [
            new AppUiBridgeGrantDescriptor { Operation = "data.read" },
            new AppUiBridgeGrantDescriptor { Operation = "data.write" },
            new AppUiBridgeGrantDescriptor { Operation = "nav.sync" },
        ],
    };

    /// <summary>A consent from before an upgrade: an older version, a narrower ceiling, and no write from the UI.</summary>
    public static AppConsentReport DriftedConsent() => new()
    {
        Slug = Slug,
        Version = "2.0.0",
        Ceiling = new AppCapabilityCeilingDescriptor
        {
            AllowedOperations = LatticeOperation.Read | LatticeOperation.RangeRead,
            ApprovedExceptionScopes = [],
        },
        BridgeGrants = [new AppUiBridgeGrantDescriptor { Operation = "data.read" }],
    };

    /// <summary>A small verified SVG icon.</summary>
    public static AppIconAsset Icon(string mediaType = "image/svg+xml") => new()
    {
        Bytes = "<svg xmlns='http://www.w3.org/2000/svg'/>"u8.ToArray(),
        MediaType = mediaType,
        Sha256 = new string('c', 64),
    };

    /// <summary>The roles as an immutable array, for records.</summary>
    /// <param name="roles">The roles.</param>
    public static ImmutableArray<AppRoleDescriptor> Roles(params AppRoleDescriptor[] roles) => [.. roles];
}
