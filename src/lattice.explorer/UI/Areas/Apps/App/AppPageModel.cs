using System.Collections.Immutable;
using Orleans.Lattice.Api.Apps;

namespace Orleans.Lattice.Explorer.UI.Areas.Apps.App;

/// <summary>
/// One installed app as its page shows it to this caller: the manifest-derived facts
/// every caller with access may see, and the administrative facts only an
/// <c>AppInstall</c> holder may see.
/// </summary>
/// <remarks>
/// The common facts come from the role holder's sanitised workspace projection when
/// the caller holds a role, and otherwise from the administrative description. Neither
/// carries a composed physical tree id, and the page never renders the administrative
/// adoption id outside the consent section.
/// </remarks>
internal sealed record AppPageModel
{
    /// <summary>The app slug.</summary>
    public required string Slug { get; init; }

    /// <summary>The installed version.</summary>
    public required string Version { get; init; }

    /// <summary>The key of the source the installed version came from, or <see langword="null"/> when not recorded.</summary>
    public string? SourceKey { get; init; }

    /// <summary>The installation's lifecycle state.</summary>
    public AppLifecycleState State { get; init; }

    /// <summary>The installed version's untrusted presentation, rendered as text only.</summary>
    public AppPresentationDescriptor? Presentation { get; init; }

    /// <summary>The verified icon as a <c>data:</c> URI for an <c>img</c>, or <see langword="null"/>.</summary>
    public string? IconDataUri { get; init; }

    /// <summary>The app's trees, by logical name.</summary>
    public ImmutableArray<AppPageTree> Trees { get; init; } = [];

    /// <summary>The names of the roles the caller holds in this app.</summary>
    public ImmutableArray<string> CallerRoleNames { get; init; } = [];

    /// <summary>The roles the caller holds, with their operations and scope templates.</summary>
    public ImmutableArray<AppRoleDescriptor> CallerRoles { get; init; } = [];

    /// <summary>The app's MCP tools.</summary>
    public ImmutableArray<AppMcpToolDescriptor> McpTools { get; init; } = [];

    /// <summary>The app's change-feed subscriptions.</summary>
    public ImmutableArray<AppSubscriptionDescriptor> Subscriptions { get; init; } = [];

    /// <summary>The app's replication intent.</summary>
    public ImmutableArray<AppReplicationDescriptor> Replication { get; init; } = [];

    /// <summary>The installed version's UI declaration, or <see langword="null"/> when it ships none.</summary>
    public AppUiDescriptor? Ui { get; init; }

    /// <summary>Whether the app ships a UI and is among the caller's own apps, so it can be opened.</summary>
    public bool CanOpen { get; init; }

    /// <summary>The administrative description, present only for an <c>AppInstall</c> holder.</summary>
    public AppDescriptor? Admin { get; init; }

    /// <summary>The effective consent, when an <c>AppInstall</c> holder could read it.</summary>
    public AppConsentReport? Consent { get; init; }

    /// <summary>How the consent differs from the installed manifest, for an <c>AppInstall</c> holder.</summary>
    public AppConsentDrift? Drift { get; init; }

    /// <summary>Whether the caller holds <c>AppInstall</c>, so the administrative sections exist.</summary>
    public bool IsAppInstallHolder => Admin is not null;

    /// <summary>The name to show: the presentation's display name, falling back to the slug.</summary>
    public string DisplayName =>
        string.IsNullOrWhiteSpace(Presentation?.DisplayName) ? Slug : Presentation!.DisplayName;
}
