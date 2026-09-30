namespace Orleans.Lattice.Explorer.UI.Areas.Apps.App;

/// <summary>
/// The sections of an app's page, each one the lower-case route segment after
/// <c>/apps/{slug}/</c>, in the order they are offered.
/// </summary>
/// <remarks>
/// A section the caller may not use is <em>absent</em>, never disabled: <see cref="Consent"/>
/// appears only for an <c>AppInstall</c> holder, and <see cref="Open"/> only for an app that
/// ships a UI and is among the caller's own apps.
/// </remarks>
internal static class AppPageTabs
{
    /// <summary>Presentation, version, source, lifecycle state and the caller's roles.</summary>
    public const string Overview = "overview";

    /// <summary>The app's trees by logical name, with their shape, retention and adoption.</summary>
    public const string Trees = "trees";

    /// <summary>The caller's roles, and for an <c>AppInstall</c> holder every declared role and its group.</summary>
    public const string Roles = "roles";

    /// <summary>The app's MCP tools and the roles they require.</summary>
    public const string Tools = "tools";

    /// <summary>The app's change-feed subscriptions.</summary>
    public const string Subscriptions = "subscriptions";

    /// <summary>The app's replication intent.</summary>
    public const string Replication = "replication";

    /// <summary>The consented ceiling, exception scopes, bridge operations and drift; <c>AppInstall</c> only.</summary>
    public const string Consent = "consent";

    /// <summary>The app's own UI in its sandboxed frame.</summary>
    public const string Open = "open";

    /// <summary>Every section, in display order.</summary>
    public static IReadOnlyList<string> All { get; } =
        [Overview, Trees, Roles, Tools, Subscriptions, Replication, Consent, Open];

    /// <summary>The section's tab label.</summary>
    /// <param name="tab">A section from <see cref="All"/>.</param>
    /// <returns>The label, such as "Overview".</returns>
    public static string Title(string tab) => tab switch
    {
        Overview => "Overview",
        Trees => "Trees",
        Roles => "Roles",
        Tools => "Tools",
        Subscriptions => "Subscriptions",
        Replication => "Replication",
        Consent => "Consent",
        Open => "Open",
        _ => throw new ArgumentOutOfRangeException(nameof(tab), tab, "Not an app page section."),
    };

    /// <summary>The sections <paramref name="model"/> offers its caller, in display order.</summary>
    /// <param name="model">The loaded app.</param>
    /// <returns>Every section the caller may use; the rest are absent.</returns>
    public static IReadOnlyList<string> For(AppPageModel model)
    {
        ArgumentNullException.ThrowIfNull(model);
        return [.. All.Where(tab => IsOffered(model, tab))];
    }

    /// <summary>Whether <paramref name="model"/> offers <paramref name="tab"/> to its caller.</summary>
    /// <param name="model">The loaded app.</param>
    /// <param name="tab">The requested section, or any other text.</param>
    /// <returns><see langword="true"/> when the section exists for this caller.</returns>
    public static bool IsOffered(AppPageModel model, string? tab)
    {
        ArgumentNullException.ThrowIfNull(model);
        return tab switch
        {
            Overview or Trees or Roles or Tools or Subscriptions or Replication => true,
            Consent => model.IsAppInstallHolder,
            Open => model.CanOpen,
            _ => false,
        };
    }
}
