using System.Collections.Immutable;
using System.Text;
using Orleans.Lattice.Api.Apps;
using Orleans.Lattice.Auth;
using Orleans.Lattice.Explorer.Shell.Design.Tokens;

namespace Orleans.Lattice.Explorer.Shell.Areas.Apps.Catalogue;

/// <summary>
/// How the Apps area words and draws what the facades report. Every app-supplied
/// string passes through here as text only: nothing is ever turned into markup
/// (epic decision E6), and an icon only ever becomes an image data URL.
/// </summary>
internal static class AppsPresentation
{
    /// <summary>The icon media types drawn through <c>&lt;img&gt;</c>; anything else is not drawn.</summary>
    public static IReadOnlySet<string> IconMediaTypes { get; } = new HashSet<string>(StringComparer.Ordinal)
    {
        "image/svg+xml",
        "image/png",
        "image/webp",
    };

    private static readonly (LatticeOperation Operation, string Text)[] OperationWords =
    [
        (LatticeOperation.Read, "read"),
        (LatticeOperation.Write, "write"),
        (LatticeOperation.Delete, "delete"),
        (LatticeOperation.RangeRead, "range read"),
        (LatticeOperation.RangeDelete, "range delete"),
        (LatticeOperation.CrdtApply, "CRDT apply"),
        (LatticeOperation.AtomicWrite, "atomic write"),
        (LatticeOperation.BulkLoad, "bulk load"),
        (LatticeOperation.Admin, "administer"),
        (LatticeOperation.Backup, "back up"),
        (LatticeOperation.Restore, "restore"),
        (LatticeOperation.SchemaAdmin, "administer schema"),
        (LatticeOperation.Telemetry, "read telemetry"),
        (LatticeOperation.Replication, "replicate"),
        (LatticeOperation.TreeLifecycle, "manage tree lifecycle"),
        (LatticeOperation.AppInstall, "install apps"),
    ];

    /// <summary>Every single operation a ceiling can approve, in display order.</summary>
    public static IReadOnlyList<LatticeOperation> Operations { get; } = [.. OperationWords.Select(pair => pair.Operation)];

    /// <summary>An app's name: its declared display name, falling back to its slug.</summary>
    /// <param name="presentation">The declared presentation, or <see langword="null"/>.</param>
    /// <param name="slug">The slug.</param>
    public static string DisplayName(AppPresentationDescriptor? presentation, string slug) =>
        string.IsNullOrWhiteSpace(presentation?.DisplayName) ? slug : presentation.DisplayName.Trim();

    /// <summary>
    /// The <c>data:</c> URL an icon is drawn from, or <see langword="null"/> when the
    /// icon is absent, empty or of a media type the area does not draw.
    /// </summary>
    /// <param name="icon">The verified icon.</param>
    public static string? IconDataUrl(AppIconAsset? icon)
    {
        if (icon is null || icon.Bytes.IsEmpty || !IconMediaTypes.Contains(icon.MediaType))
        {
            return null;
        }

        return $"data:{icon.MediaType};base64,{Convert.ToBase64String(icon.Bytes.Span)}";
    }

    /// <summary>The two-letter monogram drawn when an app has no drawable icon.</summary>
    /// <param name="slug">The slug.</param>
    public static string Monogram(string slug)
    {
        var letters = new StringBuilder(2);
        foreach (var part in slug.Split('-', StringSplitOptions.RemoveEmptyEntries))
        {
            letters.Append(part[0]);
            if (letters.Length == 2)
            {
                break;
            }
        }

        if (letters.Length < 2 && slug.Length > 1)
        {
            return slug[..2];
        }

        return letters.ToString();
    }

    /// <summary>The words for an operation mask, such as "read, write and delete".</summary>
    /// <param name="operations">The mask.</param>
    public static string OperationsText(LatticeOperation operations)
    {
        var words = OperationWords.Where(pair => operations.HasFlag(pair.Operation)).Select(pair => pair.Text).ToArray();
        return words.Length switch
        {
            0 => "nothing",
            1 => words[0],
            _ => string.Join(", ", words[..^1]) + " and " + words[^1],
        };
    }

    /// <summary>The word for one operation.</summary>
    /// <param name="operation">A single operation.</param>
    public static string OperationText(LatticeOperation operation) =>
        OperationWords.FirstOrDefault(pair => pair.Operation == operation).Text ?? operation.ToString();

    /// <summary>A role scope as a logical path, such as <c>a/crm/orders</c> or <c>a/crm/orders, keys starting "eu-"</c>.</summary>
    /// <param name="scope">The scope.</param>
    /// <param name="slug">The described app's slug, standing in when the scope names no app.</param>
    public static string ScopeText(AppRoleScope scope, string slug)
    {
        ArgumentNullException.ThrowIfNull(scope);
        return Extent(TreePath(scope.App ?? slug, scope.Tree), scope.Kind, scope.KeyOrPrefix);
    }

    /// <summary>An approved exception scope as text.</summary>
    /// <param name="scope">The scope.</param>
    public static string ScopeText(AppExceptionScope scope)
    {
        ArgumentNullException.ThrowIfNull(scope);
        var target = scope.AdoptedTreeId is { } adopted
            ? adopted + " (adopted)"
            : TreePath(scope.App ?? "?", scope.Tree ?? "?");
        return Extent(target, scope.Kind, scope.KeyOrPrefix);
    }

    /// <summary>The logical path of an app tree: <c>a/{slug}/{tree}</c>. Never a physical id.</summary>
    /// <param name="slug">The app slug.</param>
    /// <param name="tree">The app-local tree name.</param>
    public static string TreePath(string slug, string tree) => $"a/{slug}/{tree}";

    /// <summary>A bridge grant in plain language, such as "read its own trees".</summary>
    /// <param name="grant">The grant.</param>
    public static string BridgeText(AppUiBridgeGrantDescriptor grant)
    {
        ArgumentNullException.ThrowIfNull(grant);
        var trees = grant.Tree is { } tree ? $"its tree {tree}" : "its own trees";
        return grant.Operation switch
        {
            "context.read" => "know its version, the theme and your tenant's display name",
            "context.user" => "see your display name",
            "data.read" => "read " + trees,
            "data.write" => "write " + trees,
            "data.delete" => "delete keys in " + trees,
            "nav.sync" => "keep its page in the address line",
            "ui.notify" => "show you short notifications",
            _ => $"use the unrecognised operation \"{grant.Operation}\"",
        };
    }

    /// <summary>Whether a bridge operation is one this Explorer recognises.</summary>
    /// <param name="operation">The operation.</param>
    public static bool IsKnownBridgeOperation(string operation) => operation is
        "context.read" or "context.user" or "data.read" or "data.write" or "data.delete" or "nav.sync" or "ui.notify";

    /// <summary>A source's kind and capabilities, such as "Dynamic - search, several versions, acquired on install".</summary>
    /// <param name="source">The source.</param>
    public static string SourceHints(AppSourceSummary source)
    {
        ArgumentNullException.ThrowIfNull(source);
        var hints = new List<string>(4);
        if (source.Capabilities.HasFlag(AppSourceSummaryCapabilities.Search))
        {
            hints.Add("search");
        }

        if (source.Capabilities.HasFlag(AppSourceSummaryCapabilities.MultipleVersions))
        {
            hints.Add("several versions");
        }

        if (source.Capabilities.HasFlag(AppSourceSummaryCapabilities.RequiresAcquisition))
        {
            hints.Add("acquired on install");
        }

        var kind = source.Kind == AppSourceSummaryKind.Dynamic ? "Dynamic" : "Static";
        return hints.Count == 0 ? kind : kind + " - " + string.Join(", ", hints);
    }

    /// <summary>The state pill of a catalogue row.</summary>
    /// <param name="app">The row.</param>
    public static (LtStateRole Role, string Text) RowState(AvailableAppSummary app)
    {
        ArgumentNullException.ThrowIfNull(app);
        if (app.InstalledState is not { } state || state is AppLifecycleState.NotInstalled or AppLifecycleState.Uninstalled)
        {
            return (LtStateRole.Uninstalled, "Available");
        }

        if (HasUpdate(app))
        {
            return (LtStateRole.Lagging, "Update available");
        }

        return LifecycleState(state, app.InstalledVersion);
    }

    /// <summary>Whether a row's source offers a version other than the installed one.</summary>
    /// <param name="app">The row.</param>
    public static bool HasUpdate(AvailableAppSummary app) =>
        app.InstalledVersion is { } installed
        && app.InstalledState is not (null or AppLifecycleState.NotInstalled or AppLifecycleState.Uninstalled)
        && !string.Equals(installed, app.NewestVersion, StringComparison.Ordinal);

    /// <summary>The state pill of an installed app's lifecycle state.</summary>
    /// <param name="state">The state.</param>
    /// <param name="version">The installed version, or <see langword="null"/>.</param>
    public static (LtStateRole Role, string Text) LifecycleState(AppLifecycleState state, string? version) => state switch
    {
        AppLifecycleState.Enabled => (LtStateRole.Enabled, "Enabled"),
        AppLifecycleState.Disabled => (LtStateRole.Disabled, "Disabled"),
        AppLifecycleState.Failed => (LtStateRole.Failed, "Activation failed"),
        AppLifecycleState.Installed => (LtStateRole.Installed, version is null ? "Installed" : $"Installed v{version}"),
        AppLifecycleState.Uninstalled => (LtStateRole.Uninstalled, "Uninstalled"),
        _ => (LtStateRole.Uninstalled, "Available"),
    };

    /// <summary>The categories as one line, or <see langword="null"/>.</summary>
    /// <param name="categories">The declared categories.</param>
    public static string? CategoriesText(ImmutableArray<string> categories) =>
        categories.IsDefaultOrEmpty ? null : string.Join(", ", categories.Where(category => !string.IsNullOrWhiteSpace(category)));

    private static string Extent(string target, LatticeScopeKind kind, string? keyOrPrefix) => kind switch
    {
        LatticeScopeKind.Key => $"{target}, key \"{keyOrPrefix}\"",
        LatticeScopeKind.Prefix => $"{target}, keys starting \"{keyOrPrefix}\"",
        _ => target,
    };
}
