using System.Collections.Immutable;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Orleans.Lattice.Api.Apps;

namespace Orleans.Lattice.Explorer.Shell.Areas.Apps.App;

/// <summary>
/// Loads an installed app's page for the caller, per circuit, from the two read paths
/// epic decision E8 gates differently: the role holder's workspace
/// (<see cref="ILatticeAppWorkspace"/>) and, for an <c>AppInstall</c> holder, the
/// administrative description and consent (<see cref="ILatticeAppsControl"/>).
/// </summary>
/// <remarks>
/// <para>
/// It fails closed. A caller the workspace answers "not found" for and the control
/// refuses, or that either facade is not registered for, sees not found for the whole
/// <c>/apps/{slug}</c> subtree, exactly as for an app that does not exist, so app
/// existence never leaks. The control's capability probe is advisory: it only saves a
/// call that would be refused, and a probe or call that fails leaves the caller without
/// the administrative sections rather than with them.
/// </para>
/// <para>
/// A workspace fault is reported as <see cref="AppPageLoadKind.Unavailable"/> only when no
/// read path answered at all, and says nothing about the slug: the same fault would be
/// reported for any app.
/// </para>
/// </remarks>
internal sealed class AppPageLoader
{
    /// <summary>The largest icon rendered inline, in bytes; a larger one is not shown.</summary>
    public const int MaxIconBytes = 256 * 1024;

    private static readonly HashSet<string> IconMediaTypes = new(StringComparer.OrdinalIgnoreCase)
    {
        "image/svg+xml",
        "image/png",
        "image/webp",
        "image/jpeg",
        "image/gif",
    };

    private readonly ILatticeAppWorkspace? _workspace;
    private readonly ILatticeAppsControl? _control;
    private readonly ILogger _logger;

    /// <summary>Creates the loader over whichever read paths the host registered.</summary>
    /// <param name="workspace">The caller's workspace, or <see langword="null"/> when not registered.</param>
    /// <param name="control">The app control, or <see langword="null"/> when not registered.</param>
    /// <param name="logger">Where a failing read path is reported, without any slug or presentation text.</param>
    public AppPageLoader(ILatticeAppWorkspace? workspace, ILatticeAppsControl? control, ILogger<AppPageLoader>? logger = null)
    {
        _workspace = workspace;
        _control = control;
        _logger = logger ?? NullLogger<AppPageLoader>.Instance;
    }

    /// <summary>Loads <paramref name="slug"/> for the caller.</summary>
    /// <param name="slug">The app slug from the address.</param>
    /// <param name="cancellationToken">Cancelled when the page no longer wants the answer.</param>
    /// <returns>The app as the caller may see it, not found, or unavailable.</returns>
    /// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> was cancelled.</exception>
    public async Task<AppPageLoad> LoadAsync(string slug, CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(slug))
        {
            return AppPageLoad.NotFound;
        }

        var workspaceTask = ReadWorkspaceAsync(slug, cancellationToken);
        var adminTask = ReadAdminAsync(slug, cancellationToken);
        await Task.WhenAll(workspaceTask, adminTask).ConfigureAwait(false);

        var (summary, described, workspaceFaulted) = await workspaceTask.ConfigureAwait(false);
        var (admin, consent, consentRead) = await adminTask.ConfigureAwait(false);

        if (described is null && admin is null)
        {
            return workspaceFaulted ? AppPageLoad.Unavailable : AppPageLoad.NotFound;
        }

        var presentation = described?.Presentation ?? admin?.Presentation;
        var icon = described?.Presentation?.Icon is not null
            ? await ReadIconAsync(slug, cancellationToken).ConfigureAwait(false)
            : null;

        var callerRoles = described?.Roles ?? [];
        var model = new AppPageModel
        {
            Slug = described?.Slug ?? admin!.Slug,
            Version = described?.Version ?? admin!.Version,
            SourceKey = described?.SourceKey ?? admin?.SourceKey,
            State = described?.State ?? admin!.State,
            Presentation = presentation,
            IconDataUri = icon,
            Trees = described is not null
                ? [.. described.Trees.Select(AppPageTree.From)]
                : [.. admin!.Trees.Select(AppPageTree.From)],
            CallerRoleNames = summary is { Roles.IsDefaultOrEmpty: false }
                ? summary.Roles
                : [.. callerRoles.Select(role => role.Name)],
            CallerRoles = callerRoles,
            McpTools = described?.McpTools ?? admin!.McpTools,
            Subscriptions = described?.Subscriptions ?? admin!.Subscriptions,
            Replication = described?.Replication ?? admin!.Replication,
            Ui = described?.Ui ?? admin?.Ui,
            CanOpen = summary is { HasUi: true },
            Admin = admin,
            Consent = consent,
            Drift = admin is not null && consentRead ? AppConsentDrift.Analyze(admin, consent) : null,
        };

        return AppPageLoad.Loaded(model);
    }

    /// <summary>
    /// The icon bytes as a <c>data:</c> URI for an <c>img</c>, or <see langword="null"/> for an
    /// empty, oversized or non-image asset. An <c>img</c> never runs script, so even an SVG
    /// icon is inert.
    /// </summary>
    /// <param name="icon">The verified icon, or <see langword="null"/>.</param>
    /// <returns>The data URI, or <see langword="null"/>.</returns>
    internal static string? ToDataUri(AppIconAsset? icon)
    {
        if (icon is null
            || icon.Bytes.IsEmpty
            || icon.Bytes.Length > MaxIconBytes
            || !IconMediaTypes.Contains(icon.MediaType))
        {
            return null;
        }

        return "data:" + icon.MediaType.ToLowerInvariant() + ";base64," + Convert.ToBase64String(icon.Bytes.Span);
    }

    private async Task<(WorkspaceAppSummary? Summary, WorkspaceAppDescriptor? Described, bool Faulted)> ReadWorkspaceAsync(
        string slug,
        CancellationToken cancellationToken)
    {
        if (_workspace is null)
        {
            return (null, null, false);
        }

        try
        {
            var described = await _workspace.DescribeMyAppAsync(slug, cancellationToken).ConfigureAwait(false);
            if (described is null || !string.Equals(described.Slug, slug, StringComparison.Ordinal))
            {
                return (null, null, false);
            }

            ImmutableArray<WorkspaceAppSummary> mine = await _workspace.ListMyAppsAsync(cancellationToken).ConfigureAwait(false);
            var summary = mine.IsDefault
                ? null
                : mine.FirstOrDefault(app => string.Equals(app.Slug, slug, StringComparison.Ordinal));
            return (summary, described, false);
        }
        catch (Exception exception) when (exception is not OperationCanceledException)
        {
            _logger.LogWarning(exception, "The app workspace did not answer for an app page.");
            return (null, null, true);
        }
    }

    private async Task<(AppDescriptor? Admin, AppConsentReport? Consent, bool ConsentRead)> ReadAdminAsync(string slug, CancellationToken cancellationToken)
    {
        if (_control is null)
        {
            return (null, null, false);
        }

        LatticeAppsCapabilities capabilities;
        try
        {
            capabilities = await _control.GetCapabilitiesAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (Exception exception) when (exception is not OperationCanceledException)
        {
            _logger.LogInformation(exception, "The app control's capability probe failed; the administrative sections are hidden.");
            return (null, null, false);
        }

        if (!capabilities.CanDescribe)
        {
            return (null, null, false);
        }

        AppDescriptor? admin;
        try
        {
            admin = await _control.DescribeAsync(slug, cancellationToken: cancellationToken).ConfigureAwait(false);
        }
        catch (Exception exception) when (exception is not OperationCanceledException)
        {
            _logger.LogInformation(exception, "The app control refused or failed to describe an app; the administrative sections are hidden.");
            return (null, null, false);
        }

        if (admin is null
            || admin.State is AppLifecycleState.NotInstalled or AppLifecycleState.Uninstalled
            || !string.Equals(admin.Slug, slug, StringComparison.Ordinal))
        {
            return (null, null, false);
        }

        if (!capabilities.CanGetConsent)
        {
            return (admin, null, false);
        }

        try
        {
            var consent = await _control.GetConsentAsync(slug, cancellationToken).ConfigureAwait(false);
            return (admin, consent, true);
        }
        catch (Exception exception) when (exception is not OperationCanceledException)
        {
            _logger.LogInformation(exception, "The app control did not return an app's consent.");
            return (admin, null, false);
        }
    }

    private async Task<string?> ReadIconAsync(string slug, CancellationToken cancellationToken)
    {
        try
        {
            return ToDataUri(await _workspace!.GetIconAsync(slug, cancellationToken).ConfigureAwait(false));
        }
        catch (Exception exception) when (exception is not OperationCanceledException)
        {
            _logger.LogInformation(exception, "The app workspace did not return an app's icon.");
            return null;
        }
    }
}
