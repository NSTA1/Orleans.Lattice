using Microsoft.AspNetCore.Components;
using Microsoft.AspNetCore.Components.Rendering;
using Orleans.Lattice.Api.Apps;
using Orleans.Lattice.Explorer.UI.Navigation.Address;

namespace Orleans.Lattice.Explorer.UI.Areas.Apps.Catalogue;

/// <summary>
/// "Your apps" (<c>/apps</c>): the apps the caller holds a role in, the Catalogue
/// view for an <c>AppInstall</c> holder, and failed activations called out.
/// </summary>
public partial class AppsPage : IDisposable
{
    private readonly Dictionary<string, string?> _icons = new(StringComparer.Ordinal);
    private readonly CancellationTokenSource _lifetime = new();
    private AppsAccessSnapshot? _snapshot;
    private IReadOnlyList<AppSummary> _failed = [];

    [Inject]
    internal AppsAccess Access { get; set; } = default!;

    [Inject]
    internal AppsFacades Facades { get; set; } = default!;

    private string TenantPhrase => Address.Tenant is { } tenant ? $" in tenant {tenant}" : string.Empty;

    private string Lede => _snapshot?.CanBrowseCatalogue == true
        ? $"The apps you hold a role in{TenantPhrase}, and their install, consent and lifecycle. Sources are configured per cluster."
        : $"The apps you hold a role in{TenantPhrase}.";

    /// <summary>Stops listening for changes and cancels outstanding icon reads.</summary>
    public void Dispose()
    {
        Access.Changed -= OnAccessChanged;
        _lifetime.Cancel();
        _lifetime.Dispose();
    }

    /// <inheritdoc />
    protected override async Task OnInitializedAsync()
    {
        Access.Changed += OnAccessChanged;
        await LoadAsync();
    }

    private async Task LoadAsync()
    {
        try
        {
            _snapshot = await Access.GetAsync(_lifetime.Token);
        }
        catch (OperationCanceledException)
        {
            return;
        }

        _failed = [.. _snapshot.FailedActivations];
        StateHasChanged();
        await LoadIconsAsync(_snapshot);
    }

    private async Task LoadIconsAsync(AppsAccessSnapshot snapshot)
    {
        if (Facades.Workspace is not { } workspace)
        {
            return;
        }

        foreach (var app in snapshot.MyApps.Where(app => app.Presentation?.Icon is not null && !_icons.ContainsKey(app.Slug)))
        {
            try
            {
                _icons[app.Slug] = AppsPresentation.IconDataUrl(await workspace.GetIconAsync(app.Slug, _lifetime.Token));
            }
            catch (OperationCanceledException)
            {
                return;
            }
            catch (Exception)
            {
                _icons[app.Slug] = null;
            }

            StateHasChanged();
        }
    }

    private void OnAccessChanged() => _ = InvokeAsync(LoadAsync);

    private static object? RolesText(WorkspaceAppSummary app) => app.Roles.IsDefaultOrEmpty ? "-" : string.Join(", ", app.Roles);

    private string? IconOf(string slug) => _icons.GetValueOrDefault(slug);

    private string Href(ExplorerAddress address) => Navigator.Canonicalize(address).ToHref();

    private RenderFragment AppActions(WorkspaceAppSummary app) => builder => BuildActions(builder, app);

    private void BuildActions(RenderTreeBuilder builder, WorkspaceAppSummary app)
    {
        var name = AppsPresentation.DisplayName(app.Presentation, app.Slug);
        if (app.HasUi)
        {
            builder.OpenElement(0, "a");
            builder.AddAttribute(1, "class", "lt-btn");
            builder.AddAttribute(2, "href", Href(AppsRoutes.Open(Address.Tenant, app.Slug)));
            builder.AddAttribute(3, "aria-label", "Open " + name);
            builder.AddContent(4, "Open");
            builder.CloseElement();
        }

        builder.OpenElement(5, "a");
        builder.AddAttribute(6, "class", "lt-btn lt-btn--quiet");
        builder.AddAttribute(7, "href", Href(AppsRoutes.App(Address.Tenant, app.Slug)));
        builder.AddAttribute(8, "aria-label", "Details of " + name);
        builder.AddContent(9, "Details");
        builder.CloseElement();
    }
}
