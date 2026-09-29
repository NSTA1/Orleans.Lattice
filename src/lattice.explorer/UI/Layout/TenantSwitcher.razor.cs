using Microsoft.AspNetCore.Components;
using Microsoft.AspNetCore.Components.Web;
using Orleans.Lattice.Explorer.UI.Design.Components;
using Orleans.Lattice.Explorer.UI.Navigation;

namespace Orleans.Lattice.Explorer.UI.Layout;

/// <summary>
/// The top bar's tenant switcher: the active tenant, and a keyboard-complete
/// type-ahead field that switches to any other tenant the caller can reach.
/// </summary>
/// <remarks>
/// <para>
/// It is absent - not disabled - unless the caller is signed in, tenancy is on,
/// the operator-gated switcher lets this caller switch, and there are at least two
/// tenants to choose between. The tenants come from the same accessible-tenant
/// source as the address line's <c>t/</c> completions and the Tenancy directory,
/// and are read again on every navigation, so a sign-in, a sign-out or a new
/// identity is reflected at once.
/// </para>
/// <para>
/// Choosing a tenant makes the same fail-closed switch as the address line: a
/// tenant-scoped address is re-rooted at the tenant, and a cluster-wide one stays
/// where it is while the tenant changes. A refusal shows the address line's notice.
/// </para>
/// <para>
/// In the header it is a disclosure button: Enter or a click opens the field and
/// moves focus into it, Down lists the tenants, Enter switches, and Escape closes
/// the panel and returns focus to the button. <see cref="Stacked"/> renders the
/// field alone, for the compact directory sheet.
/// </para>
/// </remarks>
public partial class TenantSwitcher : IDisposable
{
    /// <summary>The most tenants the field lists at once; typing narrows the rest.</summary>
    internal const int Limit = 20;

    private readonly string _panelId = LtIds.Next("lt-shell-tenant-panel");
    private readonly CancellationTokenSource _lifetime = new();
    private TenantSwitchChoices _choices = TenantSwitchChoices.None;
    private ExplorerLocation? _readFor;
    private ElementReference _toggle;
    private LtComboBox? _field;
    private bool _open;
    private bool _focusField;
    private bool _focusToggle;
    private bool _mounted;

    /// <summary>
    /// Whether to render the field alone, full width, as the compact directory
    /// sheet does, rather than behind the header's disclosure button.
    /// </summary>
    [Parameter]
    public bool Stacked { get; set; }

    [CascadingParameter]
    internal ExplorerLocation? Location { get; set; }

    [Inject]
    internal ExplorerTenantSwitch Switch { get; set; } = default!;

    [Inject]
    internal ShellChromeInterop Interop { get; set; } = default!;

    private string? ActiveHint => _choices.Active is { } active ? "Active tenant: " + active + "." : null;

    /// <summary>Stops listening for the palette's open requests.</summary>
    public void Dispose()
    {
        Switch.OpenRequested -= OnOpenRequested;
        _lifetime.Cancel();
        _lifetime.Dispose();
        GC.SuppressFinalize(this);
    }

    /// <inheritdoc />
    protected override void OnInitialized() => Switch.OpenRequested += OnOpenRequested;

    /// <inheritdoc />
    protected override async Task OnParametersSetAsync()
    {
        // Every navigation, sign-in, sign-out and switch hands the chrome a new
        // location, and each is a reason the offer may have changed.
        if (_readFor is not null && ReferenceEquals(_readFor, Location))
        {
            return;
        }

        _readFor = Location;
        try
        {
            _choices = await Switch.RefreshAsync(_lifetime.Token);
        }
        catch (OperationCanceledException)
        {
            _choices = TenantSwitchChoices.None;
        }

        if (!_choices.Offered)
        {
            _open = false;
        }
    }

    /// <inheritdoc />
    protected override async Task OnAfterRenderAsync(bool firstRender)
    {
        if (firstRender)
        {
            _mounted = true;
        }

        // The compact sheet mounts this field in answer to the palette, and its
        // offer may arrive a render later than the sheet itself.
        if (Stacked && _field is not null && Switch.IsOpenRequested && Switch.TryTakeOpenRequest())
        {
            _focusField = true;
        }

        if (_focusField && _field is not null)
        {
            _focusField = false;
            await _field.FocusAsync();
        }
        else if (_focusToggle)
        {
            _focusToggle = false;
            await Interop.FocusAsync(_toggle);
        }
    }

    private void Toggle()
    {
        _open = !_open;
        _focusField = _open;
    }

    private async Task OnPanelKeyDownAsync(KeyboardEventArgs args)
    {
        // The field keeps Escape while its list is open; once it is closed, Escape
        // closes the panel and puts focus back on the button.
        if (args.Key == "Escape")
        {
            _open = false;
            await Interop.FocusAsync(_toggle);
        }
    }

    private async Task ChooseAsync(string tenant)
    {
        if (string.IsNullOrEmpty(tenant))
        {
            return;
        }

        _open = false;
        _focusToggle = !Stacked;
        await Switch.SwitchAsync(tenant, _lifetime.Token);
    }

    private void OnOpenRequested() => _ = InvokeAsync(() =>
    {
        // Exactly one switcher answers: the mounted one that takes the request.
        if (!_mounted || !_choices.Offered || !Switch.TryTakeOpenRequest())
        {
            return;
        }

        _open = !Stacked;
        _focusField = true;
        StateHasChanged();
    });
}
