using Microsoft.AspNetCore.Components;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Explorer.Core.Authentication;
using Orleans.Lattice.Explorer.Core.Configuration;
using Orleans.Lattice.Explorer.Core.Tenancy;
using Orleans.Lattice.Explorer.Shell.Design.Components;
using Orleans.Lattice.Explorer.Shell.Design.Tokens;

namespace Orleans.Lattice.Explorer.Shell.Session;

/// <summary>
/// The identity menu, rendered in the <c>header.identity</c> chrome slot: a Sign
/// in button when anonymous, and otherwise the signed-in display name, which
/// opens the session's details - the tenant when tenancy is on, the cluster -
/// with Reset view and Sign out.
/// </summary>
/// <remarks>
/// <para>
/// Sign out follows <see cref="SessionSignOut.Resolve"/>: a federated sign-out
/// endpoint registered through <see cref="ExplorerSignOutOptions"/> wins, so a
/// hosted-web head ends the identity-provider session too; otherwise the web
/// head posts to its local sign-out endpoint.
/// </para>
/// <para>
/// Below the small breakpoint the header folds into its overflow menu, and the
/// menu renders there folded: the same details and actions in place, with no
/// dialog of its own, because a dialog inside the overflow sheet would stack
/// one modal on another.
/// </para>
/// <para>
/// The tenant row appears only when the host registered Core's tenant view and
/// it is active. With no tenant established it says so, rather than implying the
/// caller sees everything: the view fails closed.
/// </para>
/// </remarks>
public partial class IdentityMenu
{
    private readonly EventCallback<bool> _openChanged;
    private SessionSignOutTarget _signOut;
    private bool _open;
    private bool _busy;

    /// <summary>Creates the menu, binding its dialog callback once rather than per render.</summary>
    public IdentityMenu() => _openChanged = EventCallback.Factory.Create<bool>(this, open => _open = open);

    [Inject]
    private IExplorerAuthSession Auth { get; set; } = default!;

    [Inject]
    private IExplorerSession Explorer { get; set; } = default!;

    [Inject]
    private SessionChromeState State { get; set; } = default!;

    [Inject]
    private SessionSignInOptions Options { get; set; } = default!;

    [Inject]
    private IServiceProvider Services { get; set; } = default!;

    [CascadingParameter(Name = LtBreakpointCascade.Name)]
    private LtBreakpoint? Breakpoint { get; set; }

    private bool Folded => SessionPresentation.IsFolded(Breakpoint);

    private IExplorerTenantView? TenantView { get; set; }

    /// <inheritdoc />
    public void Dispose()
    {
        Auth.AuthenticationChanged -= OnChanged;
        State.Changed -= OnChanged;
    }

    /// <inheritdoc />
    protected override void OnInitialized()
    {
        _signOut = SessionSignOut.Resolve(Options, Services.GetService<ExplorerSignOutOptions>());
        TenantView = Services.GetService<IExplorerTenantView>();
        Auth.AuthenticationChanged += OnChanged;
        State.Changed += OnChanged;
    }

    private void OpenSignIn() => State.OpenSignIn();

    private void Open() => _open = true;

    private async Task SignOutAsync()
    {
        if (_busy)
        {
            return;
        }

        _busy = true;
        try
        {
            await Auth.LogoutAsync();
            _open = false;
        }
        finally
        {
            _busy = false;
        }
    }

    private void OnChanged() => _ = InvokeAsync(StateHasChanged);
}
