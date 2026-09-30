using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Explorer.Core.Authentication;
using Orleans.Lattice.Explorer.Core.Configuration;
using Orleans.Lattice.Explorer.UI.Design.Components;
using Orleans.Lattice.Explorer.UI.Design.Tokens;

namespace Orleans.Lattice.Explorer.UI.Session;

/// <summary>
/// The sign-in dialog: every sign-in method the connected endpoint accepts that a
/// registered <see cref="IExplorerAuthMethod"/> can service, in the endpoint's
/// preference order.
/// </summary>
/// <remarks>
/// <para>
/// Discovery follows today's behaviour: the dialog asks Core which schemes the
/// endpoint advertises (<see cref="IExplorerAuthSession.DiscoverAsync"/>), and an
/// endpoint that advertises nothing, or cannot be reached, falls back to the
/// username and password flow. Which methods are offered is decided entirely by
/// Core's seam (<see cref="SessionSignInChoice"/>), so a custom method needs no
/// change here.
/// </para>
/// <para>
/// The only distinction the dialog draws is the one Core's contract draws: Core's
/// Basic method reads a username and password, and every other method runs its
/// own interactive challenge. On the web head the password form is a native post
/// to the server (<see cref="SessionSignInOptions"/>), so the password never
/// crosses the circuit.
/// </para>
/// <para>
/// Below the small breakpoint it opens as a full-screen sheet rather than a
/// centred dialog, so it works at a phone's width.
/// </para>
/// </remarks>
public partial class SignInDialog
{
    private readonly string _id = LtIds.Next("lt-signin");
    private SessionSignInChoice? _choice;
    private string _username = string.Empty;
    private string _password = string.Empty;
    private string? _error;
    private bool _busy;

    /// <summary>Raised when the dialog closes, after a successful in-circuit sign-in or on dismissal.</summary>
    [Parameter]
    public EventCallback OnClosed { get; set; }

    [Inject]
    private IExplorerAuthSession Auth { get; set; } = default!;

    [Inject]
    private IExplorerSession Explorer { get; set; } = default!;

    [Inject]
    private IEnumerable<IExplorerAuthMethod> Methods { get; set; } = [];

    [Inject]
    private SessionSignInOptions Options { get; set; } = default!;

    [CascadingParameter(Name = LtBreakpointCascade.Name)]
    private LtBreakpoint? Breakpoint { get; set; }

    private LtDialogPlacement Placement => SessionPresentation.DialogPlacement(Breakpoint);

    private string UsernameId => _id + "-username";

    private string PasswordId => _id + "-password";

    private bool ShowHeadings => _choice is { Methods.Count: > 1 };

    private string? Description => Explorer.Current?.Endpoint is { Length: > 0 } endpoint
        ? $"Sign in to the Lattice API at {endpoint}."
        : null;

    /// <inheritdoc />
    protected override async Task OnInitializedAsync()
    {
        ExplorerAuthSchemeAdvertisement advertisement;
        try
        {
            advertisement = await Auth.DiscoverAsync();
        }
        catch (Exception)
        {
            // Discovery degrading to "nothing advertised" is Core's documented
            // fallback, and it leaves the username and password flow on offer.
            advertisement = ExplorerAuthSchemeAdvertisement.Empty;
        }

        _choice = SessionSignInChoice.Resolve(advertisement, Methods);
    }

    private void OnUsernameInput(ChangeEventArgs args) => _username = args.Value?.ToString() ?? string.Empty;

    private void OnPasswordInput(ChangeEventArgs args) => _password = args.Value?.ToString() ?? string.Empty;

    private async Task SignInWithPasswordAsync()
    {
        if (_busy)
        {
            return;
        }

        if (string.IsNullOrWhiteSpace(_username))
        {
            _error = "Enter a username.";
            return;
        }

        await RunAsync(() => Auth.LoginAsync(_username.Trim(), _password));
    }

    private Task SignInWithMethodAsync(SessionSignInMethod method) =>
        _busy ? Task.CompletedTask : RunAsync(() => Auth.LoginWithMethodAsync(method.SchemeId));

    private async Task RunAsync(Func<Task> signIn)
    {
        _busy = true;
        _error = null;
        try
        {
            await signIn();
            await OnClosed.InvokeAsync();
        }
        catch (Exception ex)
        {
            _error = ex.Message;
        }
        finally
        {
            _busy = false;
        }
    }

    private Task CloseAsync() => OnClosed.InvokeAsync();

    private Task HandleOpenChangedAsync(bool open) => open ? Task.CompletedTask : OnClosed.InvokeAsync();
}
