using Orleans.Lattice.Explorer.Core.Authentication;
using Orleans.Lattice.Explorer.Core.Configuration;

namespace Orleans.Lattice.Explorer.Shell.Session;

/// <summary>
/// One circuit's session chrome: whether the Core connection and sign-in
/// services have been initialised, which session surface the circuit has asked
/// for, and whether the current sign-in has latched into re-authentication.
/// </summary>
/// <remarks>
/// <para>
/// It is the successor to the old <c>LoginDialogState</c>: any component that
/// needs a credential (an area whose gate reports that sign-in is required, or
/// the connection indicator after an authentication failure) calls
/// <see cref="OpenSignIn"/>, and the session overlay shows the one sign-in
/// dialog. It is registered scoped, so each circuit - each signed-in browser tab
/// - has its own, and it holds references only to the circuit's own scoped Core
/// services.
/// </para>
/// <para>
/// Core raises <see cref="IExplorerAuthSession.ReauthRequired"/> on a
/// thread-pool thread. This type records the latch and raises
/// <see cref="Changed"/>; components marshal back to their renderer with
/// <c>InvokeAsync</c>, as every Core event subscriber must.
/// </para>
/// </remarks>
internal sealed class SessionChromeState : IDisposable
{
    private readonly IExplorerSession _session;
    private readonly IExplorerAuthSession _auth;
    private Task? _initialization;

    /// <summary>Creates the circuit's session chrome over its Core session services.</summary>
    /// <param name="session">The circuit's connection configuration session.</param>
    /// <param name="auth">The circuit's sign-in session.</param>
    /// <exception cref="ArgumentNullException">Either argument is <see langword="null"/>.</exception>
    public SessionChromeState(IExplorerSession session, IExplorerAuthSession auth)
    {
        ArgumentNullException.ThrowIfNull(session);
        ArgumentNullException.ThrowIfNull(auth);
        _session = session;
        _auth = auth;
        _auth.ReauthRequired += OnReauthRequired;
        _auth.AuthenticationChanged += OnAuthenticationChanged;
    }

    /// <summary>Raised whenever any member of this state changes.</summary>
    public event Action? Changed;

    /// <summary>
    /// Whether the persisted configuration has been loaded and any stored
    /// credential applied, so the chrome can tell "not configured" from "not
    /// loaded yet".
    /// </summary>
    public bool IsInitialized { get; private set; }

    /// <summary>The session surface the circuit has asked for.</summary>
    public SessionOverlayKind Overlay { get; private set; }

    /// <summary>
    /// Whether the current sign-in can no longer be renewed silently and the
    /// operator must sign in again. It outranks every other session surface.
    /// </summary>
    public bool ReauthRequired { get; private set; }

    /// <summary>
    /// Loads the persisted configuration (connecting when it is valid) and then
    /// any stored credential. The first call does the work; every later call
    /// returns the same task.
    /// </summary>
    /// <param name="cancellationToken">Cancels the first call's work.</param>
    /// <returns>A task that completes once both Core sessions are initialised.</returns>
    public Task EnsureInitializedAsync(CancellationToken cancellationToken = default) =>
        _initialization ??= InitializeCoreAsync(cancellationToken);

    /// <summary>Asks for the connection settings.</summary>
    public void OpenConfiguration() => SetOverlay(SessionOverlayKind.Configuration);

    /// <summary>Asks for the sign-in dialog.</summary>
    public void OpenSignIn() => SetOverlay(SessionOverlayKind.SignIn);

    /// <summary>Closes whichever session surface is open.</summary>
    public void CloseOverlay() => SetOverlay(SessionOverlayKind.None);

    /// <inheritdoc />
    public void Dispose()
    {
        _auth.ReauthRequired -= OnReauthRequired;
        _auth.AuthenticationChanged -= OnAuthenticationChanged;
    }

    private async Task InitializeCoreAsync(CancellationToken cancellationToken)
    {
        await _session.InitializeAsync(cancellationToken);
        await _auth.InitializeAsync(cancellationToken);
        IsInitialized = true;
        Changed?.Invoke();
    }

    private void SetOverlay(SessionOverlayKind overlay)
    {
        if (Overlay == overlay)
        {
            return;
        }

        Overlay = overlay;
        Changed?.Invoke();
    }

    private void OnReauthRequired()
    {
        if (ReauthRequired)
        {
            return;
        }

        ReauthRequired = true;
        Overlay = SessionOverlayKind.None;
        Changed?.Invoke();
    }

    private void OnAuthenticationChanged()
    {
        // A fresh sign-in re-arms Core's latch, so it clears ours too.
        if (ReauthRequired && _auth.IsAuthenticated)
        {
            ReauthRequired = false;
        }

        Changed?.Invoke();
    }
}
