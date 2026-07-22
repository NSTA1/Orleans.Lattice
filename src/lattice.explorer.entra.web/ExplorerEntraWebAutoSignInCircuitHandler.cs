using Microsoft.AspNetCore.Components.Authorization;
using Microsoft.AspNetCore.Components.Server.Circuits;
using Microsoft.Extensions.Logging;
using Orleans.Lattice.Explorer.Core.Authentication;

namespace Orleans.Lattice.Explorer.Entra.Web;

/// <summary>
/// Completes the State API sign-in automatically for an already
/// browser-authenticated user when a Blazor Server circuit opens, so the console
/// connects without a manual "Sign in with Entra ID" click. Best-effort: it
/// discovers the endpoint's advertised scheme and drives the <c>entra</c> method,
/// but any failure (the endpoint is unreachable, advertises nothing, or the token
/// exchange fails) is swallowed and the user simply falls back to the interactive
/// sign-in dialog.
/// </summary>
internal sealed class ExplorerEntraWebAutoSignInCircuitHandler : CircuitHandler
{
    private readonly IExplorerAuthSession _session;
    private readonly AuthenticationStateProvider _authenticationStateProvider;
    private readonly ILogger<ExplorerEntraWebAutoSignInCircuitHandler> _logger;

    public ExplorerEntraWebAutoSignInCircuitHandler(
        IExplorerAuthSession session,
        AuthenticationStateProvider authenticationStateProvider,
        ILogger<ExplorerEntraWebAutoSignInCircuitHandler> logger)
    {
        ArgumentNullException.ThrowIfNull(session);
        ArgumentNullException.ThrowIfNull(authenticationStateProvider);
        ArgumentNullException.ThrowIfNull(logger);
        _session = session;
        _authenticationStateProvider = authenticationStateProvider;
        _logger = logger;
    }

    public override async Task OnConnectionUpAsync(Circuit circuit, CancellationToken cancellationToken)
    {
        try
        {
            if (_session.IsAuthenticated)
            {
                return;
            }

            var state = await _authenticationStateProvider.GetAuthenticationStateAsync().ConfigureAwait(false);
            if (state.User?.Identity is not { IsAuthenticated: true })
            {
                // Anonymous circuit. When a fallback authorization policy is in
                // force it will already have redirected a real page request, so an
                // anonymous circuit here usually means the browser is genuinely not
                // signed in. But it is also the exact symptom of a miswired host
                // (no AddCascadingAuthenticationState, so the circuit never sees the
                // OIDC-authenticated user): the console renders, yet every cluster
                // call is anonymous with no other trace. Log it so that failure mode
                // is diagnosable instead of silent.
                _logger.LogWarning(
                    "Blazor Server circuit is anonymous on connection up; skipping automatic Entra State API sign-in. " +
                    "The console will make cluster calls anonymously. If the browser is signed in, verify the host wires " +
                    "AddCascadingAuthenticationState() so the circuit sees the authenticated user.");
                return;
            }

            // Ensure any stored credential is loaded and the endpoint's advertised
            // scheme is known before driving the challenge; both are idempotent.
            await _session.InitializeAsync(cancellationToken).ConfigureAwait(false);
            if (_session.IsAuthenticated)
            {
                return;
            }

            var advertisement = await _session.DiscoverAsync(cancellationToken).ConfigureAwait(false);
            if (!advertisement.Schemes.Any(s => string.Equals(s.SchemeId, ExplorerAuthSchemes.Entra, StringComparison.OrdinalIgnoreCase)))
            {
                _logger.LogDebug(
                    "State API endpoint does not advertise the '{Scheme}' scheme; leaving the connection anonymous.",
                    ExplorerAuthSchemes.Entra);
                return;
            }

            await _session.LoginWithMethodAsync(ExplorerAuthSchemes.Entra, cancellationToken: cancellationToken).ConfigureAwait(false);
            _logger.LogInformation("Automatic Entra State API sign-in completed for the authenticated browser user.");
        }
        catch (Exception ex)
        {
            // Never break the page: degrade to the interactive sign-in dialog.
            _logger.LogWarning(ex, "Automatic Entra State API sign-in failed; falling back to the interactive dialog.");
        }
    }
}
