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
                // Anonymous circuit: the fallback authorization policy will have
                // redirected a real page request, so there is nothing to do here.
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
                return;
            }

            await _session.LoginWithMethodAsync(ExplorerAuthSchemes.Entra, cancellationToken: cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            // Never break the page: degrade to the interactive sign-in dialog.
            _logger.LogWarning(ex, "Automatic Entra State API sign-in failed; falling back to the interactive dialog.");
        }
    }
}
