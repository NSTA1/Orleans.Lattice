using Grpc.Core;
using Orleans.Lattice.Api.Auth;
using Orleans.Lattice.Explorer.Core.Navigation;

namespace Orleans.Lattice.Explorer.Access;

/// <summary>
/// The default <see cref="IAuthAdminCapabilityService"/>. Drives the
/// <see cref="IAuthAdminClient"/> probe surface and republishes a merged
/// <see cref="ExplorerCapabilities"/> into the <see cref="IExplorerCapabilityStore"/>.
/// The probe swallows a denial / transport failure and falls back to deny, so it
/// never breaks the shell.
/// </summary>
public sealed class AuthAdminCapabilityService(
    IAuthAdminClient client,
    IExplorerCapabilityStore store) : IAuthAdminCapabilityService
{
    private readonly IAuthAdminClient _client = client ?? throw new ArgumentNullException(nameof(client));
    private readonly IExplorerCapabilityStore _store = store ?? throw new ArgumentNullException(nameof(store));

    /// <inheritdoc />
    public async Task RefreshAsync(CancellationToken cancellationToken = default)
    {
        var allowed = await ProbeAsync(cancellationToken).ConfigureAwait(false);
        var current = _store.Current;
        _store.Set(current with { AuthAdminAllowed = allowed });
    }

    private async Task<bool> ProbeAsync(CancellationToken cancellationToken)
    {
        try
        {
            // A light, read-only list is the coarse gate: reaching it (even with an
            // empty page) means the control plane accepts the caller as an
            // administrator. It has no side effects, so it is safe to run on mount.
            await _client
                .ListUsersAsync(new AuthPageRequest { PageSize = 1 }, cancellationToken)
                .ConfigureAwait(false);
            return true;
        }
        catch (LatticeAuthorizationDeniedException)
        {
            return false;
        }
        catch (RpcException)
        {
            return false;
        }
        catch (InvalidOperationException)
        {
            // The explorer is not configured with an endpoint yet (no connection
            // client). Treat as deny; a later connection-status change re-probes.
            return false;
        }
    }
}
