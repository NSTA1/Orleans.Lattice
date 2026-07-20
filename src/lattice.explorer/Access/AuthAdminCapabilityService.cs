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
        var snapshot = await ProbeAsync(cancellationToken).ConfigureAwait(false);
        var current = _store.Current;
        _store.Set(current with
        {
            AuthAdminAllowed = snapshot.Allowed,
            AuthDirectoryAvailable = snapshot.DirectoryAvailable,
            AuthAuthenticationMode = snapshot.AuthenticationMode,
        });
    }

    private async Task<AccessCapabilitySnapshot> ProbeAsync(CancellationToken cancellationToken)
    {
        var allowed = await ProbeAdminAsync(cancellationToken).ConfigureAwait(false);
        if (!allowed)
        {
            // Not an administrator (or the endpoint is unreachable): the access
            // model is admin-gated too, so there is nothing more to learn. Publish
            // the safe deny snapshot without a second round trip.
            return AccessCapabilitySnapshot.Denied;
        }

        return await ProbeAccessModelAsync(allowed, cancellationToken).ConfigureAwait(false);
    }

    private async Task<bool> ProbeAdminAsync(CancellationToken cancellationToken)
    {
        try
        {
            // A light, read-only list is the coarse gate: reaching it (even with an
            // empty page) means the control plane accepts the caller as an
            // administrator. It has no side effects, so it is safe to run on mount.
            await _client
                .ListGroupsAsync(new AuthPageRequest { PageSize = 1 }, cancellationToken)
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

    private async Task<AccessCapabilitySnapshot> ProbeAccessModelAsync(bool allowed, CancellationToken cancellationToken)
    {
        try
        {
            var model = await _client.GetAccessModelAsync(cancellationToken).ConfigureAwait(false);
            return new AccessCapabilitySnapshot(
                allowed,
                model.DirectoryAvailable,
                MapAuthenticationMode(model.AuthenticationMode));
        }
        catch (LatticeAuthorizationDeniedException)
        {
            return new AccessCapabilitySnapshot(allowed, false, ExplorerAccessAuthenticationMode.Unknown);
        }
        catch (RpcException)
        {
            return new AccessCapabilitySnapshot(allowed, false, ExplorerAccessAuthenticationMode.Unknown);
        }
        catch (InvalidOperationException)
        {
            return new AccessCapabilitySnapshot(allowed, false, ExplorerAccessAuthenticationMode.Unknown);
        }
    }

    private static ExplorerAccessAuthenticationMode MapAuthenticationMode(AccessAuthenticationMode mode) => mode switch
    {
        AccessAuthenticationMode.Anonymous => ExplorerAccessAuthenticationMode.Anonymous,
        AccessAuthenticationMode.Claims => ExplorerAccessAuthenticationMode.Claims,
        AccessAuthenticationMode.Basic => ExplorerAccessAuthenticationMode.Basic,
        _ => ExplorerAccessAuthenticationMode.Unknown,
    };

    /// <summary>
    /// The merged outcome of the Access-area capability probe: whether the caller
    /// is an administrator, whether a searchable identity directory is available,
    /// and the active authentication mode.
    /// </summary>
    private readonly record struct AccessCapabilitySnapshot(
        bool Allowed,
        bool DirectoryAvailable,
        ExplorerAccessAuthenticationMode AuthenticationMode)
    {
        /// <summary>The safe snapshot published when the caller is not an administrator or the endpoint is unreachable.</summary>
        public static AccessCapabilitySnapshot Denied { get; } =
            new(false, false, ExplorerAccessAuthenticationMode.Unknown);
    }
}
