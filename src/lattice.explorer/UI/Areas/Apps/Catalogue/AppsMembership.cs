using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Orleans.Lattice.Explorer.Core.Authentication;

namespace Orleans.Lattice.Explorer.UI.Areas.Apps.Catalogue;

/// <summary>
/// Reads which groups the caller is in, so the Apps area can say whether the caller
/// holds an app role. It never decides who holds a role: since issue #3902 a role is
/// held only through membership of the group it is bound to, and the caller's other
/// rights - administrator included - never add one.
/// </summary>
/// <remarks>
/// <para>
/// The read goes through the auth facade under the caller's own credential, so a
/// caller who may not read membership gets <see cref="AppsCallerGroups.Unknown"/>
/// rather than a guess. So does a sign-in whose shown name is not its subject id: only
/// a Basic sign-in names the subject the cluster authenticates; a token sign-in shows a
/// display name.
/// </para>
/// <para>
/// Nothing is remembered between reads, so a group just joined is seen on the next one.
/// </para>
/// </remarks>
/// <param name="facades">The circuit's facades.</param>
/// <param name="logger">Where a refused or failing read is reported.</param>
internal sealed class AppsMembership(AppsFacades facades, ILogger<AppsMembership>? logger = null)
{
    private readonly ILogger _logger = logger ?? NullLogger<AppsMembership>.Instance;

    /// <summary>Reads the caller's groups, or answers unknown when they cannot be read.</summary>
    /// <param name="cancellationToken">Cancels the read.</param>
    /// <returns>The caller's groups.</returns>
    /// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> was cancelled.</exception>
    public async Task<AppsCallerGroups> ReadAsync(CancellationToken cancellationToken = default)
    {
        var caller = facades.Caller;
        if (!caller.Authenticated
            || !string.Equals(caller.Scheme, ExplorerAuthSchemes.Basic, StringComparison.Ordinal)
            || string.IsNullOrWhiteSpace(caller.User)
            || facades.Auth is not { } auth)
        {
            return AppsCallerGroups.Unknown;
        }

        try
        {
            var groups = await auth.ListSubjectGroupsAsync(caller.User, cancellationToken).ConfigureAwait(false);
            return new AppsCallerGroups(caller.User, [.. groups ?? []]);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception error) when (error is not OutOfMemoryException)
        {
            _logger.LogInformation(error, "The Apps area could not read the caller's group membership; it is shown as unknown.");
            return AppsCallerGroups.Unknown;
        }
    }
}
