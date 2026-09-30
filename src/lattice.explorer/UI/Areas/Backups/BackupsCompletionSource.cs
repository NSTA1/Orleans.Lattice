using Orleans.Lattice.Api.Backup;
using Orleans.Lattice.Explorer.UI.Navigation;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Explorer.UI.Transport;

namespace Orleans.Lattice.Explorer.UI.Areas.Backups;

/// <summary>
/// The address line's completions for the Backups area: <c>backup:{id}</c>
/// completes catalogued backup ids by prefix, an address under
/// <c>/backups/</c> completes the same way, and free text completes backup
/// names by prefix. Only backups the caller may read are ever listed, because
/// the facade hides the rest.
/// </summary>
internal sealed class BackupsCompletionSource : IAddressCompletionSource
{
    /// <summary>The token that asks for a backup by id.</summary>
    public const string IdToken = "backup:";

    /// <summary>How many catalogued ids a single id completion reads at most.</summary>
    internal const int MaximumScanned = 2000;

    private static readonly string AddressPrefix = "/" + BackupsAddresses.AreaKey + "/";

    private readonly ILatticeBackupControl _control;
    private readonly ShellAssertedTenant _tenant;

    /// <summary>Creates the source over the circuit's backup facade.</summary>
    /// <param name="control">The backup facade.</param>
    /// <param name="tenant">The circuit's asserted tenant: only the listing tenant's own backups complete.</param>
    public BackupsCompletionSource([FromKeyedServices(ShellFacades.Key)] ILatticeBackupControl control, ShellAssertedTenant? tenant = null)
    {
        ArgumentNullException.ThrowIfNull(control);
        _control = control;
        _tenant = tenant ?? ShellAssertedTenant.None;
    }

    /// <inheritdoc />
    public async ValueTask<IReadOnlyList<AddressCompletion>> CompleteAsync(AddressQuery query, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(query);
        var text = query.Text.Trim();

        if (query.Mode == AddressQueryMode.Address)
        {
            return text.StartsWith(AddressPrefix, StringComparison.OrdinalIgnoreCase) && text.Length > AddressPrefix.Length
                ? await ByIdAsync(text[AddressPrefix.Length..], query.Limit, cancellationToken).ConfigureAwait(false)
                : [];
        }

        if (query.Mode != AddressQueryMode.Search || text.Length == 0)
        {
            return [];
        }

        if (text.StartsWith(IdToken, StringComparison.OrdinalIgnoreCase))
        {
            return await ByIdAsync(text[IdToken.Length..], query.Limit, cancellationToken).ConfigureAwait(false);
        }

        var listing = _tenant.ListingTenant;
        var page = await _control.ListBackupsAsync(
            BackupsAccess.Narrow(new BackupCatalogRequest { PageSize = query.Limit, OrderByCreatedDescending = true, NamePrefix = text }, listing),
            cancellationToken).ConfigureAwait(false);
        return [.. page.Entries.Where(manifest => BackupsAccess.Lists(listing, manifest)).Take(query.Limit).Select(Completion)];
    }

    private async Task<IReadOnlyList<AddressCompletion>> ByIdAsync(string prefix, int limit, CancellationToken cancellationToken)
    {
        prefix = prefix.Trim().ToLowerInvariant();
        var found = new List<AddressCompletion>(Math.Min(limit, 8));
        var scanned = 0;
        var listing = _tenant.ListingTenant;
        await foreach (var manifest in _control.StreamBackupsAsync(cancellationToken).ConfigureAwait(false))
        {
            if (manifest.Id.StartsWith(prefix, StringComparison.Ordinal) && BackupsAccess.Lists(listing, manifest))
            {
                found.Add(Completion(manifest));
                if (found.Count >= limit)
                {
                    break;
                }
            }
            else if (prefix.Length > 0 && string.CompareOrdinal(manifest.Id, prefix) > 0)
            {
                // The stream is in id order, so nothing after this can match.
                break;
            }

            if (++scanned >= MaximumScanned)
            {
                break;
            }
        }

        return found;
    }

    private static AddressCompletion Completion(Orleans.Lattice.Backup.BackupManifest manifest) =>
        new(
            IdToken + manifest.Id,
            BackupsAddresses.Backup(manifest.Id),
            BackupsFormat.Name(manifest) + " - " + BackupTreeName.Parse(manifest.Scope.TreeId).Name);
}
