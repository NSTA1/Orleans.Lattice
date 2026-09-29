using Orleans.Lattice.Explorer.Core.Data;
using Orleans.Lattice.Explorer.UI.Navigation;
using Orleans.Lattice.Explorer.UI.Navigation.Address;

namespace Orleans.Lattice.Explorer.UI.Areas.Data;

/// <summary>
/// The Data area's answers in the address line: trees and views by logical id
/// (prefix-matched, bounded), app trees after <c>a/</c>, literal <c>/data/...</c>
/// addresses, and - while a tree's keys are open - key prefixes read from that
/// tree. Every answer names the logical id only.
/// </summary>
internal sealed class DataCompletionSource : IAddressCompletionSource
{
    /// <summary>The most keys one completion reads from the open tree.</summary>
    public const int KeyCompletionLimit = 8;

    private const string AreaSegment = "/" + DataArea.AreaKey;

    private readonly DataDirectory _directory;
    private readonly IServiceProvider _services;

    /// <summary>Creates the source.</summary>
    /// <param name="directory">The circuit's tree directory.</param>
    /// <param name="services">The circuit's services, from which the data reader is resolved lazily.</param>
    public DataCompletionSource(DataDirectory directory, IServiceProvider services)
    {
        ArgumentNullException.ThrowIfNull(directory);
        ArgumentNullException.ThrowIfNull(services);
        _directory = directory;
        _services = services;
    }

    /// <inheritdoc />
    public async ValueTask<IReadOnlyList<AddressCompletion>> CompleteAsync(AddressQuery query, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(query);
        var results = new List<AddressCompletion>(query.Limit);
        switch (query.Mode)
        {
            case AddressQueryMode.Search when query.Text.Length > 0:
                if (IsKeysScope(query.Current))
                {
                    await AddKeysAsync(results, query.Current, query.Text, KeyCompletionLimit, cancellationToken).ConfigureAwait(false);
                }

                await AddTreesAsync(results, query.Text, query.Limit, cancellationToken).ConfigureAwait(false);
                break;

            case AddressQueryMode.App:
                await AddTreesAsync(results, DataTreeNames.AppPrefix + query.Text, query.Limit, cancellationToken).ConfigureAwait(false);
                break;

            case AddressQueryMode.Address when TryReadAddress(query.Text, out var treeText, out var keyText):
                if (keyText is not null)
                {
                    if (ExplorerAddress.TryParse(query.Text[..query.Text.IndexOf('?', StringComparison.Ordinal)], out var tree) && tree.TreeId is not null)
                    {
                        await AddKeysAsync(results, tree, keyText, query.Limit, cancellationToken).ConfigureAwait(false);
                    }
                }
                else
                {
                    await AddTreesAsync(results, treeText, query.Limit, cancellationToken).ConfigureAwait(false);
                }

                break;
        }

        return results;
    }

    private static bool IsKeysScope(ExplorerAddress current) =>
        string.Equals(current.Area, DataArea.AreaKey, StringComparison.Ordinal)
        && current.TreeId is not null
        && DataTabs.Parse(current.GetQuery(DataTabs.TabQuery)) == DataTabs.Keys;

    private static bool TryReadAddress(string raw, out string treeText, out string? keyText)
    {
        treeText = string.Empty;
        keyText = null;
        var text = raw.Trim();
        if (text.StartsWith("/" + ExplorerAddress.TenantSegment + "/", StringComparison.OrdinalIgnoreCase))
        {
            var tenantEnd = text.IndexOf('/', 3);
            if (tenantEnd < 0)
            {
                return false;
            }

            text = text[tenantEnd..];
        }

        if (!text.StartsWith(AreaSegment + "/", StringComparison.OrdinalIgnoreCase))
        {
            return false;
        }

        var rest = text[(AreaSegment.Length + 1)..];
        var question = rest.IndexOf('?', StringComparison.Ordinal);
        if (question < 0)
        {
            treeText = Uri.UnescapeDataString(rest);
            return true;
        }

        treeText = Uri.UnescapeDataString(rest[..question]);
        foreach (var pair in rest[(question + 1)..].Split('&'))
        {
            var equals = pair.IndexOf('=', StringComparison.Ordinal);
            var name = equals < 0 ? pair : pair[..equals];
            if (string.Equals(name, ExplorerAddress.PrefixQuery, StringComparison.OrdinalIgnoreCase)
                || string.Equals(name, ExplorerAddress.KeyQuery, StringComparison.OrdinalIgnoreCase))
            {
                keyText = equals < 0 ? string.Empty : Uri.UnescapeDataString(pair[(equals + 1)..]);
            }
        }

        return true;
    }

    private async Task AddTreesAsync(List<AddressCompletion> results, string text, int limit, CancellationToken cancellationToken)
    {
        var entries = await _directory.LoadAsync(cancellationToken).ConfigureAwait(false);
        foreach (var entry in entries)
        {
            if (results.Count >= limit)
            {
                return;
            }

            if (entry.LogicalId.StartsWith(text, StringComparison.OrdinalIgnoreCase)
                || (entry.ViewName is { } view && view.StartsWith(text, StringComparison.OrdinalIgnoreCase)))
            {
                results.Add(new AddressCompletion(entry.LogicalId, entry.Address, Describe(entry)));
            }
        }
    }

    private async Task AddKeysAsync(List<AddressCompletion> results, ExplorerAddress treeAddress, string prefix, int limit, CancellationToken cancellationToken)
    {
        if (DataServices.Find<IDataReader>(_services) is not { } reader
            || await _directory.ResolveAsync(treeAddress, cancellationToken).ConfigureAwait(false) is not { } tree)
        {
            return;
        }

        var page = await reader.ScanAsync(tree.StateId, DataPaging.Increment, keyPrefix: prefix, cancellationToken: cancellationToken).ConfigureAwait(false);
        var workspace = tree.Address.WithQuery(DataTabs.TabQuery, null);
        foreach (var entry in page.Entries)
        {
            if (results.Count >= limit)
            {
                break;
            }

            results.Add(new AddressCompletion(entry.Key, workspace.WithQuery(ExplorerAddress.KeyQuery, entry.Key), "key in " + tree.LogicalId));
        }

        _ = CancelQuietlyAsync(reader, tree.StateId, page.ContinuationToken);
    }

    private static async Task CancelQuietlyAsync(IDataReader reader, string stateId, string? token)
    {
        if (string.IsNullOrEmpty(token))
        {
            return;
        }

        try
        {
            await reader.CancelScanAsync(stateId, token).ConfigureAwait(false);
        }
        catch (Exception)
        {
            // Releasing a live cursor is best effort; the server reaps it anyway.
        }
    }

    private static string Describe(DataTreeEntry entry) => entry.AppSlug is { } slug
        ? $"{entry.KindText} of app {slug}"
        : entry.KindText;
}
