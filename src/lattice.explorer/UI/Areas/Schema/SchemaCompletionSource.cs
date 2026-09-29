using Orleans.Lattice.Explorer.UI.Navigation;

namespace Orleans.Lattice.Explorer.UI.Areas.Schema;

/// <summary>
/// Completes tree names in schema scope for the address line: the trees under a
/// schema policy, a version config or an app declaration, from the directory's
/// remembered listing, so typing never pages the catalogue more than once per
/// listing lifetime.
/// </summary>
internal sealed class SchemaCompletionSource : IAddressCompletionSource
{
    private readonly SchemaDirectory _directory;

    /// <summary>Creates the source over the circuit's directory.</summary>
    /// <param name="directory">The circuit's schema directory.</param>
    public SchemaCompletionSource(SchemaDirectory directory)
    {
        ArgumentNullException.ThrowIfNull(directory);
        _directory = directory;
    }

    /// <inheritdoc />
    public async ValueTask<IReadOnlyList<AddressCompletion>> CompleteAsync(AddressQuery query, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(query);

        var read = await _directory.GetAsync(refresh: false, cancellationToken).ConfigureAwait(false);
        var tenant = query.Current.Tenant;
        var results = new List<AddressCompletion>(query.Limit);
        foreach (var row in read.Governed)
        {
            if (results.Count >= query.Limit)
            {
                break;
            }

            var target = SchemaAddresses.Tree(row.TreeId);
            var matches = query.Mode switch
            {
                AddressQueryMode.Search => row.TreeId.Contains(query.Text, StringComparison.OrdinalIgnoreCase),
                AddressQueryMode.App => row.Declaration is { } declaration
                    && declaration.Slug.StartsWith(query.Text, StringComparison.OrdinalIgnoreCase),
                AddressQueryMode.Address => target.Format().StartsWith(query.Text, StringComparison.OrdinalIgnoreCase),
                _ => false,
            };

            if (matches)
            {
                results.Add(new AddressCompletion(row.TreeId, target.WithTenant(tenant), Detail(row)));
            }
        }

        return results;
    }

    private static string Detail(SchemaTreeRow row) => row switch
    {
        { Policy: not null, Version: not null } => "Schema policy and versioning",
        { Policy: not null } => "Schema policy",
        { Version: not null } => "Schema versioning",
        _ => "Schema declared by an app",
    };
}
