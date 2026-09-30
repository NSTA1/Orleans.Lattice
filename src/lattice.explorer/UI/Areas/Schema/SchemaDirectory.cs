using Orleans.Lattice.Api.Schema;
using Orleans.Lattice.Schema;

namespace Orleans.Lattice.Explorer.UI.Areas.Schema;

/// <summary>
/// Which trees are under schema, and how: every logical tree's enforcement policy
/// and version config, read through <see cref="ILatticeSchemaControl"/>, plus the
/// schema declarations of installed apps. Read once per circuit and remembered
/// briefly.
/// </summary>
/// <remarks>
/// The facade answers per tree, so a listing asks each tree. It asks a bounded
/// number of trees (<see cref="MaximumInspected"/>) at a bounded concurrency
/// (<see cref="Concurrency"/>) and says so when a cluster holds more; any tree is
/// still reachable by its address. Everything remembered is keyed on the tenant
/// the circuit asserts, so a tenant switch reads again rather than listing one
/// tenant's trees under another.
/// </remarks>
internal sealed class SchemaDirectory
{
    /// <summary>How long a read listing is reused.</summary>
    public static readonly TimeSpan Freshness = TimeSpan.FromSeconds(30);

    /// <summary>The most trees one listing asks about.</summary>
    public const int MaximumInspected = 500;

    /// <summary>How many trees are asked about at once.</summary>
    public const int Concurrency = 8;

    private readonly SchemaFacades _facades;
    private readonly SchemaTreeCatalog _catalog;
    private readonly TimeProvider _time;
    private SchemaDirectoryRead? _read;
    private string? _readTenant;
    private IReadOnlyDictionary<string, SchemaAppDeclaration>? _declarations;
    private string? _declarationsTenant;

    /// <summary>Creates the directory over the circuit's facades and tree catalogue.</summary>
    /// <param name="facades">The area's facades.</param>
    /// <param name="catalog">The circuit's tree catalogue.</param>
    /// <param name="time">The clock freshness is measured on.</param>
    public SchemaDirectory(SchemaFacades facades, SchemaTreeCatalog catalog, TimeProvider time)
    {
        ArgumentNullException.ThrowIfNull(facades);
        ArgumentNullException.ThrowIfNull(catalog);
        ArgumentNullException.ThrowIfNull(time);
        _facades = facades;
        _catalog = catalog;
        _time = time;
    }

    /// <summary>
    /// The last listing read under the tenant the circuit asserts now, or
    /// <see langword="null"/> before the first; a listing read under another tenant
    /// is never returned.
    /// </summary>
    public SchemaDirectoryRead? Last => SameTenant(_readTenant, _facades.AssertedTenant) ? _read : null;

    /// <summary>Lists the trees and their schema state.</summary>
    /// <param name="refresh">Read again even when the remembered listing is fresh.</param>
    /// <param name="cancellationToken">Cancels the read.</param>
    /// <returns>The listing.</returns>
    /// <exception cref="InvalidOperationException">No cluster connection is configured.</exception>
    /// <exception cref="NotSupportedException">The head serves no schema administration.</exception>
    public async Task<SchemaDirectoryRead> GetAsync(bool refresh, CancellationToken cancellationToken)
    {
        var tenant = _facades.AssertedTenant;
        if (!refresh
            && _read is { } remembered
            && SameTenant(_readTenant, tenant)
            && _time.GetUtcNow() - remembered.ReadAt < Freshness)
        {
            return remembered;
        }

        var schema = _facades.RequireSchema();
        var trees = await _catalog.GetAsync(refresh, cancellationToken).ConfigureAwait(false);
        var declarations = await GetDeclarationsAsync(refresh, cancellationToken).ConfigureAwait(false);
        var inspected = trees.Count > MaximumInspected ? trees.Take(MaximumInspected).ToArray() : trees;
        var rows = new SchemaTreeRow[inspected.Count];

        await Parallel.ForEachAsync(
            Enumerable.Range(0, inspected.Count),
            new ParallelOptions { MaxDegreeOfParallelism = Concurrency, CancellationToken = cancellationToken },
            async (index, token) => rows[index] = await ReadRowAsync(schema, inspected[index], declarations, token).ConfigureAwait(false))
            .ConfigureAwait(false);

        var read = new SchemaDirectoryRead(
            rows,
            trees.Count,
            inspected.Count < trees.Count || _catalog.Truncated,
            _time.GetUtcNow());
        if (SameTenant(_facades.AssertedTenant, tenant))
        {
            _read = read;
            _readTenant = tenant;
        }

        return read;
    }

    /// <summary>Reads one tree's schema state afresh, and updates the remembered listing with it.</summary>
    /// <param name="treeId">The logical tree id.</param>
    /// <param name="cancellationToken">Cancels the read.</param>
    /// <returns>The tree's row.</returns>
    /// <exception cref="NotSupportedException">The head serves no schema administration.</exception>
    public async Task<SchemaTreeRow> ReadTreeAsync(string treeId, CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        var schema = _facades.RequireSchema();
        var declarations = await GetDeclarationsAsync(refresh: false, cancellationToken).ConfigureAwait(false);
        var row = await ReadRowAsync(schema, treeId, declarations, cancellationToken).ConfigureAwait(false);
        if (_read is { } read && SameTenant(_readTenant, _facades.AssertedTenant))
        {
            _read = read.Replace(row);
        }

        return row;
    }

    /// <summary>Whether <paramref name="treeId"/> is a tree in the catalogue.</summary>
    /// <param name="treeId">The logical tree id.</param>
    /// <param name="cancellationToken">Cancels the read.</param>
    /// <returns><see langword="true"/> when the tree exists.</returns>
    /// <exception cref="InvalidOperationException">No cluster connection is configured.</exception>
    public async Task<bool> ExistsAsync(string treeId, CancellationToken cancellationToken)
    {
        var trees = await _catalog.GetAsync(refresh: false, cancellationToken).ConfigureAwait(false);
        if (Contains(trees, treeId))
        {
            return true;
        }

        // A tree created since the last read: read the catalogue once more before saying no.
        trees = await _catalog.GetAsync(refresh: true, cancellationToken).ConfigureAwait(false);
        return Contains(trees, treeId);
    }

    /// <summary>Forgets the remembered listing and catalogue, so the next read goes to the cluster.</summary>
    public void Invalidate()
    {
        _read = null;
        _declarations = null;
        _catalog.Invalidate();
    }

    private static bool Contains(IReadOnlyList<string> trees, string treeId)
    {
        for (var i = 0; i < trees.Count; i++)
        {
            if (string.Equals(trees[i], treeId, StringComparison.Ordinal))
            {
                return true;
            }
        }

        return false;
    }

    private static async Task<SchemaTreeRow> ReadRowAsync(
        ILatticeSchemaControl schema,
        string treeId,
        IReadOnlyDictionary<string, SchemaAppDeclaration> declarations,
        CancellationToken cancellationToken)
    {
        LatticeSchemaPolicy? policy = null;
        var policyState = SchemaReadState.Read;
        try
        {
            policy = await schema.GetPolicyAsync(treeId, cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception exception)
        {
            policyState = Classify(exception);
        }

        LatticeSchemaVersionConfig? version = null;
        var versionState = SchemaReadState.Read;
        try
        {
            version = SchemaVersioning.Effective(await schema.GetVersionConfigAsync(treeId, cancellationToken).ConfigureAwait(false));
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception exception)
        {
            versionState = Classify(exception);
        }

        declarations.TryGetValue(treeId, out var declaration);
        return new SchemaTreeRow(treeId, policy, policyState, version, versionState, declaration);
    }

    private static SchemaReadState Classify(Exception exception) => exception switch
    {
        LatticeAuthorizationDeniedException => SchemaReadState.Denied,
        InvalidOperationException or NotSupportedException => SchemaReadState.Unavailable,
        _ => SchemaReadState.Failed,
    };

    private async Task<IReadOnlyDictionary<string, SchemaAppDeclaration>> GetDeclarationsAsync(bool refresh, CancellationToken cancellationToken)
    {
        var tenant = _facades.AssertedTenant;
        if (!refresh && _declarations is { } remembered && SameTenant(_declarationsTenant, tenant))
        {
            return remembered;
        }

        var declarations = new Dictionary<string, SchemaAppDeclaration>(StringComparer.Ordinal);
        if (_facades.Apps is { } apps)
        {
            try
            {
                var catalog = await apps.ListAsync(cancellationToken).ConfigureAwait(false);
                foreach (var app in catalog.Apps)
                {
                    var descriptor = await apps.DescribeAsync(app.Slug, app.Version, cancellationToken).ConfigureAwait(false);
                    if (descriptor is null)
                    {
                        continue;
                    }

                    foreach (var declared in descriptor.Schema)
                    {
                        if (string.IsNullOrEmpty(declared.Tree))
                        {
                            continue;
                        }

                        var treeId = SchemaAppDeclaration.TreeIdFor(app.Slug, declared.Tree);
                        declarations[treeId] = new SchemaAppDeclaration(
                            app.Slug,
                            app.Version,
                            treeId,
                            declared.Family,
                            declared.Version,
                            declared.StrictIngest);
                    }
                }
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception)
            {
                // The declaring app is a courtesy beside the tree's own schema state;
                // a caller who may not read the apps catalogue simply sees none.
            }
        }

        if (SameTenant(_facades.AssertedTenant, tenant))
        {
            _declarations = declarations;
            _declarationsTenant = tenant;
        }

        return declarations;
    }

    private static bool SameTenant(string? left, string? right) => string.Equals(left, right, StringComparison.Ordinal);
}
