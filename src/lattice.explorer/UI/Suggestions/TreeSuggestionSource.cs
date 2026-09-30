using Orleans.Lattice.Explorer.UI.Areas.Data;
using Orleans.Lattice.Explorer.UI.Design.Components;
using Orleans.Lattice.Explorer.UI.Navigation;

namespace Orleans.Lattice.Explorer.UI.Suggestions;

/// <summary>
/// The trees the caller can reach, by logical id: the same tenant-scoped
/// catalogue the Data area reads (<see cref="DataDirectory"/>), so a picker never
/// offers a tree the caller cannot see and never pays for the catalogue twice.
/// </summary>
/// <remarks>
/// With tenancy on, only the active tenant's trees are offered, together with the
/// trees other tenants share with it through a grant it approved (marked with
/// their owner and access). The projection
/// onto suggestions is remembered per loaded catalogue and tenant, so typing
/// matches a ready list and allocates only the bounded answer. Views are left out:
/// every field that names a tree acts on a tree, and so are shared prefixes, which
/// name no tree.
/// </remarks>
/// <param name="directory">The circuit's tree catalogue.</param>
/// <param name="tenancy">Whether, and to which tenant, the Explorer is scoped.</param>
internal sealed class TreeSuggestionSource(DataDirectory directory, ExplorerTenancy tenancy) : ILtSuggestionSource
{
    /// <summary>The note shown when the catalogue cannot be read.</summary>
    public const string UnavailableReason = "Trees could not be listed, so the id is used as typed.";

    private Projection? _projection;

    /// <inheritdoc />
    public async ValueTask<LtSuggestionSet> SuggestAsync(string text, int limit, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(text);
        IReadOnlyList<DataTreeEntry> entries;
        try
        {
            entries = await directory.LoadAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception)
        {
            return LtSuggestionSet.Unavailable(UnavailableReason);
        }

        return SuggestionMatcher.Match(Project(entries), text, limit);
    }

    private IReadOnlyList<LtSuggestion> Project(IReadOnlyList<DataTreeEntry> entries)
    {
        // With tenancy on and no tenant resolved yet, nothing matches: fail closed.
        var scoped = tenancy.IsActive;
        var tenant = scoped ? tenancy.ActiveTenant : null;
        if (_projection is { } projection
            && ReferenceEquals(projection.Entries, entries)
            && projection.Scoped == scoped
            && string.Equals(projection.Tenant, tenant, StringComparison.Ordinal))
        {
            return projection.Values;
        }

        var values = new List<LtSuggestion>(entries.Count);
        foreach (var entry in entries)
        {
            if (entry.Kind == DataTreeKind.Tree
                && (!scoped || (tenant is not null && string.Equals(entry.Tenant, tenant, StringComparison.Ordinal))))
            {
                values.Add(new LtSuggestion(entry.LogicalId, entry.IsShared
                    ? $"{entry.SharedText}, {entry.AccessText!.ToLowerInvariant()}"
                    : entry.KindText));
            }
        }

        _projection = new Projection(entries, scoped, tenant, values);
        return values;
    }

    /// <summary>A catalogue projected onto suggestions for one tenant.</summary>
    private sealed record Projection(IReadOnlyList<DataTreeEntry> Entries, bool Scoped, string? Tenant, IReadOnlyList<LtSuggestion> Values);
}
