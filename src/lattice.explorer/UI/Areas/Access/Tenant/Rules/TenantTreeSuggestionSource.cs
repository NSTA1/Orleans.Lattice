using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Explorer.UI.Areas.Data;
using Orleans.Lattice.Explorer.UI.Design.Components;
using Orleans.Lattice.Explorer.UI.Suggestions;

namespace Orleans.Lattice.Explorer.UI.Areas.Access.Tenant.Rules;

/// <summary>
/// The trees a tenant rule may govern, by the tenant-local name the tenant policy
/// takes: the tenant's own trees from the same tenant-scoped catalogue the Data
/// area reads (<see cref="DataDirectory"/>), never an app-owned, reserved or
/// system tree, a tree another tenant shares, or another tenant's tree.
/// </summary>
/// <remarks>
/// One per circuit. It remembers nothing itself: the list is the caller-keyed
/// catalogue's, filtered as typed, so no answer can outlive the caller or tenant
/// it was read for. A head without the catalogue yields an unavailable source, and
/// the field is free text that the editor still checks and the server confines.
/// </remarks>
/// <param name="services">The circuit's services, from which the catalogue is resolved on first use.</param>
/// <param name="tenant">The tenant the page's address is rooted at, read at query time; <see langword="null"/> offers nothing.</param>
internal sealed class TenantTreeSuggestionSource(IServiceProvider services, Func<string?> tenant) : ILtSuggestionSource
{
    /// <summary>The note shown when the tenant's trees cannot be listed.</summary>
    public const string UnavailableReason = "The tenant's trees could not be listed, so the name is used as typed.";

    /// <summary>The detail each offered tree carries.</summary>
    public const string Detail = "Tree";

    private readonly IServiceProvider _services = services ?? throw new ArgumentNullException(nameof(services));
    private readonly Func<string?> _tenant = tenant ?? throw new ArgumentNullException(nameof(tenant));

    /// <inheritdoc />
    public async ValueTask<LtSuggestionSet> SuggestAsync(string text, int limit, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(text);
        IReadOnlyList<DataTreeEntry> entries;
        try
        {
            var directory = _services.GetService<DataDirectory>();
            if (directory is null)
            {
                return LtSuggestionSet.Unavailable(UnavailableReason);
            }

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

        return SuggestionMatcher.Match(Project(entries, _tenant()), text, limit);
    }

    /// <summary>Whether a catalogue entry is one of <paramref name="tenant"/>'s trees a tenant rule may govern.</summary>
    /// <param name="entry">The catalogue entry.</param>
    /// <param name="tenant">The tenant.</param>
    /// <returns><see langword="true"/> for one of the tenant's own, ordinary trees.</returns>
    internal static bool Offers(DataTreeEntry entry, string tenant)
    {
        ArgumentNullException.ThrowIfNull(entry);
        return entry.Kind == DataTreeKind.Tree
            && !entry.IsShared
            && entry.AppSlug is null
            && string.Equals(entry.Tenant, tenant, StringComparison.Ordinal)
            && TenantRuleFormat.TreeProblem(entry.LogicalId) is null;
    }

    private static List<LtSuggestion> Project(IReadOnlyList<DataTreeEntry> entries, string? tenant)
    {
        var values = new List<LtSuggestion>();
        if (tenant is null)
        {
            return values;
        }

        foreach (var entry in entries)
        {
            if (Offers(entry, tenant))
            {
                values.Add(new LtSuggestion(entry.LogicalId, Detail));
            }
        }

        return values;
    }
}
