using Orleans.Lattice.Explorer.UI.Design.Components;
using Orleans.Lattice.Explorer.UI.Suggestions;

namespace Orleans.Lattice.Explorer.UI.Areas.Cluster;

/// <summary>
/// The trees the Cluster area administers, by the logical id the area shows and
/// its facades take: the same remembered catalogue its tree list reads
/// (<see cref="ClusterTreeCatalog"/>), which is keyed on the asserted tenant.
/// </summary>
/// <remarks>
/// The projection onto suggestions is remembered per catalogue read, so typing
/// matches a ready list and allocates only the bounded answer.
/// </remarks>
/// <param name="catalog">The circuit's Cluster tree catalogue.</param>
internal sealed class ClusterTreeSuggestionSource(ClusterTreeCatalog catalog) : ILtSuggestionSource
{
    /// <summary>The note shown when the catalogue cannot be read.</summary>
    public const string UnavailableReason = "The cluster's trees could not be listed, so the id is used as typed.";

    private (IReadOnlyList<ClusterTreeEntry> Trees, IReadOnlyList<LtSuggestion> Values)? _projection;

    /// <inheritdoc />
    public async ValueTask<LtSuggestionSet> SuggestAsync(string text, int limit, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(text);
        IReadOnlyList<ClusterTreeEntry> trees;
        try
        {
            trees = await catalog.GetAsync(refresh: false, cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception)
        {
            return LtSuggestionSet.Unavailable(UnavailableReason);
        }

        if (_projection is not { } projection || !ReferenceEquals(projection.Trees, trees))
        {
            var values = new List<LtSuggestion>(trees.Count);
            foreach (var tree in trees)
            {
                values.Add(new LtSuggestion(tree.TreeId, tree.Name.Ownership));
            }

            projection = (trees, values);
            _projection = projection;
        }

        return SuggestionMatcher.Match(projection.Values, text, limit);
    }
}
