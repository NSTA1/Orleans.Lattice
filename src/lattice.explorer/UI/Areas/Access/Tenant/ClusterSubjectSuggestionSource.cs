using Orleans.Lattice.Explorer.UI.Design.Components;

namespace Orleans.Lattice.Explorer.UI.Areas.Access.Tenant;

/// <summary>
/// The subject picker's <c>Cluster</c> source on a tenant page: the cluster's
/// users or groups from the identity directory, with every reserved tenant group
/// id (<c>t/...</c>) left out, so another tenant's group is never offered, and
/// each value labelled with its provenance.
/// </summary>
/// <remarks>
/// It remembers nothing: each query is the inner source's, filtered.
/// </remarks>
/// <param name="inner">The identity-directory source for users or groups.</param>
internal sealed class ClusterSubjectSuggestionSource(ILtSuggestionSource inner) : ILtSuggestionSource
{
    /// <summary>The label naming this source's provenance.</summary>
    public const string SourceLabel = "Cluster";

    /// <summary>The prefix every reserved tenant group id carries.</summary>
    public const string TenantGroupPrefix = "t/";

    /// <inheritdoc />
    public async ValueTask<LtSuggestionSet> SuggestAsync(string text, int limit, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(text);
        var answer = await inner.SuggestAsync(text, limit, cancellationToken).ConfigureAwait(false);
        if (!answer.IsAvailable)
        {
            return answer;
        }

        var values = new List<LtSuggestion>(answer.Items.Count);
        for (var i = 0; i < answer.Items.Count; i++)
        {
            var item = answer.Items[i];
            if (IsTenantGroupId(item.Value))
            {
                continue;
            }

            values.Add(item with { Detail = TenantGroupSuggestionSource.Describe(SourceLabel, item.Detail) });
        }

        return LtSuggestionSet.Of(values, answer.Truncated);
    }

    /// <summary>Whether <paramref name="id"/> lies in the reserved tenant group namespace, which only the <c>Tenant</c> source offers.</summary>
    /// <param name="id">A subject id.</param>
    public static bool IsTenantGroupId(string? id) =>
        id is not null && id.StartsWith(TenantGroupPrefix, StringComparison.Ordinal);
}
