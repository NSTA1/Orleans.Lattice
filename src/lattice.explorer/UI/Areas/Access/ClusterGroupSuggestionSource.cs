using Orleans.Lattice.Explorer.UI.Areas.Access.Tenant;
using Orleans.Lattice.Explorer.UI.Design.Components;

namespace Orleans.Lattice.Explorer.UI.Areas.Access;

/// <summary>
/// The cluster's groups from both of their sources in one list, each labelled:
/// <see cref="DirectoryLabel"/> for a group the identity directory lists,
/// <see cref="StoredLabel"/> for a group administered in the Access area itself,
/// and <see cref="BothLabel"/> for one that is both. A group created in Access can
/// so be chosen once a directory is configured, as a rule subject, a nested group
/// member or an app role binding.
/// </summary>
/// <remarks>
/// <para>
/// It remembers nothing: each query asks both sources, and the answer is the
/// typed id's exact match first, then the directory's groups, then the stored
/// ones, up to the limit. A source that cannot answer is left out, so only a group
/// some source still lists can be chosen; when neither answers, the directory's
/// reason is shown. Every reserved tenant group id (<c>t/...</c>) is left out: a
/// tenant group is never a cluster group.
/// </para>
/// <para>
/// Users are never offered here; they come from the directory alone.
/// </para>
/// </remarks>
/// <param name="directory">The identity directory's groups.</param>
/// <param name="stored">The auth store's groups (<see cref="StoredGroupSuggestionSource"/>).</param>
internal sealed class ClusterGroupSuggestionSource(ILtSuggestionSource directory, ILtSuggestionSource stored) : ILtSuggestionSource
{
    /// <summary>The label of a group the identity directory lists.</summary>
    public const string DirectoryLabel = "Directory";

    /// <summary>The label of a group administered in the Access area.</summary>
    public const string StoredLabel = "Stored";

    /// <summary>The label of a group that is both in the directory and stored.</summary>
    public const string BothLabel = "Directory and stored";

    /// <summary>Every label this source puts in front of a group's display name.</summary>
    public static IReadOnlyList<string> Labels { get; } = [BothLabel, DirectoryLabel, StoredLabel];

    /// <inheritdoc />
    public async ValueTask<LtSuggestionSet> SuggestAsync(string text, int limit, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(text);
        if (limit <= 0)
        {
            return LtSuggestionSet.Empty;
        }

        var listed = await directory.SuggestAsync(text, limit, cancellationToken).ConfigureAwait(false);
        var local = await stored.SuggestAsync(text, limit, cancellationToken).ConfigureAwait(false);
        if (!listed.IsAvailable && !local.IsAvailable)
        {
            return listed;
        }

        var directoryItems = listed.IsAvailable ? listed.Items : [];
        var storedItems = local.IsAvailable ? local.Items : [];
        var term = text.Trim();
        var values = new List<LtSuggestion>(Math.Min(limit, directoryItems.Count + storedItems.Count));

        // The exact id first, wherever it was found, so a pick-existing field accepts it.
        if (term.Length > 0 && !ClusterSubjectSuggestionSource.IsTenantGroupId(term)
            && (Find(directoryItems, term) is not null || Find(storedItems, term) is not null))
        {
            values.Add(Label(term, directoryItems, storedItems));
        }

        AddAll(values, directoryItems, directoryItems, storedItems, limit);
        AddAll(values, storedItems, directoryItems, storedItems, limit);
        var truncated = (listed.IsAvailable && listed.Truncated)
            || (local.IsAvailable && local.Truncated)
            || values.Count < Distinct(directoryItems, storedItems);
        return LtSuggestionSet.Of(values, truncated);
    }

    private static void AddAll(List<LtSuggestion> values, IReadOnlyList<LtSuggestion> from, IReadOnlyList<LtSuggestion> directoryItems, IReadOnlyList<LtSuggestion> storedItems, int limit)
    {
        for (var i = 0; i < from.Count && values.Count < limit; i++)
        {
            var id = from[i].Value;
            if (!ClusterSubjectSuggestionSource.IsTenantGroupId(id) && Find(values, id) is null)
            {
                values.Add(Label(id, directoryItems, storedItems));
            }
        }
    }

    private static LtSuggestion Label(string id, IReadOnlyList<LtSuggestion> directoryItems, IReadOnlyList<LtSuggestion> storedItems)
    {
        var listed = Find(directoryItems, id);
        var local = Find(storedItems, id);
        var label = listed is not null && local is not null ? BothLabel : listed is not null ? DirectoryLabel : StoredLabel;
        var displayName = listed?.Detail ?? local?.Detail;
        return new LtSuggestion(id, TenantGroupSuggestionSource.Describe(label, displayName));
    }

    private static int Distinct(IReadOnlyList<LtSuggestion> directoryItems, IReadOnlyList<LtSuggestion> storedItems)
    {
        var count = 0;
        for (var i = 0; i < directoryItems.Count; i++)
        {
            if (!ClusterSubjectSuggestionSource.IsTenantGroupId(directoryItems[i].Value))
            {
                count++;
            }
        }

        for (var i = 0; i < storedItems.Count; i++)
        {
            if (!ClusterSubjectSuggestionSource.IsTenantGroupId(storedItems[i].Value) && Find(directoryItems, storedItems[i].Value) is null)
            {
                count++;
            }
        }

        return count;
    }

    private static LtSuggestion? Find(IReadOnlyList<LtSuggestion> items, string id)
    {
        for (var i = 0; i < items.Count; i++)
        {
            if (string.Equals(items[i].Value, id, StringComparison.Ordinal))
            {
                return items[i];
            }
        }

        return null;
    }
}
