using System.Collections.Immutable;
using Orleans.Lattice.Explorer.UI.Design.Components;
using Orleans.Lattice.Explorer.UI.Suggestions;
using Orleans.Lattice.Explorer.UI.Transport;

namespace Orleans.Lattice.Explorer.UI.Areas.Cluster;

/// <summary>
/// The storage provider keys a WAL partition can move to: the keys the resolving
/// silo knows, as the WAL placement audit of the named tree reports them.
/// </summary>
/// <remarks>
/// Read once per tree and caller (sign-in, endpoint and asserted tenant); the keys are a silo-wide catalogue, so
/// any tree the caller may read reports them. Without a tree to audit, or when the
/// audit fails, the field accepts a typed key and says why.
/// </remarks>
/// <param name="facades">The Cluster area's facades.</param>
/// <param name="tree">The tree whose placement is audited, read at query time.</param>
internal sealed class ClusterProviderKeySuggestionSource(ClusterFacades facades, Func<string?> tree) : ILtSuggestionSource
{
    /// <summary>The note shown when there is no tree to audit yet.</summary>
    public const string NoTreeReason = "Choose the tree first to list the provider keys this silo knows.";

    /// <summary>The note shown when the audit fails.</summary>
    public const string UnavailableReason = "The provider keys could not be listed, so the key is used as typed.";

    private (string Tree, ShellCallerKey Caller, IReadOnlyList<LtSuggestion>? Keys)? _remembered;

    /// <inheritdoc />
    public async ValueTask<LtSuggestionSet> SuggestAsync(string text, int limit, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(text);
        var treeId = tree()?.Trim();
        if (string.IsNullOrEmpty(treeId))
        {
            return LtSuggestionSet.Unavailable(NoTreeReason);
        }

        var caller = facades.Caller;
        if (_remembered is not { } remembered
            || !string.Equals(remembered.Tree, treeId, StringComparison.Ordinal)
            || remembered.Caller != caller)
        {
            IReadOnlyList<LtSuggestion>? keys;
            try
            {
                var audit = await facades.RequireTreeAdmin().AuditWalPlacementAsync(treeId, cancellationToken).ConfigureAwait(false);
                keys = Keys(audit.KnownProviderKeys);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception)
            {
                keys = null;
            }

            remembered = (treeId, caller, keys);
            if (facades.Caller == caller)
            {
                _remembered = remembered;
            }
        }

        return remembered.Keys is { } list
            ? SuggestionMatcher.Match(list, text, limit)
            : LtSuggestionSet.Unavailable(UnavailableReason);
    }

    private static IReadOnlyList<LtSuggestion> Keys(ImmutableArray<string> known)
    {
        if (known.IsDefaultOrEmpty)
        {
            return [];
        }

        var keys = new List<LtSuggestion>(known.Length);
        foreach (var key in known)
        {
            keys.Add(new LtSuggestion(key));
        }

        return keys;
    }
}
