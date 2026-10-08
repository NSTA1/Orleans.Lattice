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
/// A nonempty catalogue is read once per tree and caller (sign-in, endpoint and asserted tenant);
/// the keys are silo-wide, so any tree the caller may read reports them. Without a
/// tree, when the catalogue is empty, or when the audit fails, suggestions are
/// unavailable with a reason and a later query retries. These keys are suggestions,
/// not an allow-list: a target may be known only to another silo.
/// </remarks>
/// <param name="facades">The Cluster area's facades.</param>
/// <param name="tree">The tree whose placement is audited, read at query time.</param>
internal sealed class ClusterProviderKeySuggestionSource(ClusterFacades facades, Func<string?> tree) : ILtSuggestionSource
{
    /// <summary>The note shown when there is no tree to audit yet.</summary>
    public const string NoTreeReason = "Choose the tree first to list the provider keys this silo knows.";

    /// <summary>The note shown when the audit fails.</summary>
    public const string UnavailableReason = "The provider keys could not be listed, so the key is used as typed.";

    /// <summary>The note shown when the audit reports no catalogue entries.</summary>
    public const string EmptyCatalogueReason = "This silo reports no provider keys, so the key is used as typed. Confirm the target resolves on every silo before moving.";

    private RememberedKeys? _remembered;

    private sealed record RememberedKeys(string Tree, ShellCallerKey Caller, ImmutableArray<LtSuggestion> Keys);

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
        var remembered = Volatile.Read(ref _remembered);
        if (remembered is null
            || !string.Equals(remembered.Tree, treeId, StringComparison.Ordinal)
            || remembered.Caller != caller)
        {
            ImmutableArray<LtSuggestion> keys;
            try
            {
                var audit = await facades.RequireTreeAdmin().AuditWalPlacementAsync(treeId, cancellationToken).ConfigureAwait(false);
                if (audit.KnownProviderKeys.IsDefaultOrEmpty)
                {
                    return LtSuggestionSet.Unavailable(EmptyCatalogueReason);
                }

                keys = Keys(audit.KnownProviderKeys);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception)
            {
                return LtSuggestionSet.Unavailable(UnavailableReason);
            }

            remembered = new RememberedKeys(treeId, caller, keys);
            if (facades.Caller == caller)
            {
                Volatile.Write(ref _remembered, remembered);
            }
        }

        return SuggestionMatcher.Match(remembered.Keys, text, limit);
    }

    private static ImmutableArray<LtSuggestion> Keys(ImmutableArray<string> known)
    {
        var keys = ImmutableArray.CreateBuilder<LtSuggestion>(known.Length);
        foreach (var key in known)
        {
            keys.Add(new LtSuggestion(key));
        }

        return keys.MoveToImmutable();
    }
}
