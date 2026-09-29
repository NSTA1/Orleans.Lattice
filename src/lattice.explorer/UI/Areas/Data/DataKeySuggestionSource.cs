using Orleans.Lattice.Explorer.Core.Data;
using Orleans.Lattice.Explorer.UI.Design.Components;

namespace Orleans.Lattice.Explorer.UI.Areas.Data;

/// <summary>
/// The keys of one tree that start with what is typed: one bounded prefix scan per
/// query, whose cursor is released straight after, so a key picker never walks a
/// tree and never leaves a server-side scan pinned.
/// </summary>
/// <remarks>
/// Nothing is remembered: the source belongs to one tree entry, whose state id is
/// already the tenant's, and each query reads the tree afresh. A tree the caller
/// cannot scan answers unavailable, and the field accepts the key as typed.
/// </remarks>
/// <param name="reader">The state API's data reader.</param>
/// <param name="stateId">The id the state API reads the tree under.</param>
internal sealed class DataKeySuggestionSource(IDataReader reader, string stateId) : ILtSuggestionSource
{
    /// <summary>The note shown when the tree's keys cannot be read.</summary>
    public const string UnavailableReason = "The tree's keys could not be listed, so the key is used as typed.";

    /// <summary>The id the state API reads the tree under.</summary>
    public string StateId => stateId;

    /// <inheritdoc />
    public async ValueTask<LtSuggestionSet> SuggestAsync(string text, int limit, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(text);
        DataPage page;
        try
        {
            page = await reader.ScanAsync(
                stateId,
                Math.Clamp(limit + 1, 1, DataPaging.MaxPageSize),
                keyPrefix: text,
                cancellationToken: cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception)
        {
            return LtSuggestionSet.Unavailable(UnavailableReason);
        }

        _ = ReleaseAsync(page.ContinuationToken);

        var keys = new List<LtSuggestion>(Math.Min(page.Entries.Count, limit));
        foreach (var entry in page.Entries)
        {
            if (keys.Count == limit)
            {
                break;
            }

            keys.Add(new LtSuggestion(entry.Key));
        }

        // Keys scan in ordinal order, so an exact match is the first entry whenever it exists.
        return LtSuggestionSet.Of(keys, page.Entries.Count > limit || page.ContinuationToken is not null);
    }

    private async Task ReleaseAsync(string? token)
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
            // Releasing a cursor is best effort; the server reaps an idle one anyway.
        }
    }
}
