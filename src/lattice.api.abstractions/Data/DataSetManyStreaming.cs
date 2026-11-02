namespace Orleans.Lattice.Api.Data;

/// <summary>
/// The portable body of <see cref="ILatticeDataApi.SetManyStreamAsync"/>: it cuts an
/// entry stream into batches and writes each through
/// <see cref="ILatticeDataApi.SetManyAsync"/>. When a batch is refused for an entry's own
/// sake - the caller may not write a key, the schema policy rejects a value, or the entry
/// is not a valid write - the batch is written again one entry at a time through
/// <see cref="ILatticeDataApi.SetAsync"/>, so every other entry still lands and each
/// refusal is reported against its own key. Authorization, schema enforcement and
/// dead-lettering therefore apply exactly as they do to any other write.
/// </summary>
internal static class DataSetManyStreaming
{
    /// <summary>The longest reason a rejection carries.</summary>
    internal const int MaxReasonLength = 512;

    /// <summary>Streams <paramref name="upserts"/> into <paramref name="treeId"/> through <paramref name="api"/>.</summary>
    /// <param name="api">The facade whose write verbs carry the stream.</param>
    /// <param name="treeId">The tree to write.</param>
    /// <param name="upserts">The entries, in any order.</param>
    /// <param name="progress">Receives each batch's outcome.</param>
    /// <param name="options">The batching options.</param>
    /// <param name="cancellationToken">Stops the stream.</param>
    /// <returns>The stream's totals.</returns>
    public static async Task<DataSetManyStreamResult> RunAsync(
        ILatticeDataApi api,
        string treeId,
        IAsyncEnumerable<DataEntry> upserts,
        IProgress<DataSetManyStreamBatch>? progress,
        DataSetManyStreamOptions? options,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(api);
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        ArgumentNullException.ThrowIfNull(upserts);
        options ??= DataSetManyStreamOptions.Default;
        options.Validate();

        var batch = new List<DataEntry>(options.BatchSize);
        var ordinal = options.FirstOrdinal;
        long batchIndex = 0;
        long committed = 0;
        long rejected = 0;
        await foreach (var entry in upserts.WithCancellation(cancellationToken).ConfigureAwait(false))
        {
            ArgumentNullException.ThrowIfNull(entry);
            batch.Add(entry);
            if (batch.Count == options.BatchSize)
            {
                var outcome = await WriteBatchAsync(api, treeId, batch, batchIndex++, ordinal, cancellationToken).ConfigureAwait(false);
                Tally(outcome, progress, ref ordinal, ref committed, ref rejected);
                batch = new List<DataEntry>(options.BatchSize);
            }
        }

        if (batch.Count > 0)
        {
            var outcome = await WriteBatchAsync(api, treeId, batch, batchIndex++, ordinal, cancellationToken).ConfigureAwait(false);
            Tally(outcome, progress, ref ordinal, ref committed, ref rejected);
        }

        return new DataSetManyStreamResult
        {
            TreeId = treeId,
            EntryCount = ordinal - options.FirstOrdinal,
            CommittedCount = committed,
            RejectedCount = rejected,
            BatchCount = batchIndex,
        };
    }

    /// <summary>Writes one batch, isolating the entries refused for their own sake.</summary>
    /// <param name="api">The facade.</param>
    /// <param name="treeId">The tree.</param>
    /// <param name="batch">The batch's entries.</param>
    /// <param name="batchIndex">The batch's index in the stream.</param>
    /// <param name="firstOrdinal">The ordinal of the batch's first entry.</param>
    /// <param name="cancellationToken">Stops the write.</param>
    /// <returns>The batch's outcome.</returns>
    internal static async Task<DataSetManyStreamBatch> WriteBatchAsync(
        ILatticeDataApi api,
        string treeId,
        IReadOnlyList<DataEntry> batch,
        long batchIndex,
        long firstOrdinal,
        CancellationToken cancellationToken)
    {
        try
        {
            await api.SetManyAsync(treeId, batch, cancellationToken).ConfigureAwait(false);
            return new DataSetManyStreamBatch
            {
                TreeId = treeId,
                BatchIndex = batchIndex,
                FirstOrdinal = firstOrdinal,
                EntryCount = batch.Count,
                CommittedCount = batch.Count,
            };
        }
        catch (Exception exception) when (Classify(exception) is not null)
        {
            // Every leg of a batch is checked before any is applied, so a refused batch
            // wrote nothing; writing it again entry by entry lands the rest.
        }

        var committed = 0;
        List<DataEntryRejection>? rejections = null;
        for (var i = 0; i < batch.Count; i++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var entry = batch[i];
            try
            {
                await api.SetAsync(treeId, entry.Key, entry.Value ?? [], cancellationToken).ConfigureAwait(false);
                committed++;
            }
            catch (Exception exception) when (Classify(exception) is { } refusal)
            {
                (rejections ??= []).Add(new DataEntryRejection
                {
                    Ordinal = firstOrdinal + i,
                    Key = entry.Key ?? string.Empty,
                    Code = refusal.Code,
                    Reason = refusal.Reason,
                });
            }
        }

        return new DataSetManyStreamBatch
        {
            TreeId = treeId,
            BatchIndex = batchIndex,
            FirstOrdinal = firstOrdinal,
            EntryCount = batch.Count,
            CommittedCount = committed,
            Rejections = rejections is null ? Array.Empty<DataEntryRejection>() : rejections,
        };
    }

    /// <summary>
    /// The code and reason an entry is refused with when <paramref name="exception"/> is a
    /// refusal of that entry alone, or <see langword="null"/> when it ends the stream (a
    /// cancellation, a quota, an unreachable cluster or any other fault).
    /// </summary>
    /// <param name="exception">The fault a write raised.</param>
    internal static (string Code, string Reason)? Classify(Exception exception) => exception switch
    {
        LatticeAuthorizationDeniedException => (DataEntryRejection.DeniedCode, "Your access does not allow writing this key."),
        ILatticeValueValidationFault fault => (DataEntryRejection.SchemaCode, Clip(string.IsNullOrWhiteSpace(fault.Reason)
            ? "The tree's schema policy rejected the value."
            : "The tree's schema policy rejected the value: " + fault.Reason)),
        ArgumentException argument => (DataEntryRejection.InvalidCode, Clip(argument.Message)),
        _ => null,
    };

    private static void Tally(
        DataSetManyStreamBatch outcome,
        IProgress<DataSetManyStreamBatch>? progress,
        ref long ordinal,
        ref long committed,
        ref long rejected)
    {
        ordinal += outcome.EntryCount;
        committed += outcome.CommittedCount;
        rejected += outcome.Rejections.Count;
        progress?.Report(outcome);
    }

    private static string Clip(string text)
    {
        var line = text.ReplaceLineEndings(" ");
        return line.Length <= MaxReasonLength ? line : line[..MaxReasonLength];
    }
}
