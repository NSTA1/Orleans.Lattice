using Azure;
using Azure.Data.Tables;

namespace Orleans.Lattice.Storage.AzureTable;

/// <summary>
/// The phase-2 counterpart of the phase-1 idempotent-replay proof (issue #5520).
/// </summary>
public sealed partial class AzureTableWalStorageProvider
{
    /// <summary>
    /// Decides whether a phase-2 manifest transaction that failed with
    /// <c>409 EntityAlreadyExists</c> (a manifest-row add) or <c>404 ResourceNotFound</c>
    /// (a candidate-row delete) is a replay of a transaction that already committed:
    /// the SDK retry pipeline resent it after its first attempt landed and the
    /// response was lost (issue #5520).
    /// <para>
    /// The transaction is atomic, so it is proven durable when every effect it
    /// carries is resident: each manifest row it adds exists with the same end
    /// offset and payload byte count, each candidate row it deletes is gone, and
    /// <c>TAIL</c> is at least the value it upserts. A manifest row recording a
    /// different batch, an absent manifest row, a candidate row still present, or a
    /// lower <c>TAIL</c> is not a replay, and the caller surfaces the original
    /// failure. A transient read fault propagates, and the caller treats it as not
    /// proven.
    /// </para>
    /// </summary>
    /// <param name="table">The table the transaction was submitted to.</param>
    /// <param name="actions">The transaction's actions, as submitted.</param>
    /// <param name="cancellationToken">Cancels the read-back.</param>
    /// <returns><see langword="true"/> when every effect of the transaction is resident.</returns>
    internal static async Task<bool> IsIdempotentPhaseTwoReplayAsync(
        TableClient table,
        IReadOnlyList<TableTransactionAction> actions,
        CancellationToken cancellationToken)
    {
        if (actions.Count == 0)
        {
            return false;
        }

        for (var i = 0; i < actions.Count; i++)
        {
            if (actions[i].Entity is not AzureTableWalEntity expected)
            {
                return false;
            }

            var resident = await TryReadEntityAsync(table, expected.PartitionKey, expected.RowKey, cancellationToken)
                .ConfigureAwait(false);
            var effectResident = actions[i].ActionType switch
            {
                TableTransactionActionType.Add => resident is not null
                    && resident.Offset == expected.Offset
                    && resident.PayloadBytes == expected.PayloadBytes,
                TableTransactionActionType.Delete => resident is null,
                TableTransactionActionType.UpsertReplace => resident is not null
                    && resident.Offset >= expected.Offset,
                _ => false,
            };
            if (!effectResident)
            {
                return false;
            }
        }

        return true;
    }

    private static async Task<AzureTableWalEntity?> TryReadEntityAsync(
        TableClient table,
        string partitionKey,
        string rowKey,
        CancellationToken cancellationToken)
    {
        try
        {
            var response = await table.GetEntityAsync<AzureTableWalEntity>(
                partitionKey,
                rowKey,
                cancellationToken: cancellationToken).ConfigureAwait(false);
            return response.Value;
        }
        catch (RequestFailedException rfe) when (rfe.Status == EntityNotFoundStatusCode)
        {
            return null;
        }
    }
}
