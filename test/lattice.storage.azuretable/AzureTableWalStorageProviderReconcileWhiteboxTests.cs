using System.Reflection;
using Azure;
using Azure.Data.Tables;
using NSubstitute;
using NSubstitute.ExceptionExtensions;

namespace Orleans.Lattice.Storage.AzureTable.Tests;

/// <summary>
/// White-box coverage for <c>RollBackOrphanAsync</c>'s idempotent
/// candidate-row deletion. Rollback wipes the orphan's batch partition and
/// then deletes the phase-0 candidate row unconditionally
/// (<see cref="ETag.All"/>), swallowing a <c>404</c> so that a concurrent
/// reconciliation pass - or a retried rollback whose first attempt already
/// removed the row - stays idempotent.
/// <para>
/// Real Azure Table Storage returns <c>404 ResourceNotFound</c> for a delete
/// of a missing entity, but the Azurite emulator treats an unconditional
/// delete of a missing row as an idempotent success and never surfaces the
/// 404, so the swallow arm cannot be reached through the emulator. These
/// tests drive the private static method against a substituted
/// <see cref="TableClient"/> that forces the exact 404 production storage
/// would raise, pinning that a 404 is swallowed and any other failure
/// propagates. Pure in-process unit tests; no Azurite endpoint required.
/// </para>
/// </summary>
[TestFixture]
public class AzureTableWalStorageProviderReconcileWhiteboxTests
{
    private const string TreeId = "tree-orphan";
    private const int ShardIndex = 0;

    private static readonly MethodInfo RollBackOrphanMethod =
        typeof(AzureTableWalStorageProvider).GetMethod(
            "RollBackOrphanAsync",
            BindingFlags.NonPublic | BindingFlags.Static)
        ?? throw new InvalidOperationException("RollBackOrphanAsync must be resolvable by reflection");

    private static Task InvokeRollBackOrphanAsync(
        TableClient table,
        string manifestPartitionKey,
        AzureTableWalStorageProvider.OrphanBatch orphan) =>
        (Task)RollBackOrphanMethod.Invoke(
            null,
            new object[] { table, manifestPartitionKey, orphan, CancellationToken.None })!;

    /// <summary>
    /// Builds a substitute whose batch partition is empty (so the chunked
    /// delete of the entry rows is a no-op and the code falls through to the
    /// candidate-row delete under test).
    /// </summary>
    private static TableClient CreateEmptyPartitionTable()
    {
        var table = Substitute.For<TableClient>();
        table.QueryAsync<AzureTableWalEntity>(
                Arg.Any<string>(),
                Arg.Any<int?>(),
                Arg.Any<IEnumerable<string>?>(),
                Arg.Any<CancellationToken>())
            .Returns(AsyncPageable<AzureTableWalEntity>.FromPages(Array.Empty<Page<AzureTableWalEntity>>()));
        return table;
    }

    private static AzureTableWalStorageProvider.OrphanBatch LegacyOrphan() => new(
        StartOffset: 0L,
        EndOffsetInclusive: 0L,
        BatchPartitionKey: AzureTableWalStorageProvider.BuildBatchPartitionKey(TreeId, ShardIndex, 0L),
        HasCandidateRow: true);

    /// <summary>
    /// A legacy orphan (<c>HasCandidateRow = true</c>) whose candidate row is
    /// already gone: the unconditional candidate-row delete raises 404 and the
    /// rollback must swallow it so a concurrent reconciliation pass stays
    /// idempotent.
    /// </summary>
    [Test]
    public void RollBackOrphanAsync_swallows_a_404_deleting_the_candidate_row()
    {
        var table = CreateEmptyPartitionTable();
        table.DeleteEntityAsync(
                Arg.Any<string>(),
                Arg.Any<string>(),
                Arg.Any<ETag>(),
                Arg.Any<CancellationToken>())
            .ThrowsAsync(new RequestFailedException(404, "not found", "ResourceNotFound", null));

        var manifestPartitionKey = AzureTableWalStorageProvider.BuildManifestPartitionKey(TreeId, ShardIndex);

        Assert.DoesNotThrowAsync(() => InvokeRollBackOrphanAsync(table, manifestPartitionKey, LegacyOrphan()));
    }

    /// <summary>
    /// The 404 swallow is narrow: a non-404 failure deleting the candidate row
    /// is NOT swallowed. Pins that the exception filter matches only 404 and
    /// every other status propagates to the caller.
    /// </summary>
    [Test]
    public void RollBackOrphanAsync_propagates_a_non_404_deleting_the_candidate_row()
    {
        var table = CreateEmptyPartitionTable();
        table.DeleteEntityAsync(
                Arg.Any<string>(),
                Arg.Any<string>(),
                Arg.Any<ETag>(),
                Arg.Any<CancellationToken>())
            .ThrowsAsync(new RequestFailedException(503, "server busy", "ServerBusy", null));

        var manifestPartitionKey = AzureTableWalStorageProvider.BuildManifestPartitionKey(TreeId, ShardIndex);

        var ex = Assert.ThrowsAsync<RequestFailedException>(
            () => InvokeRollBackOrphanAsync(table, manifestPartitionKey, LegacyOrphan()));
        Assert.That(ex!.Status, Is.EqualTo(503));
    }
}
