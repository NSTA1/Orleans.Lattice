using Azure;
using Azure.Data.Tables;
using Orleans.Lattice.Storage.AzureTable.Tests.Fakes;

namespace Orleans.Lattice.Storage.AzureTable.Tests;

/// <summary>
/// The recovery re-stamp (issue #5825): a reconciliation pass that rolls nothing
/// forward rewrites <c>TAIL</c> at its own value under the ETag it read, so a
/// predecessor's commit conditioned on that ETag is refused.
/// </summary>
public partial class AzureTableWalStorageProviderTrimReconcileTests
{
    [Test]
    public async Task ReconcileAsync_restamps_tail_so_a_commit_under_the_etag_it_read_is_refused()
    {
        var table = new InMemoryWalTable();
        var writer = CreateProvider(table);
        await using (writer)
        {
            await writer.AppendBatchAsync(TreeId, ShardIndex, Entries(0, 2), CancellationToken.None);
            await writer.FlushPhaseTwoAsync(CancellationToken.None);
        }

        var held = Tail(table);
        var submitted = new List<IReadOnlyList<TableTransactionAction>>();
        table.BeforeTransaction = actions =>
        {
            submitted.Add(actions.ToArray());
            EnforceTailCondition(table, actions);
        };

        var recovered = CreateProvider(table);
        await using (recovered)
        {
            await recovered.ReconcileAsync(TreeId, ShardIndex, CancellationToken.None);
        }

        var restamped = Tail(table);
        var stale = new TableTransactionAction(
            TableTransactionActionType.UpdateReplace,
            new AzureTableWalEntity { PartitionKey = held.PartitionKey, RowKey = held.RowKey, Offset = held.Offset },
            held.ETag);
        Assert.Multiple(() =>
        {
            Assert.That(submitted, Has.Count.EqualTo(1), "recovery made other than one transaction");
            Assert.That(submitted[0], Has.Count.EqualTo(1));
            Assert.That(submitted[0][0].ActionType, Is.EqualTo(TableTransactionActionType.UpdateReplace));
            Assert.That(submitted[0][0].Entity.RowKey, Is.EqualTo(AzureTableWalStorageProvider.TailRowKey));
            Assert.That(submitted[0][0].ETag, Is.EqualTo(held.ETag), "the re-stamp was not conditioned on the ETag recovery read");
            Assert.That(restamped.Offset, Is.EqualTo(held.Offset), "recovery moved TAIL");
            Assert.That(restamped.ETag, Is.Not.EqualTo(held.ETag), "recovery left TAIL's ETag current");
            Assert.That(
                async () => await table.BuildTableClient().SubmitTransactionAsync(new[] { stale }, CancellationToken.None),
                Throws.InstanceOf<RequestFailedException>().With.Property("Status").EqualTo(412),
                "a commit under the ETag a predecessor held was accepted after recovery");
        });
    }

    [Test]
    public async Task ReconcileAsync_writes_nothing_when_tail_is_absent()
    {
        var table = new InMemoryWalTable();
        var recovered = CreateProvider(table);
        await using var _ = recovered;

        await recovered.ReconcileAsync(TreeId, ShardIndex, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(table.CommittedTransactions, Is.Zero);
            Assert.That(table.Count, Is.Zero);
        });
    }

    private static AzureTableWalEntity Tail(InMemoryWalTable table) =>
        table.Snapshot().Single(r => r.RowKey == AzureTableWalStorageProvider.TailRowKey);

    /// <summary>
    /// The fake table does not evaluate conditions; this applies the service's
    /// If-Match rule to <c>TAIL</c> replaces, which is all the re-stamp relies on.
    /// </summary>
    private static void EnforceTailCondition(InMemoryWalTable table, IReadOnlyList<TableTransactionAction> actions)
    {
        foreach (var action in actions)
        {
            if (action.ActionType == TableTransactionActionType.UpdateReplace
                && action.Entity.RowKey == AzureTableWalStorageProvider.TailRowKey
                && table.Snapshot().SingleOrDefault(r => r.PartitionKey == action.Entity.PartitionKey && r.RowKey == action.Entity.RowKey) is { } stored
                && stored.ETag != action.ETag)
            {
                throw new RequestFailedException(412, "UpdateConditionNotSatisfied", "UpdateConditionNotSatisfied", innerException: null);
            }
        }
    }
}
