using Azure.Data.Tables;
using NSubstitute;

namespace Orleans.Lattice.Storage.AzureTable.Tests;

/// <summary>
/// White-box unit tests for the small, pure, internal-static helpers on
/// <see cref="AzureTableWalStorageProvider"/> that back the phase-one
/// idempotency sentinel (<c>StampPhaseOneBatchSentinel</c>,
/// <c>ComputePhaseOneBatchHash</c>, <c>IsIdempotentPhaseOneReplayAsync</c>)
/// and the candidate-row key derivation (<c>BuildCandidateRowKey</c>).
/// These pin the fail-safe / defensive branches - an empty batch, a
/// non-WAL action, a null payload, and a negative offset - that the
/// end-to-end paths never legitimately reach, so they are exercised
/// directly against the internal surface with no Azure Tables endpoint.
/// </summary>
[TestFixture]
public class AzureTableWalStorageProviderInternalHelpersTests
{
    private const string TreeId = "tree-helpers";
    private const int ShardIndex = 0;

    private static AzureTableWalEntity WalEntity(long offset, byte[]? payload) =>
        new()
        {
            PartitionKey = AzureTableWalStorageProvider.BuildBatchPartitionKey(TreeId, ShardIndex, offset),
            RowKey = AzureTableWalStorageProvider.BuildEntryRowKey(offset),
            Offset = offset,
            Payload = payload,
            Compression = (int)Orleans.Lattice.LatticeCompression.None,
        };

    [Test]
    public async Task IsIdempotentPhaseOneReplayAsync_returns_false_for_an_empty_batch()
    {
        // An empty batch can never have produced a 409, so the guard must
        // fail safe without touching the table.
        var table = Substitute.For<TableClient>();

        var result = await AzureTableWalStorageProvider.IsIdempotentPhaseOneReplayAsync(
            table, Array.Empty<TableTransactionAction>(), CancellationToken.None);

        Assert.That(result, Is.False);
    }

    [Test]
    public async Task IsIdempotentPhaseOneReplayAsync_returns_false_when_first_action_is_not_a_wal_entity()
    {
        // A batch whose first action is not the WAL entity shape cannot be
        // proven a replay; the guard fails safe before any read-back.
        var table = Substitute.For<TableClient>();
        var actions = new[]
        {
            new TableTransactionAction(TableTransactionActionType.Add, new TableEntity("pk", "rk")),
        };

        var result = await AzureTableWalStorageProvider.IsIdempotentPhaseOneReplayAsync(
            table, actions, CancellationToken.None);

        Assert.That(result, Is.False);
    }

    [Test]
    public void StampPhaseOneBatchSentinel_is_a_no_op_for_an_empty_batch()
    {
        // No first row to stamp: the method must return without throwing.
        Assert.DoesNotThrow(() =>
            AzureTableWalStorageProvider.StampPhaseOneBatchSentinel(Array.Empty<TableTransactionAction>()));
    }

    [Test]
    public void StampPhaseOneBatchSentinel_is_a_no_op_when_first_action_is_not_a_wal_entity()
    {
        // The bench-only in-process encode path submits non-WAL entities
        // that never reach Azure; stamping must skip them untouched.
        var actions = new[]
        {
            new TableTransactionAction(TableTransactionActionType.Add, new TableEntity("pk", "rk")),
        };

        Assert.DoesNotThrow(() =>
            AzureTableWalStorageProvider.StampPhaseOneBatchSentinel(actions));
    }

    [Test]
    public void ComputePhaseOneBatchHash_mixes_a_discriminator_for_a_non_wal_action()
    {
        // A non-WAL action contributes a fixed -1 discriminator so the hash
        // stays well-defined; such a batch can never match a resident WAL
        // batch. The two batches differ only in the second action's shape,
        // so their hashes must diverge.
        var walOnly = new[]
        {
            new TableTransactionAction(TableTransactionActionType.Add, WalEntity(0, new byte[] { 1 })),
            new TableTransactionAction(TableTransactionActionType.Add, WalEntity(1, new byte[] { 2 })),
        };
        var withNonWal = new[]
        {
            new TableTransactionAction(TableTransactionActionType.Add, WalEntity(0, new byte[] { 1 })),
            new TableTransactionAction(TableTransactionActionType.Add, new TableEntity("pk", "rk")),
        };

        var hashWalOnly = AzureTableWalStorageProvider.ComputePhaseOneBatchHash(walOnly);
        var hashWithNonWal = AzureTableWalStorageProvider.ComputePhaseOneBatchHash(withNonWal);

        Assert.Multiple(() =>
        {
            Assert.That(hashWithNonWal, Is.Not.Null);
            Assert.That(hashWithNonWal.Length, Is.EqualTo(16), "XxHash128 fingerprint is 16 bytes");
            Assert.That(hashWithNonWal.AsSpan().SequenceEqual(hashWalOnly), Is.False,
                "a non-WAL action must change the batch hash");
        });
    }

    [Test]
    public void ComputePhaseOneBatchHash_distinguishes_a_null_payload_from_a_zero_length_payload()
    {
        // The hash writes an explicit null/length marker (-1 for null) so a
        // null payload cannot alias a zero-length one.
        var nullPayload = new[]
        {
            new TableTransactionAction(TableTransactionActionType.Add, WalEntity(0, null)),
        };
        var emptyPayload = new[]
        {
            new TableTransactionAction(TableTransactionActionType.Add, WalEntity(0, Array.Empty<byte>())),
        };

        var hashNull = AzureTableWalStorageProvider.ComputePhaseOneBatchHash(nullPayload);
        var hashEmpty = AzureTableWalStorageProvider.ComputePhaseOneBatchHash(emptyPayload);

        Assert.That(hashNull.AsSpan().SequenceEqual(hashEmpty), Is.False,
            "a null payload must hash differently from a zero-length payload");
    }

    [Test]
    public void BuildCandidateRowKey_throws_for_a_negative_offset()
    {
        // Candidate row keys are derived from WAL offsets, which are
        // non-negative dense integers; a negative offset is a misuse.
        Assert.That(
            () => AzureTableWalStorageProvider.BuildCandidateRowKey(-1L),
            Throws.TypeOf<ArgumentOutOfRangeException>());
    }
}
