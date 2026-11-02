using Azure;
using Azure.Data.Tables;
using Orleans.Lattice.Storage.AzureTable.Tests.Fakes;

namespace Orleans.Lattice.Storage.AzureTable.Tests;

/// <summary>
/// Unit coverage for the phase-2 idempotent-replay proof
/// (<see cref="AzureTableWalStorageProvider.IsIdempotentPhaseTwoReplayAsync"/>) and for
/// the <see cref="PhaseTwoWorker"/> seam that applies it to a failed manifest
/// commit (issue #5520). A 409 / 404 is resolved as a success only when every effect
/// of the transaction is proven resident; anything else surfaces the original fault.
/// </summary>
[TestFixture]
public class AzureTableWalPhaseTwoReplayProofTests
{
    private const string TreeId = "tree-phase2-proof";
    private const int ShardIndex = 0;

    private static AzureTableWalEntity ManifestEntity(string rowKey, long offset, long payloadBytes = 0L) => new()
    {
        PartitionKey = AzureTableWalStorageProvider.BuildManifestPartitionKey(TreeId, ShardIndex),
        RowKey = rowKey,
        Offset = offset,
        PayloadBytes = payloadBytes,
    };

    private static List<TableTransactionAction> ManifestCommit(bool withCandidateDelete = false)
    {
        var actions = new List<TableTransactionAction>();
        if (withCandidateDelete)
        {
            actions.Add(new TableTransactionAction(
                TableTransactionActionType.Delete,
                ManifestEntity(AzureTableWalStorageProvider.BuildCandidateRowKey(0), 2L),
                ETag.All));
        }

        actions.Add(new TableTransactionAction(
            TableTransactionActionType.Add,
            ManifestEntity(AzureTableWalStorageProvider.BuildManifestRowKey(0), 2L, payloadBytes: 30L)));
        actions.Add(new TableTransactionAction(
            TableTransactionActionType.UpsertReplace,
            ManifestEntity(AzureTableWalStorageProvider.TailRowKey, 2L)));
        return actions;
    }

    private static Task<bool> ProveAsync(InMemoryWalTable table, List<TableTransactionAction> actions) =>
        AzureTableWalStorageProvider.IsIdempotentPhaseTwoReplayAsync(table.BuildTableClient(), actions, CancellationToken.None);

    [Test]
    public async Task IsIdempotentPhaseTwoReplayAsync_is_true_when_every_effect_is_resident()
    {
        var table = new InMemoryWalTable();
        table.Seed(ManifestEntity(AzureTableWalStorageProvider.BuildManifestRowKey(0), 2L, payloadBytes: 30L));
        table.Seed(ManifestEntity(AzureTableWalStorageProvider.TailRowKey, 2L));

        Assert.That(await ProveAsync(table, ManifestCommit()), Is.True);
        Assert.That(await ProveAsync(table, ManifestCommit(withCandidateDelete: true)), Is.True, "a deleted candidate row is resident as absent");
    }

    [Test]
    public async Task IsIdempotentPhaseTwoReplayAsync_is_true_when_TAIL_has_moved_past_the_upsert()
    {
        var table = new InMemoryWalTable();
        table.Seed(ManifestEntity(AzureTableWalStorageProvider.BuildManifestRowKey(0), 2L, payloadBytes: 30L));
        table.Seed(ManifestEntity(AzureTableWalStorageProvider.TailRowKey, 9L));

        Assert.That(await ProveAsync(table, ManifestCommit()), Is.True);
    }

    [TestCase(5L, 30L, TestName = "IsIdempotentPhaseTwoReplayAsync_is_false_when_the_manifest_row_records_another_end_offset")]
    [TestCase(2L, 31L, TestName = "IsIdempotentPhaseTwoReplayAsync_is_false_when_the_manifest_row_records_other_payload_bytes")]
    public async Task IsIdempotentPhaseTwoReplayAsync_is_false_for_a_divergent_manifest_row(long offset, long payloadBytes)
    {
        var table = new InMemoryWalTable();
        table.Seed(ManifestEntity(AzureTableWalStorageProvider.BuildManifestRowKey(0), offset, payloadBytes));
        table.Seed(ManifestEntity(AzureTableWalStorageProvider.TailRowKey, 2L));

        Assert.That(await ProveAsync(table, ManifestCommit()), Is.False);
    }

    [Test]
    public async Task IsIdempotentPhaseTwoReplayAsync_is_false_when_the_manifest_row_is_absent()
    {
        var table = new InMemoryWalTable();
        table.Seed(ManifestEntity(AzureTableWalStorageProvider.TailRowKey, 2L));

        Assert.That(await ProveAsync(table, ManifestCommit()), Is.False);
    }

    [Test]
    public async Task IsIdempotentPhaseTwoReplayAsync_is_false_when_the_candidate_row_is_still_present()
    {
        var table = new InMemoryWalTable();
        table.Seed(ManifestEntity(AzureTableWalStorageProvider.BuildCandidateRowKey(0), 2L));
        table.Seed(ManifestEntity(AzureTableWalStorageProvider.BuildManifestRowKey(0), 2L, payloadBytes: 30L));
        table.Seed(ManifestEntity(AzureTableWalStorageProvider.TailRowKey, 2L));

        Assert.That(await ProveAsync(table, ManifestCommit(withCandidateDelete: true)), Is.False);
    }

    [Test]
    public async Task IsIdempotentPhaseTwoReplayAsync_is_false_when_TAIL_is_behind_the_upsert()
    {
        var table = new InMemoryWalTable();
        table.Seed(ManifestEntity(AzureTableWalStorageProvider.BuildManifestRowKey(0), 2L, payloadBytes: 30L));
        table.Seed(ManifestEntity(AzureTableWalStorageProvider.TailRowKey, 1L));

        Assert.That(await ProveAsync(table, ManifestCommit()), Is.False);
    }

    [Test]
    public async Task IsIdempotentPhaseTwoReplayAsync_is_false_for_an_empty_transaction()
    {
        Assert.That(await ProveAsync(new InMemoryWalTable(), []), Is.False);
    }

    [Test]
    public async Task PhaseTwoWorker_fails_a_409_its_replay_proof_rejects_and_counts_no_replay()
    {
        var verified = 0;
        await using var worker = new PhaseTwoWorker(
            (_, _) => throw new RequestFailedException(409, "EntityAlreadyExists", "EntityAlreadyExists", innerException: null),
            "manifest",
            TimeSpan.Zero,
            verifyReplay: (_, _) =>
            {
                Interlocked.Increment(ref verified);
                return Task.FromResult(false);
            });

        Assert.That(
            async () => await worker.EnqueueAsync(0L, 2L, hasCandidateRow: false, payloadBytes: 30L),
            Throws.InstanceOf<RequestFailedException>().With.Property(nameof(RequestFailedException.Status)).EqualTo(409));
        Assert.That(verified, Is.EqualTo(1));
    }

    [Test]
    public async Task PhaseTwoWorker_settles_a_409_its_replay_proof_accepts()
    {
        await using var worker = new PhaseTwoWorker(
            (_, _) => throw new RequestFailedException(409, "EntityAlreadyExists", "EntityAlreadyExists", innerException: null),
            "manifest",
            TimeSpan.Zero,
            verifyReplay: (_, _) => Task.FromResult(true));

        Assert.That(async () => await worker.EnqueueAsync(0L, 2L, hasCandidateRow: false, payloadBytes: 30L), Throws.Nothing);
    }

    [Test]
    public async Task PhaseTwoWorker_never_resolves_a_non_conflict_fault_as_a_replay()
    {
        var verified = 0;
        await using var worker = new PhaseTwoWorker(
            (_, _) => throw new RequestFailedException(500, "InternalError", "InternalError", innerException: null),
            "manifest",
            TimeSpan.Zero,
            verifyReplay: (_, _) =>
            {
                Interlocked.Increment(ref verified);
                return Task.FromResult(true);
            });

        Assert.That(
            async () => await worker.EnqueueAsync(0L, 2L, hasCandidateRow: false, payloadBytes: 30L),
            Throws.InstanceOf<RequestFailedException>().With.Property(nameof(RequestFailedException.Status)).EqualTo(500));
        Assert.That(verified, Is.Zero);
    }

    [Test]
    public async Task PhaseTwoWorker_fails_a_409_when_the_replay_proof_read_itself_fails()
    {
        await using var worker = new PhaseTwoWorker(
            (_, _) => throw new RequestFailedException(409, "EntityAlreadyExists", "EntityAlreadyExists", innerException: null),
            "manifest",
            TimeSpan.Zero,
            verifyReplay: (_, _) => throw new RequestFailedException(503, "ServerBusy", "ServerBusy", innerException: null));

        Assert.That(
            async () => await worker.EnqueueAsync(0L, 2L, hasCandidateRow: false, payloadBytes: 30L),
            Throws.InstanceOf<RequestFailedException>().With.Property(nameof(RequestFailedException.Status)).EqualTo(409));
    }
}

