using Azure;
using Azure.Data.Tables;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;
using Orleans.Serialization;

namespace Orleans.Lattice.Storage.AzureTable.Tests;

/// <summary>
/// Issue #6116: a phase-1 fault that leaves open whether the batch landed is
/// reported to the WAL shard as an unknown outcome
/// (<see cref="WalAppendOutcomeUnknown"/>) with a settle signal the provider owns,
/// while a fault that proves the batch can never land is reported as before. Runs
/// against a substituted <see cref="TableClient"/>.
/// </summary>
[TestFixture]
public class AzureTableWalStorageProviderPhaseOneUnknownOutcomeTests
{
    private const string TreeId = "tree-p1unknown";
    private const int ShardIndex = 0;

    private ServiceProvider _services = null!;
    private Serializer<WalRecord> _serializer = null!;

    [OneTimeSetUp]
    public void OneTimeSetUp()
    {
        _services = new ServiceCollection().AddSerializer().BuildServiceProvider();
        _serializer = _services.GetRequiredService<Serializer<WalRecord>>();
    }

    [OneTimeTearDown]
    public void OneTimeTearDown() => _services.Dispose();

    private AzureTableWalStorageProvider CreateProvider(int maxRetries = 2)
    {
        var sut = new AzureTableWalStorageProvider(
            Options.Create(new AzureTableWalStorageOptions
            {
                ConnectionString = "UseDevelopmentStorage=true",
                TableName = "Tp1u" + Guid.NewGuid().ToString("N"),
                Compression = LatticeCompression.None,
                PhaseOneTransientRetryBaseDelay = TimeSpan.Zero,
                PhaseOneTransientRetryMaxAttempts = maxRetries,
            }),
            _serializer);
        sut.PhaseOneUnknownOutcomeFirstProbeDelay = TimeSpan.FromMilliseconds(5);
        return sut;
    }

    private static List<TableTransactionAction> Batch(long start, int count) =>
        Enumerable.Range(0, count)
            .Select(i => new TableTransactionAction(
                TableTransactionActionType.Add,
                new AzureTableWalEntity
                {
                    PartitionKey = AzureTableWalStorageProvider.BuildBatchPartitionKey(TreeId, ShardIndex, start),
                    RowKey = AzureTableWalStorageProvider.BuildEntryRowKey(start + i),
                    Offset = start + i,
                    Payload = [(byte)i],
                    Compression = (int)LatticeCompression.None,
                }))
            .ToList();

    /// <summary>
    /// A table whose submits run <paramref name="submit"/> and whose point reads of
    /// the batch's first row return 404 until <paramref name="landed"/> reports true.
    /// </summary>
    private static TableClient CreateTable(Func<int, Exception?> submit, Func<bool> landed)
    {
        var table = Substitute.For<TableClient>();
        var attempts = 0;
        table.SubmitTransactionAsync(Arg.Any<IEnumerable<TableTransactionAction>>(), Arg.Any<CancellationToken>())
            .Returns(_ => submit(++attempts) is { } fault
                ? Task.FromException<Response<IReadOnlyList<Response>>>(fault)
                : Task.FromResult(Response.FromValue<IReadOnlyList<Response>>(Array.Empty<Response>(), Substitute.For<Response>())));
        table.GetEntityAsync<AzureTableWalEntity>(
                Arg.Any<string>(), Arg.Any<string>(), Arg.Any<IEnumerable<string>?>(), Arg.Any<CancellationToken>())
            .Returns(_ => landed()
                ? Task.FromResult(Response.FromValue(new AzureTableWalEntity(), Substitute.For<Response>()))
                : Task.FromException<Response<AzureTableWalEntity>>(
                    new RequestFailedException(404, "Not found", "ResourceNotFound", innerException: null)));
        return table;
    }

    private static async Task<Exception> SubmitExpectingFaultAsync(
        AzureTableWalStorageProvider sut, TableClient table, List<TableTransactionAction> actions)
    {
        try
        {
            await sut.SubmitPhaseOneAsync(table, actions, TreeId, ShardIndex, CancellationToken.None);
        }
        catch (Exception fault)
        {
            return fault;
        }

        Assert.Fail("PRECONDITION: the submit surfaces a fault");
        throw new InvalidOperationException("unreachable");
    }

    [Test]
    public async Task An_exhausted_timeout_is_an_unknown_outcome_that_settles_when_the_batch_lands()
    {
        await using var sut = CreateProvider();
        var landed = false;
        var table = CreateTable(_ => new TimeoutException("per-attempt network timeout"), () => Volatile.Read(ref landed));

        var fault = await SubmitExpectingFaultAsync(sut, table, Batch(4, 2));
        var unknown = WalAppendOutcomeUnknown.Find(fault);

        Assert.That(unknown, Is.Not.Null, "an abandoned attempt may still land after the fault surfaces");
        Assert.Multiple(() =>
        {
            Assert.That(unknown!.Attempts, Is.EqualTo(3));
            Assert.That(unknown.ClientAbandonedAttempts, Is.EqualTo(3));
        });
        await Task.Delay(50);
        Assert.That(unknown!.Settled.IsCompleted, Is.False, "the window is fenced while the batch is absent");

        Volatile.Write(ref landed, true);
        await unknown.Settled.WaitAsync(TimeSpan.FromSeconds(15));

        var recorded = sut.GetShardActivity(AzureTableWalStorageProvider.BuildManifestPartitionKey(TreeId, ShardIndex))
            .FailedBatches.TakeAll();
        Assert.That(recorded, Is.EqualTo(new[] { (4L, 5L) }),
            "a batch with no manifest row is left for the next reconciliation to reclaim");
    }

    [Test]
    public async Task An_unknown_outcome_settles_at_the_horizon_when_the_batch_never_lands()
    {
        await using var sut = CreateProvider(maxRetries: 0);
        sut.PhaseOneUnknownOutcomeSettleHorizon = TimeSpan.FromMilliseconds(100);
        var table = CreateTable(_ => new RequestFailedException(503, "Server busy", "ServerBusy", innerException: null), () => false);

        var fault = await SubmitExpectingFaultAsync(sut, table, Batch(0, 1));
        var unknown = WalAppendOutcomeUnknown.Find(fault);

        Assert.That(unknown, Is.Not.Null);
        await unknown!.Settled.WaitAsync(TimeSpan.FromSeconds(15));
        Assert.Multiple(() =>
        {
            Assert.That(unknown.Attempts, Is.EqualTo(1));
            Assert.That(unknown.ClientAbandonedAttempts, Is.Zero);
        });
    }

    [Test]
    public async Task A_definite_fault_after_an_in_place_retry_is_still_an_unknown_outcome()
    {
        await using var sut = CreateProvider();
        var table = CreateTable(
            attempt => attempt == 1
                ? new TaskCanceledException("the client gave up on the first attempt")
                : new InvalidOperationException("a definite refusal of the retry"),
            () => false);

        var fault = await SubmitExpectingFaultAsync(sut, table, Batch(0, 1));
        var unknown = WalAppendOutcomeUnknown.Find(fault);

        Assert.That(unknown, Is.Not.Null, "the first, abandoned attempt can still land");
        Assert.Multiple(() =>
        {
            Assert.That(unknown!.Attempts, Is.EqualTo(2));
            Assert.That(unknown.ClientAbandonedAttempts, Is.EqualTo(1));
        });
    }

    [Test]
    public async Task A_fault_that_proves_the_batch_cannot_land_is_reported_as_before()
    {
        await using var sut = CreateProvider();
        var refused = await SubmitExpectingFaultAsync(
            sut, CreateTable(_ => new InvalidOperationException("non-transient"), () => false), Batch(0, 1));

        var conflicting = Substitute.For<TableClient>();
        conflicting.SubmitTransactionAsync(Arg.Any<IEnumerable<TableTransactionAction>>(), Arg.Any<CancellationToken>())
            .ThrowsAsync(new RequestFailedException(409, "exists", "EntityAlreadyExists", innerException: null));
        conflicting.GetEntityAsync<AzureTableWalEntity>(
                Arg.Any<string>(), Arg.Any<string>(), Arg.Any<IEnumerable<string>?>(), Arg.Any<CancellationToken>())
            .ThrowsAsync(new RequestFailedException(404, "Not found", "ResourceNotFound", innerException: null));
        var conflict = await SubmitExpectingFaultAsync(sut, conflicting, Batch(0, 1));

        Assert.Multiple(() =>
        {
            Assert.That(WalAppendOutcomeUnknown.Find(refused), Is.Null, "a non-transient refusal wrote nothing");
            Assert.That(WalAppendOutcomeUnknown.Find(conflict), Is.Null, "rows already at those keys mean the batch can never land");
        });
    }

    [Test]
    public async Task A_candidate_row_fault_carries_the_phase_one_unknown_outcome()
    {
        var phaseOneFault = new TimeoutException("phase 1");
        var settled = new TaskCompletionSource();
        WalAppendOutcomeUnknown.Mark(phaseOneFault, settled.Task, attempts: 1, clientAbandonedAttempts: 1);
        var candidateFault = new InvalidOperationException("phase 0");

        Exception? surfaced = null;
        try
        {
            await AzureTableWalStorageProvider.AwaitBothAsync(Task.FromException(candidateFault), Task.FromException(phaseOneFault));
        }
        catch (Exception ex)
        {
            surfaced = ex;
        }

        Assert.Multiple(() =>
        {
            Assert.That(surfaced, Is.SameAs(candidateFault), "the candidate-row fault is the one surfaced");
            Assert.That(WalAppendOutcomeUnknown.Find(surfaced)?.Settled, Is.SameAs(settled.Task));
        });
    }
}
