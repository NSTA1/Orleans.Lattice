using Azure;
using Azure.Data.Tables;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using NSubstitute;
using NSubstitute.Core;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice.Primitives;
using Orleans.Serialization;

namespace Orleans.Lattice.Storage.AzureTable.Tests;

/// <summary>
/// White-box tests for the residual phase-one submit branches on
/// <see cref="AzureTableWalStorageProvider"/> not covered by the
/// idempotent-replay regression suite: the non-transient default arm of
/// the transient-fault classifier, the positive-delay backoff path, and
/// the two legacy full-readback fail-safe returns (a later non-WAL action
/// and a resident row that 404s mid-readback). All run against a
/// substituted <see cref="TableClient"/>, so no Azure Tables endpoint is
/// required.
/// </summary>
[TestFixture]
public class AzureTableWalStorageProviderPhaseOneWhiteboxTests
{
    private const string TreeId = "tree-p1wb";
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

    private AzureTableWalStorageProvider CreateProvider(Action<AzureTableWalStorageOptions>? configure = null)
    {
        var options = new AzureTableWalStorageOptions
        {
            ConnectionString = "UseDevelopmentStorage=true",
            TableName = "Tp1wb" + Guid.NewGuid().ToString("N"),
            Compression = LatticeCompression.None,
            PhaseOneTransientRetryBaseDelay = TimeSpan.Zero,
        };
        configure?.Invoke(options);
        return new(Options.Create(options), _serializer);
    }

    private static AzureTableWalEntity WalEntity(long offset, byte[] payload) => new()
    {
        PartitionKey = AzureTableWalStorageProvider.BuildBatchPartitionKey(TreeId, ShardIndex, offset),
        RowKey = AzureTableWalStorageProvider.BuildEntryRowKey(offset),
        Offset = offset,
        Payload = payload,
        Compression = (int)LatticeCompression.None,
    };

    private static AzureTableWalEntity Clone(AzureTableWalEntity source) => new()
    {
        PartitionKey = source.PartitionKey,
        RowKey = source.RowKey,
        Offset = source.Offset,
        Payload = source.Payload is null ? null : (byte[])source.Payload.Clone(),
        Compression = source.Compression,
        BatchHash = source.BatchHash is null ? null : (byte[])source.BatchHash.Clone(),
        BatchEntryCount = source.BatchEntryCount,
    };

    private static TableClient CreateTransientThenSuccessTable(int failuresBeforeSuccess, Exception transientFault)
    {
        var table = Substitute.For<TableClient>();
        var calls = 0;
        Func<CallInfo, Response<IReadOnlyList<Response>>> handler = _ =>
        {
            calls++;
            if (calls <= failuresBeforeSuccess)
            {
                throw transientFault;
            }

            return Response.FromValue<IReadOnlyList<Response>>(Array.Empty<Response>(), Substitute.For<Response>());
        };
        table.SubmitTransactionAsync(
                Arg.Any<IEnumerable<TableTransactionAction>>(),
                Arg.Any<CancellationToken>())
            .Returns(handler);
        return table;
    }

    /// <summary>
    /// A substitute that 409s every submit (the lost-response replay
    /// shape) and serves the supplied resident rows by row key, throwing
    /// 404 for any row key not present so a full-readback can observe a
    /// missing row.
    /// </summary>
    private static TableClient CreateConflictingTableWithResident(IReadOnlyDictionary<string, AzureTableWalEntity> resident)
    {
        var table = Substitute.For<TableClient>();

        table.SubmitTransactionAsync(
                Arg.Any<IEnumerable<TableTransactionAction>>(),
                Arg.Any<CancellationToken>())
            .ThrowsAsync(new RequestFailedException(
                AzureTableWalStorageProvider.EntityAlreadyExistsStatusCode,
                "0:The specified entity already exists.",
                "EntityAlreadyExists",
                innerException: null));

        table.GetEntityAsync<AzureTableWalEntity>(
                Arg.Any<string>(),
                Arg.Any<string>(),
                Arg.Any<IEnumerable<string>?>(),
                Arg.Any<CancellationToken>())
            .Returns(callInfo =>
            {
                var rowKey = callInfo.ArgAt<string>(1);
                if (!resident.TryGetValue(rowKey, out var entity))
                {
                    throw new RequestFailedException(404, "Not found", "ResourceNotFound", innerException: null);
                }

                return Task.FromResult(Response.FromValue(Clone(entity), Substitute.For<Response>()));
            });

        return table;
    }

    [Test]
    public void SubmitPhaseOneAsync_rethrows_a_non_transient_non_request_failed_fault()
    {
        // A plain InvalidOperationException is neither a cancellation, a
        // timeout, nor a RequestFailedException, so the transient-fault
        // classifier hits its default arm and returns false; the generic
        // catch then surfaces the fault to the caller unchanged.
        var sut = CreateProvider();
        var actions = new List<TableTransactionAction>
        {
            new(TableTransactionActionType.Add, WalEntity(0, new byte[] { 1 })),
        };
        var table = Substitute.For<TableClient>();
        table.SubmitTransactionAsync(
                Arg.Any<IEnumerable<TableTransactionAction>>(),
                Arg.Any<CancellationToken>())
            .ThrowsAsync(new InvalidOperationException("non-transient phase-1 fault"));

        Assert.That(
            async () => await sut.SubmitPhaseOneAsync(table, actions, TreeId, ShardIndex, CancellationToken.None),
            Throws.TypeOf<InvalidOperationException>());
    }

    [Test]
    public async Task SubmitPhaseOneAsync_waits_a_positive_backoff_before_an_in_place_transient_retry()
    {
        // With a positive PhaseOneTransientRetryBaseDelay the provider
        // computes a jittered backoff and awaits it before resubmitting
        // the byte-identical batch. A 1 ms base keeps the test fast while
        // still exercising the positive-delay branch the zero-delay
        // idempotent-replay tests skip.
        await using var sut = CreateProvider(o => o.PhaseOneTransientRetryBaseDelay = TimeSpan.FromMilliseconds(1));
        var actions = new List<TableTransactionAction>
        {
            new(TableTransactionActionType.Add, WalEntity(0, new byte[] { 1 })),
            new(TableTransactionActionType.Add, WalEntity(1, new byte[] { 2 })),
        };
        var table = CreateTransientThenSuccessTable(
            failuresBeforeSuccess: 1,
            transientFault: new RequestFailedException(503, "Server busy", "ServerBusy", innerException: null));

        await sut.SubmitPhaseOneAsync(table, actions, TreeId, ShardIndex, CancellationToken.None);

        // One transient failure + one success = two submit attempts.
        _ = table.Received(2).SubmitTransactionAsync(
            Arg.Any<IEnumerable<TableTransactionAction>>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public void SubmitPhaseOneAsync_full_readback_fails_safe_when_a_later_action_is_not_a_wal_entity()
    {
        // A legacy resident first row (no sentinel) forces the per-entry
        // full readback. A later action that is not the WAL entity shape
        // cannot be proven durable, so the readback returns false and the
        // original 409 surfaces.
        var sut = CreateProvider();
        var first = WalEntity(0, new byte[] { 1 });
        var actions = new List<TableTransactionAction>
        {
            new(TableTransactionActionType.Add, first),
            new(TableTransactionActionType.Add, new TableEntity("pk-other", "rk-other")),
        };

        // Resident first row lacks BatchHash (legacy row) so the guard
        // falls back to the per-entry readback.
        var residentFirst = Clone(first);
        residentFirst.BatchHash = null;
        var table = CreateConflictingTableWithResident(
            new Dictionary<string, AzureTableWalEntity> { [first.RowKey] = residentFirst });

        Assert.That(
            async () => await sut.SubmitPhaseOneAsync(table, actions, TreeId, ShardIndex, CancellationToken.None),
            Throws.TypeOf<RequestFailedException>()
                .With.Property(nameof(RequestFailedException.Status))
                .EqualTo(AzureTableWalStorageProvider.EntityAlreadyExistsStatusCode));
    }

    [Test]
    public void SubmitPhaseOneAsync_full_readback_fails_safe_when_a_resident_row_is_missing()
    {
        // Legacy first row again forces the per-entry readback; the second
        // row is absent (404) mid-readback, so the batch is not provably
        // durable and the 409 is surfaced rather than masked.
        var sut = CreateProvider();
        var first = WalEntity(0, new byte[] { 1 });
        var second = WalEntity(1, new byte[] { 2 });
        var actions = new List<TableTransactionAction>
        {
            new(TableTransactionActionType.Add, first),
            new(TableTransactionActionType.Add, second),
        };

        var residentFirst = Clone(first);
        residentFirst.BatchHash = null;

        // Only the first row is resident; the second row's point-read 404s.
        var table = CreateConflictingTableWithResident(
            new Dictionary<string, AzureTableWalEntity> { [first.RowKey] = residentFirst });

        Assert.That(
            async () => await sut.SubmitPhaseOneAsync(table, actions, TreeId, ShardIndex, CancellationToken.None),
            Throws.TypeOf<RequestFailedException>()
                .With.Property(nameof(RequestFailedException.Status))
                .EqualTo(AzureTableWalStorageProvider.EntityAlreadyExistsStatusCode));
    }
}
