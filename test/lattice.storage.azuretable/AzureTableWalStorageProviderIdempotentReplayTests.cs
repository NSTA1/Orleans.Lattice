using System.Diagnostics.Metrics;
using Azure;
using Azure.Data.Tables;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice.Primitives;
using Orleans.Serialization;

namespace Orleans.Lattice.Storage.AzureTable.Tests;

/// <summary>
/// Regression coverage for the WAL phase-one idempotent-replay guard.
/// <para>
/// The Azure Tables SDK retry pipeline can resend a phase-one batch
/// whose first attempt committed server-side but whose response was
/// lost (a socket / read timeout under CPU or network pressure). The
/// resend collides with the durable rows and the service returns
/// <c>409 EntityAlreadyExists</c>. Before the fix, the provider rethrew
/// that 409 and the whole batch (up to ~4,000 operations on the bench
/// hot path) was counted as a hard write failure, collapsing write
/// throughput and - because the saturation classifier escalates on the
/// first provider failure - driving the tree into a saturation flap.
/// The fix verifies the resident rows are byte-identical to the batch
/// the call tried to write and, if so, resolves the conflict as a
/// success.
/// </para>
/// These tests drive the provider's phase-one submit path against a
/// substituted <see cref="TableClient"/> so no Azurite endpoint is
/// required; they are pure in-process unit tests and therefore carry no
/// slow-suite category.
/// </summary>
[TestFixture]
public class AzureTableWalStorageProviderIdempotentReplayTests
{
    private const string TreeId = "tree-replay";
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

    private AzureTableWalStorageProvider CreateProvider() =>
        new(
            Options.Create(new AzureTableWalStorageOptions
            {
                // Never used: these tests pass a substituted TableClient
                // straight to SubmitPhaseOneAsync and never reach the
                // provider's own EnsureTableAsync codepath. The literal
                // must still parse as a valid connection-string shape.
                ConnectionString = "UseDevelopmentStorage=true",
                TableName = "Treplay" + Guid.NewGuid().ToString("N"),
                Compression = LatticeCompression.None,
            }),
            _serializer);

    private static List<TableTransactionAction> BuildBatchActions(long firstOffset, int count)
    {
        var partitionKey = AzureTableWalStorageProvider.BuildBatchPartitionKey(TreeId, ShardIndex, firstOffset);
        var actions = new List<TableTransactionAction>(count);
        for (var i = 0; i < count; i++)
        {
            var offset = firstOffset + i;
            actions.Add(new TableTransactionAction(
                TableTransactionActionType.Add,
                new AzureTableWalEntity
                {
                    PartitionKey = partitionKey,
                    RowKey = AzureTableWalStorageProvider.BuildEntryRowKey(offset),
                    Offset = offset,
                    Payload = [(byte)offset, (byte)(offset + 1), 0xAB],
                    Compression = (int)LatticeCompression.None,
                }));
        }

        return actions;
    }

    private static AzureTableWalEntity Clone(AzureTableWalEntity source) => new()
    {
        PartitionKey = source.PartitionKey,
        RowKey = source.RowKey,
        Offset = source.Offset,
        Payload = source.Payload is null ? null : (byte[])source.Payload.Clone(),
        Compression = source.Compression,
    };

    /// <summary>
    /// Configures the substitute so the resident table contains exactly
    /// the rows in <paramref name="resident"/> keyed by row-key, with
    /// every <see cref="TableClient.SubmitTransactionAsync"/> rejected by
    /// a 409 (simulating the lost-response replay where the rows are
    /// already durable).
    /// </summary>
    private static TableClient CreateConflictingTable(IReadOnlyDictionary<string, AzureTableWalEntity> resident)
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
    public async Task SubmitPhaseOneAsync_resolves_409_as_success_when_every_resident_row_is_byte_identical()
    {
        // Lost-response replay: the first attempt committed exactly these
        // rows, the response was lost, the SDK resent, and the service
        // answered 409. The durable rows match the batch, so the call
        // must complete as a success rather than throwing.
        await using var sut = CreateProvider();
        var actions = BuildBatchActions(firstOffset: 100, count: 3);
        var resident = actions.ToDictionary(
            a => ((AzureTableWalEntity)a.Entity).RowKey,
            a => Clone((AzureTableWalEntity)a.Entity));
        var table = CreateConflictingTable(resident);

        Assert.That(
            async () => await sut.SubmitPhaseOneAsync(table, actions, TreeId, ShardIndex, CancellationToken.None),
            Throws.Nothing);
    }

    [Test]
    public void SubmitPhaseOneAsync_rethrows_409_when_a_resident_row_payload_differs()
    {
        // A genuine offset collision: a row with this offset already
        // exists but carries different bytes. Masking it would hide
        // upstream corruption, so the 409 must surface as a hard failure.
        var sut = CreateProvider();
        var actions = BuildBatchActions(firstOffset: 200, count: 3);
        var resident = actions.ToDictionary(
            a => ((AzureTableWalEntity)a.Entity).RowKey,
            a => Clone((AzureTableWalEntity)a.Entity));
        var tampered = resident[((AzureTableWalEntity)actions[1].Entity).RowKey];
        tampered.Payload = [0xFF, 0xFF, 0xFF];
        var table = CreateConflictingTable(resident);

        Assert.That(
            async () => await sut.SubmitPhaseOneAsync(table, actions, TreeId, ShardIndex, CancellationToken.None),
            Throws.TypeOf<RequestFailedException>()
                .With.Property(nameof(RequestFailedException.Status))
                .EqualTo(AzureTableWalStorageProvider.EntityAlreadyExistsStatusCode));
    }

    [Test]
    public void SubmitPhaseOneAsync_rethrows_409_when_a_resident_row_is_missing()
    {
        // Not a clean replay: one of the rows the batch tried to add is
        // absent, so we cannot prove the whole batch already committed.
        var sut = CreateProvider();
        var actions = BuildBatchActions(firstOffset: 300, count: 3);
        var resident = actions
            .Skip(1) // drop the first row from the resident set
            .ToDictionary(
                a => ((AzureTableWalEntity)a.Entity).RowKey,
                a => Clone((AzureTableWalEntity)a.Entity));
        var table = CreateConflictingTable(resident);

        Assert.That(
            async () => await sut.SubmitPhaseOneAsync(table, actions, TreeId, ShardIndex, CancellationToken.None),
            Throws.TypeOf<RequestFailedException>()
                .With.Property(nameof(RequestFailedException.Status))
                .EqualTo(AzureTableWalStorageProvider.EntityAlreadyExistsStatusCode));
    }

    [Test]
    public async Task IsIdempotentPhaseOneReplayAsync_returns_true_when_all_rows_match()
    {
        var actions = BuildBatchActions(firstOffset: 10, count: 4);
        var resident = actions.ToDictionary(
            a => ((AzureTableWalEntity)a.Entity).RowKey,
            a => Clone((AzureTableWalEntity)a.Entity));
        var table = CreateConflictingTable(resident);

        var result = await AzureTableWalStorageProvider.IsIdempotentPhaseOneReplayAsync(
            table, actions, CancellationToken.None);

        Assert.That(result, Is.True);
    }

    [Test]
    public void PhaseOneEntityPayloadMatches_is_true_for_identical_rows()
    {
        var a = new AzureTableWalEntity { Offset = 7, Compression = 0, Payload = [1, 2, 3] };
        var b = new AzureTableWalEntity { Offset = 7, Compression = 0, Payload = [1, 2, 3] };

        Assert.That(AzureTableWalStorageProvider.PhaseOneEntityPayloadMatches(a, b), Is.True);
    }

    [Test]
    public void PhaseOneEntityPayloadMatches_is_false_for_differing_payload()
    {
        var a = new AzureTableWalEntity { Offset = 7, Compression = 0, Payload = [1, 2, 3] };
        var b = new AzureTableWalEntity { Offset = 7, Compression = 0, Payload = [1, 2, 4] };

        Assert.That(AzureTableWalStorageProvider.PhaseOneEntityPayloadMatches(a, b), Is.False);
    }

    [Test]
    public void PhaseOneEntityPayloadMatches_is_false_for_differing_offset()
    {
        var a = new AzureTableWalEntity { Offset = 7, Compression = 0, Payload = [1, 2, 3] };
        var b = new AzureTableWalEntity { Offset = 8, Compression = 0, Payload = [1, 2, 3] };

        Assert.That(AzureTableWalStorageProvider.PhaseOneEntityPayloadMatches(a, b), Is.False);
    }

    [Test]
    public void PhaseOneEntityPayloadMatches_is_false_for_differing_compression_tag()
    {
        var a = new AzureTableWalEntity { Offset = 7, Compression = 0, Payload = [1, 2, 3] };
        var b = new AzureTableWalEntity { Offset = 7, Compression = 1, Payload = [1, 2, 3] };

        Assert.That(AzureTableWalStorageProvider.PhaseOneEntityPayloadMatches(a, b), Is.False);
    }

    [Test]
    public void PhaseOneEntityPayloadMatches_handles_null_payloads()
    {
        var bothNull = AzureTableWalStorageProvider.PhaseOneEntityPayloadMatches(
            new AzureTableWalEntity { Offset = 1, Payload = null },
            new AzureTableWalEntity { Offset = 1, Payload = null });
        var oneNull = AzureTableWalStorageProvider.PhaseOneEntityPayloadMatches(
            new AzureTableWalEntity { Offset = 1, Payload = null },
            new AzureTableWalEntity { Offset = 1, Payload = [1] });

        Assert.Multiple(() =>
        {
            Assert.That(bothNull, Is.True);
            Assert.That(oneNull, Is.False);
        });
    }

    [Test]
    public async Task IsIdempotentPhaseOneReplayAsync_returns_false_for_a_non_wal_entity_action()
    {
        // Defence-in-depth: an action whose entity is not the WAL entity
        // shape can never be proven a replay, so the guard must fail safe
        // without even touching the table.
        var table = Substitute.For<TableClient>();
        var actions = new List<TableTransactionAction>
        {
            new(TableTransactionActionType.Add, new TableEntity("pk", "rk")),
        };

        var result = await AzureTableWalStorageProvider.IsIdempotentPhaseOneReplayAsync(
            table, actions, CancellationToken.None);

        Assert.That(result, Is.False);
    }

    [Test]
    public void SubmitPhaseOneAsync_propagates_a_non_409_failure_without_attempting_replay_verification()
    {
        // The idempotent-replay handler must be scoped strictly to 409.
        // A throttling / server-busy failure has to flow straight through
        // the generic failure path and never trigger a read-back.
        var sut = CreateProvider();
        var actions = BuildBatchActions(firstOffset: 400, count: 3);
        var table = Substitute.For<TableClient>();
        table.SubmitTransactionAsync(
                Arg.Any<IEnumerable<TableTransactionAction>>(),
                Arg.Any<CancellationToken>())
            .ThrowsAsync(new RequestFailedException(503, "Server is busy.", "ServerBusy", innerException: null));

        Assert.That(
            async () => await sut.SubmitPhaseOneAsync(table, actions, TreeId, ShardIndex, CancellationToken.None),
            Throws.TypeOf<RequestFailedException>()
                .With.Property(nameof(RequestFailedException.Status)).EqualTo(503));

        // No verification read should ever have been issued for a non-409.
        table.DidNotReceive().GetEntityAsync<AzureTableWalEntity>(
            Arg.Any<string>(), Arg.Any<string>(), Arg.Any<IEnumerable<string>?>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public void SubmitPhaseOneAsync_rethrows_the_original_409_when_replay_verification_read_faults()
    {
        // If the verification read-back itself fails transiently we cannot
        // prove the 409 was an idempotent replay. The caller must see the
        // original 409 (the meaningful conflict), not the incidental read
        // fault, so a higher layer's retry re-drives the same batch.
        var sut = CreateProvider();
        var actions = BuildBatchActions(firstOffset: 500, count: 3);
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
                Arg.Any<string>(), Arg.Any<string>(), Arg.Any<IEnumerable<string>?>(), Arg.Any<CancellationToken>())
            .ThrowsAsync(new RequestFailedException(503, "Server is busy.", "ServerBusy", innerException: null));

        Assert.That(
            async () => await sut.SubmitPhaseOneAsync(table, actions, TreeId, ShardIndex, CancellationToken.None),
            Throws.TypeOf<RequestFailedException>()
                .With.Property(nameof(RequestFailedException.Status))
                .EqualTo(AzureTableWalStorageProvider.EntityAlreadyExistsStatusCode));
    }

    [Test]
    public async Task SubmitPhaseOneAsync_counts_a_proven_replay_on_idempotent_replays_and_not_retry_exhausted()
    {
        // The whole point of the fix: a proven replay must NOT increment
        // provider.retry.exhausted, because that counter drives the WAL
        // saturation classifier - an already-durable write must not be
        // able to flap the tree into Saturated.
        await using var sut = CreateProvider();
        var actions = BuildBatchActions(firstOffset: 600, count: 3);
        var resident = actions.ToDictionary(
            a => ((AzureTableWalEntity)a.Entity).RowKey,
            a => Clone((AzureTableWalEntity)a.Entity));
        var table = CreateConflictingTable(resident);

        using var metrics = new ProviderCounterRecorder();
        await sut.SubmitPhaseOneAsync(table, actions, TreeId, ShardIndex, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(metrics.Sum(IdempotentReplaysInstrument), Is.EqualTo(1),
                "a proven replay must increment provider.idempotent_replays exactly once");
            Assert.That(metrics.Sum(RetryExhaustedInstrument), Is.EqualTo(0),
                "a proven replay must not increment provider.retry.exhausted");
        });
    }

    [Test]
    public void SubmitPhaseOneAsync_counts_a_genuine_collision_on_retry_exhausted_and_not_idempotent_replays()
    {
        var sut = CreateProvider();
        var actions = BuildBatchActions(firstOffset: 700, count: 3);
        var resident = actions.ToDictionary(
            a => ((AzureTableWalEntity)a.Entity).RowKey,
            a => Clone((AzureTableWalEntity)a.Entity));
        resident[((AzureTableWalEntity)actions[2].Entity).RowKey].Payload = [0xDE, 0xAD];
        var table = CreateConflictingTable(resident);

        using var metrics = new ProviderCounterRecorder();
        Assert.That(
            async () => await sut.SubmitPhaseOneAsync(table, actions, TreeId, ShardIndex, CancellationToken.None),
            Throws.TypeOf<RequestFailedException>());

        Assert.Multiple(() =>
        {
            Assert.That(metrics.Sum(RetryExhaustedInstrument), Is.EqualTo(1),
                "a genuine offset collision must surface on provider.retry.exhausted");
            Assert.That(metrics.Sum(IdempotentReplaysInstrument), Is.EqualTo(0),
                "a genuine offset collision must not be masked as an idempotent replay");
        });
    }

    private const string IdempotentReplaysInstrument = "orleans.lattice.provider.idempotent_replays";
    private const string RetryExhaustedInstrument = "orleans.lattice.provider.retry.exhausted";

    /// <summary>
    /// Captures <see cref="Counter{T}"/> measurements published on
    /// <see cref="LatticeMetrics.Meter"/> for the phase-one provider
    /// counters so a test can assert which counter a 409 was attributed
    /// to. Scoped to the instruments under test to keep the listener
    /// cheap and deterministic.
    /// </summary>
    private sealed class ProviderCounterRecorder : IDisposable
    {
        private readonly MeterListener _listener = new();
        private readonly Dictionary<string, long> _sums = new(StringComparer.Ordinal);
        private readonly object _gate = new();

        public ProviderCounterRecorder()
        {
            _listener.InstrumentPublished = (instrument, listener) =>
            {
                if (ReferenceEquals(instrument.Meter, LatticeMetrics.Meter)
                    && (instrument.Name == IdempotentReplaysInstrument || instrument.Name == RetryExhaustedInstrument))
                {
                    listener.EnableMeasurementEvents(instrument);
                }
            };
            _listener.SetMeasurementEventCallback<long>((instrument, measurement, _, _) =>
            {
                lock (_gate)
                {
                    _sums[instrument.Name] = _sums.GetValueOrDefault(instrument.Name) + measurement;
                }
            });
            _listener.Start();
        }

        public long Sum(string instrumentName)
        {
            _listener.RecordObservableInstruments();
            lock (_gate)
            {
                return _sums.GetValueOrDefault(instrumentName);
            }
        }

        public void Dispose() => _listener.Dispose();
    }
}
