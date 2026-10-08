using System.Globalization;
using Azure;
using Azure.Core;
using Azure.Data.Tables;
using Azure.Data.Tables.Models;
using NSubstitute;

namespace Orleans.Lattice.Storage.AzureTable.Tests.Fakes;

/// <summary>
/// An in-memory stand-in for a single Azure Table, together with the
/// <see cref="TableClient"/> / <see cref="TableServiceClient"/> substitutes
/// that project it.
/// <para>
/// The provider's whole behavioural surface - the read path, the three-phase
/// append pipeline, trim, and activation-time reconciliation - reaches Azure
/// only through five <see cref="TableClient"/> members, every one of which the
/// SDK declares <c>virtual</c> on a non-sealed class with a protected
/// parameterless constructor. Substituting that surface and backing it with a
/// real ordered store therefore exercises the production code paths verbatim,
/// with no Azurite endpoint and no <c>AzureStorageEmulator</c> category gate,
/// which is what lets these fixtures run under the default test filter.
/// </para>
/// <para>
/// The store is deliberately behavioural rather than a per-test stub: rows are
/// held in one ordinal-sorted map keyed by (PartitionKey, RowKey) exactly as
/// Azure Tables orders them, transactions are all-or-nothing, an
/// <see cref="TableTransactionActionType.Add"/> of a resident row raises the
/// same <c>409 EntityAlreadyExists</c> the service raises, and a missing row
/// raises <c>404 ResourceNotFound</c>. Assertions can therefore be made against
/// round-tripped values and against the residual row set, not merely against
/// "a catch block was entered".
/// </para>
/// </summary>
internal sealed class InMemoryWalTable
{
    /// <summary>
    /// Azure Tables returns rows ordered by PartitionKey then RowKey, both
    /// ordinal. The provider's offset arithmetic depends on that order (its
    /// row keys are zero-padded to a fixed width precisely so ordinal order is
    /// numeric order), so the store must reproduce it rather than any
    /// insertion order.
    /// </summary>
    private readonly SortedDictionary<(string PartitionKey, string RowKey), AzureTableWalEntity> _rows =
        new(RowKeyComparer.Instance);

    private readonly object _gate = new();
    private long _eTagCounter;

    /// <summary>Number of <c>CreateIfNotExists</c> calls the client received.</summary>
    public int CreateIfNotExistsCalls { get; private set; }

    /// <summary>Number of transactions the client committed successfully.</summary>
    public int CommittedTransactions { get; private set; }

    /// <summary>Number of queries the client served.</summary>
    public int Queries { get; private set; }

    /// <summary>
    /// Partition keys whose <see cref="TableTransactionActionType.Add"/>
    /// actions must fail with <c>409 EntityAlreadyExists</c> even when the
    /// partition is empty. Models the lost-response replay in which the rows
    /// did commit server-side.
    /// </summary>
    public HashSet<string> ForceConflictPartitions { get; } = new(StringComparer.Ordinal);

    /// <summary>
    /// A hook invoked before every transaction commits, given the actions
    /// about to be applied. Throwing from it aborts the transaction with no
    /// rows written, which is how the fault-injection fixtures model a
    /// service-side rejection at an exact point in the pipeline.
    /// </summary>
    public Action<IReadOnlyList<TableTransactionAction>>? BeforeTransaction { get; set; }

    /// <summary>
    /// A hook invoked before every single-entity upsert, given the entity
    /// about to be written. Throwing from it fails that write alone, which is
    /// how a phase-0 candidate-row failure is modelled without disturbing the
    /// phase-1 transaction running beside it.
    /// </summary>
    public Action<AzureTableWalEntity>? BeforeUpsert { get; set; }

    /// <summary>
    /// A hook invoked before every query is served, given the OData filter.
    /// Throwing from it fails that query, which is how a read-side transport
    /// fault is injected at one specific scan.
    /// </summary>
    public Action<string>? BeforeQuery { get; set; }

    /// <summary>Total live row count.</summary>
    public int Count
    {
        get
        {
            lock (_gate)
            {
                return _rows.Count;
            }
        }
    }

    /// <summary>Snapshot of every live row, in Azure's (PartitionKey, RowKey) order.</summary>
    public IReadOnlyList<AzureTableWalEntity> Snapshot()
    {
        lock (_gate)
        {
            return _rows.Values.Select(Clone).ToList();
        }
    }

    /// <summary>Snapshot of one partition's live rows, in RowKey order.</summary>
    public IReadOnlyList<AzureTableWalEntity> Partition(string partitionKey)
    {
        lock (_gate)
        {
            return _rows
                .Where(kv => string.Equals(kv.Key.PartitionKey, partitionKey, StringComparison.Ordinal))
                .Select(kv => Clone(kv.Value))
                .ToList();
        }
    }

    /// <summary>Directly seeds a row, bypassing the client surface.</summary>
    public void Seed(AzureTableWalEntity entity)
    {
        lock (_gate)
        {
            Upsert(entity);
        }
    }

    /// <summary>
    /// Builds a <see cref="TableServiceClient"/> substitute whose
    /// <c>GetTableClient</c> returns a <see cref="TableClient"/> substitute
    /// projecting this store. Assign the result to
    /// <c>AzureTableWalStorageOptions.ServiceClient</c>.
    /// </summary>
    public TableServiceClient BuildServiceClient()
    {
        var tableClient = BuildTableClient();
        var serviceClient = Substitute.For<TableServiceClient>();
        serviceClient.GetTableClient(Arg.Any<string>()).Returns(tableClient);
        return serviceClient;
    }

    /// <summary>Builds a <see cref="TableClient"/> substitute projecting this store.</summary>
    public TableClient BuildTableClient()
    {
        var client = Substitute.For<TableClient>();

        client.CreateIfNotExistsAsync(Arg.Any<CancellationToken>())
            .Returns(_ =>
            {
                lock (_gate)
                {
                    CreateIfNotExistsCalls++;
                }

                return Task.FromResult(Response.FromValue(
                    TableModelFactory.TableItem("wal"),
                    (Response)new FakeResponse(204)));
            });

        client.QueryAsync<AzureTableWalEntity>(
                Arg.Any<string>(),
                Arg.Any<int?>(),
                Arg.Any<IEnumerable<string>>(),
                Arg.Any<CancellationToken>())
            .Returns(call => Query(
                call.ArgAt<string>(0),
                call.ArgAt<int?>(1),
                call.ArgAt<IEnumerable<string>?>(2)));

        client.SubmitTransactionAsync(
                Arg.Any<IEnumerable<TableTransactionAction>>(),
                Arg.Any<CancellationToken>())
            .Returns(call => SubmitTransaction(call.ArgAt<IEnumerable<TableTransactionAction>>(0)));

        client.GetEntityAsync<AzureTableWalEntity>(
                Arg.Any<string>(),
                Arg.Any<string>(),
                Arg.Any<IEnumerable<string>>(),
                Arg.Any<CancellationToken>())
            .Returns(call => GetEntity(call.ArgAt<string>(0), call.ArgAt<string>(1)));

        client.UpsertEntityAsync(
                Arg.Any<AzureTableWalEntity>(),
                Arg.Any<TableUpdateMode>(),
                Arg.Any<CancellationToken>())
            .Returns(call =>
            {
                var entity = call.ArgAt<AzureTableWalEntity>(0);
                BeforeUpsert?.Invoke(entity);
                lock (_gate)
                {
                    Upsert(entity);
                }

                return Task.FromResult<Response>(new FakeResponse(204));
            });

        client.DeleteEntityAsync(
                Arg.Any<string>(),
                Arg.Any<string>(),
                Arg.Any<ETag>(),
                Arg.Any<CancellationToken>())
            .Returns(call =>
            {
                lock (_gate)
                {
                    _rows.Remove((call.ArgAt<string>(0), call.ArgAt<string>(1)));
                }

                // Azure Tables treats a delete of an absent row as a success
                // when the caller supplies ETag.All, which is the only form
                // the provider uses.
                return Task.FromResult<Response>(new FakeResponse(204));
            });

        return client;
    }

    private Task<Response<AzureTableWalEntity>> GetEntity(string partitionKey, string rowKey)
    {
        lock (_gate)
        {
            if (!_rows.TryGetValue((partitionKey, rowKey), out var found))
            {
                throw new RequestFailedException(404, "ResourceNotFound", "ResourceNotFound", innerException: null);
            }

            return Task.FromResult(Response.FromValue(Clone(found), (Response)new FakeResponse(200)));
        }
    }

    private Task<Response<IReadOnlyList<Response>>> SubmitTransaction(IEnumerable<TableTransactionAction> actions)
    {
        var materialised = actions as IReadOnlyList<TableTransactionAction> ?? actions.ToList();
        BeforeTransaction?.Invoke(materialised);

        lock (_gate)
        {
            // Azure Tables applies a transaction atomically, so validate the
            // whole action list before mutating anything. A conflict must
            // leave the store exactly as it was.
            foreach (var action in materialised)
            {
                var entity = (AzureTableWalEntity)action.Entity;
                if (action.ActionType != TableTransactionActionType.Add)
                {
                    continue;
                }

                if (_rows.ContainsKey((entity.PartitionKey, entity.RowKey))
                    || ForceConflictPartitions.Contains(entity.PartitionKey))
                {
                    throw new RequestFailedException(
                        409, "EntityAlreadyExists", "EntityAlreadyExists", innerException: null);
                }
            }

            foreach (var action in materialised)
            {
                var entity = (AzureTableWalEntity)action.Entity;
                if (action.ActionType == TableTransactionActionType.Delete)
                {
                    _rows.Remove((entity.PartitionKey, entity.RowKey));
                }
                else
                {
                    Upsert(entity);
                }
            }

            CommittedTransactions++;
        }

        IReadOnlyList<Response> perAction = materialised.Select(_ => (Response)new FakeResponse(204)).ToList();
        return Task.FromResult(Response.FromValue(perAction, (Response)new FakeResponse(202)));
    }

    private void Upsert(AzureTableWalEntity entity)
    {
        var stored = Clone(entity);
        stored.ETag = NextETag();
        stored.Timestamp = DateTimeOffset.UnixEpoch;
        _rows[(stored.PartitionKey, stored.RowKey)] = stored;
    }

    private ETag NextETag() =>
        new(string.Create(CultureInfo.InvariantCulture, $"W/\"{++_eTagCounter}\""));

    private AsyncPageable<AzureTableWalEntity> Query(string filter, int? maxPerPage, IEnumerable<string>? select)
    {
        BeforeQuery?.Invoke(filter);

        List<AzureTableWalEntity> matches;
        var predicate = WalTableFilter.Parse(filter);
        var projection = select?.ToArray();

        lock (_gate)
        {
            Queries++;
            matches = _rows.Values
                .Where(predicate)
                .Select(row => projection is null || projection.Length == 0 ? Clone(row) : Project(row, projection))
                .ToList();
        }

        // maxPerPage of null means "let the service choose"; the provider only
        // relies on the page boundary for its Top(1) probes, so one page is a
        // faithful default.
        var pageSize = maxPerPage is > 0 ? maxPerPage.Value : Math.Max(matches.Count, 1);
        return new ListAsyncPageable(matches, pageSize);
    }

    /// <summary>
    /// Reproduces Azure's <c>$select</c>: columns outside the projection come
    /// back at their CLR default. The provider's three projecting call sites
    /// read only the columns they asked for, so a fake that returned whole
    /// rows would hide a future regression that reads an unselected column.
    /// </summary>
    private static AzureTableWalEntity Project(AzureTableWalEntity row, string[] select)
    {
        var projected = new AzureTableWalEntity();
        foreach (var column in select)
        {
            switch (column)
            {
                case nameof(AzureTableWalEntity.PartitionKey):
                    projected.PartitionKey = row.PartitionKey;
                    break;
                case nameof(AzureTableWalEntity.RowKey):
                    projected.RowKey = row.RowKey;
                    break;
                case nameof(AzureTableWalEntity.Offset):
                    projected.Offset = row.Offset;
                    break;
                case nameof(AzureTableWalEntity.Payload):
                    projected.Payload = row.Payload;
                    break;
                case nameof(AzureTableWalEntity.PayloadBytes):
                    projected.PayloadBytes = row.PayloadBytes;
                    break;
                case nameof(AzureTableWalEntity.Compression):
                    projected.Compression = row.Compression;
                    break;
                case nameof(AzureTableWalEntity.BatchHash):
                    projected.BatchHash = row.BatchHash;
                    break;
                case nameof(AzureTableWalEntity.BatchEntryCount):
                    projected.BatchEntryCount = row.BatchEntryCount;
                    break;
                default:
                    throw new InvalidOperationException($"Unsupported projection column '{column}'.");
            }
        }

        projected.ETag = row.ETag;
        projected.Timestamp = row.Timestamp;
        return projected;
    }

    private static AzureTableWalEntity Clone(AzureTableWalEntity source) => new()
    {
        PartitionKey = source.PartitionKey,
        RowKey = source.RowKey,
        Timestamp = source.Timestamp,
        ETag = source.ETag,
        Offset = source.Offset,
        Payload = source.Payload is null ? null : (byte[])source.Payload.Clone(),
        PayloadBytes = source.PayloadBytes,
        Compression = source.Compression,
        BatchHash = source.BatchHash is null ? null : (byte[])source.BatchHash.Clone(),
        BatchEntryCount = source.BatchEntryCount,
    };

    private sealed class RowKeyComparer : IComparer<(string PartitionKey, string RowKey)>
    {
        public static readonly RowKeyComparer Instance = new();

        public int Compare((string PartitionKey, string RowKey) x, (string PartitionKey, string RowKey) y)
        {
            var byPartition = string.CompareOrdinal(x.PartitionKey, y.PartitionKey);
            return byPartition != 0 ? byPartition : string.CompareOrdinal(x.RowKey, y.RowKey);
        }
    }

    private sealed class FakeResponse(int status) : Response
    {
        public override int Status { get; } = status;

        public override string ReasonPhrase => string.Empty;

        public override Stream? ContentStream { get; set; }

        public override string ClientRequestId { get; set; } = string.Empty;

        public override void Dispose()
        {
        }

        protected override bool ContainsHeader(string name) => false;

        protected override IEnumerable<HttpHeader> EnumerateHeaders() => [];

        protected override bool TryGetHeader(string name, out string value)
        {
            value = string.Empty;
            return false;
        }

        protected override bool TryGetHeaderValues(string name, out IEnumerable<string> values)
        {
            values = Array.Empty<string>();
            return false;
        }
    }

    private sealed class ListAsyncPageable(List<AzureTableWalEntity> items, int pageSize)
        : AsyncPageable<AzureTableWalEntity>
    {
        public override async IAsyncEnumerable<Page<AzureTableWalEntity>> AsPages(
            string? continuationToken = null,
            int? pageSizeHint = null)
        {
            await Task.Yield();
            var effective = pageSizeHint is > 0 ? pageSizeHint.Value : pageSize;
            if (items.Count == 0)
            {
                yield return Page<AzureTableWalEntity>.FromValues(
                    Array.Empty<AzureTableWalEntity>(), null, new FakeResponse(200));
                yield break;
            }

            for (var i = 0; i < items.Count; i += effective)
            {
                var slice = items.GetRange(i, Math.Min(effective, items.Count - i));
                var more = i + effective < items.Count;
                yield return Page<AzureTableWalEntity>.FromValues(
                    slice,
                    more ? (i + effective).ToString(CultureInfo.InvariantCulture) : null,
                    new FakeResponse(200));
            }
        }
    }
}
