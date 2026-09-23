using Azure;
using Azure.Data.Tables;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Testing;
using Orleans.Serialization;

namespace Orleans.Lattice.Storage.AzureTable.Tests;

/// <summary>
/// Runs the shared <see cref="WalOffsetAllocationContractTestsBase"/> suite
/// against <see cref="AzureTableWalStorageProvider"/>.
/// <para>
/// This provider never had the issue #3366 defect - it point-reads a dedicated
/// persisted <c>TAIL</c> row that a trim never moves back, which is what makes
/// it the reference shape the other two were corrected towards. It is enrolled
/// here for two reasons. First, the suite has to pass unchanged against a
/// provider that was already correct, or the contract it encodes is the suite's
/// invention rather than the system's rule. Second, TAIL's never-lowered
/// property is currently load-bearing but only implicitly tested, so a future
/// change that folded the tail read into the manifest scan would be caught
/// here rather than in production.
/// </para>
/// <para>
/// It is also the provider the reconcile half of the suite was written against:
/// its pipelined phase-2 commit is what makes a post-failure reconcile
/// non-quiescent, and issue #3348 found its reconcile rolling back live batches
/// and lowering TAIL there.
/// </para>
/// <para>
/// Emulator-gated exactly like the other Azure fixtures. Note that when Azurite
/// is absent these tests report neither pass nor fail - only a lower
/// <c>Total</c> - so a green run is not evidence this provider was covered.
/// </para>
/// </summary>
[TestFixture]
[Category("AzureStorageEmulator")]
public sealed class AzureTableWalOffsetAllocationContractTests : WalOffsetAllocationContractTestsBase
{
    private const string AzuriteConnectionString = "UseDevelopmentStorage=true";

    private ServiceProvider _services = null!;
    private Serializer<WalRecord> _serializer = null!;
    private TableServiceClient _adminClient = null!;
    private string _tableName = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _services = new ServiceCollection().AddSerializer().BuildServiceProvider();
        _serializer = _services.GetRequiredService<Serializer<WalRecord>>();
        _adminClient = new TableServiceClient(AzuriteConnectionString);

        try
        {
            await foreach (var _ in _adminClient.QueryAsync(maxPerPage: 1))
            {
                break;
            }
        }
        catch (Exception ex)
        {
            Assert.Inconclusive(
                $"Azurite is not reachable on the default development endpoint ({AzuriteConnectionString}). "
                + $"Start it via 'azurite --silent --location <dir>' or skip the AzureStorageEmulator category. "
                + $"Underlying error: {ex.GetType().Name}: {ex.Message}");
        }
    }

    [OneTimeTearDown]
    public void OneTimeTearDown() => _services.Dispose();

    [SetUp]
    public void SetUp() => _tableName = "T" + Guid.NewGuid().ToString("N");

    [TearDown]
    public async Task TearDown()
    {
        try
        {
            await _adminClient.DeleteTableAsync(_tableName);
        }
        catch (RequestFailedException)
        {
            // Best-effort cleanup; a missing table is acceptable.
        }
    }

    protected override Task<IWalOffsetAllocationProbe> CreateProbeAsync() =>
        Task.FromResult<IWalOffsetAllocationProbe>(new Probe(_tableName, _serializer));

    private sealed class Probe(string tableName, Serializer<WalRecord> serializer) : IWalOffsetAllocationProbe
    {
        private const string TreeId = "tree-offset-contract";
        private const int ShardIndex = 0;

        private AzureTableWalStorageProvider _provider = Create(tableName, serializer);

        private static AzureTableWalStorageProvider Create(
            string tableName,
            Serializer<WalRecord> serializer) =>
            new(
                Options.Create(new AzureTableWalStorageOptions
                {
                    ConnectionString = AzuriteConnectionString,
                    TableName = tableName,
                    Compression = LatticeCompression.None,
                }),
                serializer);

        /// <summary>
        /// Appends, then crosses the provider's phase-2 durability barrier.
        /// <para>
        /// This provider commits in two phases: the append writes entry rows,
        /// and a worker later lands the manifest row and TAIL in one atomic
        /// transaction. <c>ReadAsync</c> streams through manifest rows and
        /// <c>TrimAsync</c> deletes through them, so without this flush both
        /// operate on a manifest that does not yet mention the batch - the trim
        /// finds nothing to delete and the entries resurface when phase 2 lands
        /// afterwards. The real caller crosses the same barrier before treating
        /// an append as durable, so flushing here models it rather than
        /// papering over it.
        /// </para>
        /// </summary>
        public async Task AppendAsync(IReadOnlyList<long> offsets, CancellationToken cancellationToken)
        {
            ArgumentNullException.ThrowIfNull(offsets);
            var entries = offsets.Select(Entry).ToArray();
            await _provider.AppendBatchAsync(TreeId, ShardIndex, entries, cancellationToken)
                .ConfigureAwait(false);
            await _provider.FlushPhaseTwoAsync(cancellationToken).ConfigureAwait(false);
        }

        /// <summary>
        /// The provider's own acknowledgement: phase 1 is durable and the
        /// previous slot's phase 2 has been observed, but this batch's manifest
        /// row and TAIL may still be queued in the phase-2 worker. That pending
        /// commit is exactly what the post-failure resync must not mistake for
        /// an orphan (#3348).
        /// </summary>
        public Task AppendAcknowledgedAsync(IReadOnlyList<long> offsets, CancellationToken cancellationToken)
        {
            ArgumentNullException.ThrowIfNull(offsets);
            var entries = offsets.Select(Entry).ToArray();
            return _provider.AppendBatchAsync(TreeId, ShardIndex, entries, cancellationToken);
        }

        public Task ReconcileAsync(CancellationToken cancellationToken) =>
            _provider.ReconcileAsync(TreeId, ShardIndex, cancellationToken);

        public Task TrimAsync(long throughOffsetInclusive, CancellationToken cancellationToken) =>
            _provider.TrimAsync(TreeId, ShardIndex, throughOffsetInclusive, cancellationToken);

        public Task<long> GetHighestOffsetAsync(CancellationToken cancellationToken) =>
            _provider.GetHighestOffsetAsync(TreeId, ShardIndex, cancellationToken);

        public Task<long> GetLowestOffsetAsync(CancellationToken cancellationToken) =>
            _provider.GetLowestOffsetAsync(TreeId, ShardIndex, cancellationToken);

        public async Task<IReadOnlyList<long>> ReadLiveOffsetsAsync(CancellationToken cancellationToken)
        {
            var offsets = new List<long>();
            await foreach (var entry in _provider
                .ReadAsync(TreeId, ShardIndex, -1L, 1024, cancellationToken)
                .ConfigureAwait(false))
            {
                offsets.Add(entry.Offset);
            }

            return offsets;
        }

        /// <summary>
        /// Rebuilds the provider over the same table, so the next read resolves
        /// TAIL from storage rather than from any cached client-side state.
        /// </summary>
        public Task ReopenAsync(CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            _provider = Create(tableName, serializer);
            return Task.CompletedTask;
        }

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;

        private static WalEntry Entry(long offset) => new()
        {
            Offset = offset,
            Mutation = new LatticeMutation
            {
                TreeId = TreeId,
                Kind = MutationKind.Set,
                Key = $"k{offset}",
                Value = [(byte)(offset & 0xFF)],
                Timestamp = HybridLogicalClock.Tick(HybridLogicalClock.Zero),
                OriginClusterId = "site-a",
            },
        };
    }
}
