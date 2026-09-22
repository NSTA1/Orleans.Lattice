using Orleans.Lattice.Primitives;
using Orleans.Lattice.Testing;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Runs the shared <see cref="WalOffsetAllocationContractTestsBase"/> suite
/// against <see cref="InMemoryWalStorageProvider"/>.
/// <para>
/// This provider had the same defect as the file provider under issue #3366,
/// with a different consequence: it holds no durable state, so it cannot lose
/// an entry across a process restart. What it could do is hand a reactivated
/// grain an offset that shipping and materialisation cursors had already
/// consumed - corruption rather than loss, and only within a process lifetime.
/// The offset-allocation property is identical either way, which is why one
/// suite covers both.
/// </para>
/// <para>
/// <see cref="IWalOffsetAllocationProbe.ReopenAsync"/> is a no-op here, and
/// deliberately so: the provider instance outlives the grain that reads from
/// it, so grain reactivation against a surviving provider is exactly what the
/// no-op models.
/// </para>
/// </summary>
[TestFixture]
public sealed class InMemoryWalOffsetAllocationContractTests : WalOffsetAllocationContractTestsBase
{
    protected override Task<IWalOffsetAllocationProbe> CreateProbeAsync() =>
        Task.FromResult<IWalOffsetAllocationProbe>(new Probe());

    private sealed class Probe : IWalOffsetAllocationProbe
    {
        private const string TreeId = "tree-offset-contract";
        private const int ShardIndex = 0;

        private readonly InMemoryWalStorageProvider _provider = new();

        public Task AppendAsync(IReadOnlyList<long> offsets, CancellationToken cancellationToken)
        {
            ArgumentNullException.ThrowIfNull(offsets);
            var entries = offsets.Select(Entry).ToArray();
            return _provider.AppendBatchAsync(TreeId, ShardIndex, entries, cancellationToken);
        }

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

        public Task ReopenAsync(CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
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
