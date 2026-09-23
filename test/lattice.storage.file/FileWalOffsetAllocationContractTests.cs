using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using NUnit.Framework;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Testing;
using Orleans.Serialization;

namespace Orleans.Lattice.Storage.File.Tests;

/// <summary>
/// Runs the shared <see cref="WalOffsetAllocationContractTestsBase"/> suite
/// against <see cref="FileWalStorageProvider"/>.
/// <para>
/// This is the provider that actually lost data under issue #3366: its
/// highest-offset report answered from the recovered live entries, which
/// <c>RecoverFromDisk</c> populates with only those above the durable trim
/// watermark, so a fully trimmed shard reported <c>-1</c> and allocation
/// restarted at 0 beneath its own floor.
/// </para>
/// </summary>
[TestFixture]
public sealed class FileWalOffsetAllocationContractTests : WalOffsetAllocationContractTestsBase
{
    private ServiceProvider _services = null!;
    private Serializer<WalRecord> _serializer = null!;
    private string _root = null!;

    [OneTimeSetUp]
    public void OneTimeSetUp()
    {
        _services = new ServiceCollection().AddSerializer().BuildServiceProvider();
        _serializer = _services.GetRequiredService<Serializer<WalRecord>>();
    }

    [OneTimeTearDown]
    public void OneTimeTearDown() => _services.Dispose();

    [SetUp]
    public void SetUp()
    {
        _root = Path.Combine(
            Path.GetTempPath(),
            "lattice-wal-offset-contract",
            Guid.NewGuid().ToString("N"));
        System.IO.Directory.CreateDirectory(_root);
    }

    [TearDown]
    public void TearDown()
    {
        try
        {
            if (System.IO.Directory.Exists(_root))
            {
                System.IO.Directory.Delete(_root, recursive: true);
            }
        }
        catch (IOException)
        {
            // Best-effort cleanup; a leaked temp directory does not fail the test.
        }
    }

    protected override Task<IWalOffsetAllocationProbe> CreateProbeAsync() =>
        Task.FromResult<IWalOffsetAllocationProbe>(new Probe(_root, _serializer));

    private sealed class Probe(string root, Serializer<WalRecord> serializer) : IWalOffsetAllocationProbe
    {
        private const string TreeId = "tree-offset-contract";
        private const int ShardIndex = 0;

        private FileWalStorageProvider _provider = Create(root, serializer);

        private static FileWalStorageProvider Create(string root, Serializer<WalRecord> serializer) =>
            new(
                Options.Create(new FileWalStorageOptions
                {
                    RootDirectory = root,
                    FlushToDisk = true,
                }),
                serializer);

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

        // TODO(#3448): enrol this provider in the reconcile half of the contract.
        // Stubbed out of the #3348 change's scope; until it lands the reconcile
        // contract tests report Ignored for this provider rather than passing.
        public Task AppendAcknowledgedAsync(IReadOnlyList<long> offsets, CancellationToken cancellationToken)
        {
            Assert.Ignore("TODO(#3448): reconcile contract not yet enrolled for this provider.");
            return Task.CompletedTask;
        }

        // TODO(#3448): see AppendAcknowledgedAsync.
        public Task ReconcileAsync(CancellationToken cancellationToken)
        {
            Assert.Ignore("TODO(#3448): reconcile contract not yet enrolled for this provider.");
            return Task.CompletedTask;
        }

        /// <summary>
        /// Drops the provider entirely and rebuilds it over the same directory,
        /// so the next call re-runs the real on-disk recovery path rather than
        /// reading cached in-memory state.
        /// </summary>
        public Task ReopenAsync(CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            _provider.Dispose();
            _provider = Create(root, serializer);
            return Task.CompletedTask;
        }

        public ValueTask DisposeAsync()
        {
            _provider.Dispose();
            return ValueTask.CompletedTask;
        }

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
