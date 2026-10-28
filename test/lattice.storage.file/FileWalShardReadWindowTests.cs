using System.Buffers;
using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Storage.File.Tests;

[TestFixture]
public sealed class FileWalShardReadWindowTests
{
    private string _root = null!;

    [SetUp]
    public void SetUp()
    {
        _root = Path.Combine(Path.GetTempPath(), "lattice-file-wal-shared-window", Guid.NewGuid().ToString("N"));
        System.IO.Directory.CreateDirectory(_root);
    }

    [TearDown]
    public void TearDown() => System.IO.Directory.Delete(_root, recursive: true);

    [TestCase(-1L, 8, 1024L, new long[] { 0, 2, 5, long.MaxValue })]
    [TestCase(0L, 8, 1024L, new long[] { 2, 5, long.MaxValue })]
    [TestCase(1L, 8, 1024L, new long[] { 2, 5, long.MaxValue })]
    [TestCase(5L, 8, 1024L, new long[] { long.MaxValue })]
    [TestCase(long.MaxValue, 8, 1024L, new long[] { })]
    [TestCase(long.MinValue, 8, 1024L, new long[] { 0, 2, 5, long.MaxValue })]
    [TestCase(-1L, 2, 1024L, new long[] { 0, 2 })]
    [TestCase(-1L, 8, 8L, new long[] { 0, 2 })]
    [TestCase(-1L, 8, 1L, new long[] { 0 })]
    public async Task Snapshot_paths_select_the_same_count_and_byte_bounded_window(
        long fromOffsetExclusive, int maxEntries, long maxBytes, long[] expectedOffsets)
    {
        var governor = new ReadBudgetGovernor();
        using var shard = await CreateShardAsync(governor);

        var owned = await shard.SnapshotAsync(fromOffsetExclusive, maxEntries, maxBytes, CancellationToken.None);
        var decoded = await shard.SnapshotDecodedAsync(
            fromOffsetExclusive, maxEntries, maxBytes, sequence => sequence.ToArray(), CancellationToken.None);
        var filtered = await shard.SnapshotFilteredAsync(
            fromOffsetExclusive, long.MaxValue, maxEntries, maxBytes,
            new WalKeyFilter(null, null), routing: null, Decode, CancellationToken.None);

        try
        {
            Assert.Multiple(() =>
            {
                Assert.That(owned.Offsets, Is.EqualTo(expectedOffsets));
                Assert.That(decoded.Offsets, Is.EqualTo(expectedOffsets));
                Assert.That(filtered.Offsets.Take(filtered.Count), Is.EqualTo(expectedOffsets));
                Assert.That(decoded.Values, Is.EqualTo(owned.Payloads));
                Assert.That(filtered.Records.Take(filtered.Count).Select(r => r.Value), Is.EqualTo(owned.Payloads));
                Assert.That(governor.Calls, Is.EqualTo(expectedOffsets.Length == 0 ? 0 : 3),
                    "Only non-empty windows consult the governor, once per read.");
            });
        }
        finally
        {
            ReturnPage(filtered.Offsets, filtered.Records);
        }
    }

    [TestCase(-1L, 2L, new long[] { 0, 2 })]
    [TestCase(0L, 4L, new long[] { 2 })]
    [TestCase(2L, 5L, new long[] { 5 })]
    [TestCase(2L, 4L, new long[] { })]
    [TestCase(5L, 5L, new long[] { })]
    [TestCase(5L, 2L, new long[] { })]
    public async Task SnapshotFilteredAsync_honors_the_inclusive_upper_offset(
        long fromOffsetExclusive, long toOffsetInclusive, long[] expectedOffsets)
    {
        var governor = new ReadBudgetGovernor();
        using var shard = await CreateShardAsync(governor);

        var page = await shard.SnapshotFilteredAsync(
            fromOffsetExclusive, toOffsetInclusive, 8, 1024,
            new WalKeyFilter(null, null), routing: null, Decode, CancellationToken.None);

        try
        {
            Assert.Multiple(() =>
            {
                Assert.That(page.Offsets.Take(page.Count), Is.EqualTo(expectedOffsets));
                Assert.That(governor.Calls, Is.EqualTo(expectedOffsets.Length == 0 ? 0 : 1));
            });
        }
        finally
        {
            ReturnPage(page.Offsets, page.Records);
        }
    }

    private async Task<FileWalShard> CreateShardAsync(ReadBudgetGovernor governor)
    {
        var shard = new FileWalShard(
            _root, new FileWalStorageOptions { RootDirectory = _root, FlushToDisk = false },
            "tree-window", 0, governor);
        foreach (var offset in new[] { 0L, 2L, 5L, long.MaxValue })
        {
            await shard.AppendAsync(
                new[] { new PreparedWalRecord(offset, new byte[] { (byte)offset, 1, 2, 3 }) },
                CancellationToken.None);
        }

        return shard;
    }

    private static WalRecord Decode(ReadOnlySequence<byte> sequence) =>
        new() { Op = MutationKind.Set, Key = "key", Value = sequence.ToArray() };

    private static void ReturnPage(long[] offsets, WalRecord[] records)
    {
        if (offsets.Length > 0)
        {
            ArrayPool<long>.Shared.Return(offsets);
            ArrayPool<WalRecord>.Shared.Return(records, clearArray: true);
        }
    }

    private sealed class ReadBudgetGovernor : IWalReadPressureGovernor
    {
        internal int Calls { get; private set; }

        public long NarrowBudget(long configuredMaxBytes)
        {
            Calls++;
            return configuredMaxBytes;
        }

        public byte[] Allocate(int byteCount) => new byte[byteCount];
    }
}
