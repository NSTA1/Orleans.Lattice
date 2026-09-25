using System.Buffers;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Testing;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Serialization;
using Orleans.Serialization.Session;

namespace Orleans.Lattice.Storage.File.Tests;

/// <summary>
/// <see cref="FileWalShard.SnapshotFilteredAsync"/>, the file provider's
/// filtered page read (issue #3565). With a routing reader, an excluded record
/// costs a short prefix read and nothing else: it is never decoded, and the
/// page allocates in proportion to what it keeps. These tests count decodes
/// and allocations directly at the shard, where both are observable.
/// </summary>
[TestFixture]
public sealed class FileWalShardFilteredReadTests
{
    private const string TreeId = "tree-file-filtered";

    private ServiceProvider _services = null!;
    private Serializer<WalRecord> _serializer = null!;
    private OrleansBinaryWalRecordEncoder _encoder = null!;
    private WalRecordRoutingReader _routing = null!;
    private string _root = null!;

    [OneTimeSetUp]
    public void OneTimeSetUp()
    {
        _services = new ServiceCollection().AddSerializer().BuildServiceProvider();
        _serializer = _services.GetRequiredService<Serializer<WalRecord>>();
        _encoder = new OrleansBinaryWalRecordEncoder(_serializer);
        _routing = new WalRecordRoutingReader(_services.GetRequiredService<SerializerSessionPool>());
    }

    [OneTimeTearDown]
    public void OneTimeTearDown() => _services.Dispose();

    [SetUp]
    public void SetUp()
    {
        _root = Path.Combine(Path.GetTempPath(), "lattice-file-wal-filtered", Guid.NewGuid().ToString("N"));
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
        }
    }

    private FileWalShard CreateShard(string name = "shard") =>
        new(Path.Combine(_root, name), new FileWalStorageOptions { RootDirectory = _root });

    private async Task AppendAsync(FileWalShard shard, IReadOnlyList<string> keys, int valueBytes = 64)
    {
        var records = new PreparedWalRecord[keys.Count];
        for (var i = 0; i < keys.Count; i++)
        {
            var record = new WalRecord
            {
                TreeId = TreeId,
                Op = MutationKind.Set,
                Key = keys[i],
                Value = Enumerable.Repeat((byte)i, valueBytes).ToArray(),
                OriginClusterId = "site-a",
            };
            var writer = new ArrayBufferWriter<byte>();
            _encoder.Encode(in record, writer);
            records[i] = new PreparedWalRecord(i, writer.WrittenMemory.ToArray());
        }

        await shard.AppendAsync(records, CancellationToken.None);
    }

    private async Task<List<(long Offset, WalRecord Record)>> SnapshotAsync(
        FileWalShard shard,
        WalKeyFilter filter,
        WalRecordRoutingReader? routing,
        Func<ReadOnlySequence<byte>, WalRecord> decode,
        int maxEntries = 1024)
    {
        var (offsets, records, count) = await shard.SnapshotFilteredAsync(
            -1, long.MaxValue, maxEntries, FileWalStorageOptions.DefaultMaxReadBatchBytes, filter, routing, decode, CancellationToken.None);
        try
        {
            return Enumerable.Range(0, count).Select(i => (offsets[i], records[i])).ToList();
        }
        finally
        {
            if (offsets.Length > 0)
            {
                ArrayPool<long>.Shared.Return(offsets);
                ArrayPool<WalRecord>.Shared.Return(records, clearArray: true);
            }
        }
    }

    [Test]
    public async Task Filtered_snapshot_applies_the_shard_axis()
    {
        using var shard = CreateShard();
        var keys = Enumerable.Range(0, 40).Select(i => $"k{i:D2}").ToArray();
        await AppendAsync(shard, keys);
        var map = ShardMap.CreateDefault(64, 4);
        var filter = new WalKeyFilter(null, null, map, 1);

        var read = await SnapshotAsync(shard, filter, _routing, _serializer.Deserialize);

        var owned = keys.Where(k => map.Resolve(k) == 1).ToArray();
        var kept = read.Where(r => r.Record.Value is not null).Select(r => r.Record.Key).ToArray();
        Assert.Multiple(() =>
        {
            Assert.That(kept, Is.EqualTo(owned));
            Assert.That(read[^1].Offset, Is.EqualTo(39), "The last examined record is always delivered.");
        });
    }

    [Test]
    public async Task Filtered_snapshot_decodes_only_the_records_it_keeps()
    {
        using var shard = CreateShard();
        await AppendAsync(shard, ["a0", "m1", "b2", "m3", "c4", "z5"]);
        var decodes = 0;
        WalRecord Counting(ReadOnlySequence<byte> payload)
        {
            decodes++;
            return _serializer.Deserialize(payload);
        }

        var read = await SnapshotAsync(shard, new WalKeyFilter("m", "n"), _routing, Counting);

        Assert.Multiple(() =>
        {
            Assert.That(read.Select(r => r.Offset), Is.EqualTo(new[] { 1L, 3L, 5L }));
            Assert.That(decodes, Is.EqualTo(2),
                "Only the two owned records are decoded; the four excluded ones - the trailing routing-only one included - are classified from their prefix.");
            Assert.That(read[^1].Record.Key, Is.EqualTo("z5"));
            Assert.That(read[^1].Record.Value, Is.Null);
        });
    }

    [Test]
    public async Task Filtered_snapshot_decodes_a_record_whose_key_outruns_the_routing_prefix()
    {
        // A key longer than the prefix cannot be classified from it, so the
        // shard must fall back to the full decode rather than guess.
        using var shard = CreateShard();
        var longForeign = new string('z', FileWalShard.RoutingPrefixBytes + 200);
        var longOwned = "m" + new string('m', FileWalShard.RoutingPrefixBytes + 200);
        await AppendAsync(shard, [longForeign, longOwned, "a2"]);
        var decodes = 0;
        WalRecord Counting(ReadOnlySequence<byte> payload)
        {
            decodes++;
            return _serializer.Deserialize(payload);
        }

        var read = await SnapshotAsync(shard, new WalKeyFilter("m", "n"), _routing, Counting);

        Assert.Multiple(() =>
        {
            Assert.That(read.Select(r => r.Offset), Is.EqualTo(new[] { 1L, 2L }));
            Assert.That(read[0].Record.Key, Is.EqualTo(longOwned));
            Assert.That(decodes, Is.EqualTo(2), "Both long keys fall back to the full decode; the short trailing one does not.");
        });
    }

    [Test]
    public async Task Filtered_snapshot_without_a_routing_reader_returns_the_same_records()
    {
        var keys = new[] { "a0", "m1", "\u00FCber", "m\uD83D\uDE00", "b4", "m5", "z6" };
        using var routed = CreateShard("routed");
        using var unrouted = CreateShard("unrouted");
        await AppendAsync(routed, keys);
        await AppendAsync(unrouted, keys);
        var filter = new WalKeyFilter("m", "n");

        var fast = await SnapshotAsync(routed, filter, _routing, _serializer.Deserialize);
        var slow = await SnapshotAsync(unrouted, filter, routing: null, _serializer.Deserialize);

        static string Render((long Offset, WalRecord Record) r) =>
            $"{r.Offset}:{r.Record.Op}:{r.Record.Key}:{Convert.ToHexString(r.Record.Value ?? [])}";

        Assert.That(fast.Select(Render), Is.EqualTo(slow.Select(Render)));
    }

    [Test]
    public async Task Filtered_snapshot_narrows_and_retries_when_a_decode_runs_out_of_memory()
    {
        using var shard = CreateShard();
        await AppendAsync(shard, ["m0", "m1", "m2", "m3", "m4", "m5", "m6", "m7"]);
        var failures = 1;
        WalRecord FailOnce(ReadOnlySequence<byte> payload)
        {
            if (failures-- > 0)
            {
                throw new OutOfMemoryException("scripted: the page could not be afforded.");
            }

            return _serializer.Deserialize(payload);
        }

        var read = await SnapshotAsync(shard, new WalKeyFilter("m", "n"), _routing, FailOnce, maxEntries: 8);

        Assert.Multiple(() =>
        {
            Assert.That(read.Select(r => r.Offset), Is.EqualTo(new[] { 0L, 1L }),
                "The retry examines a quarter of the window and keeps what it owns there.");
            Assert.That(shard.ReadPressureDegradations, Is.EqualTo(1));
        });
    }

    [Test]
    public async Task Filtered_snapshot_skips_excluded_records_without_allocating_per_record()
    {
        // The claim the push-down exists for: an excluded record costs a prefix
        // read and nothing on the heap, so a window of other leaves' records
        // allocates the same whatever its size. Measured differentially through
        // the shared probe; the read is async, so it needs an optimized build and
        // the process-wide counter.
        AllocationContract.RequireOptimizedBuild(
            typeof(FileWalShard).Assembly, typeof(WalKeyFilter).Assembly, typeof(FileWalShardFilteredReadTests).Assembly);

        using var shard = CreateShard();
        await AppendAsync(shard, Enumerable.Range(0, 512).Select(i => $"a{i:D4}").ToArray(), valueBytes: 4096);
        var filter = new WalKeyFilter("m", "n");
        var routing = _routing;
        var decode = (Func<ReadOnlySequence<byte>, WalRecord>)_serializer.Deserialize;
        var lastExamined = -1L;

        var growth = AllocationProbe.Growth(
            prepare: _ => shard,
            measure: (s, size) =>
            {
                var (offsets, records, count) = s
                    .SnapshotFilteredAsync(-1, long.MaxValue, size, FileWalStorageOptions.DefaultMaxReadBatchBytes, filter, routing, decode, CancellationToken.None)
                    .GetAwaiter()
                    .GetResult();
                lastExamined = offsets[count - 1];
                ArrayPool<long>.Shared.Return(offsets);
                ArrayPool<WalRecord>.Shared.Return(records, clearArray: true);
            },
            smallSize: 128,
            largeSize: 512,
            crossesThreads: true);

        TestContext.Out.WriteLine($"Growth over 384 extra excluded 4 KiB records: {growth} bytes.");
        Assert.Multiple(() =>
        {
            // A page narrowed by memory pressure would examine fewer records and
            // report less growth, so pin the window before trusting the figure.
            Assert.That(lastExamined, Is.EqualTo(511), "The large sample must have examined the whole window.");
            Assert.That(growth, Is.LessThan(384 * 64),
                "An excluded record must not allocate: 384 extra 4 KiB records decoded would cost well over a megabyte, "
                + "and even a small per-record object would exceed this bound.");
        });
    }
}
