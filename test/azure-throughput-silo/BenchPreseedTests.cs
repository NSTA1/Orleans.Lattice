using NSubstitute;
using Orleans.Lattice;
using VehicleFleetSimulator.AzureThroughput.Engine;

namespace VehicleFleetSimulator.AzureThroughput.Silo.Tests;

/// <summary>
/// Pins the read-mode preseed (#3474): it must write exactly the keys the
/// producer's generator reads, only for the read modes, and the Layer 3 seed
/// must bulk-append them in ascending, bounded, re-drivable chunks.
/// </summary>
[TestFixture]
public class BenchPreseedTests
{
    // The producer's ChannelGenerator recipe, verbatim (uint literals).
    private static string GeneratorKey(int i)
    {
        Span<byte> idBytes = stackalloc byte[16];
        BitConverter.TryWriteBytes(idBytes[..4], i);
        BitConverter.TryWriteBytes(idBytes.Slice(4, 4), 0xC0FFEE);
        BitConverter.TryWriteBytes(idBytes.Slice(8, 4), 0xDEADBEEF);
        BitConverter.TryWriteBytes(idBytes.Slice(12, 4), 0xCAFEBABE);
        return new Guid(idBytes).ToString("N");
    }

    [TestCase(0)]
    [TestCase(1)]
    [TestCase(255)]
    [TestCase(1_000_000)]
    public void KeyFor_matches_generator_vehicle_key(int index)
    {
        Assert.That(BenchPreseed.KeyFor(index), Is.EqualTo(GeneratorKey(index)));
        Assert.That(BenchPreseed.KeyFor(index), Has.Length.EqualTo(32));
    }

    [TestCase(BenchWorkloadMode.GetPoint, 10, true)]
    [TestCase(BenchWorkloadMode.GetMany, 10, true)]
    [TestCase(BenchWorkloadMode.GetPoint, 0, false)]
    [TestCase(BenchWorkloadMode.SetPoint, 10, false)]
    [TestCase(BenchWorkloadMode.SetPointMv, 10, false)]
    [TestCase(BenchWorkloadMode.SetMany, 10, false)]
    public void IsRequired_only_for_read_modes_with_keys(BenchWorkloadMode mode, int keyCount, bool expected)
    {
        Assert.That(BenchPreseed.IsRequired(mode, keyCount), Is.EqualTo(expected));
    }

    [TestCase(0, 300)]
    [TestCase(256_000, 300)]
    [TestCase(300_000, 300)]
    [TestCase(300_001, 301)]
    [TestCase(512_000, 512)]
    [TestCase(1_024_000, 1024)]
    public void BudgetFor_scales_with_key_count_above_the_floor(int keyCount, int expectedSec)
    {
        Assert.That(BenchPreseed.BudgetFor(keyCount), Is.EqualTo(TimeSpan.FromSeconds(expectedSec)));
    }

    [Test]
    public void BudgetFor_rejects_a_negative_key_count()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => BenchPreseed.BudgetFor(-1));
    }

    [Test]
    public void BuildEntries_is_deterministic_with_fixed_payload_size()
    {
        var entries = BenchPreseed.BuildEntries(3);

        Assert.That(entries.Select(e => e.Key), Is.EqualTo(new[] { GeneratorKey(0), GeneratorKey(1), GeneratorKey(2) }));
        Assert.That(entries.All(e => e.Value.Length == BenchPreseed.PayloadBytes), Is.True);
        Assert.That(entries[1].Value[0], Is.EqualTo(1));
        Assert.That(entries[1].Value[BenchPreseed.PayloadBytes - 1], Is.EqualTo((byte)BenchPreseed.PayloadBytes));
        Assert.That(BenchPreseed.BuildEntries(3)[2].Value, Is.EqualTo(entries[2].Value));
    }

    [Test]
    public void BuildSortedEntries_holds_every_key_in_ascending_ordinal_order()
    {
        var sorted = BenchPreseed.BuildSortedEntries(500);

        Assert.That(sorted.Select(e => e.Key), Is.EqualTo(Enumerable.Range(0, 500).Select(GeneratorKey).Order(StringComparer.Ordinal)));
        Assert.That(sorted.All(e => e.Value.SequenceEqual(BenchPreseed.PayloadFor(Index(e.Key)))), Is.True, "each key keeps its own payload");
    }

    [Test]
    public void ChunkOperationId_is_stable_per_chunk_and_distinct_across_chunks()
    {
        Assert.That(BenchPreseed.ChunkOperationId("t", 16_384), Is.EqualTo(BenchPreseed.ChunkOperationId("t", 16_384)));
        Assert.That(BenchPreseed.ChunkOperationId("t", 0), Is.Not.EqualTo(BenchPreseed.ChunkOperationId("t", 16_384)));
        Assert.That(BenchPreseed.ChunkOperationId("a", 0), Is.Not.EqualTo(BenchPreseed.ChunkOperationId("b", 0)));
    }

    [Test]
    public async Task SeedAsync_bulk_appends_every_key_in_ascending_bounded_chunks()
    {
        var lattice = Substitute.For<ILattice>();
        var chunkSizes = new List<int>();
        var keys = new List<string>();
        var operationIds = new List<string>();
        lattice.BulkAppendChunkAsync(Arg.Any<string>(), Arg.Any<IReadOnlyList<KeyValuePair<string, byte[]>>>(), Arg.Any<CancellationToken>())
            .Returns(ci =>
            {
                var chunk = ci.Arg<IReadOnlyList<KeyValuePair<string, byte[]>>>();
                operationIds.Add(ci.Arg<string>());
                chunkSizes.Add(chunk.Count);
                keys.AddRange(chunk.Select(e => e.Key));
                return Task.FromResult(chunk.Count);
            });

        var written = await BenchPreseed.SeedAsync(lattice, count: 10, chunkSize: 4, "t", CancellationToken.None);

        Assert.That(written, Is.EqualTo(10));
        Assert.That(chunkSizes, Is.EqualTo(new[] { 4, 4, 2 }));
        Assert.That(keys, Is.EqualTo(SortedKeys(10)), "keys ascend within and across chunks");
        Assert.That(operationIds, Is.EqualTo(new[] { "preseed-t-0", "preseed-t-4", "preseed-t-8" }));
        await lattice.DidNotReceiveWithAnyArgs().SetManyAsync(default!, default);
    }

    [Test]
    public async Task SeedAsync_resumes_by_re_driving_the_failed_chunk_under_its_operation_id()
    {
        var lattice = Substitute.For<ILattice>();
        var keys = new List<string>();
        var operationIds = new List<string>();
        var calls = 0;
        lattice.BulkAppendChunkAsync(Arg.Any<string>(), Arg.Any<IReadOnlyList<KeyValuePair<string, byte[]>>>(), Arg.Any<CancellationToken>())
            .Returns(ci =>
            {
                operationIds.Add(ci.Arg<string>());
                if (++calls == 2)
                {
                    return Task.FromException<int>(new InvalidOperationException("saturated"));
                }

                var chunk = ci.Arg<IReadOnlyList<KeyValuePair<string, byte[]>>>();
                keys.AddRange(chunk.Select(e => e.Key));
                return Task.FromResult(chunk.Count);
            });
        var landed = 0;

        Assert.ThrowsAsync<InvalidOperationException>(() =>
            BenchPreseed.SeedAsync(lattice, count: 10, chunkSize: 4, "t", CancellationToken.None, landed, o => landed = o));
        Assert.That(landed, Is.EqualTo(4));

        var reported = new List<int>();
        var written = await BenchPreseed.SeedAsync(lattice, 10, 4, "t", CancellationToken.None, landed, o => { landed = o; reported.Add(o); });

        Assert.That(written, Is.EqualTo(10));
        Assert.That(reported, Is.EqualTo(new[] { 8, 10 }));
        Assert.That(keys, Is.EqualTo(SortedKeys(10)), "no landed chunk is re-written");
        Assert.That(operationIds, Is.EqualTo(new[] { "preseed-t-0", "preseed-t-4", "preseed-t-4", "preseed-t-8" }), "the failed chunk is re-driven under the same id");
    }

    [Test]
    public async Task SeedAsync_with_start_offset_at_count_writes_nothing()
    {
        var lattice = Substitute.For<ILattice>();

        var written = await BenchPreseed.SeedAsync(lattice, 10, 4, "t", CancellationToken.None, startOffset: 10);

        Assert.That(written, Is.EqualTo(10));
        await lattice.DidNotReceiveWithAnyArgs().BulkAppendChunkAsync(default!, default!, default);
    }

    [TestCase(-1)]
    [TestCase(11)]
    public void SeedAsync_rejects_a_start_offset_outside_the_seed(int startOffset)
    {
        var lattice = Substitute.For<ILattice>();

        Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => BenchPreseed.SeedAsync(lattice, 10, 4, "t", CancellationToken.None, startOffset));
    }

    [Test]
    public void SeedAsync_rejects_non_positive_chunk_size_and_empty_tree_id()
    {
        var lattice = Substitute.For<ILattice>();

        Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => BenchPreseed.SeedAsync(lattice, 1, 0, "t", CancellationToken.None));
        Assert.ThrowsAsync<ArgumentException>(() => BenchPreseed.SeedAsync(lattice, 1, 1, "", CancellationToken.None));
    }

    [Test]
    public async Task SeedWithRetryAsync_progress_resets_attempt_budget_for_each_chunk()
    {
        var lattice = Substitute.For<ILattice>();
        var attempts = new Dictionary<string, int>();
        var written = new List<string>();
        lattice.BulkAppendChunkAsync(Arg.Any<string>(), Arg.Any<IReadOnlyList<KeyValuePair<string, byte[]>>>(), Arg.Any<CancellationToken>())
            .Returns(ci =>
            {
                var key = ci.Arg<IReadOnlyList<KeyValuePair<string, byte[]>>>()[0].Key;
                attempts.TryGetValue(key, out var previous);
                attempts[key] = previous + 1;
                if (previous == 0) return Task.FromException<int>(new TimeoutException("transient chunk"));
                written.Add(key);
                return Task.FromResult(1);
            });

        await BenchPreseed.SeedWithRetryAsync(lattice, 9, 1, "test", TimeSpan.FromSeconds(60), CancellationToken.None);

        Assert.That(written, Is.EqualTo(SortedKeys(9)));
        Assert.That(attempts.Values, Is.All.EqualTo(2));
    }

    [Test]
    public async Task SeedWithRetryAsync_retries_a_storage_server_timeout_under_the_same_operation_id()
    {
        var lattice = Substitute.For<ILattice>();
        var operationIds = new List<string>();
        lattice.BulkAppendChunkAsync(Arg.Any<string>(), Arg.Any<IReadOnlyList<KeyValuePair<string, byte[]>>>(), Arg.Any<CancellationToken>())
            .Returns(ci =>
            {
                operationIds.Add(ci.Arg<string>());
                return operationIds.Count == 2
                    ? Task.FromException<int>(new InvalidOperationException("Operation could not be completed within the specified time.\nErrorCode: OperationTimedOut"))
                    : Task.FromResult(ci.Arg<IReadOnlyList<KeyValuePair<string, byte[]>>>().Count);
            });

        await BenchPreseed.SeedWithRetryAsync(lattice, 3, 1, "t", TimeSpan.FromSeconds(60), CancellationToken.None);

        Assert.That(operationIds, Is.EqualTo(new[] { "preseed-t-0", "preseed-t-1", "preseed-t-1", "preseed-t-2" }));
    }

    [Test]
    public void SeedWithRetryAsync_persistently_failing_chunk_stops_at_eight_attempts()
    {
        var lattice = Substitute.For<ILattice>();
        var calls = 0;
        lattice.BulkAppendChunkAsync(Arg.Any<string>(), Arg.Any<IReadOnlyList<KeyValuePair<string, byte[]>>>(), Arg.Any<CancellationToken>())
            .Returns(_ =>
            {
                calls++;
                return Task.FromException<int>(new TimeoutException("persistent chunk"));
            });

        Assert.ThrowsAsync<TimeoutException>(() =>
            BenchPreseed.SeedWithRetryAsync(lattice, 1, 1, "test", TimeSpan.FromSeconds(60), CancellationToken.None));
        Assert.That(calls, Is.EqualTo(8));
    }

    [Test]
    public void SeedWithRetryAsync_wall_clock_budget_bounds_a_hung_call()
    {
        var lattice = Substitute.For<ILattice>();
        var pending = new TaskCompletionSource<int>(TaskCreationOptions.RunContinuationsAsynchronously);
        lattice.BulkAppendChunkAsync(Arg.Any<string>(), Arg.Any<IReadOnlyList<KeyValuePair<string, byte[]>>>(), Arg.Any<CancellationToken>())
            .Returns(pending.Task);

        try
        {
            Assert.CatchAsync<OperationCanceledException>(() =>
                BenchPreseed.SeedWithRetryAsync(lattice, 1, 1, "test", TimeSpan.FromMilliseconds(100), CancellationToken.None));
        }
        finally { pending.SetResult(0); }
    }

    [Test]
    public void SeedWithRetryAsync_terminal_fault_is_not_retried()
    {
        var lattice = Substitute.For<ILattice>();
        var calls = 0;
        lattice.BulkAppendChunkAsync(Arg.Any<string>(), Arg.Any<IReadOnlyList<KeyValuePair<string, byte[]>>>(), Arg.Any<CancellationToken>())
            .Returns(_ =>
            {
                calls++;
                return Task.FromException<int>(new InvalidOperationException("terminal"));
            });

        Assert.ThrowsAsync<InvalidOperationException>(() =>
            BenchPreseed.SeedWithRetryAsync(lattice, 1, 1, "test", TimeSpan.FromSeconds(60), CancellationToken.None));
        Assert.That(calls, Is.EqualTo(1));
    }

    private static string[] SortedKeys(int count) =>
        Enumerable.Range(0, count).Select(GeneratorKey).Order(StringComparer.Ordinal).ToArray();

    private static int Index(string key) =>
        Enumerable.Range(0, 500).First(i => GeneratorKey(i) == key);
}