using System.Collections.Concurrent;
using System.Text;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Chaos coverage for the bounded read-through cache
/// (<see cref="LatticeOptions.MaxCacheValueBytes"/>). The fixture pins the
/// value-payload budget so small that almost every cached payload is evicted to
/// the metadata sentinel, so nearly every read exercises the
/// eviction-delegation path back to the primary leaf. The invariant proven here
/// is the whole point of the value-payload-only eviction design: trimming the
/// mirror must never turn a live key into a false miss and must never surface a
/// stale or cross-key payload, even under concurrent overwrite churn.
/// </summary>
[TestFixture]
[Category("Chaos")]
public class BoundedCacheEvictionChaosTests
{
    private BoundedCacheClusterFixture _fixture = null!;

    private const int KeyCount = 64;
    private const int ValueBytes = 2048; // 64 x 2 KiB = 128 KiB >> 8 KiB budget
    private const int Rounds = 6;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new BoundedCacheClusterFixture();
        await _fixture.InitializeAsync();
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown() => await _fixture.DisposeAsync();

    private static string Key(int i) => $"chaos-key-{i:D4}";

    // A deterministic 2 KiB payload that self-describes its key and round so a
    // reader can detect a false miss (null), a cross-key payload (wrong key
    // header), or an impossible round (a value never written).
    private static byte[] Value(int keyIndex, int round)
    {
        var header = $"{Key(keyIndex)}|{round}|";
        var bytes = new byte[ValueBytes];
        var headerBytes = Encoding.UTF8.GetBytes(header);
        Array.Copy(headerBytes, bytes, headerBytes.Length);
        for (var i = headerBytes.Length; i < bytes.Length; i++)
            bytes[i] = (byte)('a' + ((keyIndex + round + i) % 26));
        return bytes;
    }

    private static (int keyIndex, int round) DecodeHeader(byte[] value)
    {
        var text = Encoding.UTF8.GetString(value, 0, Math.Min(64, value.Length));
        var parts = text.Split('|');
        var keyIndex = int.Parse(parts[0]["chaos-key-".Length..]);
        var round = int.Parse(parts[1]);
        return (keyIndex, round);
    }

    [Test]
    public async Task Bounded_cache_never_false_misses_or_serves_stale_payloads_under_churn()
    {
        var tree = await _fixture.CreateTreeAsync("bounded-cache-chaos");

        // Seed round 0.
        for (var k = 0; k < KeyCount; k++)
            await tree.SetAsync(Key(k), Value(k, 0));

        var failures = new ConcurrentQueue<string>();

        for (var round = 1; round <= Rounds; round++)
        {
            var currentRound = round;
            using var writePhaseDone = new CancellationTokenSource();

            // Background readers hammer the keyspace while the overwrite is in
            // flight. During the churn a read may observe the previous or the
            // new round, but it must never be null (false miss) and must always
            // be a well-formed payload for the requested key with a round that
            // was actually written (0..currentRound).
            var readers = Enumerable.Range(0, 4).Select(_ => Task.Run(async () =>
            {
                var rng = new Random(Environment.CurrentManagedThreadId);
                while (!writePhaseDone.IsCancellationRequested)
                {
                    var k = rng.Next(KeyCount);
                    var got = await tree.GetAsync(Key(k));
                    if (got is null)
                    {
                        failures.Enqueue($"false miss: key {k} returned null in round {currentRound}");
                        continue;
                    }
                    var (decodedKey, decodedRound) = DecodeHeader(got);
                    if (decodedKey != k)
                        failures.Enqueue($"cross-key payload: asked key {k}, got key {decodedKey}");
                    if (decodedRound < 0 || decodedRound > currentRound)
                        failures.Enqueue($"impossible round: key {k} returned round {decodedRound} (max {currentRound})");
                    if (!got.SequenceEqual(Value(decodedKey, decodedRound)))
                        failures.Enqueue($"corrupt payload body: key {k} round {decodedRound}");
                }
            })).ToArray();

            // Overwrite every key to the new round.
            for (var k = 0; k < KeyCount; k++)
                await tree.SetAsync(Key(k), Value(k, currentRound));

            writePhaseDone.Cancel();
            await Task.WhenAll(readers);

            // Once the writes have settled, every key must read back as exactly
            // the current round - proving the eviction-delegation path returns
            // the authoritative payload, not a stale cached one.
            await Parallel.ForEachAsync(Enumerable.Range(0, KeyCount), async (k, _) =>
            {
                var got = await tree.GetAsync(Key(k));
                if (got is null || !got.SequenceEqual(Value(k, currentRound)))
                    failures.Enqueue($"settled read mismatch: key {k} round {currentRound}");
            });
        }

        Assert.That(failures, Is.Empty,
            "Bounded cache eviction must preserve read correctness under churn: " +
            string.Join("; ", failures.Take(10)));
    }
}
