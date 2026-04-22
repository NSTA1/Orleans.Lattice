using NSubstitute;
using Orleans.Lattice.Primitives;
using Orleans.Runtime;
using System.Text;
using System.Text.Json;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Unit tests for the resilient scan extension wrappers
/// (<see cref="LatticeExtensions.ScanKeysAsync"/>,
/// <see cref="LatticeExtensions.ScanEntriesAsync"/>, and the typed
/// <see cref="TypedLatticeExtensions.ScanEntriesAsync{T}(ILattice, ILatticeSerializer{T}, string?, string?, bool, bool?, int?, System.Threading.CancellationToken)"/>).
/// </summary>
public class ResilientScanExtensionsTests
{
    private record TestItem(string Name, int Score);

    private static readonly ILatticeSerializer<TestItem> Serializer = JsonLatticeSerializer<TestItem>.Default;

    // ── ScanKeysAsync ──────────────────────────────────────────

    [Test]
    public async Task ScanKeysAsync_yields_all_keys_when_no_abort()
    {
        var lattice = Substitute.For<ILattice>();
        StubKeys(lattice, _ => ScriptedKeys(new[] { "a", "b", "c" }, abortAfter: int.MaxValue));

        var keys = await CollectAsync(lattice.ScanKeysAsync());

        Assert.That(keys, Is.EqualTo(new[] { "a", "b", "c" }));
    }

    [Test]
    public async Task ScanKeysAsync_resumes_after_single_abort_with_successor_key()
    {
        var lattice = Substitute.For<ILattice>();
        var calls = new List<string?>();
        var callIndex = 0;
        lattice.KeysAsync(
            Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<bool>(), Arg.Any<bool?>(), Arg.Any<CancellationToken>())
            .Returns(ci =>
            {
                calls.Add(ci.ArgAt<string?>(0));
                var idx = callIndex++;
                return idx == 0
                    ? ScriptedKeys(new[] { "a", "b" }, abortAfter: 2)
                    : ScriptedKeys(new[] { "c", "d" }, abortAfter: int.MaxValue);
            });

        var keys = await CollectAsync(lattice.ScanKeysAsync());

        Assert.That(keys, Is.EqualTo(new[] { "a", "b", "c", "d" }));
        Assert.That(calls, Has.Count.EqualTo(2));
        Assert.That(calls[0], Is.Null, "first call uses caller's original start bound");
        Assert.That(calls[1], Is.EqualTo("b\u0000"), "second call resumes at successor of last yielded key");
    }

    [Test]
    public async Task ScanKeysAsync_resumes_after_multiple_aborts_without_duplicates()
    {
        var lattice = Substitute.For<ILattice>();
        var callIndex = 0;
        lattice.KeysAsync(
            Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<bool>(), Arg.Any<bool?>(), Arg.Any<CancellationToken>())
            .Returns(_ =>
            {
                var idx = callIndex++;
                return idx switch
                {
                    0 => ScriptedKeys(new[] { "a" }, abortAfter: 1),
                    1 => ScriptedKeys(new[] { "b" }, abortAfter: 1),
                    2 => ScriptedKeys(new[] { "c" }, abortAfter: 1),
                    _ => ScriptedKeys(new[] { "d", "e" }, abortAfter: int.MaxValue),
                };
            });

        var keys = await CollectAsync(lattice.ScanKeysAsync());

        Assert.That(keys, Is.EqualTo(new[] { "a", "b", "c", "d", "e" }));
    }

    [Test]
    public void ScanKeysAsync_rethrows_after_max_attempts_exhausted()
    {
        var lattice = Substitute.For<ILattice>();
        StubKeys(lattice, _ => ScriptedKeys(Array.Empty<string>(), abortAfter: 0));

        Assert.ThrowsAsync<EnumerationAbortedException>(async () =>
        {
            await foreach (var _ in lattice.ScanKeysAsync(maxAttempts: 2))
            {
            }
        });
    }

    [Test]
    public async Task ScanKeysAsync_reverse_resumes_with_last_key_as_upper_bound()
    {
        var lattice = Substitute.For<ILattice>();
        var callEnds = new List<string?>();
        var callIndex = 0;
        lattice.KeysAsync(
            Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<bool>(), Arg.Any<bool?>(), Arg.Any<CancellationToken>())
            .Returns(ci =>
            {
                callEnds.Add(ci.ArgAt<string?>(1));
                var idx = callIndex++;
                return idx == 0
                    ? ScriptedKeys(new[] { "d", "c" }, abortAfter: 2)
                    : ScriptedKeys(new[] { "b", "a" }, abortAfter: int.MaxValue);
            });

        var keys = await CollectAsync(lattice.ScanKeysAsync(reverse: true));

        Assert.That(keys, Is.EqualTo(new[] { "d", "c", "b", "a" }));
        Assert.That(callEnds[1], Is.EqualTo("c"), "reverse resume tightens endExclusive to last yielded key");
    }

    [Test]
    public void ScanKeysAsync_propagates_non_abort_exceptions_immediately()
    {
        var lattice = Substitute.For<ILattice>();
        StubKeys(lattice, _ => ThrowAsync<string>(new InvalidOperationException("boom")));

        Assert.ThrowsAsync<InvalidOperationException>(async () =>
        {
            await foreach (var _ in lattice.ScanKeysAsync())
            {
            }
        });
    }

    [Test]
    public void ScanKeysAsync_honors_cancellation_token()
    {
        var lattice = Substitute.For<ILattice>();
        StubKeys(lattice, _ => ScriptedKeys(new[] { "a", "b" }, abortAfter: int.MaxValue));
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        Assert.ThrowsAsync<OperationCanceledException>(async () =>
        {
            await foreach (var _ in lattice.ScanKeysAsync(cancellationToken: cts.Token))
            {
            }
        });
    }

    [Test]
    public void ScanKeysAsync_throws_for_null_lattice()
    {
        ILattice? lattice = null;
        Assert.ThrowsAsync<ArgumentNullException>(async () =>
        {
            await foreach (var _ in lattice!.ScanKeysAsync())
            {
            }
        });
    }

    // ── ScanEntriesAsync ───────────────────────────────────────

    [Test]
    public async Task ScanEntriesAsync_resumes_after_abort_preserving_values()
    {
        var lattice = Substitute.For<ILattice>();
        var starts = new List<string?>();
        var callIndex = 0;
        lattice.EntriesAsync(
            Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<bool>(), Arg.Any<bool?>(), Arg.Any<CancellationToken>())
            .Returns(ci =>
            {
                starts.Add(ci.ArgAt<string?>(0));
                var idx = callIndex++;
                return idx == 0
                    ? ScriptedEntries(new[] { ("a", 1), ("b", 2) }, abortAfter: 2)
                    : ScriptedEntries(new[] { ("c", 3) }, abortAfter: int.MaxValue);
            });

        var entries = new List<KeyValuePair<string, byte[]>>();
        await foreach (var e in lattice.ScanEntriesAsync()) entries.Add(e);

        Assert.That(entries.Select(e => e.Key).ToArray(), Is.EqualTo(new[] { "a", "b", "c" }));
        Assert.That(entries.Select(e => Encoding.UTF8.GetString(e.Value)).ToArray(),
            Is.EqualTo(new[] { "1", "2", "3" }));
        Assert.That(starts[1], Is.EqualTo("b\u0000"));
    }

    [Test]
    public void ScanEntriesAsync_throws_for_null_lattice()
    {
        ILattice? lattice = null;
        Assert.ThrowsAsync<ArgumentNullException>(async () =>
        {
            await foreach (var _ in lattice!.ScanEntriesAsync())
            {
            }
        });
    }

    // ── Typed ScanEntriesAsync<T> ──────────────────────────────

    [Test]
    public async Task ScanEntriesAsyncT_deserializes_values_across_reconnects()
    {
        var lattice = Substitute.For<ILattice>();
        var item1 = new TestItem("alice", 10);
        var item2 = new TestItem("bob", 20);
        var callIndex = 0;
        lattice.EntriesAsync(
            Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<bool>(), Arg.Any<bool?>(), Arg.Any<CancellationToken>())
            .Returns(_ =>
            {
                var idx = callIndex++;
                return idx == 0
                    ? ScriptedBytesEntries(new[] { ("a", item1) }, abortAfter: 1)
                    : ScriptedBytesEntries(new[] { ("b", item2) }, abortAfter: int.MaxValue);
            });

        var results = new List<KeyValuePair<string, TestItem>>();
        await foreach (var e in lattice.ScanEntriesAsync(Serializer)) results.Add(e);

        Assert.That(results.Select(r => r.Key), Is.EqualTo(new[] { "a", "b" }));
        Assert.That(results[0].Value, Is.EqualTo(item1));
        Assert.That(results[1].Value, Is.EqualTo(item2));
    }

    [Test]
    public void ScanEntriesAsyncT_throws_for_null_serializer()
    {
        var lattice = Substitute.For<ILattice>();
        Assert.ThrowsAsync<ArgumentNullException>(async () =>
        {
            await foreach (var _ in lattice.ScanEntriesAsync<TestItem>(serializer: null!))
            {
            }
        });
    }

    [Test]
    public async Task ScanEntriesAsyncT_default_serializer_roundtrips()
    {
        var lattice = Substitute.For<ILattice>();
        var item = new TestItem("carol", 30);
        lattice.EntriesAsync(
            Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<bool>(), Arg.Any<bool?>(), Arg.Any<CancellationToken>())
            .Returns(ScriptedBytesEntries(new[] { ("k", item) }, abortAfter: int.MaxValue));

        var results = new List<KeyValuePair<string, TestItem>>();
        await foreach (var e in lattice.ScanEntriesAsync<TestItem>()) results.Add(e);

        Assert.That(results, Has.Count.EqualTo(1));
        Assert.That(results[0].Value, Is.EqualTo(item));
    }

    // ── Parity + edge-case coverage ────────────────────────────

    [Test]
    public void ScanEntriesAsync_rethrows_after_max_attempts_exhausted()
    {
        var lattice = Substitute.For<ILattice>();
        lattice.EntriesAsync(
            Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<bool>(), Arg.Any<bool?>(), Arg.Any<CancellationToken>())
            .Returns(_ => ScriptedEntries(Array.Empty<(string, int)>(), abortAfter: 0));

        Assert.ThrowsAsync<EnumerationAbortedException>(async () =>
        {
            await foreach (var _ in lattice.ScanEntriesAsync(maxAttempts: 2))
            {
            }
        });
    }

    [Test]
    public async Task ScanEntriesAsync_reverse_resumes_with_last_key_as_upper_bound()
    {
        var lattice = Substitute.For<ILattice>();
        var callEnds = new List<string?>();
        var callIndex = 0;
        lattice.EntriesAsync(
            Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<bool>(), Arg.Any<bool?>(), Arg.Any<CancellationToken>())
            .Returns(ci =>
            {
                callEnds.Add(ci.ArgAt<string?>(1));
                var idx = callIndex++;
                return idx == 0
                    ? ScriptedEntries(new[] { ("d", 4), ("c", 3) }, abortAfter: 2)
                    : ScriptedEntries(new[] { ("b", 2), ("a", 1) }, abortAfter: int.MaxValue);
            });

        var keys = new List<string>();
        await foreach (var e in lattice.ScanEntriesAsync(reverse: true)) keys.Add(e.Key);

        Assert.That(keys, Is.EqualTo(new[] { "d", "c", "b", "a" }));
        Assert.That(callEnds[1], Is.EqualTo("c"));
    }

    [Test]
    public void ScanEntriesAsync_propagates_non_abort_exceptions_immediately()
    {
        var lattice = Substitute.For<ILattice>();
        lattice.EntriesAsync(
            Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<bool>(), Arg.Any<bool?>(), Arg.Any<CancellationToken>())
            .Returns(_ => ThrowAsync<KeyValuePair<string, byte[]>>(new InvalidOperationException("boom")));

        Assert.ThrowsAsync<InvalidOperationException>(async () =>
        {
            await foreach (var _ in lattice.ScanEntriesAsync())
            {
            }
        });
    }

    [Test]
    public void ScanEntriesAsync_honors_cancellation_token()
    {
        var lattice = Substitute.For<ILattice>();
        lattice.EntriesAsync(
            Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<bool>(), Arg.Any<bool?>(), Arg.Any<CancellationToken>())
            .Returns(_ => ScriptedEntries(new[] { ("a", 1), ("b", 2) }, abortAfter: int.MaxValue));
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        Assert.ThrowsAsync<OperationCanceledException>(async () =>
        {
            await foreach (var _ in lattice.ScanEntriesAsync(cancellationToken: cts.Token))
            {
            }
        });
    }

    [Test]
    public void ScanEntriesAsyncT_propagates_non_abort_exceptions_immediately()
    {
        var lattice = Substitute.For<ILattice>();
        lattice.EntriesAsync(
            Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<bool>(), Arg.Any<bool?>(), Arg.Any<CancellationToken>())
            .Returns(_ => ThrowAsync<KeyValuePair<string, byte[]>>(new InvalidOperationException("boom")));

        Assert.ThrowsAsync<InvalidOperationException>(async () =>
        {
            await foreach (var _ in lattice.ScanEntriesAsync(Serializer))
            {
            }
        });
    }

    [Test]
    public void ScanEntriesAsyncT_honors_cancellation_token()
    {
        var lattice = Substitute.For<ILattice>();
        var item = new TestItem("dan", 40);
        lattice.EntriesAsync(
            Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<bool>(), Arg.Any<bool?>(), Arg.Any<CancellationToken>())
            .Returns(_ => ScriptedBytesEntries(new[] { ("a", item) }, abortAfter: int.MaxValue));
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        Assert.ThrowsAsync<OperationCanceledException>(async () =>
        {
            await foreach (var _ in lattice.ScanEntriesAsync(Serializer, cancellationToken: cts.Token))
            {
            }
        });
    }

    [Test]
    public async Task ScanEntriesAsyncT_reverse_resumes_with_last_key_as_upper_bound()
    {
        var lattice = Substitute.For<ILattice>();
        var i1 = new TestItem("d", 4);
        var i2 = new TestItem("c", 3);
        var i3 = new TestItem("b", 2);
        var callEnds = new List<string?>();
        var callIndex = 0;
        lattice.EntriesAsync(
            Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<bool>(), Arg.Any<bool?>(), Arg.Any<CancellationToken>())
            .Returns(ci =>
            {
                callEnds.Add(ci.ArgAt<string?>(1));
                var idx = callIndex++;
                return idx == 0
                    ? ScriptedBytesEntries(new[] { ("d", i1), ("c", i2) }, abortAfter: 2)
                    : ScriptedBytesEntries(new[] { ("b", i3) }, abortAfter: int.MaxValue);
            });

        var keys = new List<string>();
        await foreach (var e in lattice.ScanEntriesAsync(Serializer, reverse: true)) keys.Add(e.Key);

        Assert.That(keys, Is.EqualTo(new[] { "d", "c", "b" }));
        Assert.That(callEnds[1], Is.EqualTo("c"));
    }

    [Test]
    public void ScanKeysAsync_maxAttempts_zero_fails_on_first_abort()
    {
        var lattice = Substitute.For<ILattice>();
        var callIndex = 0;
        lattice.KeysAsync(
            Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<bool>(), Arg.Any<bool?>(), Arg.Any<CancellationToken>())
            .Returns(_ =>
            {
                callIndex++;
                return ScriptedKeys(Array.Empty<string>(), abortAfter: 0);
            });

        Assert.ThrowsAsync<EnumerationAbortedException>(async () =>
        {
            await foreach (var _ in lattice.ScanKeysAsync(maxAttempts: 0))
            {
            }
        });
        Assert.That(callIndex, Is.EqualTo(1),
            "With maxAttempts=0 the first abort must rethrow without reopening the scan.");
    }

    [Test]
    public void ScanKeysAsync_negative_maxAttempts_is_clamped_to_zero()
    {
        var lattice = Substitute.For<ILattice>();
        var callIndex = 0;
        lattice.KeysAsync(
            Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<bool>(), Arg.Any<bool?>(), Arg.Any<CancellationToken>())
            .Returns(_ =>
            {
                callIndex++;
                return ScriptedKeys(Array.Empty<string>(), abortAfter: 0);
            });

        Assert.ThrowsAsync<EnumerationAbortedException>(async () =>
        {
            await foreach (var _ in lattice.ScanKeysAsync(maxAttempts: -5))
            {
            }
        });
        Assert.That(callIndex, Is.EqualTo(1),
            "Negative budget clamps to zero — no reconnects attempted.");
    }

    [Test]
    public async Task ScanKeysAsync_forwards_prefetch_and_initial_bounds()
    {
        var lattice = Substitute.For<ILattice>();
        string? observedStart = null;
        string? observedEnd = null;
        bool? observedPrefetch = null;
        bool observedReverse = false;
        lattice.KeysAsync(
            Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<bool>(), Arg.Any<bool?>(), Arg.Any<CancellationToken>())
            .Returns(ci =>
            {
                observedStart = ci.ArgAt<string?>(0);
                observedEnd = ci.ArgAt<string?>(1);
                observedReverse = ci.ArgAt<bool>(2);
                observedPrefetch = ci.ArgAt<bool?>(3);
                return ScriptedKeys(new[] { "m", "n" }, abortAfter: int.MaxValue);
            });

        var keys = await CollectAsync(lattice.ScanKeysAsync("k", "z", reverse: false, prefetch: true));

        Assert.That(keys, Is.EqualTo(new[] { "m", "n" }));
        Assert.That(observedStart, Is.EqualTo("k"));
        Assert.That(observedEnd, Is.EqualTo("z"));
        Assert.That(observedReverse, Is.False);
        Assert.That(observedPrefetch, Is.True);
    }

    [Test]
    public async Task ScanEntriesAsync_forwards_prefetch_and_initial_bounds()
    {
        var lattice = Substitute.For<ILattice>();
        bool? observedPrefetch = null;
        string? observedStart = null;
        string? observedEnd = null;
        lattice.EntriesAsync(
            Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<bool>(), Arg.Any<bool?>(), Arg.Any<CancellationToken>())
            .Returns(ci =>
            {
                observedStart = ci.ArgAt<string?>(0);
                observedEnd = ci.ArgAt<string?>(1);
                observedPrefetch = ci.ArgAt<bool?>(3);
                return ScriptedEntries(new[] { ("m", 13) }, abortAfter: int.MaxValue);
            });

        var items = new List<KeyValuePair<string, byte[]>>();
        await foreach (var e in lattice.ScanEntriesAsync("k", "z", prefetch: false)) items.Add(e);

        Assert.That(items, Has.Count.EqualTo(1));
        Assert.That(observedStart, Is.EqualTo("k"));
        Assert.That(observedEnd, Is.EqualTo("z"));
        Assert.That(observedPrefetch, Is.False);
    }

    [Test]
    public async Task ScanKeysAsync_applies_backoff_delay_on_second_reconnect()
    {
        // Attempt 1 (first reconnect) is immediate; attempt 2 applies
        // 10 ms × 2 = 20 ms backoff. Pin the floor with a generous slack.
        var lattice = Substitute.For<ILattice>();
        var callIndex = 0;
        lattice.KeysAsync(
            Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<bool>(), Arg.Any<bool?>(), Arg.Any<CancellationToken>())
            .Returns(_ =>
            {
                var idx = callIndex++;
                return idx switch
                {
                    0 => ScriptedKeys(new[] { "a" }, abortAfter: 1),
                    1 => ScriptedKeys(new[] { "b" }, abortAfter: 1),
                    _ => ScriptedKeys(new[] { "c" }, abortAfter: int.MaxValue),
                };
            });

        var sw = System.Diagnostics.Stopwatch.StartNew();
        var keys = await CollectAsync(lattice.ScanKeysAsync());
        sw.Stop();

        Assert.That(keys, Is.EqualTo(new[] { "a", "b", "c" }));
        Assert.That(sw.ElapsedMilliseconds, Is.GreaterThanOrEqualTo(15),
            "Second reconnect must apply at least ~20 ms of linear backoff (15 ms floor to absorb timer jitter).");
    }

    // ── Helpers ────────────────────────────────────────────────

    private static void StubKeys(ILattice lattice, Func<string?, IAsyncEnumerable<string>> producer)
    {
        lattice.KeysAsync(
            Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<bool>(), Arg.Any<bool?>(), Arg.Any<CancellationToken>())
            .Returns(ci => producer(ci.ArgAt<string?>(0)));
    }

    private static async IAsyncEnumerable<string> ScriptedKeys(string[] keys, int abortAfter)
    {
        var yielded = 0;
        foreach (var k in keys)
        {
            if (yielded >= abortAfter) throw new EnumerationAbortedException();
            yielded++;
            yield return k;
            await Task.Yield();
        }
        if (yielded < abortAfter) yield break;
        throw new EnumerationAbortedException();
    }

    private static async IAsyncEnumerable<KeyValuePair<string, byte[]>> ScriptedEntries(
        (string Key, int Value)[] entries, int abortAfter)
    {
        var yielded = 0;
        foreach (var (k, v) in entries)
        {
            if (yielded >= abortAfter) throw new EnumerationAbortedException();
            yielded++;
            yield return new KeyValuePair<string, byte[]>(k, Encoding.UTF8.GetBytes(v.ToString()));
            await Task.Yield();
        }
        if (yielded < abortAfter) yield break;
        throw new EnumerationAbortedException();
    }

    private static async IAsyncEnumerable<KeyValuePair<string, byte[]>> ScriptedBytesEntries(
        (string Key, TestItem Value)[] entries, int abortAfter)
    {
        var yielded = 0;
        foreach (var (k, v) in entries)
        {
            if (yielded >= abortAfter) throw new EnumerationAbortedException();
            yielded++;
            yield return new KeyValuePair<string, byte[]>(k, JsonSerializer.SerializeToUtf8Bytes(v));
            await Task.Yield();
        }
        if (yielded < abortAfter) yield break;
        throw new EnumerationAbortedException();
    }

#pragma warning disable CS1998
    private static async IAsyncEnumerable<T> ThrowAsync<T>(Exception ex)
    {
        throw ex;
#pragma warning disable CS0162
        yield break;
#pragma warning restore CS0162
    }
#pragma warning restore CS1998

    private static async Task<List<T>> CollectAsync<T>(IAsyncEnumerable<T> source)
    {
        var list = new List<T>();
        await foreach (var item in source) list.Add(item);
        return list;
    }
}
