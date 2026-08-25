using NSubstitute;
using Orleans.Lattice.Primitives;
using Orleans.Runtime;
using System.Text;
using System.Text.Json;

namespace Orleans.Lattice.Tests.Views;

/// <summary>
/// Unit tests for the resilient view-scan extension wrappers
/// (<see cref="LatticeViewExtensions.ScanKeysAsync"/>,
/// <see cref="LatticeViewExtensions.ScanEntriesAsync"/>, and the typed
/// <see cref="TypedLatticeViewExtensions.ScanEntriesAsync{T}(ILatticeView, ILatticeSerializer{T}, string?, string?, int?, System.Threading.CancellationToken)"/>),
/// mirroring <c>ResilientScanExtensionsTests</c> for the <see cref="ILattice"/>
/// wrappers. The view surface enumerates forward only, so there is no reverse
/// coverage.
/// </summary>
public class ResilientViewScanExtensionsTests
{
    private record TestItem(string Name, int Score);

    private static readonly ILatticeSerializer<TestItem> Serializer = JsonLatticeSerializer<TestItem>.Default;

    // ── ScanKeysAsync ──────────────────────────────────────────

    [Test]
    public async Task ScanKeysAsync_yields_all_keys_when_no_abort()
    {
        var view = Substitute.For<ILatticeView>();
        StubKeys(view, _ => ScriptedKeys(new[] { "a", "b", "c" }, abortAfter: int.MaxValue));

        var keys = await CollectAsync(view.ScanKeysAsync());

        Assert.That(keys, Is.EqualTo(new[] { "a", "b", "c" }));
    }

    [Test]
    public async Task ScanKeysAsync_resumes_after_single_abort_with_successor_key()
    {
        var view = Substitute.For<ILatticeView>();
        var calls = new List<string?>();
        var callIndex = 0;
        view.KeysAsync(Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<CancellationToken>())
            .Returns(ci =>
            {
                calls.Add(ci.ArgAt<string?>(0));
                var idx = callIndex++;
                return idx == 0
                    ? ScriptedKeys(new[] { "a", "b" }, abortAfter: 2)
                    : ScriptedKeys(new[] { "c", "d" }, abortAfter: int.MaxValue);
            });

        var keys = await CollectAsync(view.ScanKeysAsync());

        Assert.That(keys, Is.EqualTo(new[] { "a", "b", "c", "d" }));
        Assert.That(calls, Has.Count.EqualTo(2));
        Assert.That(calls[0], Is.Null, "first call passes the caller's original start bound through unchanged");
        Assert.That(calls[1], Is.EqualTo("b\u0000"), "second call resumes at successor of last yielded key");
    }

    [Test]
    public async Task ScanKeysAsync_resumes_after_multiple_aborts_without_duplicates()
    {
        var view = Substitute.For<ILatticeView>();
        var callIndex = 0;
        view.KeysAsync(Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<CancellationToken>())
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

        var keys = await CollectAsync(view.ScanKeysAsync());

        Assert.That(keys, Is.EqualTo(new[] { "a", "b", "c", "d", "e" }));
    }

    [Test]
    public void ScanKeysAsync_rethrows_after_max_attempts_exhausted()
    {
        var view = Substitute.For<ILatticeView>();
        StubKeys(view, _ => ScriptedKeys(Array.Empty<string>(), abortAfter: 0));

        Assert.ThrowsAsync<EnumerationAbortedException>(async () =>
        {
            await foreach (var _ in view.ScanKeysAsync(maxAttempts: 2))
            {
            }
        });
    }

    [Test]
    public void ScanKeysAsync_propagates_non_abort_exceptions_immediately()
    {
        var view = Substitute.For<ILatticeView>();
        StubKeys(view, _ => ThrowAsync<string>(new InvalidOperationException("boom")));

        Assert.ThrowsAsync<InvalidOperationException>(async () =>
        {
            await foreach (var _ in view.ScanKeysAsync())
            {
            }
        });
    }

    [Test]
    public void ScanKeysAsync_honors_cancellation_token()
    {
        var view = Substitute.For<ILatticeView>();
        StubKeys(view, _ => ScriptedKeys(new[] { "a", "b" }, abortAfter: int.MaxValue));
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        Assert.ThrowsAsync<OperationCanceledException>(async () =>
        {
            await foreach (var _ in view.ScanKeysAsync(cancellationToken: cts.Token))
            {
            }
        });
    }

    [Test]
    public void ScanKeysAsync_throws_for_null_view()
    {
        ILatticeView? view = null;
        Assert.ThrowsAsync<ArgumentNullException>(async () =>
        {
            await foreach (var _ in view!.ScanKeysAsync())
            {
            }
        });
    }

    [Test]
    public void ScanKeysAsync_maxAttempts_zero_fails_on_first_abort()
    {
        var view = Substitute.For<ILatticeView>();
        var callIndex = 0;
        view.KeysAsync(Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<CancellationToken>())
            .Returns(_ =>
            {
                callIndex++;
                return ScriptedKeys(Array.Empty<string>(), abortAfter: 0);
            });

        Assert.ThrowsAsync<EnumerationAbortedException>(async () =>
        {
            await foreach (var _ in view.ScanKeysAsync(maxAttempts: 0))
            {
            }
        });
        Assert.That(callIndex, Is.EqualTo(1),
            "With maxAttempts=0 the first abort must rethrow without reopening the scan.");
    }

    [Test]
    public void ScanKeysAsync_negative_maxAttempts_is_clamped_to_zero()
    {
        var view = Substitute.For<ILatticeView>();
        var callIndex = 0;
        view.KeysAsync(Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<CancellationToken>())
            .Returns(_ =>
            {
                callIndex++;
                return ScriptedKeys(Array.Empty<string>(), abortAfter: 0);
            });

        Assert.ThrowsAsync<EnumerationAbortedException>(async () =>
        {
            await foreach (var _ in view.ScanKeysAsync(maxAttempts: -5))
            {
            }
        });
        Assert.That(callIndex, Is.EqualTo(1),
            "Negative budget clamps to zero - no reconnects attempted.");
    }

    [Test]
    public async Task ScanKeysAsync_forwards_initial_bounds()
    {
        var view = Substitute.For<ILatticeView>();
        string? observedStart = null;
        string? observedEnd = null;
        view.KeysAsync(Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<CancellationToken>())
            .Returns(ci =>
            {
                observedStart = ci.ArgAt<string?>(0);
                observedEnd = ci.ArgAt<string?>(1);
                return ScriptedKeys(new[] { "m", "n" }, abortAfter: int.MaxValue);
            });

        var keys = await CollectAsync(view.ScanKeysAsync("k", "z"));

        Assert.That(keys, Is.EqualTo(new[] { "m", "n" }));
        Assert.That(observedStart, Is.EqualTo("k"));
        Assert.That(observedEnd, Is.EqualTo("z"));
    }

    [Test]
    public async Task ScanKeysAsync_passes_null_start_through_so_the_view_applies_its_reserved_floor()
    {
        // The wrapper must not synthesize a start bound on the first segment: it
        // passes the caller's start through unchanged so LatticeView applies its
        // own ReservedFloor (aggregation views start above the reserved NUL rows).
        var view = Substitute.For<ILatticeView>();
        var observedStarts = new List<string?>();
        var callIndex = 0;
        view.KeysAsync(Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<CancellationToken>())
            .Returns(ci =>
            {
                observedStarts.Add(ci.ArgAt<string?>(0));
                var idx = callIndex++;
                return idx == 0
                    ? ScriptedKeys(new[] { "g0" }, abortAfter: 1)
                    : ScriptedKeys(new[] { "g1" }, abortAfter: int.MaxValue);
            });

        var keys = await CollectAsync(view.ScanKeysAsync());

        Assert.That(keys, Is.EqualTo(new[] { "g0", "g1" }));
        Assert.That(observedStarts[0], Is.Null, "first segment delegates the floor decision to the view");
        Assert.That(observedStarts[1], Is.EqualTo("g0\u0000"), "resume bound is the successor of the last yielded key");
    }

    // ── ScanEntriesAsync ───────────────────────────────────────

    [Test]
    public async Task ScanEntriesAsync_resumes_after_abort_preserving_values()
    {
        var view = Substitute.For<ILatticeView>();
        var starts = new List<string?>();
        var callIndex = 0;
        view.EntriesAsync(Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<CancellationToken>())
            .Returns(ci =>
            {
                starts.Add(ci.ArgAt<string?>(0));
                var idx = callIndex++;
                return idx == 0
                    ? ScriptedEntries(new[] { ("a", 1), ("b", 2) }, abortAfter: 2)
                    : ScriptedEntries(new[] { ("c", 3) }, abortAfter: int.MaxValue);
            });

        var entries = new List<KeyValuePair<string, byte[]>>();
        await foreach (var e in view.ScanEntriesAsync()) entries.Add(e);

        Assert.That(entries.Select(e => e.Key).ToArray(), Is.EqualTo(new[] { "a", "b", "c" }));
        Assert.That(entries.Select(e => Encoding.UTF8.GetString(e.Value)).ToArray(),
            Is.EqualTo(new[] { "1", "2", "3" }));
        Assert.That(starts[1], Is.EqualTo("b\u0000"));
    }

    [Test]
    public void ScanEntriesAsync_throws_for_null_view()
    {
        ILatticeView? view = null;
        Assert.ThrowsAsync<ArgumentNullException>(async () =>
        {
            await foreach (var _ in view!.ScanEntriesAsync())
            {
            }
        });
    }

    [Test]
    public void ScanEntriesAsync_rethrows_after_max_attempts_exhausted()
    {
        var view = Substitute.For<ILatticeView>();
        view.EntriesAsync(Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<CancellationToken>())
            .Returns(_ => ScriptedEntries(Array.Empty<(string, int)>(), abortAfter: 0));

        Assert.ThrowsAsync<EnumerationAbortedException>(async () =>
        {
            await foreach (var _ in view.ScanEntriesAsync(maxAttempts: 2))
            {
            }
        });
    }

    [Test]
    public void ScanEntriesAsync_propagates_non_abort_exceptions_immediately()
    {
        var view = Substitute.For<ILatticeView>();
        view.EntriesAsync(Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<CancellationToken>())
            .Returns(_ => ThrowAsync<KeyValuePair<string, byte[]>>(new InvalidOperationException("boom")));

        Assert.ThrowsAsync<InvalidOperationException>(async () =>
        {
            await foreach (var _ in view.ScanEntriesAsync())
            {
            }
        });
    }

    [Test]
    public void ScanEntriesAsync_honors_cancellation_token()
    {
        var view = Substitute.For<ILatticeView>();
        view.EntriesAsync(Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<CancellationToken>())
            .Returns(_ => ScriptedEntries(new[] { ("a", 1), ("b", 2) }, abortAfter: int.MaxValue));
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        Assert.ThrowsAsync<OperationCanceledException>(async () =>
        {
            await foreach (var _ in view.ScanEntriesAsync(cancellationToken: cts.Token))
            {
            }
        });
    }

    [Test]
    public async Task ScanEntriesAsync_forwards_initial_bounds()
    {
        var view = Substitute.For<ILatticeView>();
        string? observedStart = null;
        string? observedEnd = null;
        view.EntriesAsync(Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<CancellationToken>())
            .Returns(ci =>
            {
                observedStart = ci.ArgAt<string?>(0);
                observedEnd = ci.ArgAt<string?>(1);
                return ScriptedEntries(new[] { ("m", 13) }, abortAfter: int.MaxValue);
            });

        var items = new List<KeyValuePair<string, byte[]>>();
        await foreach (var e in view.ScanEntriesAsync("k", "z")) items.Add(e);

        Assert.That(items, Has.Count.EqualTo(1));
        Assert.That(observedStart, Is.EqualTo("k"));
        Assert.That(observedEnd, Is.EqualTo("z"));
    }

    // ── Typed ScanEntriesAsync<T> ──────────────────────────────

    [Test]
    public async Task ScanEntriesAsyncT_deserializes_values_across_reconnects()
    {
        var view = Substitute.For<ILatticeView>();
        var item1 = new TestItem("alice", 10);
        var item2 = new TestItem("bob", 20);
        var callIndex = 0;
        view.EntriesAsync(Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<CancellationToken>())
            .Returns(_ =>
            {
                var idx = callIndex++;
                return idx == 0
                    ? ScriptedBytesEntries(new[] { ("a", item1) }, abortAfter: 1)
                    : ScriptedBytesEntries(new[] { ("b", item2) }, abortAfter: int.MaxValue);
            });

        var results = new List<KeyValuePair<string, TestItem>>();
        await foreach (var e in view.ScanEntriesAsync(Serializer)) results.Add(e);

        Assert.That(results.Select(r => r.Key), Is.EqualTo(new[] { "a", "b" }));
        Assert.That(results[0].Value, Is.EqualTo(item1));
        Assert.That(results[1].Value, Is.EqualTo(item2));
    }

    [Test]
    public void ScanEntriesAsyncT_throws_for_null_serializer()
    {
        var view = Substitute.For<ILatticeView>();
        Assert.ThrowsAsync<ArgumentNullException>(async () =>
        {
            await foreach (var _ in view.ScanEntriesAsync<TestItem>(serializer: null!))
            {
            }
        });
    }

    [Test]
    public void ScanEntriesAsyncT_throws_for_null_view()
    {
        ILatticeView? view = null;
        Assert.ThrowsAsync<ArgumentNullException>(async () =>
        {
            await foreach (var _ in view!.ScanEntriesAsync(Serializer))
            {
            }
        });
    }

    [Test]
    public async Task ScanEntriesAsyncT_default_serializer_roundtrips()
    {
        var view = Substitute.For<ILatticeView>();
        var item = new TestItem("carol", 30);
        view.EntriesAsync(Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<CancellationToken>())
            .Returns(ScriptedBytesEntries(new[] { ("k", item) }, abortAfter: int.MaxValue));

        var results = new List<KeyValuePair<string, TestItem>>();
        await foreach (var e in view.ScanEntriesAsync<TestItem>()) results.Add(e);

        Assert.That(results, Has.Count.EqualTo(1));
        Assert.That(results[0].Value, Is.EqualTo(item));
    }

    [Test]
    public void ScanEntriesAsyncT_propagates_non_abort_exceptions_immediately()
    {
        var view = Substitute.For<ILatticeView>();
        view.EntriesAsync(Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<CancellationToken>())
            .Returns(_ => ThrowAsync<KeyValuePair<string, byte[]>>(new InvalidOperationException("boom")));

        Assert.ThrowsAsync<InvalidOperationException>(async () =>
        {
            await foreach (var _ in view.ScanEntriesAsync(Serializer))
            {
            }
        });
    }

    [Test]
    public void ScanEntriesAsyncT_honors_cancellation_token()
    {
        var view = Substitute.For<ILatticeView>();
        var item = new TestItem("dan", 40);
        view.EntriesAsync(Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<CancellationToken>())
            .Returns(_ => ScriptedBytesEntries(new[] { ("a", item) }, abortAfter: int.MaxValue));
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        Assert.ThrowsAsync<OperationCanceledException>(async () =>
        {
            await foreach (var _ in view.ScanEntriesAsync(Serializer, cancellationToken: cts.Token))
            {
            }
        });
    }

    // ── Credential re-assertion across reopen (regression) ─────

    [Test]
    public async Task ScanEntriesAsync_reasserts_caller_credential_across_reopen()
    {
        // A resilient view scan driven under a pure credential scope (no
        // system-origin) must re-assert that credential on every reopened segment.
        // Orleans resets the caller-established RequestContext in the iterator's
        // execution flow after the first segment completes; without the
        // re-assertion a credential-scoped scan loses its credential on reopen, the
        // resumed segment resolves to an anonymous subject, and a fail-closed gate
        // silently truncates the scan. The reset is simulated by clearing the
        // ambient credential inside the aborting first segment.
        var credential = new LatticeCredential("run-subject", "test", "run-subject");
        var observed = new List<LatticeCredential?>();
        var callIndex = 0;
        var view = Substitute.For<ILatticeView>();
        view.EntriesAsync(Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<CancellationToken>())
            .Returns(_ =>
            {
                observed.Add(LatticeCredentialContext.Current);
                var idx = callIndex++;
                if (idx == 0)
                {
                    LatticeCredentialContext.Current = null; // simulate the RequestContext reset
                    return ScriptedEntries(Array.Empty<(string, int)>(), abortAfter: 0);
                }
                return ScriptedEntries(new[] { ("c", 3) }, abortAfter: int.MaxValue);
            });

        List<KeyValuePair<string, byte[]>> entries;
        using (LatticeCredentialContext.With(credential))
        {
            entries = new List<KeyValuePair<string, byte[]>>();
            await foreach (var e in view.ScanEntriesAsync()) entries.Add(e);
        }

        Assert.That(callIndex, Is.EqualTo(2), "the scan must reopen once after the abort");
        Assert.That(observed[0], Is.EqualTo(credential), "the first segment carries the caller credential");
        Assert.That(observed[1], Is.EqualTo(credential),
            "the reopened segment must re-assert the caller credential (anonymous before the fix)");
        Assert.That(entries.Select(e => e.Key), Is.EqualTo(new[] { "c" }));
    }

    [Test]
    public async Task ScanKeysAsync_reasserts_caller_credential_across_reopen()
    {
        var credential = new LatticeCredential("run-subject", "test", "run-subject");
        var observed = new List<LatticeCredential?>();
        var callIndex = 0;
        var view = Substitute.For<ILatticeView>();
        view.KeysAsync(Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<CancellationToken>())
            .Returns(_ =>
            {
                observed.Add(LatticeCredentialContext.Current);
                var idx = callIndex++;
                if (idx == 0)
                {
                    LatticeCredentialContext.Current = null; // simulate the RequestContext reset
                    return ScriptedKeys(Array.Empty<string>(), abortAfter: 0);
                }
                return ScriptedKeys(new[] { "c" }, abortAfter: int.MaxValue);
            });

        List<string> keys;
        using (LatticeCredentialContext.With(credential))
        {
            keys = await CollectAsync(view.ScanKeysAsync());
        }

        Assert.That(callIndex, Is.EqualTo(2), "the scan must reopen once after the abort");
        Assert.That(observed[0], Is.EqualTo(credential), "the first segment carries the caller credential");
        Assert.That(observed[1], Is.EqualTo(credential),
            "the reopened segment must re-assert the caller credential (anonymous before the fix)");
        Assert.That(keys, Is.EqualTo(new[] { "c" }));
    }

    // ── Helpers ────────────────────────────────────────────────

    private static void StubKeys(ILatticeView view, Func<string?, IAsyncEnumerable<string>> producer)
    {
        view.KeysAsync(Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<CancellationToken>())
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
