using NSubstitute;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Runtime;
using System.Text;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Unit tests for the internal resilient scan wrapper
/// <see cref="SystemLatticeScanExtensions.ScanEntriesAsync"/> over
/// <see cref="ISystemLattice"/>, mirroring the coverage of the public
/// <see cref="LatticeExtensions.ScanEntriesAsync(ILattice, string?, string?, bool, bool?, int?, System.Threading.CancellationToken)"/>
/// wrapper. The reserved-system-tree surface has the same
/// <c>EnumerationAbortedException</c> recovery contract as the public tree.
/// </summary>
public class SystemLatticeScanExtensionsTests
{
    [Test]
    public async Task ScanEntriesAsync_yields_all_entries_when_no_abort()
    {
        var tree = Substitute.For<ISystemLattice>();
        tree.EntriesAsync(
            Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<bool>(), Arg.Any<bool?>(), Arg.Any<CancellationToken>())
            .Returns(_ => ScriptedEntries(new[] { ("a", 1), ("b", 2), ("c", 3) }, abortAfter: int.MaxValue));

        var keys = new List<string>();
        await foreach (var e in tree.ScanEntriesAsync()) keys.Add(e.Key);

        Assert.That(keys, Is.EqualTo(new[] { "a", "b", "c" }));
    }

    [Test]
    public async Task ScanEntriesAsync_resumes_after_abort_with_successor_key()
    {
        var tree = Substitute.For<ISystemLattice>();
        var starts = new List<string?>();
        var callIndex = 0;
        tree.EntriesAsync(
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
        await foreach (var e in tree.ScanEntriesAsync()) entries.Add(e);

        Assert.That(entries.Select(e => e.Key).ToArray(), Is.EqualTo(new[] { "a", "b", "c" }));
        Assert.That(entries.Select(e => Encoding.UTF8.GetString(e.Value)).ToArray(),
            Is.EqualTo(new[] { "1", "2", "3" }));
        Assert.That(starts[1], Is.EqualTo("b\u0000"), "second segment resumes at successor of last yielded key");
    }

    [Test]
    public async Task ScanEntriesAsync_reverse_resumes_with_last_key_as_upper_bound()
    {
        var tree = Substitute.For<ISystemLattice>();
        var callEnds = new List<string?>();
        var callIndex = 0;
        tree.EntriesAsync(
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
        await foreach (var e in tree.ScanEntriesAsync(reverse: true)) keys.Add(e.Key);

        Assert.That(keys, Is.EqualTo(new[] { "d", "c", "b", "a" }));
        Assert.That(callEnds[1], Is.EqualTo("c"), "reverse resume tightens endExclusive to last yielded key");
    }

    [Test]
    public void ScanEntriesAsync_rethrows_after_max_attempts_exhausted()
    {
        var tree = Substitute.For<ISystemLattice>();
        tree.EntriesAsync(
            Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<bool>(), Arg.Any<bool?>(), Arg.Any<CancellationToken>())
            .Returns(_ => ScriptedEntries(Array.Empty<(string, int)>(), abortAfter: 0));

        Assert.ThrowsAsync<EnumerationAbortedException>(async () =>
        {
            await foreach (var _ in tree.ScanEntriesAsync(maxAttempts: 2))
            {
            }
        });
    }

    [Test]
    public void ScanEntriesAsync_propagates_non_abort_exceptions_immediately()
    {
        var tree = Substitute.For<ISystemLattice>();
        tree.EntriesAsync(
            Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<bool>(), Arg.Any<bool?>(), Arg.Any<CancellationToken>())
            .Returns(_ => ThrowAsync<KeyValuePair<string, byte[]>>(new InvalidOperationException("boom")));

        Assert.ThrowsAsync<InvalidOperationException>(async () =>
        {
            await foreach (var _ in tree.ScanEntriesAsync())
            {
            }
        });
    }

    [Test]
    public void ScanEntriesAsync_honors_cancellation_token()
    {
        var tree = Substitute.For<ISystemLattice>();
        tree.EntriesAsync(
            Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<bool>(), Arg.Any<bool?>(), Arg.Any<CancellationToken>())
            .Returns(_ => ScriptedEntries(new[] { ("a", 1), ("b", 2) }, abortAfter: int.MaxValue));
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        Assert.ThrowsAsync<OperationCanceledException>(async () =>
        {
            await foreach (var _ in tree.ScanEntriesAsync(cancellationToken: cts.Token))
            {
            }
        });
    }

    [Test]
    public void ScanEntriesAsync_throws_for_null_tree()
    {
        ISystemLattice? tree = null;
        Assert.ThrowsAsync<ArgumentNullException>(async () =>
        {
            await foreach (var _ in tree!.ScanEntriesAsync())
            {
            }
        });
    }

    [Test]
    public async Task ScanEntriesAsync_forwards_prefetch_and_initial_bounds()
    {
        var tree = Substitute.For<ISystemLattice>();
        string? observedStart = null;
        string? observedEnd = null;
        bool? observedPrefetch = null;
        tree.EntriesAsync(
            Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<bool>(), Arg.Any<bool?>(), Arg.Any<CancellationToken>())
            .Returns(ci =>
            {
                observedStart = ci.ArgAt<string?>(0);
                observedEnd = ci.ArgAt<string?>(1);
                observedPrefetch = ci.ArgAt<bool?>(3);
                return ScriptedEntries(new[] { ("m", 13) }, abortAfter: int.MaxValue);
            });

        var items = new List<KeyValuePair<string, byte[]>>();
        await foreach (var e in tree.ScanEntriesAsync("k", "z", prefetch: false)) items.Add(e);

        Assert.That(items, Has.Count.EqualTo(1));
        Assert.That(observedStart, Is.EqualTo("k"));
        Assert.That(observedEnd, Is.EqualTo("z"));
        Assert.That(observedPrefetch, Is.False);
    }

    [Test]
    public void ScanEntriesAsync_negative_maxAttempts_is_clamped_to_zero()
    {
        var tree = Substitute.For<ISystemLattice>();
        var callIndex = 0;
        tree.EntriesAsync(
            Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<bool>(), Arg.Any<bool?>(), Arg.Any<CancellationToken>())
            .Returns(_ =>
            {
                callIndex++;
                return ScriptedEntries(Array.Empty<(string, int)>(), abortAfter: 0);
            });

        Assert.ThrowsAsync<EnumerationAbortedException>(async () =>
        {
            await foreach (var _ in tree.ScanEntriesAsync(maxAttempts: -5))
            {
            }
        });
        Assert.That(callIndex, Is.EqualTo(1), "Negative budget clamps to zero - no reconnects attempted.");
    }

    // -- Helpers --------------------------------------------------

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

#pragma warning disable CS1998
    private static async IAsyncEnumerable<T> ThrowAsync<T>(Exception ex)
    {
        throw ex;
#pragma warning disable CS0162
        yield break;
#pragma warning restore CS0162
    }
#pragma warning restore CS1998
}
