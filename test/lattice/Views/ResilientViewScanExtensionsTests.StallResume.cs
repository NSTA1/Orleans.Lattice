using NSubstitute;
using System.Text;

namespace Orleans.Lattice.Tests.Views;

/// <summary>
/// Coverage for the <see cref="ScanPageStalledException"/> resume path added to
/// the resilient view-scan wrappers by issue 2398, mirroring
/// <c>ResilientScanExtensionsTests</c> for the <see cref="ILattice"/> wrappers.
/// The view surface enumerates forward only, so there is no reverse coverage.
/// </summary>
public partial class ResilientViewScanExtensionsTests
{
    private const double TestStallCeilingSeconds = 0.02;

    [Test]
    public async Task ScanKeysAsync_resumes_from_last_key_after_scan_page_stall()
    {
        var view = Substitute.For<ILatticeView>();
        var calls = new List<string?>();
        var callIndex = 0;
        view.KeysAsync(Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<CancellationToken>())
            .Returns(ci =>
            {
                calls.Add(ci.ArgAt<string?>(0));
                return callIndex++ == 0
                    ? StalledKeys(new[] { "a", "b" }, stallAfter: 2)
                    : ScriptedKeys(new[] { "c" }, abortAfter: int.MaxValue);
            });

        var keys = await CollectAsync(view.ScanKeysAsync());

        Assert.That(keys, Is.EqualTo(new[] { "a", "b", "c" }));
        Assert.That(calls[1], Is.EqualTo("b\u0000"));
    }

    [Test]
    public async Task ScanEntriesAsync_resumes_from_last_key_after_scan_page_stall()
    {
        var view = Substitute.For<ILatticeView>();
        var calls = new List<string?>();
        var callIndex = 0;
        view.EntriesAsync(Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<CancellationToken>())
            .Returns(ci =>
            {
                calls.Add(ci.ArgAt<string?>(0));
                return callIndex++ == 0
                    ? StalledEntries(new[] { ("a", 1), ("b", 2) }, stallAfter: 2)
                    : ScriptedEntries(new[] { ("c", 3) }, abortAfter: int.MaxValue);
            });

        var entries = await CollectAsync(view.ScanEntriesAsync());

        Assert.That(entries.Select(e => e.Key), Is.EqualTo(new[] { "a", "b", "c" }));
        Assert.That(calls[1], Is.EqualTo("b\u0000"));
    }

    [Test]
    public void ScanKeysAsync_rethrows_a_stall_that_fires_before_any_key_is_yielded()
    {
        var view = Substitute.For<ILatticeView>();
        var calls = 0;
        StubKeys(view, _ =>
        {
            calls++;
            return StalledKeys(Array.Empty<string>(), stallAfter: 0);
        });

        Assert.ThrowsAsync<ScanPageStalledException>(async () =>
        {
            await foreach (var _ in view.ScanKeysAsync())
            {
            }
        });
        Assert.That(calls, Is.EqualTo(1), "no budget is spent restarting from the origin");
    }

    [Test]
    public void ScanEntriesAsync_rethrows_a_stall_that_fires_before_any_entry_is_yielded()
    {
        var view = Substitute.For<ILatticeView>();
        var calls = 0;
        view.EntriesAsync(Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<CancellationToken>())
            .Returns(_ =>
            {
                calls++;
                return StalledEntries(Array.Empty<(string, int)>(), stallAfter: 0);
            });

        Assert.ThrowsAsync<ScanPageStalledException>(async () =>
        {
            await foreach (var _ in view.ScanEntriesAsync())
            {
            }
        });
        Assert.That(calls, Is.EqualTo(1));
    }

    [Test]
    public void ScanKeysAsync_rethrows_when_a_stall_repeats_at_an_unchanged_continuation_token()
    {
        var view = Substitute.For<ILatticeView>();
        var calls = 0;
        var callIndex = 0;
        view.KeysAsync(Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<CancellationToken>())
            .Returns(_ =>
            {
                calls++;
                return callIndex++ == 0
                    ? StalledKeys(new[] { "a" }, stallAfter: 1)
                    : StalledKeys(Array.Empty<string>(), stallAfter: 0);
            });

        Assert.ThrowsAsync<ScanPageStalledException>(async () =>
        {
            await foreach (var _ in view.ScanKeysAsync())
            {
            }
        });
        Assert.That(calls, Is.EqualTo(2));
    }

    [Test]
    public void ScanKeysAsync_rethrows_the_stall_once_the_resume_budget_is_exhausted()
    {
        var view = Substitute.For<ILatticeView>();
        var calls = 0;
        var next = 'a';
        StubKeys(view, _ =>
        {
            calls++;
            return StalledKeys(new[] { next++.ToString() }, stallAfter: 1);
        });

        Assert.ThrowsAsync<ScanPageStalledException>(async () =>
        {
            await foreach (var _ in view.ScanKeysAsync())
            {
            }
        });
        Assert.That(calls, Is.EqualTo(1 + LatticeExtensions.DefaultScanStallResumeAttempts));
    }

    [Test]
    public void ScanKeysAsync_does_not_resume_a_stall_when_max_attempts_is_zero()
    {
        var view = Substitute.For<ILatticeView>();
        var calls = 0;
        StubKeys(view, _ =>
        {
            calls++;
            return StalledKeys(new[] { "a" }, stallAfter: 1);
        });

        Assert.ThrowsAsync<ScanPageStalledException>(async () =>
        {
            await foreach (var _ in view.ScanKeysAsync(maxAttempts: 0))
            {
            }
        });
        Assert.That(calls, Is.EqualTo(1));
    }

    [Test]
    public void ScanKeysAsync_never_ends_a_stalled_view_scan_as_though_it_had_completed()
    {
        var view = Substitute.For<ILatticeView>();
        var next = 'a';
        StubKeys(view, _ => StalledKeys(new[] { next++.ToString() }, stallAfter: 1));

        var yielded = new List<string>();
        var completedNormally = false;

        Assert.ThrowsAsync<ScanPageStalledException>(async () =>
        {
            await foreach (var k in view.ScanKeysAsync())
            {
                yielded.Add(k);
            }

            completedNormally = true;
        });

        Assert.That(completedNormally, Is.False, "a truncated scan must not look complete");
        Assert.That(yielded, Is.Not.Empty);
    }

    private static ScanPageStalledException NewStall() => new("scripted stall")
    {
        TreeId = "t",
        ShardIndex = 0,
        Operation = "GetSortedKeysBatchAsync",
        Phase = "leaf-walk",
        TimeoutSeconds = TestStallCeilingSeconds,
    };

    private static async IAsyncEnumerable<string> StalledKeys(string[] keys, int stallAfter)
    {
        var yielded = 0;
        foreach (var k in keys)
        {
            if (yielded >= stallAfter) throw NewStall();
            yielded++;
            yield return k;
            await Task.Yield();
        }

        if (yielded < stallAfter) yield break;
        throw NewStall();
    }

    private static async IAsyncEnumerable<KeyValuePair<string, byte[]>> StalledEntries(
        (string Key, int Value)[] entries, int stallAfter)
    {
        var yielded = 0;
        foreach (var (k, v) in entries)
        {
            if (yielded >= stallAfter) throw NewStall();
            yielded++;
            yield return new KeyValuePair<string, byte[]>(k, Encoding.UTF8.GetBytes(v.ToString()));
            await Task.Yield();
        }

        if (yielded < stallAfter) yield break;
        throw NewStall();
    }
}
