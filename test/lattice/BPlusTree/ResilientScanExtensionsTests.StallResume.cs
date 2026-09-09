using NSubstitute;
using Orleans.Lattice.Testing;
using System.Text;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Coverage for the <see cref="ScanPageStalledException"/> resume path added to
/// the resilient scan wrappers by issue 2398.
/// <para>
/// A shard-root page fill that exceeds
/// <see cref="LatticeOptions.MaxScanPageStallDuration"/> is abandoned so the
/// deliberately non-reentrant shard is released and its queue can drain; the
/// caller is expected to resume from its last continuation token. These tests
/// pin that resume, and - more importantly - pin the three ways it must refuse
/// to resume, because retrying a timeout under the contention that caused it is
/// only safe while every attempt strictly advances.
/// </para>
/// </summary>
public partial class ResilientScanExtensionsTests
{
    // A stall ceiling small enough that the derived backoff is a few
    // milliseconds. The backoff is a fraction of the ceiling the stall reports,
    // so a scripted stall controls its own retry latency and these tests do not
    // sleep for the production ceiling.
    private const double TestStallCeilingSeconds = 0.02;

    // ── resume ─────────────────────────────────────────────────

    [Test]
    public async Task ScanKeysAsync_resumes_from_last_key_after_scan_page_stall()
    {
        var lattice = Substitute.For<ILattice>();
        var calls = new List<string?>();
        var callIndex = 0;
        lattice.KeysAsync(
            Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<bool>(), Arg.Any<bool?>(), Arg.Any<CancellationToken>())
            .Returns(ci =>
            {
                calls.Add(ci.ArgAt<string?>(0));
                return callIndex++ == 0
                    ? StalledKeys(new[] { "a", "b" }, stallAfter: 2)
                    : ScriptedKeys(new[] { "c", "d" }, abortAfter: int.MaxValue);
            });

        var keys = await CollectAsync(lattice.ScanKeysAsync());

        Assert.That(keys, Is.EqualTo(new[] { "a", "b", "c", "d" }));
        Assert.That(calls, Has.Count.EqualTo(2));
        Assert.That(calls[1], Is.EqualTo("b\u0000"), "resume starts at the successor of the last yielded key");
    }

    [Test]
    public async Task ScanEntriesAsync_resumes_from_last_key_after_scan_page_stall()
    {
        var lattice = Substitute.For<ILattice>();
        var calls = new List<string?>();
        var callIndex = 0;
        lattice.EntriesAsync(
            Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<bool>(), Arg.Any<bool?>(), Arg.Any<CancellationToken>())
            .Returns(ci =>
            {
                calls.Add(ci.ArgAt<string?>(0));
                return callIndex++ == 0
                    ? StalledEntries(new[] { ("a", 1), ("b", 2) }, stallAfter: 2)
                    : ScriptedEntries(new[] { ("c", 3) }, abortAfter: int.MaxValue);
            });

        var entries = await CollectAsync(lattice.ScanEntriesAsync());

        Assert.That(entries.Select(e => e.Key), Is.EqualTo(new[] { "a", "b", "c" }));
        Assert.That(calls[1], Is.EqualTo("b\u0000"));
    }

    [Test]
    public async Task ScanKeysAsync_reverse_resumes_with_last_key_as_upper_bound_after_stall()
    {
        var lattice = Substitute.For<ILattice>();
        var callEnds = new List<string?>();
        var callIndex = 0;
        lattice.KeysAsync(
            Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<bool>(), Arg.Any<bool?>(), Arg.Any<CancellationToken>())
            .Returns(ci =>
            {
                callEnds.Add(ci.ArgAt<string?>(1));
                return callIndex++ == 0
                    ? StalledKeys(new[] { "d", "c" }, stallAfter: 2)
                    : ScriptedKeys(new[] { "b", "a" }, abortAfter: int.MaxValue);
            });

        var keys = await CollectAsync(lattice.ScanKeysAsync(reverse: true));

        Assert.That(keys, Is.EqualTo(new[] { "d", "c", "b", "a" }));
        Assert.That(callEnds[1], Is.EqualTo("c"));
    }

    // ── refusals ───────────────────────────────────────────────

    [Test]
    public void ScanKeysAsync_rethrows_a_stall_that_fires_before_any_key_is_yielded()
    {
        // The origin case, and the one that most needs pinning. There is no
        // continuation token yet, so a "resume" here would re-issue the exact
        // request that just stalled - a restart, not a resume - and would do it
        // on the full budget. It is also the most likely shape of the fault: a
        // cold tree whose leaves are replaying their WAL windows stalls at or
        // near the origin.
        var lattice = Substitute.For<ILattice>();
        var calls = 0;
        StubKeys(lattice, _ =>
        {
            calls++;
            return StalledKeys(Array.Empty<string>(), stallAfter: 0);
        });

        Assert.ThrowsAsync<ScanPageStalledException>(async () =>
        {
            await foreach (var _ in lattice.ScanKeysAsync())
            {
            }
        });
        Assert.That(calls, Is.EqualTo(1), "no budget is spent restarting from the origin");
    }

    [Test]
    public void ScanEntriesAsync_rethrows_a_stall_that_fires_before_any_entry_is_yielded()
    {
        var lattice = Substitute.For<ILattice>();
        var calls = 0;
        lattice.EntriesAsync(
            Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<bool>(), Arg.Any<bool?>(), Arg.Any<CancellationToken>())
            .Returns(_ =>
            {
                calls++;
                return StalledEntries(Array.Empty<(string, int)>(), stallAfter: 0);
            });

        Assert.ThrowsAsync<ScanPageStalledException>(async () =>
        {
            await foreach (var _ in lattice.ScanEntriesAsync())
            {
            }
        });
        Assert.That(calls, Is.EqualTo(1));
    }

    [Test]
    public void ScanKeysAsync_rethrows_when_a_stall_repeats_at_an_unchanged_continuation_token()
    {
        // The no-progress spin. The budget alone would allow a second attempt,
        // but the second attempt would descend on the same parked read and burn
        // another whole ceiling for nothing.
        var lattice = Substitute.For<ILattice>();
        var calls = 0;
        var callIndex = 0;
        lattice.KeysAsync(
            Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<bool>(), Arg.Any<bool?>(), Arg.Any<CancellationToken>())
            .Returns(_ =>
            {
                calls++;
                return callIndex++ == 0
                    ? StalledKeys(new[] { "a", "b" }, stallAfter: 2)
                    : StalledKeys(Array.Empty<string>(), stallAfter: 0);
            });

        var yielded = new List<string>();
        Assert.ThrowsAsync<ScanPageStalledException>(async () =>
        {
            await foreach (var k in lattice.ScanKeysAsync())
            {
                yielded.Add(k);
            }
        });

        Assert.That(calls, Is.EqualTo(2), "one resume, then the position had not advanced");
        Assert.That(yielded, Is.EqualTo(new[] { "a", "b" }));
    }

    [Test]
    public void ScanKeysAsync_rethrows_the_stall_once_the_resume_budget_is_exhausted()
    {
        var lattice = Substitute.For<ILattice>();
        var calls = 0;
        var next = 'a';
        StubKeys(lattice, _ =>
        {
            calls++;
            return StalledKeys(new[] { next++.ToString() }, stallAfter: 1);
        });

        Assert.ThrowsAsync<ScanPageStalledException>(async () =>
        {
            await foreach (var _ in lattice.ScanKeysAsync())
            {
            }
        });
        Assert.That(calls, Is.EqualTo(1 + LatticeExtensions.DefaultScanStallResumeAttempts));
    }

    [Test]
    public void ScanKeysAsync_stall_resume_budget_is_not_widened_by_a_larger_reconnect_budget()
    {
        // maxAttempts governs enumerator reconnects, which are cheap. A stall
        // costs a whole ceiling, so it draws on its own much smaller budget and
        // a caller that raises maxAttempts for a long walk does not silently
        // raise its tolerance for stalls with it.
        var lattice = Substitute.For<ILattice>();
        var calls = 0;
        var next = 'a';
        StubKeys(lattice, _ =>
        {
            calls++;
            return StalledKeys(new[] { next++.ToString() }, stallAfter: 1);
        });

        Assert.ThrowsAsync<ScanPageStalledException>(async () =>
        {
            await foreach (var _ in lattice.ScanKeysAsync(maxAttempts: 64))
            {
            }
        });
        Assert.That(calls, Is.EqualTo(1 + LatticeExtensions.DefaultScanStallResumeAttempts));
    }

    [Test]
    public void ScanKeysAsync_does_not_resume_a_stall_when_max_attempts_is_zero()
    {
        // maxAttempts: 0 already means fail-fast, which is why no separate
        // opt-out knob was added for stall resumption.
        var lattice = Substitute.For<ILattice>();
        var calls = 0;
        StubKeys(lattice, _ =>
        {
            calls++;
            return StalledKeys(new[] { "a" }, stallAfter: 1);
        });

        Assert.ThrowsAsync<ScanPageStalledException>(async () =>
        {
            await foreach (var _ in lattice.ScanKeysAsync(maxAttempts: 0))
            {
            }
        });
        Assert.That(calls, Is.EqualTo(1));
    }

    [Test]
    public void ScanKeysAsync_never_ends_a_stalled_scan_as_though_it_had_completed()
    {
        // The load-bearing safety property. Every caller of these wrappers
        // treats a normal end of enumeration as "the range is now fully read":
        // the repository-context reconcile treats an unseen key as deleted, and
        // the vector writer's own comment says a short list orphans vectors. A
        // resume must therefore either finish the range or rethrow - there is no
        // path that quietly returns a short prefix.
        var lattice = Substitute.For<ILattice>();
        var next = 'a';
        StubKeys(lattice, _ => StalledKeys(new[] { next++.ToString() }, stallAfter: 1));

        var yielded = new List<string>();
        var completedNormally = false;

        Assert.ThrowsAsync<ScanPageStalledException>(async () =>
        {
            await foreach (var k in lattice.ScanKeysAsync())
            {
                yielded.Add(k);
            }

            completedNormally = true;
        });

        Assert.That(completedNormally, Is.False, "a truncated scan must not look complete");
        Assert.That(yielded, Is.Not.Empty, "the keys read before the stall are still delivered");
    }

    [Test]
    public void ScanKeysAsync_still_propagates_a_stall_wrapped_in_an_unrelated_exception()
    {
        var lattice = Substitute.For<ILattice>();
        StubKeys(lattice, _ => ThrowAsync<string>(
            new InvalidOperationException("outer", new ScanPageStalledException("inner"))));

        Assert.ThrowsAsync<InvalidOperationException>(async () =>
        {
            await foreach (var _ in lattice.ScanKeysAsync())
            {
            }
        });
    }

    // ── observability ──────────────────────────────────────────

    [Test]
    public void ScanKeysAsync_records_each_stall_decision_with_its_outcome()
    {
        var outcomes = new List<string>();
        using var listener = MeterListening.StartForInstrument(
            LatticeMetrics.ScanStallResumptions,
            l => l.SetMeasurementEventCallback<long>((_, _, tags, _) =>
            {
                foreach (var tag in tags)
                {
                    if (tag.Key == LatticeMetrics.TagOutcome)
                    {
                        lock (outcomes) outcomes.Add(tag.Value?.ToString() ?? string.Empty);
                    }
                }
            }));

        var lattice = Substitute.For<ILattice>();
        var next = 'a';
        StubKeys(lattice, _ => StalledKeys(new[] { next++.ToString() }, stallAfter: 1));

        Assert.ThrowsAsync<ScanPageStalledException>(async () =>
        {
            await foreach (var _ in lattice.ScanKeysAsync())
            {
            }
        });

        listener.Dispose();

        Assert.That(
            outcomes.Count(o => o == "resumed"),
            Is.EqualTo(LatticeExtensions.DefaultScanStallResumeAttempts));
        Assert.That(outcomes, Has.Exactly(1).EqualTo("budget-exhausted"));
    }

    [Test]
    public void ScanKeysAsync_records_a_refused_origin_stall_as_no_progress()
    {
        var outcomes = new List<string>();
        using var listener = MeterListening.StartForInstrument(
            LatticeMetrics.ScanStallResumptions,
            l => l.SetMeasurementEventCallback<long>((_, _, tags, _) =>
            {
                foreach (var tag in tags)
                {
                    if (tag.Key == LatticeMetrics.TagOutcome)
                    {
                        lock (outcomes) outcomes.Add(tag.Value?.ToString() ?? string.Empty);
                    }
                }
            }));

        var lattice = Substitute.For<ILattice>();
        StubKeys(lattice, _ => StalledKeys(Array.Empty<string>(), stallAfter: 0));

        Assert.ThrowsAsync<ScanPageStalledException>(async () =>
        {
            await foreach (var _ in lattice.ScanKeysAsync())
            {
            }
        });

        listener.Dispose();

        Assert.That(outcomes, Is.EqualTo(new[] { "no-progress" }));
    }

    // ── helpers under test ─────────────────────────────────────

    [Test]
    public void ComputeScanStallResumeBudget_is_capped_by_the_reconnect_budget()
    {
        Assert.Multiple(() =>
        {
            Assert.That(LatticeExtensions.ComputeScanStallResumeBudget(0), Is.Zero);
            Assert.That(LatticeExtensions.ComputeScanStallResumeBudget(1), Is.EqualTo(1));
            Assert.That(
                LatticeExtensions.ComputeScanStallResumeBudget(LatticeExtensions.DefaultScanReconnectAttempts),
                Is.EqualTo(LatticeExtensions.DefaultScanStallResumeAttempts));
            Assert.That(
                LatticeExtensions.ComputeScanStallResumeBudget(1024),
                Is.EqualTo(LatticeExtensions.DefaultScanStallResumeAttempts));
        });
    }

    [Test]
    public void ScanStallResumeMakesProgress_treats_an_unstarted_scan_as_no_progress()
    {
        Assert.Multiple(() =>
        {
            Assert.That(LatticeExtensions.ScanStallResumeMakesProgress(null, null), Is.False);
            Assert.That(LatticeExtensions.ScanStallResumeMakesProgress("a", null), Is.True);
            Assert.That(LatticeExtensions.ScanStallResumeMakesProgress("a", "a"), Is.False);
            Assert.That(LatticeExtensions.ScanStallResumeMakesProgress("b", "a"), Is.True);
        });
    }

    [Test]
    public void ComputeScanStallResumeDelayMs_derives_the_backoff_from_the_reported_ceiling()
    {
        // 20s ceiling, first attempt: a quarter of the ceiling.
        Assert.That(LatticeExtensions.ComputeScanStallResumeDelayMs(20, 1), Is.EqualTo(5000));
        // Linear in the attempt number.
        Assert.That(LatticeExtensions.ComputeScanStallResumeDelayMs(20, 2), Is.EqualTo(10000));
    }

    [Test]
    public void ComputeScanStallResumeDelayMs_never_waits_longer_than_the_ceiling_itself()
    {
        Assert.That(LatticeExtensions.ComputeScanStallResumeDelayMs(20, 99), Is.EqualTo(20000));
    }

    [Test]
    public void ComputeScanStallResumeDelayMs_falls_back_to_the_page_duration_when_no_ceiling_is_reported()
    {
        // A default-constructed stall reports a zero ceiling; the wait is still
        // derived from a real bound rather than from a literal.
        var expected = (int)Math.Ceiling(
            LatticeOptions.DefaultMaxScanPageDuration.TotalSeconds
            * LatticeExtensions.ScanStallResumeBackoffFraction
            * 1000.0);

        Assert.Multiple(() =>
        {
            Assert.That(LatticeExtensions.ComputeScanStallResumeDelayMs(0, 1), Is.EqualTo(expected));
            Assert.That(LatticeExtensions.ComputeScanStallResumeDelayMs(-1, 1), Is.EqualTo(expected));
            Assert.That(LatticeExtensions.ComputeScanStallResumeDelayMs(double.NaN, 1), Is.EqualTo(expected));
        });
    }

    [Test]
    public void ComputeScanStallResumeDelayMs_is_zero_below_the_first_attempt()
    {
        Assert.That(LatticeExtensions.ComputeScanStallResumeDelayMs(20, 0), Is.Zero);
    }

    // ── scripted producers ─────────────────────────────────────

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
