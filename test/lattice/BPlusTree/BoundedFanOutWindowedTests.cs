using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Unit tests for the two corpus-sized fan-out shapes on
/// <see cref="BoundedFanOut"/>: <see cref="BoundedFanOut.ForEachAsync"/> and
/// <see cref="BoundedFanOut.ReadAheadAsync"/>.
/// <para>
/// These exist because several maintenance sweeps walk a set whose size is a
/// function of the stored corpus rather than of the cluster topology - the keys
/// of a view generation being cleared, the internal nodes of a shard being
/// purged, the source keys a view rebuild projects - and used to issue one grain
/// call per item, awaiting each before issuing the next. The properties pinned
/// here are the ones those call sites depend on: every item is covered exactly
/// once, the calls genuinely overlap, the overlap is bounded by a constant
/// rather than by the corpus, and - for the read-ahead form - the consumer still
/// observes results in strict input order so a sequential loop body keeps the
/// sequence it had when the reads were serial.
/// </para>
/// </summary>
[TestFixture]
public sealed class BoundedFanOutWindowedTests
{
    /// <summary>Raises <paramref name="peak"/> to <paramref name="current"/> if it is higher.</summary>
    private static void RecordPeak(ref int peak, int current)
    {
        int observed;
        do
        {
            observed = Volatile.Read(ref peak);
            if (current <= observed)
            {
                return;
            }
        }
        while (Interlocked.CompareExchange(ref peak, current, observed) != observed);
    }

    private static string[] Items(int count) => [.. Enumerable.Range(0, count).Select(i => $"k{i:D4}")];

    // --- ForEachAsync ---

    [Test]
    public async Task ForEachAsync_applies_the_body_to_every_item_exactly_once()
    {
        var items = Items(70);
        var seen = new List<string>();
        var gate = new object();

        await BoundedFanOut.ForEachAsync(items, 32, item =>
        {
            lock (gate)
            {
                seen.Add(item);
            }

            return Task.CompletedTask;
        });

        // 70 against a width of 32 is two full waves plus a partial: the
        // trailing partial wave must not be dropped.
        Assert.That(seen.OrderBy(s => s, StringComparer.Ordinal), Is.EqualTo(items).AsCollection);
    }

    [Test]
    public async Task ForEachAsync_overlaps_the_work_rather_than_running_it_serially()
    {
        const int Width = 8;
        var inFlight = 0;
        var peak = 0;
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        await BoundedFanOut.ForEachAsync(Items(64), Width, async _ =>
        {
            var current = Interlocked.Increment(ref inFlight);
            RecordPeak(ref peak, current);
            if (current >= Width)
            {
                release.TrySetResult();
            }

            await release.Task;
            Interlocked.Decrement(ref inFlight);
        });

        Assert.That(Volatile.Read(ref peak), Is.EqualTo(Width),
            "a serial loop would never see more than one call in flight");
    }

    [Test]
    public async Task ForEachAsync_never_exceeds_the_width()
    {
        const int Width = 4;
        var inFlight = 0;
        var peak = 0;

        await BoundedFanOut.ForEachAsync(Items(200), Width, async _ =>
        {
            RecordPeak(ref peak, Interlocked.Increment(ref inFlight));
            await Task.Yield();
            Interlocked.Decrement(ref inFlight);
        });

        Assert.That(Volatile.Read(ref peak), Is.LessThanOrEqualTo(Width),
            "an unbounded wave over a whole tree would burst in proportion to the corpus");
    }

    [Test]
    public async Task ForEachAsync_on_an_empty_list_does_nothing()
    {
        var ran = 0;

        await BoundedFanOut.ForEachAsync(Array.Empty<string>(), 32, _ =>
        {
            Interlocked.Increment(ref ran);
            return Task.CompletedTask;
        });

        Assert.That(ran, Is.Zero);
    }

    [Test]
    public async Task ForEachAsync_on_a_single_item_still_runs_it()
    {
        // The single-item path skips the wave list entirely, so it needs its own
        // coverage or the allocation-free shortcut goes untested.
        var ran = 0;

        await BoundedFanOut.ForEachAsync(Items(1), 32, _ =>
        {
            Interlocked.Increment(ref ran);
            return Task.CompletedTask;
        });

        Assert.That(ran, Is.EqualTo(1));
    }

    [Test]
    public async Task ForEachAsync_clamps_a_width_below_one_instead_of_stalling()
    {
        var ran = 0;

        await BoundedFanOut.ForEachAsync(Items(5), 0, _ =>
        {
            Interlocked.Increment(ref ran);
            return Task.CompletedTask;
        });

        Assert.That(ran, Is.EqualTo(5));
    }

    [Test]
    public void ForEachAsync_surfaces_a_faulted_item()
    {
        var items = Items(10);

        Assert.That(
            async () => await BoundedFanOut.ForEachAsync(items, 4, item =>
                item == "k0007"
                    ? Task.FromException(new InvalidOperationException("boom"))
                    : Task.CompletedTask),
            Throws.InstanceOf<InvalidOperationException>());
    }

    // --- ReadAheadAsync ---

    [Test]
    public async Task ReadAheadAsync_yields_results_in_input_order_even_when_later_reads_finish_first()
    {
        // Later items complete sooner, so a yield-as-they-finish implementation
        // would hand the projection loop a different sequence than the serial
        // form produced.
        var items = Items(16);
        var yielded = new List<string>();

        await foreach (var value in BoundedFanOut.ReadAheadAsync(items, 8, async item =>
        {
            await Task.Delay((16 - int.Parse(item[1..])) * 2);
            return item;
        }))
        {
            yielded.Add(value);
        }

        Assert.That(yielded, Is.EqualTo(items).AsCollection);
    }

    [Test]
    public async Task ReadAheadAsync_reads_every_item_exactly_once()
    {
        var items = Items(97);
        var reads = new List<string>();
        var gate = new object();

        var count = 0;
        await foreach (var _ in BoundedFanOut.ReadAheadAsync(items, 32, item =>
        {
            lock (gate)
            {
                reads.Add(item);
            }

            return Task.FromResult(item);
        }))
        {
            count++;
        }

        Assert.Multiple(() =>
        {
            Assert.That(count, Is.EqualTo(items.Length), "every item must reach the consumer");
            Assert.That(reads.OrderBy(s => s, StringComparer.Ordinal), Is.EqualTo(items).AsCollection,
                "a ring slot reused too early would re-read or drop an item");
        });
    }

    [Test]
    public async Task ReadAheadAsync_keeps_reads_in_flight_ahead_of_the_consumer()
    {
        // The point of the change: while the consumer is still working on the
        // first result, the reads for the rest of the window are already issued.
        const int Width = 16;
        var issued = 0;
        var issuedWhenFirstConsumed = -1;

        await foreach (var _ in BoundedFanOut.ReadAheadAsync(Items(256), Width, item =>
        {
            Interlocked.Increment(ref issued);
            return Task.FromResult(item);
        }))
        {
            if (issuedWhenFirstConsumed < 0)
            {
                issuedWhenFirstConsumed = Volatile.Read(ref issued);
            }
        }

        Assert.That(issuedWhenFirstConsumed, Is.EqualTo(Width),
            "the serial form issued exactly one read before the first result");
    }

    [Test]
    public async Task ReadAheadAsync_never_exceeds_the_width()
    {
        const int Width = 8;
        var inFlight = 0;
        var peak = 0;

        await foreach (var _ in BoundedFanOut.ReadAheadAsync(Items(300), Width, async item =>
        {
            RecordPeak(ref peak, Interlocked.Increment(ref inFlight));
            await Task.Yield();
            Interlocked.Decrement(ref inFlight);
            return item;
        }))
        {
            // Drain.
        }

        Assert.That(Volatile.Read(ref peak), Is.LessThanOrEqualTo(Width));
    }

    [Test]
    public async Task ReadAheadAsync_overlaps_the_reads_rather_than_running_them_serially()
    {
        const int Width = 8;
        var inFlight = 0;
        var peak = 0;
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        await foreach (var _ in BoundedFanOut.ReadAheadAsync(Items(64), Width, async item =>
        {
            var current = Interlocked.Increment(ref inFlight);
            RecordPeak(ref peak, current);
            if (current >= Width)
            {
                release.TrySetResult();
            }

            await release.Task;
            Interlocked.Decrement(ref inFlight);
            return item;
        }))
        {
            // Drain.
        }

        Assert.That(Volatile.Read(ref peak), Is.EqualTo(Width));
    }

    [Test]
    public async Task ReadAheadAsync_on_an_empty_list_yields_nothing_and_reads_nothing()
    {
        var reads = 0;
        var yielded = 0;

        await foreach (var _ in BoundedFanOut.ReadAheadAsync(Array.Empty<string>(), 32, item =>
        {
            Interlocked.Increment(ref reads);
            return Task.FromResult(item);
        }))
        {
            yielded++;
        }

        Assert.Multiple(() =>
        {
            Assert.That(reads, Is.Zero);
            Assert.That(yielded, Is.Zero);
        });
    }

    [Test]
    public async Task ReadAheadAsync_clamps_a_width_below_one_instead_of_stalling()
    {
        var yielded = new List<string>();

        await foreach (var value in BoundedFanOut.ReadAheadAsync(Items(4), 0, Task.FromResult))
        {
            yielded.Add(value);
        }

        Assert.That(yielded, Is.EqualTo(Items(4)).AsCollection);
    }

    [Test]
    public void ReadAheadAsync_surfaces_a_faulted_read_at_its_own_position()
    {
        var items = Items(20);
        var yielded = new List<string>();

        Assert.That(async () =>
        {
            await foreach (var value in BoundedFanOut.ReadAheadAsync(items, 8, item =>
                item == "k0005"
                    ? Task.FromException<string>(new InvalidOperationException("boom"))
                    : Task.FromResult(item)))
            {
                yielded.Add(value);
            }
        }, Throws.InstanceOf<InvalidOperationException>());

        Assert.That(yielded, Is.EqualTo(items.Take(5)).AsCollection,
            "results before the faulted read must still have been delivered in order");
    }

    [Test]
    public async Task ReadAheadAsync_stops_issuing_reads_once_the_consumer_breaks_out()
    {
        // Abandoning the enumeration must not keep walking the input, and the
        // disposal path (which observes the reads still outstanding) must not
        // throw or hang.
        const int Width = 8;
        var issued = 0;

        await foreach (var _ in BoundedFanOut.ReadAheadAsync(Items(500), Width, item =>
        {
            Interlocked.Increment(ref issued);
            return Task.FromResult(item);
        }))
        {
            break;
        }

        Assert.That(Volatile.Read(ref issued), Is.EqualTo(Width),
            "only the first window may have been issued when the consumer bailed out");
    }

    [Test]
    public async Task ReadAheadAsync_disposal_does_not_rethrow_an_abandoned_faulted_read()
    {
        // A read that faults *after* the consumer has bailed out is observed on
        // disposal rather than left to resurface as an unobserved task
        // exception - and observing it must not turn into a throw from the
        // enumerator's own DisposeAsync.
        var faulted = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);

        await foreach (var _ in BoundedFanOut.ReadAheadAsync(Items(64), 8, item =>
            item == "k0000" ? Task.FromResult(item) : faulted.Task))
        {
            break;
        }

        faulted.SetException(new InvalidOperationException("late boom"));

        Assert.That(faulted.Task.IsFaulted, Is.True);
    }

    [Test]
    public void ReadAheadAsync_honours_cancellation()
    {
        using var cts = new CancellationTokenSource();

        Assert.That(async () =>
        {
            await foreach (var value in BoundedFanOut.ReadAheadAsync(
                Items(64), 8, Task.FromResult, cts.Token))
            {
                if (value == "k0002")
                {
                    cts.Cancel();
                }
            }
        }, Throws.InstanceOf<OperationCanceledException>());
    }

    [Test]
    public void DefaultWidth_matches_the_router_grains_local_worker_count()
    {
        // The window is sized to LatticeGrain's [StatelessWorker(maxLocalWorkers: 32)].
        // A wider window would buy no further overlap while bursting more calls.
        Assert.That(BoundedFanOut.DefaultWidth, Is.EqualTo(32));
    }
}
