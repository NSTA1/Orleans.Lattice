using Microsoft.Coyote.Runtime;
using Microsoft.Coyote.Specifications;
using Orleans.Lattice;
using Orleans.Lattice.Testing.Coyote;

namespace Orleans.Lattice.Tests.BPlusTree.Coyote;

/// <summary>
/// Which per-consumer cursor-merge rule a <see cref="WalCursorMonotonicityModel"/>
/// run drives, so the safety test can prove the registry's monotonic (max-merge)
/// advance is load-bearing by replacing it with last-writer-wins and asserting
/// Coyote re-finds a cursor regressing below a frontier it had already reported.
/// </summary>
public enum WalCursorMonotonicityMode
{
    /// <summary>
    /// The fix: every report is driven through the real
    /// <see cref="InMemoryWalCursorRegistry"/>, whose per-consumer merge takes the
    /// pointwise <b>maximum</b> of the existing and reported cursor. A stale or
    /// out-of-order re-delivery is therefore a no-op and a consumer's reported
    /// frontier never regresses, no matter how the interleaving reorders reports.
    /// </summary>
    RegistryMaxMerge,

    /// <summary>
    /// The guard removed: a model-side registry that <b>replaces</b> the stored
    /// cursor with each report (last-writer-wins). A re-delivered stale cursor
    /// then pulls the consumer's frontier backwards, so the GC trim floor derived
    /// from it can regress and un-retain progress a consumer already durably made.
    /// </summary>
    LastWriterWinsReplace,
}

/// <summary>
/// A Coyote concurrency model of the WAL cursor registry's per-consumer merge
/// under reordered / re-delivered cursor reports, driving the <b>real</b>
/// <see cref="InMemoryWalCursorRegistry"/> in the safety mode. Because the model
/// reports through the production registry and reads its
/// <see cref="InMemoryWalCursorRegistry.SnapshotAsync"/> back, a violation Coyote
/// finds is a violation of the registry every WAL GC pass floors its trim point
/// against.
/// <para>
/// The scenario models <c>consumerCount</c> consumers, each acknowledging a dense
/// frontier that climbs through ticks <c>1..</c><see cref="TickCount"/>. Each
/// advance may be followed by a re-delivery of the consumer's <em>previous</em>
/// tick - the network-reordering / duplicate-report hazard - and the Coyote
/// scheduler explores which consumer advances next and whether a stale
/// re-delivery is interleaved.
/// </para>
/// <para>
/// The safety property is <b>a consumer's cursor never regresses below the
/// highest value ever delivered for it</b>. A registry that let a stale report
/// pull the cursor backwards would lower the GC's min-cursor floor and trim past
/// a consumer's durable frontier - the retention contract the trim floor relies
/// on being monotone.
/// </para>
/// </summary>
public sealed class WalCursorMonotonicityModel : ICoyoteModel
{
    /// <summary>The dense frontier length every consumer climbs through.</summary>
    private const int TickCount = 3;

    private const string TreeName = "coyote-cursor-monotonicity";

    private readonly int _consumerCount;
    private readonly WalCursorMonotonicityMode _mode;

    /// <summary>
    /// Creates the model for <paramref name="consumerCount"/> consumers reporting
    /// concurrently under the chosen cursor-merge <paramref name="mode"/>.
    /// </summary>
    public WalCursorMonotonicityModel(int consumerCount, WalCursorMonotonicityMode mode)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(consumerCount, 2);
        _consumerCount = consumerCount;
        _mode = mode;
    }

    /// <inheritdoc />
    public void Run(ICoyoteRuntime runtime)
    {
        // The real production registry backs the safety mode; the guard mode keeps
        // a model-side last-writer-wins map so the two share one delivery path and
        // differ only in the merge rule under test.
        var registry = new InMemoryWalCursorRegistry();
        var lww = new long[_consumerCount];

        var nextTick = new long[_consumerCount];
        for (var c = 0; c < _consumerCount; c++)
        {
            nextTick[c] = 1;
        }

        var highestDelivered = new long[_consumerCount];

        // Each consumer must deliver TickCount advances; one guaranteed advance per
        // iteration bounds the loop, and an optional stale re-delivery of the prior
        // tick after each advance manufactures the reordering hazard.
        var remainingAdvances = _consumerCount * TickCount;
        while (remainingAdvances > 0)
        {
            var c = SelectAdvanceable(nextTick, runtime);
            var priorHighest = highestDelivered[c];

            var value = nextTick[c];
            nextTick[c]++;
            remainingAdvances--;
            Deliver(runtime, registry, lww, highestDelivered, c, value);

            // Optionally re-deliver the consumer's previous (now stale) tick. It is
            // only a valid registry report when strictly positive, and only regresses
            // a last-writer-wins store when a higher value already landed.
            if (priorHighest >= 1 && runtime.RandomBoolean())
            {
                Deliver(runtime, registry, lww, highestDelivered, c, priorHighest);
            }
        }

        // Liveness: once every consumer has reported the full frontier, the
        // registry's min cursor must equal it - nothing was lost to reordering.
        if (_mode == WalCursorMonotonicityMode.RegistryMaxMerge)
        {
            var min = registry.GetMinCursorAsync(TreeName).GetAwaiter().GetResult();
            Specification.Assert(
                min is { } m && m.WallClockTicks == TickCount,
                $"registry min cursor did not converge to the full frontier {TickCount}: "
                + $"min={(min is { } mv ? mv.WallClockTicks : -1)} (reordering must not lose progress)");
        }
    }

    /// <summary>
    /// Applies one cursor report under the mode's merge rule - through the real
    /// registry (max-merge) or the model-side last-writer-wins map - then reads the
    /// stored cursor back and asserts it never sits below the highest value ever
    /// delivered for that consumer.
    /// </summary>
    private void Deliver(
        ICoyoteRuntime runtime,
        InMemoryWalCursorRegistry registry,
        long[] lww,
        long[] highestDelivered,
        int consumer,
        long value)
    {
        if (value > highestDelivered[consumer])
        {
            highestDelivered[consumer] = value;
        }

        long stored;
        if (_mode == WalCursorMonotonicityMode.RegistryMaxMerge)
        {
            registry
                .ReportCursorAsync(TreeName, ConsumerId(consumer), new HybridLogicalClock { WallClockTicks = value })
                .GetAwaiter()
                .GetResult();
            stored = ReadStoredCursor(registry, consumer);
        }
        else
        {
            // The removed guard: replace rather than max-merge.
            lww[consumer] = value;
            stored = lww[consumer];
        }

        // A Coyote scheduling point between the write and the check lets any other
        // pending interleaving run first, so the assertion holds against every
        // reachable ordering rather than only the straight-line one.
        _ = runtime.RandomBoolean();

        Specification.Assert(
            stored >= highestDelivered[consumer],
            $"consumer {consumer} cursor regressed to {stored} below its highest reported frontier "
            + $"{highestDelivered[consumer]}: a stale re-delivery pulled the cursor backwards and would "
            + "lower the GC trim floor past durable progress");
    }

    /// <summary>Reads one consumer's stored cursor tick from the real registry snapshot.</summary>
    private long ReadStoredCursor(InMemoryWalCursorRegistry registry, int consumer)
    {
        var snapshot = registry.SnapshotAsync(TreeName).GetAwaiter().GetResult();
        var id = ConsumerId(consumer);
        for (var i = 0; i < snapshot.Count; i++)
        {
            if (string.Equals(snapshot[i].ConsumerId, id, StringComparison.Ordinal))
            {
                return snapshot[i].Cursor.WallClockTicks;
            }
        }

        return 0;
    }

    /// <summary>
    /// Picks which consumer reports next, driving the choice through the runtime so
    /// the harness explores every interleaving of concurrent consumer progress.
    /// </summary>
    private int SelectAdvanceable(long[] nextTick, ICoyoteRuntime runtime)
    {
        var fallback = -1;
        for (var i = 0; i < nextTick.Length; i++)
        {
            if (nextTick[i] > TickCount)
            {
                continue;
            }

            if (fallback < 0)
            {
                fallback = i;
            }

            if (runtime.RandomBoolean())
            {
                return i;
            }
        }

        return fallback;
    }

    private static string ConsumerId(int consumer) => $"consumer-{consumer}";
}
