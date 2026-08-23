using Microsoft.Coyote.Runtime;
using Microsoft.Coyote.Specifications;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Testing.Coyote;

namespace Orleans.Lattice.Tests.BPlusTree.Coyote;

/// <summary>
/// Which offset-exposure rule a <see cref="WalShippingWatermarkModel"/> run drives,
/// so the safety test can prove the durable-contiguous watermark is load-bearing by
/// removing it and asserting Coyote re-finds the cross-cluster prefix-hole data loss.
/// </summary>
public enum WalShippingWatermarkMode
{
    /// <summary>
    /// The fix: the reader clamps every page at the real
    /// <see cref="WalShippingWatermark.DurableContiguousTail"/> (the lowest
    /// in-flight flush start offset), so no schedule of out-of-order flush
    /// completions lets a cursor-advancing reader be handed an offset above an
    /// unfilled prefix hole.
    /// </summary>
    DurableContiguousWatermark,

    /// <summary>
    /// The guard removed: the reader clamps at the raw next-offset tail instead,
    /// ignoring in-flight flushes. A higher window that persists before a lower
    /// in-flight one is then exposed, the reader advances its cursor past the hole,
    /// and the still-in-flight lower offset is stranded forever - the #1076 /
    /// cold-partition first-batch replication gap.
    /// </summary>
    RawNextOffsetTail,
}

/// <summary>
/// A Coyote concurrency model of the WAL shard shipping-read seam under multiple
/// concurrent in-flight flushes (<see cref="LatticeOptions.WalMaxPendingBatches"/>
/// &gt; 1), driving the <b>production</b> watermark rule
/// (<see cref="WalShippingWatermark"/>) under systematic schedule exploration.
/// Because the model executes the same offset-exposure decision Orleans runs in
/// <c>WalShardGrain.ReadShippingAsync</c> / <c>ReadAsync</c>, a violation Coyote
/// finds is a violation of the shipping read path.
/// <para>
/// The scenario models <c>offsetCount</c> single-offset flushes assigned in order
/// (offset <c>i</c>), each persisting to the provider <b>independently and out of
/// completion order</b>. The Coyote scheduler interleaves {complete a
/// scheduler-chosen in-flight flush, reader poll} so a higher offset can become
/// durable while a lower one is still in flight, manufacturing the transient prefix
/// hole exhaustively rather than by the single gated schedule the deterministic
/// <c>WalShardGrainTests.ReadShippingAsync_does_not_expose_offsets_above_an_in_flight_prefix_hole</c>
/// test drives.
/// </para>
/// <para>
/// The safety property is <b>no hole shipped</b>: whenever the reader advances its
/// durable cursor to <c>C</c>, every offset in <c>[0, C)</c> is already persisted.
/// A cursor that moves past a not-yet-persisted offset has stranded it - a silent,
/// permanent replication / projection gap.
/// </para>
/// </summary>
public sealed class WalShippingWatermarkModel : ICoyoteModel
{
    private readonly int _offsetCount;
    private readonly WalShippingWatermarkMode _mode;

    /// <summary>
    /// Creates the model for an <paramref name="offsetCount"/>-offset flush burst
    /// under the chosen exposure <paramref name="mode"/>.
    /// </summary>
    public WalShippingWatermarkModel(int offsetCount, WalShippingWatermarkMode mode)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(offsetCount, 2);
        _offsetCount = offsetCount;
        _mode = mode;
    }

    /// <inheritdoc />
    public void Run(ICoyoteRuntime runtime)
    {
        // Every offset [0, offsetCount) is assigned up front and starts in flight,
        // ordered oldest-first so index 0 is always the lowest still-in-flight
        // start offset (exactly WalShardGrain's oldest-first _inFlight list).
        var inFlight = new List<long>(_offsetCount);
        for (var i = 0; i < _offsetCount; i++)
        {
            inFlight.Add(i);
        }

        var persisted = new bool[_offsetCount];
        var cursor = 0L;

        // Drive exactly offsetCount completions so exploration terminates, with a
        // scheduler-chosen reader poll optionally interleaved before and after each
        // completion. Every completion order and poll interleaving is explored.
        while (inFlight.Count > 0)
        {
            if (runtime.RandomBoolean())
            {
                cursor = ReaderPoll(inFlight, persisted, cursor);
            }

            // Complete a scheduler-chosen in-flight flush: it persists to the
            // provider and leaves the oldest-first in-flight list (which stays
            // sorted, so index 0 remains the lowest in-flight start offset).
            var k = SelectIndex(inFlight.Count, runtime);
            var completed = inFlight[k];
            persisted[completed] = true;
            inFlight.RemoveAt(k);

            if (runtime.RandomBoolean())
            {
                cursor = ReaderPoll(inFlight, persisted, cursor);
            }
        }

        // Once every flush has landed, a final poll must drain the whole contiguous
        // log: nothing was lost, only deferred (a bounded-progress liveness check).
        cursor = ReaderPoll(inFlight, persisted, cursor);
        Specification.Assert(
            cursor == _offsetCount,
            $"reader did not catch up after every flush landed: cursor={cursor} of {_offsetCount} "
            + "(the deferred tail must ship once the prefix hole fills)");
    }

    /// <summary>
    /// One shipping-read poll. Computes the durable-contiguous tail through the
    /// production watermark rule (or the raw tail under the removed-guard mode),
    /// collects the persisted offsets the provider would return ascending from the
    /// cursor - stopping at the first the exposure rule rejects - advances the
    /// cursor, and asserts no hole was shipped.
    /// </summary>
    private long ReaderPoll(List<long> inFlight, bool[] persisted, long cursor)
    {
        var tail = _mode == WalShippingWatermarkMode.DurableContiguousWatermark
            ? WalShippingWatermark.DurableContiguousTail(
                inFlight.Count != 0,
                inFlight.Count != 0 ? inFlight[0] : 0L,
                _offsetCount)
            // The removed guard: clamp at the raw next-offset tail, ignoring the
            // in-flight windows entirely.
            : _offsetCount;

        // The provider returns persisted rows strictly ascending from the cursor,
        // skipping not-yet-persisted holes; the grain stops the page at the first
        // offset the exposure rule rejects.
        var next = cursor;
        for (var offset = cursor; offset < _offsetCount; offset++)
        {
            if (!persisted[offset])
            {
                continue;
            }

            if (!WalShippingWatermark.IsOffsetExposable(offset, tail))
            {
                break;
            }

            next = offset + 1;
        }

        // No-hole safety: advancing the durable cursor to `next` certifies every
        // offset below it as shipped, so every such offset must already be
        // persisted. A gap here is a stranded, permanently-lost entry.
        for (var offset = 0L; offset < next; offset++)
        {
            Specification.Assert(
                persisted[offset],
                $"shipping cursor advanced to {next} past an unpersisted offset {offset}: "
                + "the reader shipped an offset above a transient prefix hole and stranded the "
                + "still-in-flight lower entry (cross-cluster replication gap)");
        }

        return next;
    }

    /// <summary>
    /// Picks which in-flight flush completes next, driving the choice through the
    /// runtime so the harness explores every completion order. Scans the in-flight
    /// windows and takes the first the runtime accepts, defaulting to the oldest.
    /// </summary>
    private static int SelectIndex(int count, ICoyoteRuntime runtime)
    {
        for (var i = 0; i < count; i++)
        {
            if (runtime.RandomBoolean())
            {
                return i;
            }
        }

        return 0;
    }
}
