using Microsoft.Coyote.Runtime;
using Microsoft.Coyote.Specifications;
using Orleans.Lattice.Testing.Coyote;

namespace Orleans.Lattice.Tests.BPlusTree.Coyote;

/// <summary>
/// Which blocked-floor meet a <see cref="WalBlockedFloorLifecycleModel"/> run
/// drives, so the safety test can prove the min-pin meet is load-bearing by
/// removing it (joining at the <em>maximum</em> live pin instead of meeting at
/// the minimum) and asserting Coyote re-finds a buffering consumer whose live pin
/// the GC trimmed past.
/// </summary>
public enum WalBlockedFloorMode
{
    /// <summary>
    /// The fix: the blocked floor is the <b>minimum</b> live buffer pin across
    /// consumers, computed by folding every consumer's pin through the production
    /// core (<see cref="Orleans.Lattice.WalBlockedFloorCore.Meet"/>) exactly as
    /// <c>InMemoryWalCursorRegistry.ComputeBlockedFloor</c> does. The slowest
    /// (lowest-pinned) buffering consumer therefore holds the floor down and the
    /// GC never trims an entry any live buffer still needs.
    /// </summary>
    MinPinMeet,

    /// <summary>
    /// The guard removed: the floor is computed as the <b>maximum</b> live pin (as
    /// if a lower-pinned consumer were dropped from the meet), so once buffering
    /// consumers diverge the floor rises above a slower consumer's live pin and
    /// the GC trims an entry that consumer is still holding in its buffer - the
    /// receiver can no longer recover from buffer state.
    /// </summary>
    MaxPinJoinNoLaggard,
}

/// <summary>
/// A Coyote concurrency model of the WAL cursor registry's <b>blocked-floor</b>
/// lifecycle under multiple consumers taking, raising, and clearing their buffer
/// pins concurrently with GC floor reads, driving the <b>production</b> meet
/// (<see cref="Orleans.Lattice.WalBlockedFloorCore.Meet"/>) that
/// <c>InMemoryWalCursorRegistry.GetBlockedFloorAsync</c> folds over every consumer
/// snapshot. Because the model executes the same fold Orleans runs, a violation
/// Coyote finds is a violation of the real blocked-floor path.
/// <para>
/// Each consumer owns one buffer pin and walks a strictly-advancing lifecycle -
/// not pinned, then pinned at its base HLC (buffering its oldest staged entry),
/// then raised as the buffer drains, then cleared (buffer empty) - and the Coyote
/// scheduler interleaves {advance a scheduler-chosen consumer's pin by one step,
/// take a GC floor read} so a floor read can observe the consumers' pins at any
/// relative point in their lifecycles, including one consumer pinned far below the
/// others.
/// </para>
/// <para>
/// The safety property is <b>never trim past a live pin</b>: the floor sampled by
/// any GC read (the blocked-floor clause holds back every entry whose HLC is at or
/// after the floor, so entries strictly below the floor are trimmable) must be at
/// or below every consumer's current live pin. A floor above a live pin lets the
/// GC reap an entry a buffering receiver still needs, so it can never rebuild its
/// partially-staged atomic batch.
/// </para>
/// </summary>
public sealed class WalBlockedFloorLifecycleModel : ICoyoteModel
{
    private readonly int _consumerCount;
    private readonly WalBlockedFloorMode _mode;

    /// <summary>
    /// Creates the model for <paramref name="consumerCount"/> concurrently
    /// buffering consumers under the chosen blocked-floor <paramref name="mode"/>.
    /// At least two are required so their live pins can diverge and the removed
    /// guard has a lower pin to strand.
    /// </summary>
    public WalBlockedFloorLifecycleModel(int consumerCount, WalBlockedFloorMode mode)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(consumerCount, 2);
        _consumerCount = consumerCount;
        _mode = mode;
    }

    /// <inheritdoc />
    public void Run(ICoyoteRuntime runtime)
    {
        // Each consumer's pin walks a fixed, strictly-advancing lifecycle so
        // exploration terminates: not pinned -> pinned at base -> raised (buffer
        // partially drained) -> cleared. The base HLCs are staggered per consumer
        // so live pins genuinely diverge (consumer i buffers lower than i+1).
        var schedule = new long?[_consumerCount][];
        for (var i = 0; i < _consumerCount; i++)
        {
            var basePin = i + 1;
            var raisedPin = basePin + _consumerCount;
            schedule[i] = new long?[] { basePin, raisedPin, null };
        }

        var step = new int[_consumerCount];
        var pin = new long?[_consumerCount];

        // One guaranteed pin transition per iteration bounds the loop, while an
        // optional floor read before and after each transition lets the scheduler
        // sample the floor at every point in the consumers' lifecycles.
        var remaining = _consumerCount * 3;
        while (remaining > 0)
        {
            if (runtime.RandomBoolean())
            {
                FloorRead(pin);
            }

            var c = SelectPending(step, runtime);
            pin[c] = schedule[c][step[c]];
            step[c]++;
            remaining--;

            if (runtime.RandomBoolean())
            {
                FloorRead(pin);
            }
        }

        // Every consumer ended cleared, so the floor is null (nothing pinned) and
        // the GC is unconstrained by buffers - the buffers all drained cleanly.
        Specification.Assert(
            ComputeFloor(pin) is null,
            "blocked floor is non-null after every consumer cleared its buffer pin: a drained buffer "
            + "is still holding the GC back");
    }

    /// <summary>
    /// One GC floor read. Samples the blocked floor (the minimum live pin under
    /// the production meet, or the maximum under the removed-guard mode) and
    /// asserts it never sits above any consumer's current live pin - trimming
    /// entries strictly below the floor must strand no buffering consumer.
    /// </summary>
    private void FloorRead(long?[] pin)
    {
        var floor = ComputeFloor(pin);
        if (floor is not { } floorTick)
        {
            return;
        }

        for (var i = 0; i < pin.Length; i++)
        {
            if (pin[i] is not { } livePin)
            {
                continue;
            }

            Specification.Assert(
                floorTick.WallClockTicks <= livePin,
                $"blocked floor {floorTick.WallClockTicks} rose above consumer {i}'s live buffer pin {livePin}: "
                + "the GC would trim an entry that consumer is still buffering (buffer-recovery data loss)");
        }
    }

    /// <summary>
    /// Computes the blocked floor across the current pins. The safe mode folds
    /// every pin through the real production meet; the removed-guard mode takes
    /// the maximum live pin instead, as if the lowest-pinned consumer were dropped.
    /// </summary>
    private HybridLogicalClock? ComputeFloor(long?[] pin)
    {
        if (_mode == WalBlockedFloorMode.MinPinMeet)
        {
            HybridLogicalClock? floor = null;
            foreach (var p in pin)
            {
                var consumerPin = p is { } tick
                    ? new HybridLogicalClock { WallClockTicks = tick }
                    : (HybridLogicalClock?)null;
                floor = Orleans.Lattice.WalBlockedFloorCore.Meet(floor, consumerPin);
            }

            return floor;
        }

        // The removed guard: join at the maximum live pin.
        long? max = null;
        foreach (var p in pin)
        {
            if (p is { } tick && (max is null || tick > max.Value))
            {
                max = tick;
            }
        }

        return max is { } m ? new HybridLogicalClock { WallClockTicks = m } : null;
    }

    /// <summary>
    /// Picks which consumer's pin advances next, driving the choice through the
    /// runtime so the harness explores every lifecycle interleaving.
    /// </summary>
    private static int SelectPending(int[] step, ICoyoteRuntime runtime)
    {
        var fallback = -1;
        for (var i = 0; i < step.Length; i++)
        {
            if (step[i] >= 3)
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
}
