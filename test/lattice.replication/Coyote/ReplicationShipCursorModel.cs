using Microsoft.Coyote.Runtime;
using Microsoft.Coyote.Specifications;
using Orleans.Lattice.Testing.Coyote;

namespace Orleans.Lattice.Replication.Tests.Coyote;

/// <summary>
/// How a <see cref="ReplicationShipCursorModel"/> run computes the
/// <c>legacyMigrationPending</c> input it feeds the real
/// <see cref="ReplicationShipEligibility.IsBelowLegacyScalarCursor"/>.
/// </summary>
public enum ReplicationShipCursorMode
{
    /// <summary>
    /// The fix, computed as <c>ReplicationShipperGrain.InitializeDrainTickAsync</c> computes it:
    /// pending only while no partition cursor has been saved and the scalar cursor is non-zero.
    /// </summary>
    PartitionCursorAuthoritative,

    /// <summary>
    /// The guard removed, reproducing the shipper half of #1060: the scalar HLC cursor is a skip
    /// criterion on every tick.
    /// </summary>
    ScalarCursorAlwaysFilters,

    /// <summary>
    /// The anti-vacuity witness: the fix, plus an assertion that the merge never consumes an
    /// entry below the scalar cursor. Coyote must refute it, which proves the run reaches the
    /// below-cursor new write the scalar filter used to drop.
    /// </summary>
    BelowCursorProbe,
}

/// <summary>
/// A Coyote model of one shipper partition whose entries interleave two leaves' independent
/// clocks, so a genuinely new write routinely carries an HLC below the scalar cursor. It drives
/// the real <see cref="ReplicationShipEligibility.IsBelowLegacyScalarCursor"/> - the filter
/// <c>ReplicationShipperGrain</c>'s partition merge runs - over batched shipping with lost
/// acknowledgements, and is the executable counterpart of the TLA+ property
/// <c>CursorNeverSkipsUnshipped</c> in <c>spec/replication/Replication.tla</c>.
/// <para>
/// The shipper drains a batch from its partition cursor, consumes (and does not ship) what the
/// filter drops, ships the rest, and on a positive acknowledgement moves the partition cursor
/// past the batch and the scalar cursor to the highest acknowledged HLC. A lost acknowledgement
/// leaves both cursors in place, so the batch is shipped again. The property, asserted at the
/// end: every entry reached the receiver.
/// </para>
/// </summary>
public sealed class ReplicationShipCursorModel : ICoyoteModel
{
    private readonly ReplicationShipCursorMode _mode;
    private readonly int _lostAcks;

    /// <summary>Creates the model for one filter mode and a lost-ack allowance.</summary>
    public ReplicationShipCursorModel(ReplicationShipCursorMode mode, int lostAcks = 1)
    {
        _mode = mode;
        _lostAcks = lostAcks;
    }

    /// <inheritdoc />
    public void Run(ICoyoteRuntime runtime)
    {
        var budget = new FaultBudget(_lostAcks, duplicates: 0, restarts: 0);

        // Two leaves, each with its own clock, commit in a scheduler-chosen interleaving.
        var partition = new List<HybridLogicalClock>();
        var leafClock = new long[] { 1 + Choose(runtime, 3), 1 + Choose(runtime, 3) };
        for (var i = 0; i < 4; i++)
        {
            var leaf = runtime.RandomBoolean() ? 0 : 1;
            partition.Add(new HybridLogicalClock { WallClockTicks = leafClock[leaf]++ });
        }

        var received = new HashSet<int>();
        var partitionCursor = 0;
        var partitionCursorSaved = false;
        var scalarCursor = HybridLogicalClock.Zero;

        for (var tick = 0; tick < 12 && partitionCursor < partition.Count; tick++)
        {
            var legacyMigrationPending = _mode switch
            {
                ReplicationShipCursorMode.ScalarCursorAlwaysFilters => true,
                _ => !partitionCursorSaved && scalarCursor != HybridLogicalClock.Zero,
            };

            var batchEnd = Math.Min(partition.Count, partitionCursor + 1 + Choose(runtime, 2));
            var shipped = new List<int>();
            for (var i = partitionCursor; i < batchEnd; i++)
            {
                if (_mode == ReplicationShipCursorMode.BelowCursorProbe)
                {
                    Specification.Assert(
                        partition[i].CompareTo(scalarCursor) > 0,
                        "PROBE: the merge consumed an entry at or below the scalar cursor");
                }

                // Pinned: saga prepare-phase entries are excluded from the filter and are #4436's
                // scope, so no entry here is one.
                const bool isPreparedAtomicBatch = false;
                if (!ReplicationShipEligibility.IsBelowLegacyScalarCursor(
                        legacyMigrationPending, isPreparedAtomicBatch, partition[i], scalarCursor))
                {
                    shipped.Add(i);
                }
            }

            foreach (var i in shipped)
            {
                received.Add(i);
            }

            if (shipped.Count > 0 && budget.TryDrop(runtime.RandomBoolean))
            {
                // The acknowledgement is lost: neither cursor moves and the batch ships again.
                continue;
            }

            partitionCursor = batchEnd;
            partitionCursorSaved = true;
            foreach (var i in shipped)
            {
                if (partition[i].CompareTo(scalarCursor) > 0)
                {
                    scalarCursor = partition[i];
                }
            }
        }

        for (var i = 0; i < partition.Count; i++)
        {
            Specification.Assert(
                received.Contains(i),
                $"CursorNeverSkipsUnshipped: entry {i} at HLC {partition[i]} was consumed but never shipped");
        }
    }

    private static int Choose(ICoyoteRuntime runtime, int count)
    {
        for (var i = 0; i < count - 1; i++)
        {
            if (runtime.RandomBoolean())
            {
                return i;
            }
        }

        return count - 1;
    }
}
