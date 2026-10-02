using System;
using System.Buffers;
using System.Collections.Generic;
using BenchmarkDotNet.Attributes;
using Orleans.Lattice;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Benchmark.Microbench;

/// <summary>
/// Isolates the transient <c>WalRecord</c> batch buffer that three of the leaf
/// grain's four batched commit-log dispatches still allocate per call.
/// <para>
/// <c>CommitSetManyAsync</c> already solved this: it builds the batch into a
/// <c>WalRecord[]</c> that is rented from <see cref="ArrayPool{T}"/> above a
/// threshold and allocated directly below it, passes an
/// <see cref="ArraySegment{T}"/> slice to <c>ICommitLogWriter.AppendManyAsync</c>,
/// and returns the rental after clearing the written prefix (the record holds
/// <see cref="string"/>, <see cref="byte"/>[] and <c>VersionVector</c>
/// references, which must not be pinned in a pool slot between rents). The CRDT
/// batch-apply path and the two merge-batch paths were never converted and
/// still build a <c>List&lt;WalRecord&gt;</c>.
/// </para>
/// <para>
/// <c>WalRecord</c> is a <c>readonly record struct</c> of twenty-six fields -
/// roughly 130 bytes per slot - so the list's backing array is the dominant
/// per-batch allocation on those paths. The CRDT path pays it <b>twice</b>: its
/// dispatch site calls <c>walEntries.ToArray()</c> because
/// <c>AppendManyAsync</c> takes an <see cref="ArraySegment{T}"/>, so a second
/// full-size array is copied out and immediately discarded.
/// </para>
/// <para>
/// The shipped XML doc at that site argues the copy is unavoidable, on the
/// grounds that the obvious alternative - a reusable <c>WalRecord[]</c> field on
/// the grain - is unsafe because the grain carries <c>[AlwaysInterleave]</c>
/// methods and a second turn can interleave at any <c>await</c> inside the fill
/// loop. That argument is correct about a <b>grain field</b> and says nothing
/// about a <b>per-call pool rental</b>, which is per-call state exactly as the
/// list it replaces is, and which the sibling path already does across its own
/// awaits.
/// </para>
/// <para>
/// Read this suite for <b>bytes</b>. The expected value is closed-form: the
/// baseline allocates one <c>List&lt;WalRecord&gt;</c> header plus a
/// <c>WalRecord[count]</c> backing array, and the CRDT lane a second
/// <c>WalRecord[count]</c>; above the pool threshold the optimized lane
/// allocates neither. The lanes sort nothing and call nothing virtual, so
/// timing is reported only to show the trim does not cost time - no speedup is
/// claimed.
/// </para>
/// <para>
/// <see cref="BatchSize"/> straddles the production threshold deliberately. 16
/// is the <b>below-threshold control lane</b>: the optimized shape must there
/// fall back to a direct <c>WalRecord[count]</c> allocation and so can only win
/// the list header and (on the CRDT lane) the copy - a lane that showed a pool
/// win at 16 would be measuring something other than the shipped shape. 256 is
/// the dominant foreground and merge batch width, where the rental is live.
/// </para>
/// <para>
/// Run it via <c>BENCH_MICROBENCH_SUITE=walbatchbuffertrims</c> (or
/// <c>--suite walbatchbuffertrims</c>); see <c>Program.cs</c>. No Orleans silo
/// is involved, so it runs cheaply at <c>BENCH_MICROBENCH_FIDELITY=full</c>.
/// </para>
/// </summary>
[MemoryDiagnoser]
public class WalBatchBufferTrimBenchmarks
{
    /// <summary>
    /// The production pool threshold, copied verbatim from
    /// <c>BPlusLeafGrain.CommitSetManyPoolThreshold</c>. Below it the pool's
    /// smallest bucket (16 slots) and its clear cost dominate the saving, so
    /// the shipped shape allocates directly; the optimized lanes here reproduce
    /// that branch rather than pooling unconditionally.
    /// </summary>
    private const int PoolThreshold = 128;

    /// <summary>
    /// Entries per batch. 16 is the below-threshold control (the optimized lane
    /// must take the direct-allocation branch); 256 is the foreground
    /// commit/merge width where the rental is live.
    /// </summary>
    [Params(16, 256)]
    public int BatchSize { get; set; }

    private List<KeyValuePair<string, byte[]>> _entries = null!;

    [GlobalSetup]
    public void Setup()
    {
        _entries = new List<KeyValuePair<string, byte[]>>(BatchSize);
        for (var i = 0; i < BatchSize; i++)
        {
            _entries.Add(new KeyValuePair<string, byte[]>($"key-{i:D6}", BitConverter.GetBytes(i)));
        }

        AssertEquivalence();
    }

    /// <summary>
    /// Proves the pooled shape produces a batch byte-identical to the list
    /// shape - same length, same records, same order - before any measurement
    /// is attributed to it. Both widths are checked, so the below-threshold
    /// branch is covered as well as the rental branch.
    /// </summary>
    private void AssertEquivalence()
    {
        var viaList = BuildViaList();
        var viaPool = BuildViaPoolForAssertion();

        if (viaList.Count != viaPool.Count)
        {
            throw new InvalidOperationException(
                $"WAL batch length diverged: list={viaList.Count}, pooled={viaPool.Count}.");
        }

        for (var i = 0; i < viaList.Count; i++)
        {
            if (!viaList[i].Equals(viaPool[i]))
            {
                throw new InvalidOperationException($"WAL batch record diverged at {i}.");
            }
        }
    }

    private ArraySegment<WalRecord> BuildViaList()
    {
        var walEntries = new List<WalRecord>(_entries.Count);
        for (var i = 0; i < _entries.Count; i++)
        {
            walEntries.Add(Record(_entries[i], i));
        }

        return new ArraySegment<WalRecord>(walEntries.ToArray(), 0, walEntries.Count);
    }

    private ArraySegment<WalRecord> BuildViaPoolForAssertion()
    {
        var count = _entries.Count;
        var buffer = count >= PoolThreshold ? ArrayPool<WalRecord>.Shared.Rent(count) : new WalRecord[count];
        for (var i = 0; i < count; i++)
        {
            buffer[i] = Record(_entries[i], i);
        }

        // Copied out rather than returned over the rental, so the assertion
        // cannot read a slot the pool has since handed to someone else.
        return new ArraySegment<WalRecord>(buffer.AsSpan(0, count).ToArray(), 0, count);
    }

    /// <summary>
    /// The per-entry record shape the CRDT and merge batch paths build, with
    /// the same reference-carrying fields (key, value, vector clock) that force
    /// the pooled path to clear its written prefix before returning.
    /// </summary>
    private static WalRecord Record(KeyValuePair<string, byte[]> entry, int index) => new()
    {
        TreeId = "bench-tree",
        Op = MutationKind.Set,
        Key = entry.Key,
        Value = entry.Value,
        Timestamp = new HybridLogicalClock { WallClockTicks = index, Counter = 0 },
        IsTombstone = false,
        ExpiresAtTicks = 0,
        OriginClusterId = "bench-cluster",
        VectorClock = null,
        TransactionId = Guid.Empty,
        Category = MutationCategory.User,
        IsPrepared = false,
        IsMerge = true,
        ShardIndex = 0,
    };

    /// <summary>
    /// Baseline for the CRDT batch-apply dispatch: a presized
    /// <c>List&lt;WalRecord&gt;</c> filled by <c>Add</c>, then copied out with
    /// <c>ToArray</c> because the writer takes an <see cref="ArraySegment{T}"/>.
    /// Verbatim the shipped body.
    /// </summary>
    [Benchmark(Baseline = true)]
    public int CrdtBatch_ListToArray()
    {
        var walEntries = new List<WalRecord>(_entries.Count);
        for (var i = 0; i < _entries.Count; i++)
        {
            walEntries.Add(Record(_entries[i], i));
        }

        var segment = new ArraySegment<WalRecord>(walEntries.ToArray(), 0, walEntries.Count);
        return Consume(segment);
    }

    /// <summary>
    /// The CRDT batch-apply dispatch after the change: one threshold-gated
    /// buffer, sliced straight into the segment the writer wants, with the
    /// written prefix cleared before the rental goes back so no record's string
    /// or byte[] references stay reachable from a pool slot.
    /// </summary>
    [Benchmark]
    public int CrdtBatch_Pooled()
    {
        var count = _entries.Count;
        var rentedFromPool = count >= PoolThreshold;
        var walEntries = rentedFromPool ? ArrayPool<WalRecord>.Shared.Rent(count) : new WalRecord[count];
        try
        {
            for (var i = 0; i < count; i++)
            {
                walEntries[i] = Record(_entries[i], i);
            }

            return Consume(new ArraySegment<WalRecord>(walEntries, 0, count));
        }
        finally
        {
            if (rentedFromPool)
            {
                walEntries.AsSpan(0, count).Clear();
                ArrayPool<WalRecord>.Shared.Return(walEntries);
            }
        }
    }

    /// <summary>
    /// Baseline for the two merge-batch dispatches, which pass the list
    /// straight to <c>AppendManyAsync(IReadOnlyList&lt;WalRecord&gt;)</c> and so
    /// pay the backing array but not a copy. Verbatim the shipped body.
    /// </summary>
    [Benchmark]
    public int MergeBatch_List()
    {
        var walEntries = new List<WalRecord>(_entries.Count);
        for (var i = 0; i < _entries.Count; i++)
        {
            walEntries.Add(Record(_entries[i], i));
        }

        return Consume(walEntries);
    }

    /// <summary>
    /// The merge-batch dispatch after the change. <see cref="ArraySegment{T}"/>
    /// implements <see cref="IReadOnlyList{T}"/>, so the writer signature is
    /// unchanged; the segment boxes once (~24 bytes) where the list allocated a
    /// header plus a ~130-byte-per-slot backing array.
    /// </summary>
    [Benchmark]
    public int MergeBatch_Pooled()
    {
        var count = _entries.Count;
        var rentedFromPool = count >= PoolThreshold;
        var walEntries = rentedFromPool ? ArrayPool<WalRecord>.Shared.Rent(count) : new WalRecord[count];
        try
        {
            for (var i = 0; i < count; i++)
            {
                walEntries[i] = Record(_entries[i], i);
            }

            return Consume(new ArraySegment<WalRecord>(walEntries, 0, count));
        }
        finally
        {
            if (rentedFromPool)
            {
                walEntries.AsSpan(0, count).Clear();
                ArrayPool<WalRecord>.Shared.Return(walEntries);
            }
        }
    }

    /// <summary>
    /// Stands in for <c>ICommitLogWriter.AppendManyAsync</c>: reads the batch
    /// through the same <see cref="IReadOnlyList{T}"/> surface the writer does,
    /// so both lanes pay the identical read-side cost (including the interface
    /// dispatch) and the delta is the buffer alone.
    /// </summary>
    private static int Consume(IReadOnlyList<WalRecord> batch)
    {
        var sink = 0;
        for (var i = 0; i < batch.Count; i++)
        {
            sink += batch[i].Key.Length;
        }

        return sink;
    }
}
