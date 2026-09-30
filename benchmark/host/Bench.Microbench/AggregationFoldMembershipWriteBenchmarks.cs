using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Views;

namespace Orleans.Lattice.Benchmark.Microbench;

/// <summary>
/// Isolates the membership write elided from the custom-fold contribution path
/// in <c>Orleans.Lattice.Views.AggregationApplier</c>.
/// <para>
/// <b>What was there.</b> <c>ContributeFoldAsync</c> closed every contribution by
/// unconditionally writing the source key's membership row. That row is a pure
/// back-pointer on the fold path - it records only which group the key belongs
/// to, and is always encoded as <c>(GroupKey, 0, null)</c>. A source key
/// contributing repeatedly to the <i>same</i> group therefore re-encoded and
/// re-persisted a row byte-identical to the one already stored, on every single
/// contribution: an encode plus a store round trip that no reader could ever
/// observe.
/// </para>
/// <para>
/// <b>What ships.</b> The write is skipped when the stored row is provably the
/// row about to replace it. The group key is settled by the fusion test the
/// method already computes; the numeric is compared <i>on its bits</i>, because
/// <c>-0.0 == 0.0</c> holds while the two encode to different bytes; and the
/// member flag now rides on <c>MembershipHead</c>, which already read it to
/// decide whether to skip the member's bytes. Anything failing those tests is
/// still written, so a re-grouping key, a numeric row, or a set-union row is
/// untouched.
/// </para>
/// <para>
/// <b>Baselines are verbatim.</b> The baseline lane drives the same shipped
/// applier and then performs the elided encode-and-write itself, through the
/// same codec and the same store, so it pays exactly the removed work and
/// nothing else. That is the honest shape here: the branch is inside a private
/// async method, so a copied body would diverge from the real one on every
/// unrelated edit.
/// </para>
/// <para>
/// <b>Controls, and an honest caveat.</b> The <b>re-group</b> lanes move the key
/// to a different group on each contribution, which is exactly the case the
/// guard must decline to skip; they must show parity. A counting store makes the
/// saving <i>deterministic</i> rather than statistical -
/// <see cref="ElidedWritesPerContribution"/> is asserted in
/// <c>[GlobalSetup]</c>. The time column <b>understates the real saving</b>: a
/// <c>SetAsync</c> here is a dictionary insert, where in a silo it is a grain
/// call against persistent storage. Read the write count as the result and the
/// time as a floor.
/// </para>
/// <para>
/// Run it via <c>BENCH_MICROBENCH_SUITE=aggfoldwrite</c> (or
/// <c>--suite aggfoldwrite</c>). No Orleans silo is involved, so it is cheap at
/// <c>BENCH_MICROBENCH_FIDELITY=full</c>.
/// </para>
/// </summary>
[MemoryDiagnoser]
public class AggregationFoldMembershipWriteBenchmarks
{
    private const string SourceKey = "tenant-7/customer-00429/orders";
    private const string GroupKey = "group-a";
    private const string MembershipPrefix = "\u0000m";

    private CountingAggregationViewStore _store = null!;
    private AggregationApplier _applier = null!;
    private AggregationContribution _fused;
    private AggregationContribution[] _regroup = [];
    private byte[] _payload = [];

    /// <summary>
    /// The number of store writes a repeated same-group contribution no longer
    /// performs. Asserted in <c>[GlobalSetup]</c>, so the suite fails rather than
    /// reports a saving it did not make.
    /// </summary>
    public int ElidedWritesPerContribution { get; private set; }

    [GlobalSetup]
    public void Setup()
    {
        _store = new CountingAggregationViewStore();
        _applier = new AggregationApplier(
            _store,
            AggregationKind.Fold,
            fanout: 1,
            maxGroupEntries: 4096,
            operationEpoch: "bench-epoch",
            fold: new ConcatFoldProjection());

        _payload = System.Text.Encoding.UTF8.GetBytes("order-total=17.50");
        _fused = AggregationContribution.Fold(
            GroupKey,
            SourceKey,
            _payload,
            new HybridLogicalClock { WallClockTicks = 638_000_000_000_000_000, Counter = 3 });

        _regroup =
        [
            AggregationContribution.Fold(GroupKey, SourceKey, _payload, new HybridLogicalClock { WallClockTicks = 638_000_000_000_000_001, Counter = 4 }),
            AggregationContribution.Fold("group-b", SourceKey, _payload, new HybridLogicalClock { WallClockTicks = 638_000_000_000_000_002, Counter = 5 }),
        ];

        AssertEquivalence();
    }

    /// <summary>
    /// Proves the elision is invisible: after the same sequence of contributions
    /// the store holds byte-identical rows with and without the write, and the
    /// saving is a real, counted write rather than an artefact of the harness.
    /// Also proves the guard declines to skip when the stored row is <i>not</i>
    /// the row about to replace it - a re-grouping key, and a key whose stored
    /// row carries a set-union member.
    /// </summary>
    private void AssertEquivalence()
    {
        // Warm the membership row so the contribution under test is the fused,
        // steady-state one rather than the first-write one.
        _applier.ApplyAsync(_fused, CancellationToken.None).GetAwaiter().GetResult();

        var before = _store.Snapshot();
        var writesBefore = _store.MembershipWrites;
        _applier.ApplyAsync(_fused, CancellationToken.None).GetAwaiter().GetResult();
        var shippedWrites = _store.MembershipWrites - writesBefore;
        var shippedState = _store.Snapshot();

        _store.Restore(before);
        writesBefore = _store.MembershipWrites;
        ApplyWithLegacyWriteAsync(_fused).GetAwaiter().GetResult();
        var legacyWrites = _store.MembershipWrites - writesBefore;
        var legacyState = _store.Snapshot();

        if (legacyState.Count != shippedState.Count)
        {
            throw new InvalidOperationException("Membership write elision changed the stored row set.");
        }

        foreach (var (key, value) in legacyState)
        {
            if (!shippedState.TryGetValue(key, out var other) || !value.AsSpan().SequenceEqual(other))
            {
                throw new InvalidOperationException($"Membership write elision changed the stored row for '{key}'.");
            }
        }

        ElidedWritesPerContribution = legacyWrites - shippedWrites;
        if (ElidedWritesPerContribution != 1)
        {
            throw new InvalidOperationException(
                $"Expected the fused fold contribution to elide exactly one store write; it elided {ElidedWritesPerContribution}.");
        }

        // Control: a re-grouping contribution must still write, so the guard is
        // not simply dropping every membership write.
        _store.Restore(before);
        writesBefore = _store.MembershipWrites;
        _applier.ApplyAsync(_regroup[1], CancellationToken.None).GetAwaiter().GetResult();
        if (_store.MembershipWrites - writesBefore == 0)
        {
            throw new InvalidOperationException("A re-grouping fold contribution wrote no membership row.");
        }

        // Control: a stored row carrying a set-union member is not byte-identical
        // to the fold row, so it must be rewritten even though the group matches.
        _store.Restore(before);
        _store.Seed(
            MembershipKeyFor(SourceKey),
            AggregationRowCodec.EncodeMembership(new AggregationRowCodec.MembershipRow(GroupKey, 0, "member-x")));
        writesBefore = _store.MembershipWrites;
        _applier.ApplyAsync(_fused, CancellationToken.None).GetAwaiter().GetResult();
        if (_store.MembershipWrites - writesBefore == 0)
        {
            throw new InvalidOperationException("A stored set-union membership row was not rewritten by the fold path.");
        }

        // Control: a stored row carrying negative zero encodes differently from
        // the row about to replace it, so the bitwise numeric test must reject it.
        _store.Restore(before);
        _store.Seed(
            MembershipKeyFor(SourceKey),
            AggregationRowCodec.EncodeMembership(new AggregationRowCodec.MembershipRow(GroupKey, -0.0, null)));
        writesBefore = _store.MembershipWrites;
        _applier.ApplyAsync(_fused, CancellationToken.None).GetAwaiter().GetResult();
        if (_store.MembershipWrites - writesBefore == 0)
        {
            throw new InvalidOperationException("A stored negative-zero membership row was not rewritten by the fold path.");
        }

        _store.Restore(before);
    }

    private static string MembershipKeyFor(string sourceKey) =>
        AggregationRowCodec.MembershipKey(sourceKey);

    private async Task ApplyWithLegacyWriteAsync(AggregationContribution contribution)
    {
        var before = _store.MembershipWrites;
        await _applier.ApplyAsync(contribution, CancellationToken.None);

        // The pre-change path wrote the membership row on every contribution,
        // without exception. Restoring that write only when the shipped guard
        // declined to perform one reconstructs exactly that invariant, so the
        // control lanes - where the guard does not elide - stay a fair
        // comparison rather than paying the write twice. The reconstruction
        // costs one integer compare, which is not the subject of any lane.
        if (_store.MembershipWrites == before)
        {
            await _store.SetAsync(
                MembershipKeyFor(contribution.SourceKey),
                AggregationRowCodec.EncodeMembership(new AggregationRowCodec.MembershipRow(contribution.GroupKey, 0, null)),
                CancellationToken.None);
        }
    }

    /// <summary>The pre-change fold contribution, which always rewrote the membership row.</summary>
    [Benchmark(Baseline = true, Description = "Fold contribute, same group (baseline)")]
    public Task FusedLegacy() => ApplyWithLegacyWriteAsync(_fused);

    /// <summary>The shipped fold contribution, which skips the byte-identical membership row.</summary>
    [Benchmark(Description = "Fold contribute, same group (shipped)")]
    public Task FusedShipped() => _applier.ApplyAsync(_fused, CancellationToken.None);

    /// <summary>Control: a re-grouping contribution, where the write is not elidable.</summary>
    [Benchmark(Description = "Fold contribute, re-group (control, baseline)")]
    public async Task RegroupLegacy()
    {
        await ApplyWithLegacyWriteAsync(_regroup[0]);
        await ApplyWithLegacyWriteAsync(_regroup[1]);
    }

    /// <summary>Control: the shipped re-grouping contribution; must show parity.</summary>
    [Benchmark(Description = "Fold contribute, re-group (control, shipped)")]
    public async Task RegroupShipped()
    {
        await _applier.ApplyAsync(_regroup[0], CancellationToken.None);
        await _applier.ApplyAsync(_regroup[1], CancellationToken.None);
    }

    /// <summary>A minimal fold that concatenates its inputs, so the projection itself is not the subject.</summary>
    private sealed class ConcatFoldProjection : ILatticeFoldProjection
    {
        public string ProjectionVersion => "bench-concat-1";

        public AggregationKind Aggregation => AggregationKind.Fold;

        public IEnumerable<AggregationContribution> Project(LatticeMutation mutation) => [];

        public byte[] Initial() => [];

        public byte[] Apply(byte[] accumulator, string sourceKey, byte[] sourceValue, HybridLogicalClock timestamp)
        {
            var result = new byte[accumulator.Length + sourceValue.Length];
            accumulator.CopyTo(result, 0);
            sourceValue.CopyTo(result, accumulator.Length);
            return result;
        }
    }

    /// <summary>
    /// An in-memory view store that counts the writes it is asked to perform, so
    /// the elision is asserted as a deterministic count rather than inferred from
    /// a timing difference.
    /// </summary>
    private sealed class CountingAggregationViewStore : IAggregationViewStore
    {
        private readonly Dictionary<string, byte[]> _rows = new(StringComparer.Ordinal);

        public int Writes { get; private set; }

        /// <summary>
        /// Writes landing on a membership row specifically, so the baseline lane
        /// can reconstruct the pre-change "always exactly one" invariant without
        /// duplicating the guard it is measuring.
        /// </summary>
        public int MembershipWrites { get; private set; }

        public void Seed(string key, byte[] value) => _rows[key] = value;

        public Dictionary<string, byte[]> Snapshot() => new(_rows, StringComparer.Ordinal);

        public void Restore(Dictionary<string, byte[]> snapshot)
        {
            _rows.Clear();
            foreach (var (key, value) in snapshot)
            {
                _rows[key] = value;
            }
        }

        public Task<byte[]?> GetAsync(string key, CancellationToken cancellationToken = default) =>
            Task.FromResult(_rows.TryGetValue(key, out var value) ? value : null);

        public Task<Dictionary<string, byte[]>> GetManyAsync(List<string> keys, CancellationToken cancellationToken = default)
        {
            var result = new Dictionary<string, byte[]>(keys.Count, StringComparer.Ordinal);
            foreach (var key in keys)
            {
                if (_rows.TryGetValue(key, out var value))
                {
                    result[key] = value;
                }
            }

            return Task.FromResult(result);
        }

        public Task SetAsync(string key, byte[] value, CancellationToken cancellationToken = default)
        {
            Writes++;
            if (key.StartsWith(MembershipPrefix, StringComparison.Ordinal))
            {
                MembershipWrites++;
            }

            _rows[key] = value;
            return Task.CompletedTask;
        }

        public Task DeleteAsync(string key, CancellationToken cancellationToken = default)
        {
            _rows.Remove(key);
            return Task.CompletedTask;
        }

        public Task SetManyAtomicAsync(List<KeyValuePair<string, byte[]>> entries, string operationId, CancellationToken cancellationToken = default)
        {
            foreach (var (key, value) in entries)
            {
                Writes++;
                if (key.StartsWith(MembershipPrefix, StringComparison.Ordinal))
                {
                    MembershipWrites++;
                }

                _rows[key] = value;
            }

            return Task.CompletedTask;
        }
    }
}
