using NUnit.Framework;
using Orleans.Lattice;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Views;

namespace Orleans.Lattice.Tests.Views;

/// <summary>
/// The numeric contribution and retraction paths read the prior intent, the
/// membership row and (for a contribution) the target accumulator slot in one
/// overlapping wave rather than as serial round trips. These tests assert the
/// overlap structurally - by the peak number of point reads in flight before
/// the atomic flip - and that a same-group re-contribution does not read its
/// slot a second time.
/// </summary>
[TestFixture]
public sealed class AggregationApplierReadWaveTests
{
    /// <summary>
    /// An in-memory store whose point reads complete asynchronously, recording
    /// how many were outstanding at once and which keys were read before the
    /// first atomic flip.
    /// </summary>
    private sealed class OverlapRecordingStore : IAggregationViewStore
    {
        private readonly Dictionary<string, byte[]> _map = new(StringComparer.Ordinal);
        private readonly object _gate = new();
        private int _inFlight;
        private bool _flipped;

        public int PeakInFlight { get; private set; }

        public List<string> ReadsBeforeFlip { get; } = [];

        public bool Recording { get; set; }

        public async Task<byte[]?> GetAsync(string key, CancellationToken cancellationToken = default)
        {
            lock (_gate)
            {
                if (Recording && !_flipped)
                {
                    ReadsBeforeFlip.Add(key);
                }

                _inFlight++;
                PeakInFlight = Math.Max(PeakInFlight, Recording ? _inFlight : 0);
            }

            try
            {
                await Task.Delay(20, cancellationToken);
                lock (_gate)
                {
                    return _map.TryGetValue(key, out var v) ? v : null;
                }
            }
            finally
            {
                lock (_gate)
                {
                    _inFlight--;
                }
            }
        }

        public Task<Dictionary<string, byte[]>> GetManyAsync(List<string> keys, CancellationToken cancellationToken = default)
        {
            lock (_gate)
            {
                var result = new Dictionary<string, byte[]>(StringComparer.Ordinal);
                foreach (var key in keys)
                {
                    if (_map.TryGetValue(key, out var v))
                    {
                        result[key] = v;
                    }
                }

                return Task.FromResult(result);
            }
        }

        public Task SetAsync(string key, byte[] value, CancellationToken cancellationToken = default)
        {
            lock (_gate)
            {
                _map[key] = value;
            }

            return Task.CompletedTask;
        }

        public Task DeleteAsync(string key, CancellationToken cancellationToken = default)
        {
            lock (_gate)
            {
                _map.Remove(key);
            }

            return Task.CompletedTask;
        }

        public Task SetManyAtomicAsync(List<KeyValuePair<string, byte[]>> entries, string operationId, CancellationToken cancellationToken = default)
        {
            lock (_gate)
            {
                if (Recording)
                {
                    _flipped = true;
                }

                foreach (var e in entries)
                {
                    _map[e.Key] = e.Value;
                }
            }

            return Task.CompletedTask;
        }
    }

    private static HybridLogicalClock Hlc() => HybridLogicalClock.Tick(new HybridLogicalClock());

    [Test]
    public async Task Same_group_contribution_reads_intent_membership_and_slot_in_one_wave()
    {
        var store = new OverlapRecordingStore();
        var applier = new AggregationApplier(store, AggregationKind.Sum, fanout: 4, maxGroupEntries: 0, operationEpoch: "e1");
        await applier.ApplyAsync(AggregationContribution.OfNumeric("g", "s1", 1.0, Hlc()));

        store.Recording = true;
        await applier.ApplyAsync(AggregationContribution.OfNumeric("g", "s1", 5.0, Hlc()));

        var g = await store.GetAsync("g");
        Assert.Multiple(() =>
        {
            Assert.That(store.PeakInFlight, Is.EqualTo(3),
                "the intent, membership and slot reads must be outstanding together, not serialised");
            Assert.That(store.ReadsBeforeFlip.Count(k => k.StartsWith("\u0000a", StringComparison.Ordinal)), Is.EqualTo(1),
                "a same-group overwrite retracts from the slot the wave already read; it must not read it again");
            Assert.That(LatticeAggregationValue.DecodeDouble(g!), Is.EqualTo(5.0).Within(1e-9),
                "the overwrite must replace the prior numeric exactly");
        });
    }

    [Test]
    public async Task Regroup_reads_the_old_slot_after_membership_and_lands_both_groups_exactly()
    {
        var store = new OverlapRecordingStore();
        var applier = new AggregationApplier(store, AggregationKind.Sum, fanout: 4, maxGroupEntries: 0, operationEpoch: "e1");
        await applier.ApplyAsync(AggregationContribution.OfNumeric("a", "s1", 2.0, Hlc()));
        await applier.ApplyAsync(AggregationContribution.OfNumeric("b", "s2", 3.0, Hlc()));

        store.Recording = true;
        await applier.ApplyAsync(AggregationContribution.OfNumeric("b", "s1", 4.0, Hlc()));

        var a = await store.GetAsync("a");
        var b = await store.GetAsync("b");
        Assert.Multiple(() =>
        {
            Assert.That(store.ReadsBeforeFlip.Count(k => k.StartsWith("\u0000a", StringComparison.Ordinal)), Is.EqualTo(2),
                "a regroup reads the new slot in the wave and the old slot once membership names it");
            Assert.That(a, Is.Null.Or.Matches<byte[]>(v => LatticeAggregationValue.DecodeDouble(v) == 0.0),
                "the old group no longer carries the moved member");
            Assert.That(LatticeAggregationValue.DecodeDouble(b!), Is.EqualTo(7.0).Within(1e-9),
                "the new group gains the moved member's numeric");
        });
    }

    [Test]
    public async Task Retraction_reads_intent_and_membership_in_one_wave()
    {
        var store = new OverlapRecordingStore();
        var applier = new AggregationApplier(store, AggregationKind.Count, fanout: 1, maxGroupEntries: 0, operationEpoch: "e1");
        await applier.ApplyAsync(AggregationContribution.OfNumeric("g", "s1", 1.0, Hlc()));

        store.Recording = true;
        await applier.ApplyAsync(AggregationContribution.Retract("s1", Hlc()));

        var g = await store.GetAsync("g");
        Assert.Multiple(() =>
        {
            Assert.That(store.PeakInFlight, Is.GreaterThanOrEqualTo(2),
                "the intent and membership reads must be outstanding together");
            Assert.That(g, Is.Null,
                "retracting the only member removes the group");
        });
    }
}
