using System.Collections.Concurrent;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;
using Orleans.Storage;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Regression coverage for issue #2096: a bucketed pin write that fails against
/// an ETag-enforcing provider must not leave the conflicting holder cached, or
/// the retry that <c>WriteDurableAsync</c> deliberately arms reuses the stale
/// ETag and conflicts identically forever.
/// </summary>
/// <remarks>
/// These tests deliberately use their own <see cref="IGrainStorage"/> double
/// rather than the one in <c>WalMaterialiserPinGrainBucketTests</c>, because
/// that one does not model ETags at all. A concurrency regression asserted
/// against a store with no concurrency control passes whether or not the bug is
/// present, so the store itself is the load-bearing part of this fixture.
/// </remarks>
[TestFixture]
public sealed class WalMaterialiserPinGrainEtagTests
{
    private const string Tree = "tree-2096";
    private const string ConsumerA = "_lattice_materialiser_tree-2096_leaf-A";

    private static HybridLogicalClock Hlc(long ticks, int counter = 0) =>
        new() { WallClockTicks = ticks, Counter = counter };

    private static async Task<WalMaterialiserPinGrain> ActivateAsync(EtagBucketStore store, int buckets)
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("wal-materialiser-pin", Tree));

        var legacy = new FakePersistentState<WalMaterialiserPinState>();
        var options = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        options.Get(Arg.Any<string>()).Returns(new LatticeOptions
        {
            WalMaterialiserPinBuckets = buckets,
            WalMaterialiserPinFlushIntervalMs = 0,
        });

        var grain = new WalMaterialiserPinGrain(context, legacy, options, logger: null, pinStorage: store);
        await ((IGrainBase)grain).OnActivateAsync(CancellationToken.None);
        return grain;
    }

    [Test]
    public async Task A_conflicting_bucket_write_does_not_wedge_later_writes()
    {
        var store = new EtagBucketStore();
        var grain = await ActivateAsync(store, buckets: 8);
        var slot = WalMaterialiserPinRouting.BucketStateName(ConsumerA, 8);

        // Land one write so the grain caches this bucket's holder and its ETag.
        await grain.ReportAsync(ConsumerA, Hlc(100));

        // Something else advances the same slot, so the cached ETag is now stale.
        store.MutateExternally(slot);

        Assert.That(
            async () => await grain.ReportAsync(ConsumerA, Hlc(200)),
            Throws.InstanceOf<InconsistentStateException>(),
            "the conflicting write must surface, not be silently swallowed");

        // The retry WriteDurableAsync arms must be able to succeed. Before the
        // fix the holder stayed cached carrying the ETag that just conflicted,
        // so every subsequent attempt conflicted identically and the grain could
        // never persist a pin again for the life of the activation.
        Assert.That(
            async () => await grain.ReportAsync(ConsumerA, Hlc(300)),
            Throws.Nothing,
            "a retry after a conflict must re-read the slot for a fresh ETag rather than reusing the stale one");

        Assert.That(store.Snapshot(slot)!.Pins[ConsumerA], Is.EqualTo(Hlc(300)),
            "the recovered write must actually land the newest frontier");
    }

    [Test]
    public async Task A_write_that_lands_but_reports_failure_does_not_wedge_later_writes()
    {
        var store = new EtagBucketStore();
        var grain = await ActivateAsync(store, buckets: 8);
        var slot = WalMaterialiserPinRouting.BucketStateName(ConsumerA, 8);

        await grain.ReportAsync(ConsumerA, Hlc(100));

        // The dangerous transient shape is not a write that never happened - that
        // leaves the cached ETag still valid - but one that COMMITS and then
        // reports failure (a timeout after the provider durably applied it). The
        // slot's ETag has advanced while the holder still carries the old one.
        store.FailNextWriteAfterCommitting = true;
        Assert.That(
            async () => await grain.ReportAsync(ConsumerA, Hlc(200)),
            Throws.InstanceOf<InvalidOperationException>());

        Assert.That(
            async () => await grain.ReportAsync(ConsumerA, Hlc(300)),
            Throws.Nothing,
            "a retry after a write of unknown outcome must re-read rather than reuse a holder whose ETag may have advanced");

        Assert.That(store.Snapshot(slot)!.Pins[ConsumerA], Is.EqualTo(Hlc(300)));
    }

    [Test]
    public async Task Successful_writes_keep_using_the_cached_holder()
    {
        var store = new EtagBucketStore();
        var grain = await ActivateAsync(store, buckets: 8);

        await grain.ReportAsync(ConsumerA, Hlc(100));
        store.Reads.Clear();
        await grain.ReportAsync(ConsumerA, Hlc(200));

        Assert.That(store.Reads, Is.Empty,
            "the ETag cache is the point of the holder; only a failed write may invalidate it");
    }

    /// <summary>
    /// An <see cref="IGrainStorage"/> double that enforces optimistic
    /// concurrency, so a write carrying a stale ETag is rejected exactly as a
    /// real provider would reject it.
    /// </summary>
    private sealed class EtagBucketStore : IGrainStorage
    {
        private readonly ConcurrentDictionary<string, (WalMaterialiserPinState State, int Etag)> _slots =
            new(StringComparer.Ordinal);

        public List<string> Reads { get; } = new();

        public bool FailNextWriteAfterCommitting { get; set; }

        public WalMaterialiserPinState? Snapshot(string stateName) =>
            _slots.TryGetValue(stateName, out var slot) ? Clone(slot.State) : null;

        /// <summary>Advances a slot's ETag behind the grain's back.</summary>
        public void MutateExternally(string stateName)
        {
            var current = _slots.TryGetValue(stateName, out var slot)
                ? slot
                : (State: new WalMaterialiserPinState(), Etag: 0);
            _slots[stateName] = (current.State, current.Etag + 1);
        }

        public Task ReadStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState)
        {
            Reads.Add(stateName);
            if (_slots.TryGetValue(stateName, out var slot))
            {
                grainState.State = (T)(object)Clone(slot.State);
                grainState.ETag = slot.Etag.ToString(System.Globalization.CultureInfo.InvariantCulture);
                grainState.RecordExists = true;
            }
            else
            {
                grainState.State = (T)(object)new WalMaterialiserPinState();
                grainState.ETag = null;
                grainState.RecordExists = false;
            }

            return Task.CompletedTask;
        }

        public Task WriteStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState)
        {
            var exists = _slots.TryGetValue(stateName, out var slot);
            var expected = exists ? slot.Etag.ToString(System.Globalization.CultureInfo.InvariantCulture) : null;
            if (!string.Equals(grainState.ETag, expected, StringComparison.Ordinal))
            {
                return Task.FromException(new InconsistentStateException(
                    $"ETag mismatch writing {stateName}.", expected ?? "<none>", grainState.ETag ?? "<none>"));
            }

            var next = (exists ? slot.Etag : 0) + 1;
            _slots[stateName] = (Clone((WalMaterialiserPinState)(object)grainState.State!), next);

            if (FailNextWriteAfterCommitting)
            {
                // Durably applied, then reported failure. The caller's holder is
                // left carrying the pre-write ETag, which the store has moved
                // past - the shape a post-commit timeout produces.
                FailNextWriteAfterCommitting = false;
                return Task.FromException(new InvalidOperationException("durable pin store timed out after committing"));
            }

            grainState.ETag = next.ToString(System.Globalization.CultureInfo.InvariantCulture);
            grainState.RecordExists = true;
            return Task.CompletedTask;
        }

        public Task ClearStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState)
        {
            _slots.TryRemove(stateName, out _);
            grainState.ETag = null;
            grainState.RecordExists = false;
            return Task.CompletedTask;
        }

        private static WalMaterialiserPinState Clone(WalMaterialiserPinState source) => new()
        {
            Pins = new Dictionary<string, HybridLogicalClock>(source.Pins, StringComparer.Ordinal),
            Offsets = new Dictionary<string, long>(source.Offsets, StringComparer.Ordinal),
            PersistedBucketCount = source.PersistedBucketCount,
        };
    }
}
