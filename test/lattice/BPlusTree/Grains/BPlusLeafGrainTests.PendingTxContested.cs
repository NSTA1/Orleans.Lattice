using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Regression tests for keys covered by more than one saga's pending bucket on
/// one leaf, the shape a silo restart produces: a saga parked by the restart
/// keeps its prepare (replayed into <c>_pendingTx</c> on reactivation, still
/// <see cref="TxStatus.InFlight"/>) while every later saga on the same keys
/// prepares a second bucket beside it.
/// <para>
/// The multi-key and scan read paths used to keep whichever bucket dictionary
/// enumeration reached first, so the long-undecided bucket shadowed the newer
/// committed one and the leaf served the pre-saga value while sibling leaves
/// served the committed batch. Each test drives the leaf's real read path under
/// a fixed registry snapshot (<see cref="LatticeRegistrySnapshotContext"/>), so
/// the outcome per saga is deterministic and no registry grain is involved. The
/// selection rule itself is pinned by <see cref="AtomicVisibilityGateTests"/>.
/// </para>
/// </summary>
public partial class BPlusLeafGrainTests
{
    private static readonly byte[] PreSagaValue = [0];
    private static readonly byte[] OlderSagaValue = [1];
    private static readonly byte[] NewerSagaValue = [2];

    /// <summary>
    /// Seeds <paramref name="keys"/> at <see cref="PreSagaValue"/>, then has an
    /// older saga and a newer saga prepare every key, in that order, so the older
    /// saga's bucket is the one dictionary enumeration reaches first.
    /// </summary>
    private static async Task<(BPlusLeafGrain Grain, Guid Older, Guid Newer)> PrepareTwoSagasOverAsync(params string[] keys)
    {
        var grain = CreateGrain();
        foreach (var key in keys)
            await grain.SetAsync(key, PreSagaValue);

        var older = Guid.NewGuid();
        var newer = Guid.NewGuid();
        foreach (var key in keys)
            await PreparedSetAsync(grain, older, key, OlderSagaValue);
        foreach (var key in keys)
            await PreparedSetAsync(grain, newer, key, NewerSagaValue);
        return (grain, older, newer);
    }

    [Test]
    public async Task GetManyAsync_surfaces_the_committed_saga_when_an_older_in_flight_prepare_also_covers_the_key()
    {
        var (grain, older, newer) = await PrepareTwoSagasOverAsync("k1", "k2");
        var snapshot = new Dictionary<Guid, TxStatus>
        {
            [older] = TxStatus.InFlight,
            [newer] = TxStatus.Committed,
        };

        using (LatticeRegistrySnapshotContext.BeginScope(snapshot))
        {
            var result = await grain.GetManyAsync(["k1", "k2"]);

            Assert.That(result, Is.EquivalentTo(new Dictionary<string, byte[]>
            {
                ["k1"] = NewerSagaValue,
                ["k2"] = NewerSagaValue,
            }), "The parked saga's in-flight prepare must not shadow the committed saga: a pre-saga "
                + "value here, while sibling leaves surface the committed batch, is a torn read.");
        }
    }

    [Test]
    public async Task GetManyAsync_surfaces_an_older_committed_saga_when_a_newer_prepare_is_still_in_flight()
    {
        var (grain, older, newer) = await PrepareTwoSagasOverAsync("k1", "k2");
        var snapshot = new Dictionary<Guid, TxStatus>
        {
            [older] = TxStatus.Committed,
            [newer] = TxStatus.InFlight,
        };

        using (LatticeRegistrySnapshotContext.BeginScope(snapshot))
        {
            var result = await grain.GetManyAsync(["k1", "k2"]);

            Assert.That(result["k1"], Is.EqualTo(OlderSagaValue));
            Assert.That(result["k2"], Is.EqualTo(OlderSagaValue));
        }
    }

    [Test]
    public async Task GetManyAsync_serves_the_pre_saga_row_when_every_prepare_is_undecided()
    {
        var (grain, older, newer) = await PrepareTwoSagasOverAsync("k1");
        var snapshot = new Dictionary<Guid, TxStatus>
        {
            [older] = TxStatus.InFlight,
            [newer] = TxStatus.Aborted,
        };

        using (LatticeRegistrySnapshotContext.BeginScope(snapshot))
        {
            var result = await grain.GetManyAsync(["k1"]);

            Assert.That(result["k1"], Is.EqualTo(PreSagaValue));
        }
    }

    [Test]
    public async Task Reads_serve_a_newer_row_over_an_older_committed_prepare_it_supersedes()
    {
        // The older saga committed but its drain would be skipped: a newer
        // non-saga write already superseded the key. Surfacing the older
        // prepare would serve a value the key can never settle on.
        var grain = CreateGrain();
        var older = Guid.NewGuid();
        var newer = Guid.NewGuid();
        await PreparedSetAsync(grain, older, "k1", OlderSagaValue);
        await grain.SetAsync("k1", [9]);
        await PreparedSetAsync(grain, newer, "k1", NewerSagaValue);
        var snapshot = new Dictionary<Guid, TxStatus>
        {
            [older] = TxStatus.Committed,
            [newer] = TxStatus.InFlight,
        };

        using (LatticeRegistrySnapshotContext.BeginScope(snapshot))
        {
            var many = await grain.GetManyAsync(["k1"]);
            var single = await grain.GetAsync("k1");

            Assert.That(many["k1"], Is.EqualTo(new byte[] { 9 }));
            Assert.That(single, Is.EqualTo(new byte[] { 9 }));
        }
    }

    [Test]
    public async Task Scan_reads_resolve_a_contested_key_like_GetManyAsync()
    {
        var (grain, older, newer) = await PrepareTwoSagasOverAsync("k1", "k2");
        var snapshot = new Dictionary<Guid, TxStatus>
        {
            [older] = TxStatus.InFlight,
            [newer] = TxStatus.Committed,
        };

        using (LatticeRegistrySnapshotContext.BeginScope(snapshot))
        {
            var entries = await grain.GetEntriesAsync();
            var live = await grain.GetLiveEntriesAsync();

            Assert.That(entries.ToDictionary(e => e.Key, e => e.Value), Is.EquivalentTo(new Dictionary<string, byte[]>
            {
                ["k1"] = NewerSagaValue,
                ["k2"] = NewerSagaValue,
            }));
            Assert.That(live.Values, Has.All.EqualTo(NewerSagaValue));
            Assert.That(await grain.CountAsync(), Is.EqualTo(2));
        }
    }

    [Test]
    public async Task GetAsync_and_GetManyAsync_agree_on_a_contested_key([Values] bool olderCommitted)
    {
        // The single-key paths used to pick the newest bucket and the multi-key
        // paths the first enumerated one, so the same leaf could answer one key
        // two different ways under one registry view.
        var (grain, older, newer) = await PrepareTwoSagasOverAsync("k1");
        var snapshot = new Dictionary<Guid, TxStatus>
        {
            [older] = olderCommitted ? TxStatus.Committed : TxStatus.InFlight,
            [newer] = olderCommitted ? TxStatus.InFlight : TxStatus.Committed,
        };

        using (LatticeRegistrySnapshotContext.BeginScope(snapshot))
        {
            var single = await grain.GetAsync("k1");
            var many = await grain.GetManyAsync(["k1"]);
            var exists = await grain.ExistsAsync("k1");
            var versioned = await grain.GetWithVersionAsync("k1");

            var expected = olderCommitted ? OlderSagaValue : NewerSagaValue;
            Assert.Multiple(() =>
            {
                Assert.That(single, Is.EqualTo(expected));
                Assert.That(many["k1"], Is.EqualTo(expected));
                Assert.That(exists, Is.True);
                Assert.That(versioned.Value, Is.EqualTo(expected));
            });
        }
    }
}
