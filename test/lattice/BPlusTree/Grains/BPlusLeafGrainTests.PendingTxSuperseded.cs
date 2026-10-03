using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Regression tests for a key covered by a <b>single</b> saga's pending bucket
/// whose saga committed but whose terminal never reached this leaf, while later
/// writes to the key did. A silo restart produces it: the saga records its
/// commit, its terminal broadcast is refused on the way to this leaf (a resize's
/// shadow-forwarded copy whose shard root could not append the terminal while
/// its silo shut down), and every later round drains a newer row beside the
/// orphan.
/// <para>
/// Only keys covered by more than one bucket went through the supersession
/// check in <see cref="AtomicVisibilityGate.SelectDecidingPrepare"/>; a lone
/// bucket was resolved straight through <see cref="AtomicVisibilityGate.ResolveKey"/>
/// and surfaced its committed prepare, so the leaf served the orphan's old round
/// over the newer committed rows while sibling leaves served the current one - a
/// torn read that lasted until the parked saga resumed. The commit drain would
/// skip that prepare (the orphan-drain guard in
/// <c>BPlusLeafGrain.ApplyTxCommit</c>), so the row is the only value the key can
/// settle on, whatever the saga's outcome.
/// </para>
/// </summary>
public partial class BPlusLeafGrainTests
{
    private static readonly byte[] OrphanRoundValue = [1];
    private static readonly byte[] LaterRowValue = [7];

    /// <summary>
    /// Prepares <paramref name="keys"/> under one saga, then writes a newer row
    /// for each, so the saga's lone bucket is older than the committed row.
    /// </summary>
    private static async Task<(BPlusLeafGrain Grain, Guid Orphan)> PrepareSupersededOrphanAsync(params string[] keys)
    {
        var grain = CreateGrain();
        var orphan = Guid.NewGuid();
        foreach (var key in keys)
            await PreparedSetAsync(grain, orphan, key, OrphanRoundValue);
        foreach (var key in keys)
            await grain.SetAsync(key, LaterRowValue);
        return (grain, orphan);
    }

    [Test]
    public async Task Reads_serve_a_newer_row_over_a_lone_committed_prepare_it_supersedes()
    {
        var (grain, orphan) = await PrepareSupersededOrphanAsync("k1", "k2");
        var snapshot = new Dictionary<Guid, TxStatus> { [orphan] = TxStatus.Committed };

        using (LatticeRegistrySnapshotContext.BeginScope(snapshot))
        {
            var many = await grain.GetManyAsync(["k1", "k2"]);
            var single = await grain.GetAsync("k1");
            var versioned = await grain.GetWithVersionAsync("k1");
            var exists = await grain.ExistsAsync("k1");

            Assert.Multiple(() =>
            {
                Assert.That(many, Is.EquivalentTo(new Dictionary<string, byte[]>
                {
                    ["k1"] = LaterRowValue,
                    ["k2"] = LaterRowValue,
                }), "A committed prepare whose drain the newer row would skip must not shadow that "
                    + "row: surfacing it serves an older round than every sibling leaf.");
                Assert.That(single, Is.EqualTo(LaterRowValue));
                Assert.That(versioned.Value, Is.EqualTo(LaterRowValue));
                Assert.That(exists, Is.True);
            });
        }
    }

    [Test]
    public async Task Scan_reads_serve_a_newer_row_over_a_lone_committed_prepare_it_supersedes()
    {
        var (grain, orphan) = await PrepareSupersededOrphanAsync("k1", "k2");
        var snapshot = new Dictionary<Guid, TxStatus> { [orphan] = TxStatus.Committed };

        using (LatticeRegistrySnapshotContext.BeginScope(snapshot))
        {
            var entries = await grain.GetEntriesAsync();
            var live = await grain.GetLiveEntriesAsync();

            Assert.That(entries.ToDictionary(e => e.Key, e => e.Value), Is.EquivalentTo(new Dictionary<string, byte[]>
            {
                ["k1"] = LaterRowValue,
                ["k2"] = LaterRowValue,
            }));
            Assert.That(live.Values, Has.All.EqualTo(LaterRowValue));
        }
    }

    [Test]
    public async Task Reads_serve_a_newer_row_over_a_lone_indeterminate_prepare_it_supersedes()
    {
        // An indeterminate outcome hides a key only while its saga's value could
        // still be the one the key settles on. A newer row the drain would never
        // overwrite settles it whichever way the saga went.
        var (grain, orphan) = await PrepareSupersededOrphanAsync("k1");
        var snapshot = new Dictionary<Guid, TxStatus> { [orphan] = TxStatus.Indeterminate };

        using (LatticeRegistrySnapshotContext.BeginScope(snapshot))
        {
            Assert.That((await grain.GetManyAsync(["k1"]))["k1"], Is.EqualTo(LaterRowValue));
            Assert.That(await grain.GetAsync("k1"), Is.EqualTo(LaterRowValue));
        }
    }

    [Test]
    public async Task Reads_need_no_registry_view_when_every_pending_key_is_superseded()
    {
        // Every pending key is superseded, so no outcome is consulted: the read
        // completes even without a single decision view for the fan-out.
        var (grain, _) = await PrepareSupersededOrphanAsync("k1");

        using (LatticeRegistrySnapshotContext.BeginUnavailableScope())
        {
            var many = await grain.GetManyAsync(["k1"]);

            Assert.That(many["k1"], Is.EqualTo(LaterRowValue));
        }
    }

    [Test]
    public async Task Reads_still_surface_a_lone_committed_prepare_newer_than_the_row()
    {
        // The control: a committed saga whose terminal has not landed yet, and
        // whose prepare is newer than the row, is the key's value.
        var grain = CreateGrain();
        await grain.SetAsync("k1", LaterRowValue);
        var saga = Guid.NewGuid();
        await PreparedSetAsync(grain, saga, "k1", NewerSagaValue);
        var snapshot = new Dictionary<Guid, TxStatus> { [saga] = TxStatus.Committed };

        using (LatticeRegistrySnapshotContext.BeginScope(snapshot))
        {
            Assert.That((await grain.GetManyAsync(["k1"]))["k1"], Is.EqualTo(NewerSagaValue));
            Assert.That(await grain.GetAsync("k1"), Is.EqualTo(NewerSagaValue));
        }
    }
}
