using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Issue #4428: an orphan bucket on a leaf that has already applied its saga's
/// terminal must not hide the key while the registry row is masked
/// (<see cref="TxStatus.Indeterminate"/>). The leaf's row is the saga's outcome,
/// materialised, so every read path serves it; before the fix the gate tested
/// the Indeterminate arm ahead of the orphan guard and every read path hid the
/// key for as long as the row stayed masked.
/// <para>
/// A live prepared write can no longer seed the orphan (issue #4385); activation
/// replay still can, so these tests plant it with <c>PlantPreparedMutationForTest</c>,
/// as the sibling orphan fixtures do.
/// </para>
/// </summary>
public partial class BPlusLeafGrainTests
{
    private static async Task<(BPlusLeafGrain Grain, Guid TxId)> CommittedOrphanUnderMaskAsync()
    {
        var grain = CreateGrain();
        var txid = Guid.NewGuid();

        // The saga committed here: its terminal wrote the committed values.
        await grain.ApplyTxTerminalAsync(
            txid,
            committed: true,
            committedValues: new Dictionary<string, byte[]> { ["a"] = [1], ["b"] = [2] });

        // A replayed shadow-forward re-installs a bucket for the same saga.
        grain.PlantPreparedMutationForTest(txid, "a", [1]);
        return (grain, txid);
    }

    [Test]
    public async Task Point_reads_serve_the_row_of_an_already_terminal_orphan_under_a_masked_registry_row()
    {
        var (grain, txid) = await CommittedOrphanUnderMaskAsync();
        var snapshot = new Dictionary<Guid, TxStatus> { [txid] = TxStatus.Indeterminate };

        using (LatticeRegistrySnapshotContext.BeginScope(snapshot))
        {
            var get = await grain.GetAsync("a");
            var versioned = await grain.GetWithVersionAsync("a");
            var exists = await grain.ExistsAsync("a");

            Assert.Multiple(() =>
            {
                Assert.That(get, Is.EqualTo(new byte[] { 1 }), "the committed value is materialised on this leaf");
                Assert.That(versioned.Value, Is.EqualTo(new byte[] { 1 }));
                Assert.That(exists, Is.True);
            });
        }
    }

    [Test]
    public async Task Multi_key_and_scan_reads_serve_the_whole_batch_of_an_already_terminal_orphan_under_a_masked_registry_row()
    {
        var (grain, txid) = await CommittedOrphanUnderMaskAsync();
        var snapshot = new Dictionary<Guid, TxStatus> { [txid] = TxStatus.Indeterminate };

        using (LatticeRegistrySnapshotContext.BeginScope(snapshot))
        {
            var many = await grain.GetManyAsync(["a", "b"]);
            var entries = (await grain.GetEntriesAsync()).ToDictionary(e => e.Key, e => e.Value);

            Assert.Multiple(() =>
            {
                Assert.That(many.Keys, Is.EquivalentTo(new[] { "a", "b" }),
                    "hiding 'a' while 'b' reads committed would be a torn read of one batch");
                Assert.That(many["a"], Is.EqualTo(new byte[] { 1 }));
                Assert.That(entries.Keys, Is.EquivalentTo(new[] { "a", "b" }));
                Assert.That(entries["a"], Is.EqualTo(new byte[] { 1 }));
            });
        }
    }

    [Test]
    public async Task Reads_still_hide_a_masked_prepare_whose_terminal_has_not_landed_here()
    {
        // The control: with no terminal applied on this leaf the Indeterminate arm
        // still hides the key rather than assert either value.
        var grain = CreateGrain();
        var txid = Guid.NewGuid();
        await grain.SetAsync("a", [9]);
        await PreparedSetAsync(grain, txid, "a", [1]);
        var snapshot = new Dictionary<Guid, TxStatus> { [txid] = TxStatus.Indeterminate };

        using (LatticeRegistrySnapshotContext.BeginScope(snapshot))
        {
            Assert.That(await grain.GetAsync("a"), Is.Null);
        }
    }
}
