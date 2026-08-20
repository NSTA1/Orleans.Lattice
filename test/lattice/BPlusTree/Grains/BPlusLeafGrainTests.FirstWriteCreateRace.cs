using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Storage;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Regression tests for #1557: the transient duplicate-activation
/// first-create race on a brand-new leaf grain. On a fresh repocontext
/// data volume (cold silo, empty B+ tree), the fresh-silo grain-directory
/// warmup can transiently materialise two activations of the same
/// deterministic-id leaf grain. Each activation read an empty storage row
/// at activation (<c>RecordExists == false</c>), so each issues its first
/// <see cref="Orleans.Lattice.BPlusTree.Grains.BPlusLeafGrain.SetTreeIdAsync"/>
/// seed write with a null/empty expected etag. The loser of the storage
/// insert compare-and-swap throws
/// <see cref="InconsistentStateException"/> with <em>both</em> etags empty
/// - the signature of two concurrent first-writes to the same brand-new
/// grain.
/// <para>
/// The per-activation <c>_splitGate</c> that serialises the etag-race
/// fixed in <c>BPlusLeafGrainTests.EtagRace</c> cannot help here: this is
/// a <em>cross-activation</em> race, not two turns on one activation.
/// Because the leaf's grain id is deterministic per shard and the only
/// writers of it are the shard root seeding the same tree, the winner's
/// durably-committed row already satisfies the seed this activation
/// intended, so the benign lost race must converge by re-reading rather
/// than failing the cold-start bulk apply with a spurious
/// <c>fail</c>-level run abort. A genuine stale-state conflict on an
/// already-existing row (non-empty etags) must still surface loudly.
/// </para>
/// </summary>
public partial class BPlusLeafGrainTests
{
    [Test]
    public async Task SetTreeId_converges_when_concurrent_first_write_wins_the_create_race()
    {
        // Fresh leaf: no storage row exists yet, so the first
        // WriteStateAsync is an insert with an empty expected etag.
        var state = new FakePersistentState<LeafNodeState> { RecordExistsValue = false };
        var grain = CreateGrain(state);

        // A concurrent duplicate activation wins the insert CAS first, so
        // this activation's first WriteStateAsync throws the empty/empty
        // InconsistentStateException. When the leaf re-reads to recover,
        // storage now delivers the winner's durably-committed row (same
        // deterministic TreeId, because both racers seed the same leaf).
        state.ThrowOnWrite = new InconsistentStateException(
            "Version conflict (WriteState): ETag=. Expected Etag= Received Etag=",
            storedEtag: string.Empty,
            currentEtag: string.Empty);
        state.OnReadState = s =>
        {
            s.RecordExistsValue = true;
            s.State.TreeId = "fox";
        };

        // Before the fix the empty/empty conflict propagates out of
        // SetTreeIdAsync (its Class B revert catch rethrows), failing the
        // cold-start apply. After the fix PersistAsync recognises the
        // benign first-create lost race, adopts the winner's row, and
        // converges without throwing.
        Assert.DoesNotThrowAsync(async () => await grain.SetTreeIdAsync("fox"),
            "A benign empty/empty first-create lost race on a brand-new leaf "
            + "must converge by re-reading the winner's row, not fail the "
            + "cold-start bulk apply.");

        Assert.Multiple(() =>
        {
            Assert.That(state.State.TreeId, Is.EqualTo("fox"),
                "The leaf must converge on the winner's seeded TreeId.");
            Assert.That(state.ReadCount, Is.GreaterThanOrEqualTo(1),
                "The benign-race resolution must re-read storage to adopt "
                + "the winner's durably-committed row.");
            Assert.That(state.WriteCount, Is.EqualTo(0),
                "The loser must not blindly re-issue a write over the "
                + "winner's row; adopting via re-read is sufficient.");
        });
    }

    [Test]
    public void SetTreeId_still_throws_on_a_genuine_stale_state_conflict()
    {
        // Guards the fail-loud contract: a conflict on an already-existing
        // row (RecordExists == true, non-empty etags) is NOT the benign
        // first-create race and must still surface, so the fix cannot mask
        // a real version conflict (e.g. the #1560 fall-off-the-log path).
        var state = new FakePersistentState<LeafNodeState> { RecordExistsValue = true };
        var grain = CreateGrain(state);

        state.ThrowOnWrite = new InconsistentStateException(
            "Version conflict (WriteState) on an existing row.",
            storedEtag: "7",
            currentEtag: "9");

        Assert.ThrowsAsync<InconsistentStateException>(
            async () => await grain.SetTreeIdAsync("fox"),
            "A genuine stale-state conflict on an existing row must still "
            + "surface loudly and not be swallowed as a benign create race.");
    }
}
