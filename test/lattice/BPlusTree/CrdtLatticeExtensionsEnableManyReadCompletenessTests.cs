using NSubstitute;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Covers the read-completeness property of the batched OR-Flag enable helpers
/// (issue #2208).
///
/// <para>
/// <see cref="CrdtLatticeExtensions.EnableManyAsync"/> and
/// <see cref="CrdtLatticeExtensions.StageEnableManyAsync"/> mint each key's
/// enable dot from a single batched
/// <see cref="ILattice.GetManyAsync(List{string}, System.Threading.CancellationToken)"/>.
/// At that seam a key the read did not return is indistinguishable from a key
/// that does not exist: both decode as an empty flag.
/// </para>
///
/// <para>
/// That conflation is only harmless if the minted dot is fresh. It is not:
/// deriving the counter from the snapshot mints counter 1 for an unreturned
/// row, and OR-Flag cancellation is coverage-based, so a stored tombstone at
/// any counter at or above the minted one cancels the enable on merge. The
/// write reports success and the flag stays disabled - permanently, because the
/// next attempt repeats the same read and mints the same dead dot.
/// </para>
///
/// <para>
/// These tests pin the observable property that closes that hole: an enable
/// must take effect whether or not the batched read returned the row, while a
/// genuinely absent row must still enable exactly as before.
/// </para>
/// </summary>
[TestFixture]
public class CrdtLatticeExtensionsEnableManyReadCompletenessTests
{
    private const string Key = "membership/source-id";
    private const string Replica = "r1";

    /// <summary>
    /// Builds a row that was enabled and then disabled, so it carries a
    /// tombstone at <paramref name="counter"/> and reads as disabled.
    /// </summary>
    private static OrFlag DisabledRowTombstonedAt(long counter)
    {
        var flag = new OrFlag();
        flag.Enable(Replica, counter);
        flag.Disable();
        return flag;
    }

    /// <summary>
    /// An <see cref="ILattice"/> whose batched read returns
    /// <paramref name="visible"/> only, and whose batched CRDT apply folds every
    /// delta into <paramref name="stored"/> - the row as it really is on the
    /// server. Setting <paramref name="visible"/> empty models a read that did
    /// not return an existing row.
    /// </summary>
    private static ILattice LatticeWhoseReadReturns(
        Dictionary<string, byte[]> visible,
        OrFlag stored)
    {
        var lattice = Substitute.For<ILattice>();
        lattice.GetManyAsync(Arg.Any<List<string>>(), Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromResult(new Dictionary<string, byte[]>(visible)));
        lattice.ApplyCrdtDeltaManyAsync(
                Arg.Any<List<KeyValuePair<string, byte[]>>>(),
                Arg.Any<LatticeMergeMode>(),
                Arg.Any<CancellationToken>())
            .Returns(call =>
            {
                foreach (var pair in call.Arg<List<KeyValuePair<string, byte[]>>>())
                {
                    stored.MergeDelta(
                        JsonLatticeSerializer<OrFlagDelta>.Default.Deserialize(pair.Value));
                }

                return Task.CompletedTask;
            });

        return lattice;
    }

    /// <summary>
    /// The defect in issue #2208, reduced to one key: the row exists and is
    /// tombstoned, the batched read does not return it, and the enable is
    /// therefore minted at a counter the tombstone already covers.
    /// </summary>
    [Test]
    public async Task EnableManyBatch_enables_a_tombstoned_row_the_batched_read_did_not_return()
    {
        var stored = DisabledRowTombstonedAt(1);
        Assert.That(stored.IsEnabled, Is.False, "Precondition: the stored row is disabled.");

        var lattice = LatticeWhoseReadReturns(new Dictionary<string, byte[]>(), stored);

        await lattice.EnableManyAsync([Key], Replica);

        Assert.That(stored.IsEnabled, Is.True,
            "An enable minted while the batched read omitted the row must still take effect; " +
            "otherwise the write reports success and the flag stays disabled forever.");
    }

    /// <summary>
    /// The same hole at a higher tombstone counter. A read-derived mint always
    /// produces counter 1 for an unreturned row, so the deeper the row's dot
    /// history, the more certainly the enable is dead on arrival.
    /// </summary>
    [Test]
    public async Task EnableManyBatch_enables_a_row_whose_tombstone_sits_far_above_a_read_derived_dot()
    {
        var stored = DisabledRowTombstonedAt(5_000);
        var lattice = LatticeWhoseReadReturns(new Dictionary<string, byte[]>(), stored);

        await lattice.EnableManyAsync([Key], Replica);

        Assert.That(stored.IsEnabled, Is.True,
            "The minted dot must not be coverable by a pre-existing tombstone at any counter.");
    }

    /// <summary>
    /// The staging helper mints from the same snapshot and so carries the same
    /// defect. It folds the delta into the token itself, so the token can be
    /// inspected directly.
    /// </summary>
    [Test]
    public async Task StageEnableManyBatch_stages_a_live_enable_for_a_row_the_read_did_not_return()
    {
        var stored = DisabledRowTombstonedAt(1);
        var lattice = LatticeWhoseReadReturns(new Dictionary<string, byte[]>(), stored);

        var staged = await lattice.StageEnableManyAsync([Key], Replica);
        stored.MergeDelta(
            JsonLatticeSerializer<OrFlagDelta>.Default.Deserialize(staged[0].Delta!));

        Assert.That(stored.IsEnabled, Is.True,
            "A staged enable must also be minted fresh, or the atomic write commits a dead dot.");
    }

    /// <summary>
    /// The regression guard on the other side of the fix: a key that genuinely
    /// has no row must still enable. This is the case the read-derived mint was
    /// written for, and it must keep working.
    /// </summary>
    [Test]
    public async Task EnableManyBatch_still_enables_a_row_that_genuinely_does_not_exist()
    {
        var stored = new OrFlag();
        var lattice = LatticeWhoseReadReturns(new Dictionary<string, byte[]>(), stored);

        await lattice.EnableManyAsync([Key], Replica);

        Assert.That(stored.IsEnabled, Is.True,
            "A first-ever enable on an absent row must still take effect.");
    }

    /// <summary>
    /// And the ordinary path: when the read does return the row, the enable must
    /// take effect exactly as before.
    /// </summary>
    [Test]
    public async Task EnableManyBatch_enables_a_tombstoned_row_the_batched_read_did_return()
    {
        var stored = DisabledRowTombstonedAt(3);
        var visible = new Dictionary<string, byte[]>
        {
            [Key] = JsonLatticeSerializer<OrFlag>.Default.Serialize(stored),
        };
        var lattice = LatticeWhoseReadReturns(visible, stored);

        await lattice.EnableManyAsync([Key], Replica);

        Assert.That(stored.IsEnabled, Is.True,
            "A complete read must keep enabling the row, so the fix is not a behaviour change there.");
    }

    /// <summary>
    /// Re-enabling twice in quick succession must not mint the same dot twice,
    /// or a disable interleaved between them would cancel the second enable as
    /// well. Pins strict monotonicity rather than a wall-clock read.
    /// </summary>
    [Test]
    public async Task EnableManyBatch_mints_a_distinct_dot_on_each_call_for_the_same_replica()
    {
        var stored = new OrFlag();
        var lattice = LatticeWhoseReadReturns(new Dictionary<string, byte[]>(), stored);

        await lattice.EnableManyAsync([Key], Replica);
        var first = stored.Enables.Single().Counter;

        stored.Disable();
        Assert.That(stored.IsEnabled, Is.False, "Precondition: disabled between the two enables.");

        await lattice.EnableManyAsync([Key], Replica);

        Assert.That(stored.IsEnabled, Is.True,
            "The second enable must out-rank the tombstone the disable just wrote.");
        Assert.That(stored.Enables.Single().Counter, Is.GreaterThan(first),
            "Successive mints for one replica must strictly increase.");
    }
}
