using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Deterministic tests for <see cref="MovedAwaySealInheritance"/>, the pure rule
/// deciding what moved-away seal a leaf must carry once another leaf's seal is
/// seeded onto it at birth.
/// <para>
/// The defect these cover (issue 3121) is <b>structural, not a race</b>: a sibling
/// minted from a sealed donor was born unsealed and served the migrated orphans the
/// seal exists to suppress, on every ordering rather than on an unlucky one. That is
/// why the primary instrument here is a deterministic unit test rather than a chaos
/// arm - the concurrency dimension is covered separately by
/// <c>MovedAwaySealInheritanceCoyoteTests</c>, which proves only that no ordering
/// escapes the rule.
/// </para>
/// </summary>
[TestFixture]
public sealed class MovedAwaySealInheritanceTests
{
    /// <summary>
    /// The commonest call by far: a donor that holds no seal has nothing to pass on,
    /// so the receiver is left exactly as it was and the caller is told not to
    /// persist. Every split of an unsealed leaf takes this path, so it is also the
    /// one that must not allocate.
    /// </summary>
    [Test]
    public void An_unsealed_donor_leaves_the_receiver_untouched()
    {
        var existing = new[] { 4, 9 };

        var inherited = MovedAwaySealInheritance.TryInherit(
            existing, 16, null, null, out var slots, out var vsc);

        Assert.Multiple(() =>
        {
            Assert.That(inherited, Is.False, "an unsealed donor has nothing to inherit");
            Assert.That(slots, Is.SameAs(existing), "the receiver's own seal must survive verbatim");
            Assert.That(vsc, Is.EqualTo(16));
        });
    }

    /// <summary>
    /// A virtual shard count with no slots is the stamp left behind when a seal is
    /// lifted, not a seal. Propagating it would record a slot space on a sibling that
    /// holds no sealed slot at all, which is harmless today but would make a later
    /// genuine inheritance under a different count look like an incomparable-space
    /// conflict and be declined. It must stay inert.
    /// </summary>
    [Test]
    public void A_lifted_seal_stamp_is_not_propagated_as_a_seal()
    {
        var inherited = MovedAwaySealInheritance.TryInherit(
            null, null, Array.Empty<int>(), 16, out var slots, out var vsc);

        Assert.Multiple(() =>
        {
            Assert.That(inherited, Is.False, "an empty slot set is a lifted-seal stamp, not a seal");
            Assert.That(slots, Is.Null);
            Assert.That(vsc, Is.Null, "the bare count must not be recorded on the receiver");
        });
    }

    /// <summary>
    /// Slots without the count they were recorded under are meaningless, because a
    /// slot index only identifies a residue class relative to its own virtual shard
    /// count. Such a donor is treated as holding nothing rather than having its
    /// indices reinterpreted under whatever count the receiver happens to hold.
    /// </summary>
    [Test]
    public void A_donor_whose_slots_carry_no_virtual_shard_count_is_ignored()
    {
        var inherited = MovedAwaySealInheritance.TryInherit(
            null, null, new[] { 3 }, null, out var slots, out var vsc);

        Assert.Multiple(() =>
        {
            Assert.That(inherited, Is.False);
            Assert.That(slots, Is.Null);
            Assert.That(vsc, Is.Null);
        });
    }

    /// <summary>
    /// A non-positive count cannot describe any slot space, so it is rejected on the
    /// same reasoning as a missing one rather than being allowed to reach the
    /// comparison below and match some other corrupt value.
    /// </summary>
    [Test]
    public void A_donor_with_a_non_positive_virtual_shard_count_is_ignored(
        [Values(0, -1)] int count)
    {
        var inherited = MovedAwaySealInheritance.TryInherit(
            null, null, new[] { 3 }, count, out var slots, out _);

        Assert.Multiple(() =>
        {
            Assert.That(inherited, Is.False);
            Assert.That(slots, Is.Null);
        });
    }

    /// <summary>
    /// The universal production case - a freshly minted sibling holds no seal, so it
    /// takes the donor's verbatim. This is the path that actually closes issue 3121.
    /// </summary>
    [Test]
    public void A_receiver_with_no_seal_takes_the_donor_seal_whole(
        [Values(null, new int[0])] int[]? receiverSlots)
    {
        var donor = new[] { 2, 7, 11 };

        var inherited = MovedAwaySealInheritance.TryInherit(
            receiverSlots, null, donor, 16, out var slots, out var vsc);

        Assert.Multiple(() =>
        {
            Assert.That(inherited, Is.True);
            Assert.That(slots, Is.EqualTo(new[] { 2, 7, 11 }));
            Assert.That(vsc, Is.EqualTo(16));
        });
    }

    /// <summary>
    /// The allocation contract on the universal path, asserted by reference identity
    /// rather than by value. Every split of a sealed donor runs this path, so the core
    /// hands back the donor's own array instead of copying it, which is what keeps it
    /// a pure, allocation-free decision.
    /// <para>
    /// Whether that reference may be <em>retained</em> is the caller's question.
    /// <c>BPlusLeafGrain.InitializeSiblingAsync</c> copies before persisting, because
    /// the array reaches it inside an <c>[Immutable]</c> payload whose same-silo deep
    /// copy is elided; that behaviour is pinned separately by
    /// <c>Split_gives_the_sibling_its_own_copy_of_the_donor_seal_array</c>. A caller
    /// that only reads the result needs no copy, which is precisely why the decision
    /// is left here rather than forced on every caller.
    /// </para>
    /// </summary>
    [Test]
    public void The_donor_array_is_shared_rather_than_copied_on_the_universal_path()
    {
        var donor = new[] { 2, 7, 11 };

        MovedAwaySealInheritance.TryInherit(null, null, donor, 16, out var slots, out _);

        Assert.That(slots, Is.SameAs(donor), "the birth seam must not allocate a copy of the donor's seal");
    }

    /// <summary>
    /// Two seals recorded under different virtual shard counts describe different
    /// slot spaces, so neither merging nor overwriting is correct: merging blends the
    /// spaces and overwriting drops live seals. Declining leaves the receiver's own
    /// seal intact, which is the only answer that cannot lose information.
    /// </summary>
    [Test]
    public void Incomparable_slot_spaces_leave_the_receiver_seal_intact()
    {
        var existing = new[] { 5 };

        var inherited = MovedAwaySealInheritance.TryInherit(
            existing, 32, new[] { 3 }, 16, out var slots, out var vsc);

        Assert.Multiple(() =>
        {
            Assert.That(inherited, Is.False);
            Assert.That(slots, Is.SameAs(existing), "the receiver's live seal must not be dropped");
            Assert.That(vsc, Is.EqualTo(32), "nor may its slot space be rewritten");
        });
    }

    /// <summary>
    /// A receiver holding slots but no count is a corrupt pairing. It is treated as
    /// incomparable rather than as an empty seal, so the fix cannot convert corrupt
    /// state into a confident overwrite.
    /// </summary>
    [Test]
    public void A_receiver_whose_slots_carry_no_count_is_treated_as_incomparable()
    {
        var existing = new[] { 5 };

        var inherited = MovedAwaySealInheritance.TryInherit(
            existing, null, new[] { 3 }, 16, out var slots, out _);

        Assert.Multiple(() =>
        {
            Assert.That(inherited, Is.False);
            Assert.That(slots, Is.SameAs(existing));
        });
    }

    /// <summary>
    /// Idempotence, which is what makes a recovery-path re-call against a
    /// partially-seeded sibling free as well as correct: when every donor slot is
    /// already present there is no change to persist, so the caller is told so and
    /// nothing is allocated.
    /// </summary>
    [Test]
    public void A_donor_seal_already_wholly_present_reports_no_change()
    {
        var existing = new[] { 2, 7, 11 };

        var inherited = MovedAwaySealInheritance.TryInherit(
            existing, 16, new[] { 2, 11 }, 16, out var slots, out var vsc);

        Assert.Multiple(() =>
        {
            Assert.That(inherited, Is.False, "a re-call must not force a redundant persist");
            Assert.That(slots, Is.SameAs(existing), "and must not allocate a copy");
            Assert.That(vsc, Is.EqualTo(16));
        });
    }

    /// <summary>
    /// The genuine merge. Both sides hold a seal under the same count, so the result
    /// is their union - sorted, distinct, and exactly sized. A union is used rather
    /// than an assignment because a seal is sticky: dropping either side's slot would
    /// resurface an orphan the other side had already hidden.
    /// </summary>
    [Test]
    public void Two_comparable_seals_merge_into_their_sorted_union()
    {
        var inherited = MovedAwaySealInheritance.TryInherit(
            new[] { 1, 6, 9 }, 16, new[] { 3, 6, 12 }, 16, out var slots, out var vsc);

        Assert.Multiple(() =>
        {
            Assert.That(inherited, Is.True);
            Assert.That(slots, Is.EqualTo(new[] { 1, 3, 6, 9, 12 }));
            Assert.That(vsc, Is.EqualTo(16));
        });
    }

    /// <summary>
    /// Monotonicity stated directly as the property, over every interleaving of two
    /// small seals rather than over one hand-picked pair. Whatever the merge returns,
    /// it contains every slot either side held - a seal can only ever grow.
    /// </summary>
    [Test]
    [Pairwise]
    public void A_merge_never_drops_a_slot_from_either_side(
        [Values(0, 1, 5)] int existingLow,
        [Values(6, 9, 14)] int existingHigh,
        [Values(0, 3, 9)] int donorLow,
        [Values(9, 12, 15)] int donorHigh)
    {
        var existing = existingLow == existingHigh
            ? new[] { existingLow }
            : new[] { existingLow, existingHigh };
        var donor = donorLow == donorHigh
            ? new[] { donorLow }
            : new[] { donorLow, donorHigh };

        var inherited = MovedAwaySealInheritance.TryInherit(
            existing, 16, donor, 16, out var slots, out _);

        var result = slots ?? Array.Empty<int>();

        Assert.Multiple(() =>
        {
            Assert.That(result, Is.SupersetOf(existing), "the receiver's own seal must survive");
            Assert.That(result, Is.SupersetOf(donor), "and the donor's must be absorbed");
            Assert.That(result, Is.Ordered.Ascending, "the persisted array must stay searchable");
            Assert.That(result, Is.Unique, "and must not accumulate duplicates");
            Assert.That(
                inherited,
                Is.EqualTo(result.Length > existing.Length),
                "a change must be reported exactly when one occurred");
        });
    }

    /// <summary>
    /// The counting pre-pass sizes the result array exactly, so an input carrying
    /// adjacent duplicates would over-count and leave trailing zeros - which read as
    /// a sealed slot 0 and would hide live rows at random. Production keeps its
    /// arrays distinct, so this is defence for a precondition rather than a scenario;
    /// it is asserted because the failure mode is silent data suppression rather than
    /// an exception.
    /// </summary>
    [Test]
    public void A_donor_carrying_duplicate_slots_does_not_corrupt_the_union()
    {
        var inherited = MovedAwaySealInheritance.TryInherit(
            new[] { 4 }, 16, new[] { 3, 3, 8, 8 }, 16, out var slots, out _);

        Assert.Multiple(() =>
        {
            Assert.That(inherited, Is.True);
            Assert.That(slots, Is.EqualTo(new[] { 3, 4, 8 }), "no phantom slot 0 may appear");
        });
    }
}
