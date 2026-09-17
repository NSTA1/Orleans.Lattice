using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;
using System.Text;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Birth-time inheritance of the moved-away seal by a split sibling (issue 3121).
/// <para>
/// A leaf split moves every committed row at or above the split key onto a freshly
/// minted sibling, unfiltered. The moved-away seal that stops the donor resurfacing
/// an orphan snapshot of a migrated slot did not travel with those rows, so the
/// sibling was born unsealed while holding them and served them through every read
/// path. The seal is keyed by the key's HASH rather than by the leaf's declared
/// range, so a sealed slot is a residue class scattered across the whole keyspace:
/// any non-trivial division of a sealed donor leaks, and there is no pivot that
/// avoids it.
/// </para>
/// <para>
/// The orphan never heals, which is what makes this a correctness defect rather than
/// a staleness window. The authoritative value lives on the destination shard and
/// writes for that slot route there, so nothing will ever correct the copy the
/// sibling holds.
/// </para>
/// <para>
/// These tests are deliberately split into two independently diagnosable halves. The
/// <b>populate</b> half pins that the donor puts its seal on the wire, asserted on the
/// captured <see cref="SiblingInitialization"/>; the <b>apply</b> half pins that a
/// leaf receiving such an initialization honours it on reads. A failure in either half
/// names which side broke without the other having to be reasoned about.
/// </para>
/// </summary>
public partial class BPlusLeafGrainTests
{
    /// <summary>
    /// Every <see cref="SiblingInitialization"/> the donor seeded onto the sibling.
    /// </summary>
    private static List<SiblingInitialization> SiblingInitializationsOn(IBPlusLeafGrain sibling)
        => sibling.ReceivedCalls()
            .Where(c => c.GetMethodInfo().Name == nameof(IBPlusLeafGrain.InitializeSiblingAsync))
            .Select(c => (SiblingInitialization)c.GetArguments()[0]!)
            .ToList();

    // --- populate: the donor puts its seal on the wire ---

    /// <summary>
    /// The regression proper. A donor holding a seal divides, and the sibling's
    /// initialization must carry that seal - otherwise the sibling receives the
    /// migrated rows with nothing to suppress them.
    /// </summary>
    [Test]
    public async Task Split_seeds_the_sibling_with_the_donor_moved_away_seal()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var sibling = CreateSplitSiblingStub();
        var grain = CreateGrain(state, siblingStub: sibling);

        await grain.SetAsync("a", Encoding.UTF8.GetBytes("1"));
        await grain.SetAsync("z", Encoding.UTF8.GetBytes("2"));

        var movedSlot = ShardMap.GetVirtualSlot("z", 16);
        await grain.MarkSlotsMovedAwayAsync(new[] { movedSlot }, 16);

        ArmInterruptedSplit(state, sibling, splitKey: "m");
        await grain.SetAsync("b", Encoding.UTF8.GetBytes("3"));

        var inits = SiblingInitializationsOn(sibling);
        Assert.That(inits, Has.Count.EqualTo(1), "the sibling is seeded exactly once");
        Assert.Multiple(() =>
        {
            Assert.That(
                inits[0].MovedAwaySlots,
                Is.EqualTo(new[] { movedSlot }),
                "the sibling receives the rows for this slot, so it must receive the seal too");
            Assert.That(inits[0].MovedAwayVirtualShardCount, Is.EqualTo(16),
                "a slot index is meaningless without the count it was recorded under");
        });
    }

    /// <summary>
    /// The seal is copied, not moved. The donor may still hold rows in the sealed slot
    /// below the pivot, so stripping its own seal while seeding the sibling would
    /// simply relocate the defect onto the other half.
    /// </summary>
    [Test]
    public async Task Split_leaves_the_donor_seal_intact_after_seeding_the_sibling()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var sibling = CreateSplitSiblingStub();
        var grain = CreateGrain(state, siblingStub: sibling);

        await grain.SetAsync("a", Encoding.UTF8.GetBytes("1"));
        await grain.SetAsync("z", Encoding.UTF8.GetBytes("2"));

        var movedSlot = ShardMap.GetVirtualSlot("z", 16);
        await grain.MarkSlotsMovedAwayAsync(new[] { movedSlot }, 16);

        ArmInterruptedSplit(state, sibling, splitKey: "m");
        await grain.SetAsync("b", Encoding.UTF8.GetBytes("3"));

        Assert.Multiple(() =>
        {
            Assert.That(state.State.MovedAwaySlots, Is.EqualTo(new[] { movedSlot }));
            Assert.That(state.State.MovedAwayVirtualShardCount, Is.EqualTo(16));
        });
    }

    /// <summary>
    /// The seal must be armed before any migrated row becomes visible on the sibling,
    /// which is the same ordering rationale the split already applies to shadow
    /// markers. A seeding step that landed after the row transfer would leave a window
    /// in which the sibling serves exactly the orphans this fix exists to hide.
    /// </summary>
    [Test]
    public async Task Split_arms_the_sibling_seal_before_transferring_any_row()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var sibling = CreateSplitSiblingStub();
        var grain = CreateGrain(state, siblingStub: sibling);

        await grain.SetAsync("a", Encoding.UTF8.GetBytes("1"));
        await grain.SetAsync("z", Encoding.UTF8.GetBytes("2"));
        await grain.MarkSlotsMovedAwayAsync(new[] { ShardMap.GetVirtualSlot("z", 16) }, 16);

        ArmInterruptedSplit(state, sibling, splitKey: "m");
        await grain.SetAsync("b", Encoding.UTF8.GetBytes("3"));

        var order = sibling.ReceivedCalls()
            .Select(c => c.GetMethodInfo().Name)
            .ToList();

        var initAt = order.IndexOf(nameof(IBPlusLeafGrain.InitializeSiblingAsync));
        var mergeAt = order.IndexOf(nameof(IBPlusLeafGrain.MergeEntriesAsync));

        Assert.Multiple(() =>
        {
            Assert.That(initAt, Is.GreaterThanOrEqualTo(0), "the sibling must be initialized");
            Assert.That(mergeAt, Is.GreaterThanOrEqualTo(0), "rows must be transferred");
            Assert.That(initAt, Is.LessThan(mergeAt),
                "the seal must be armed before the migrated rows become visible");
        });
    }

    /// <summary>
    /// The no-regression half of the populate side. An unsealed donor must seed nothing
    /// - not an empty array, and not a bare virtual shard count, either of which would
    /// stamp a slot space onto a leaf that holds no sealed slot at all.
    /// </summary>
    [Test]
    public async Task Split_seeds_no_seal_when_the_donor_holds_none()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var sibling = CreateSplitSiblingStub();
        var grain = CreateGrain(state, siblingStub: sibling);

        await grain.SetAsync("a", Encoding.UTF8.GetBytes("1"));
        await grain.SetAsync("z", Encoding.UTF8.GetBytes("2"));

        ArmInterruptedSplit(state, sibling, splitKey: "m");
        await grain.SetAsync("b", Encoding.UTF8.GetBytes("3"));

        var inits = SiblingInitializationsOn(sibling);
        Assert.That(inits, Has.Count.EqualTo(1));
        Assert.Multiple(() =>
        {
            Assert.That(inits[0].MovedAwaySlots, Is.Null);
            Assert.That(inits[0].MovedAwayVirtualShardCount, Is.Null);
        });
    }

    // --- apply: a leaf honours the seal it was seeded with ---

    /// <summary>
    /// The sibling must not retain the donor's own array. <see cref="SiblingInitialization"/>
    /// is <c>[Immutable]</c>, so a co-located donor's payload is handed over without a
    /// deep copy; persisting that instance would alias two leaves' durable state to one
    /// array, and a later in-place edit on either side - or a future writer that stops
    /// replacing the array - would silently reseal or unseal the other leaf.
    /// <para>
    /// This is the invariant <c>ImmutableGrainBoundaryContractTests</c> enforces
    /// generally; pinned here on the specific seam so a regression names the cause
    /// rather than only the rule.
    /// </para>
    /// </summary>
    [Test]
    public async Task Split_gives_the_sibling_its_own_copy_of_the_donor_seal_array()
    {
        var donorState = new FakePersistentState<LeafNodeState>();
        var sibling = CreateSplitSiblingStub();
        var donor = CreateGrain(donorState, siblingStub: sibling);

        await donor.SetAsync("a", Encoding.UTF8.GetBytes("1"));
        await donor.SetAsync("z", Encoding.UTF8.GetBytes("2"));
        await donor.MarkSlotsMovedAwayAsync(new[] { ShardMap.GetVirtualSlot("z", 16) }, 16);

        ArmInterruptedSplit(donorState, sibling, splitKey: "m");
        await donor.SetAsync("b", Encoding.UTF8.GetBytes("3"));

        var init = SiblingInitializationsOn(sibling).Single();

        // Feed the captured wire payload to a real leaf, exactly as a co-located
        // sibling would receive it: the same array instance, no deep copy.
        var siblingState = new FakePersistentState<LeafNodeState>();
        var realSibling = CreateGrain(siblingState, replicaId: "sibling-leaf");
        await realSibling.InitializeSiblingAsync(init);

        Assert.Multiple(() =>
        {
            Assert.That(siblingState.State.MovedAwaySlots, Is.EqualTo(init.MovedAwaySlots),
                "the sibling must end up sealed for the same slots");
            Assert.That(siblingState.State.MovedAwaySlots, Is.Not.SameAs(init.MovedAwaySlots),
                "but must not retain the donor's own array in its durable state");
            Assert.That(siblingState.State.MovedAwaySlots, Is.Not.SameAs(donorState.State.MovedAwaySlots),
                "nor, transitively, the donor's persisted array");
        });
    }

    /// <summary>
    /// The other half of the fix, and the one the user-visible symptom lives in: a leaf
    /// initialized with a moved-away seal must refuse every read of a key in a sealed
    /// slot, exactly as if it had recorded the seal itself.
    /// </summary>
    [Test]
    public async Task Initialize_sibling_applies_the_seeded_seal_to_reads()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);

        await grain.SetAsync("k1", Encoding.UTF8.GetBytes("orphan"));
        Assert.That(await grain.GetAsync("k1"), Is.Not.Null, "precondition: the row is served");

        await grain.InitializeSiblingAsync(new SiblingInitialization
        {
            TreeId = "test-tree",
            ShardIndex = 0,
            LowKeyInclusive = "k1",
            HighKeyExclusive = null,
            MovedAwaySlots = new[] { ShardMap.GetVirtualSlot("k1", 16) },
            MovedAwayVirtualShardCount = 16,
        });

        Assert.Multiple(async () =>
        {
            Assert.That(await grain.GetAsync("k1"), Is.Null, "a migrated slot must not be served");
            Assert.That(await grain.ExistsAsync("k1"), Is.False);
            Assert.That(state.State.MovedAwayVirtualShardCount, Is.EqualTo(16));
        });
    }

    /// <summary>
    /// Inheritance is a union rather than an assignment, because a seal is sticky: a
    /// leaf that has already recorded a migration of its own must not have it dropped
    /// by the seeding step, which would resurface the orphan the leaf had already
    /// hidden.
    /// </summary>
    [Test]
    public async Task Initialize_sibling_unions_the_seeded_seal_with_an_existing_one()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);

        var own = ShardMap.GetVirtualSlot("k1", 16);
        var seeded = ShardMap.GetVirtualSlot("k2", 16);
        if (own == seeded)
        {
            Assert.Ignore("k1 and k2 share a slot under this hash; the union is untestable here");
        }

        await grain.SetAsync("k1", Encoding.UTF8.GetBytes("1"));
        await grain.SetAsync("k2", Encoding.UTF8.GetBytes("2"));
        await grain.MarkSlotsMovedAwayAsync(new[] { own }, 16);

        await grain.InitializeSiblingAsync(new SiblingInitialization
        {
            TreeId = "test-tree",
            ShardIndex = 0,
            LowKeyInclusive = "k1",
            HighKeyExclusive = null,
            MovedAwaySlots = new[] { seeded },
            MovedAwayVirtualShardCount = 16,
        });

        Assert.Multiple(async () =>
        {
            Assert.That(await grain.GetAsync("k1"), Is.Null, "the leaf's own seal must survive");
            Assert.That(await grain.GetAsync("k2"), Is.Null, "and the seeded one must take effect");
            Assert.That(state.State.MovedAwaySlots, Is.EqualTo(new[] { own, seeded }.Order().ToArray()));
        });
    }

    /// <summary>
    /// Two seals recorded under different virtual shard counts describe different slot
    /// spaces, so there is no correct union to take. Declining leaves the leaf's own
    /// seal intact, which is the only answer that cannot lose information; an
    /// overwrite would silently drop a live seal.
    /// </summary>
    [Test]
    public async Task Initialize_sibling_declines_a_seal_from_an_incomparable_slot_space()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);

        var own = ShardMap.GetVirtualSlot("k1", 32);
        await grain.SetAsync("k1", Encoding.UTF8.GetBytes("1"));
        await grain.MarkSlotsMovedAwayAsync(new[] { own }, 32);

        await grain.InitializeSiblingAsync(new SiblingInitialization
        {
            TreeId = "test-tree",
            ShardIndex = 0,
            LowKeyInclusive = "k1",
            HighKeyExclusive = null,
            MovedAwaySlots = new[] { ShardMap.GetVirtualSlot("k2", 16) },
            MovedAwayVirtualShardCount = 16,
        });

        Assert.Multiple(() =>
        {
            Assert.That(state.State.MovedAwaySlots, Is.EqualTo(new[] { own }));
            Assert.That(state.State.MovedAwayVirtualShardCount, Is.EqualTo(32),
                "the leaf's own slot space must not be rewritten");
        });
    }

    /// <summary>
    /// The recovery path can re-seed a partially initialized sibling, so a repeated
    /// initialization must be a no-op rather than accumulating duplicate slots.
    /// </summary>
    [Test]
    public async Task Initialize_sibling_is_idempotent_for_the_seeded_seal()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);
        var slot = ShardMap.GetVirtualSlot("k1", 16);

        var init = new SiblingInitialization
        {
            TreeId = "test-tree",
            ShardIndex = 0,
            LowKeyInclusive = "k1",
            HighKeyExclusive = null,
            MovedAwaySlots = new[] { slot },
            MovedAwayVirtualShardCount = 16,
        };

        await grain.InitializeSiblingAsync(init);
        await grain.InitializeSiblingAsync(init);

        Assert.Multiple(() =>
        {
            Assert.That(state.State.MovedAwaySlots, Is.EqualTo(new[] { slot }));
            Assert.That(state.State.MovedAwayVirtualShardCount, Is.EqualTo(16));
        });
    }

    /// <summary>
    /// The no-regression half of the apply side, and the one that matters most: a leaf
    /// seeded with no seal must keep serving everything it holds. An over-defensive
    /// change that stamped an empty seal or a bare count would hide live rows rather
    /// than orphans, which is a strictly worse failure than the one being fixed.
    /// </summary>
    [Test]
    public async Task Initialize_sibling_without_a_seal_still_serves_every_row()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);

        await grain.SetAsync("k1", Encoding.UTF8.GetBytes("1"));

        await grain.InitializeSiblingAsync(new SiblingInitialization
        {
            TreeId = "test-tree",
            ShardIndex = 0,
            LowKeyInclusive = "k1",
            HighKeyExclusive = null,
        });

        Assert.Multiple(async () =>
        {
            Assert.That(await grain.GetAsync("k1"), Is.Not.Null);
            Assert.That(state.State.MovedAwaySlots, Is.Null);
            Assert.That(state.State.MovedAwayVirtualShardCount, Is.Null);
        });
    }

    /// <summary>
    /// The seal joins the existing all-or-nothing revert: if the single persist that
    /// covers the whole birth batch throws, every seeded slot must go back to what it
    /// was, the seal included.
    /// <para>
    /// Leaving a rolled-back seal applied in memory would be the worse half of the
    /// bargain. The durable state says unsealed, so a reactivation serves the rows
    /// again, but until then the live activation hides them - a leaf that answers
    /// differently before and after a restart, with no write anywhere to explain it.
    /// </para>
    /// </summary>
    [Test]
    public async Task Initialize_sibling_reverts_the_seal_when_the_persist_throws()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);

        await grain.SetAsync("k1", Encoding.UTF8.GetBytes("1"));
        var slot = ShardMap.GetVirtualSlot("k1", 16);

        // One-shot: the fake clears it after throwing, so the read path below is
        // exercised against a leaf that is merely un-persisted, not broken.
        state.ThrowOnWrite = new InvalidOperationException("persist failed");

        var init = new SiblingInitialization
        {
            TreeId = "test-tree",
            ShardIndex = 0,
            LowKeyInclusive = "k1",
            HighKeyExclusive = null,
            MovedAwaySlots = new[] { slot },
            MovedAwayVirtualShardCount = 16,
        };

        Assert.That(
            async () => await grain.InitializeSiblingAsync(init),
            Throws.InstanceOf<InvalidOperationException>(),
            "the persist failure must surface rather than being swallowed");

        Assert.Multiple(async () =>
        {
            Assert.That(state.State.MovedAwaySlots, Is.Null,
                "the seal must be rolled back with the rest of the batch");
            Assert.That(state.State.MovedAwayVirtualShardCount, Is.Null);
            Assert.That(await grain.GetAsync("k1"), Is.Not.Null,
                "and the in-memory leaf must not be left hiding rows its durable state still serves");
        });
    }
}
