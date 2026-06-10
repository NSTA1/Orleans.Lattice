using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Unit tests for the internal-node digest-aggregation surface
/// (<see cref="BPlusInternalGrain.OnChildDigestPublishedAsync"/>,
/// <see cref="BPlusInternalGrain.GetSubtreeProjectionDigestAsync"/>,
/// <see cref="BPlusInternalGrain.GetChildDigestSnapshotAsync"/>,
/// <see cref="BPlusInternalGrain.SetParentAsync"/>). Validates the
/// XOR-fold algebra, max-reduced checkpoint offset, persistence,
/// upward propagation, and the empty / legacy-state shapes.
/// </summary>
public partial class BPlusInternalGrainTests
{
    private static readonly GrainId DigestChild0 = GrainId.Create("leaf", "digest-child-0");
    private static readonly GrainId DigestChild1 = GrainId.Create("leaf", "digest-child-1");
    private static readonly GrainId DigestParent = GrainId.Create("internal", "digest-parent");

    private static (BPlusInternalGrain Grain, FakePersistentState<InternalNodeState> State, IGrainFactory Factory)
        CreateDigestGrain(FakePersistentState<InternalNodeState>? state = null)
    {
        state ??= new FakePersistentState<InternalNodeState>();
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("internal", "digest-self"));
        var grainFactory = Substitute.For<IGrainFactory>();
        var optionsResolver = TestOptionsResolver.Create(factory: grainFactory);
        return (new BPlusInternalGrain(context, state, grainFactory, optionsResolver), state, grainFactory);
    }

    private static (BPlusInternalGrain Grain, FakePersistentState<InternalNodeState> State, IGrainFactory Factory)
        CreateDigestGrain(LatticeOptions baseOptions, FakePersistentState<InternalNodeState>? state = null)
    {
        state ??= new FakePersistentState<InternalNodeState>();
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("internal", "digest-self"));
        var grainFactory = Substitute.For<IGrainFactory>();
        var optionsResolver = TestOptionsResolver.Create(baseOptions: baseOptions, factory: grainFactory);
        return (new BPlusInternalGrain(context, state, grainFactory, optionsResolver), state, grainFactory);
    }

    private static byte[] Bytes16(byte fill)
    {
        var b = new byte[16];
        Array.Fill(b, fill);
        return b;
    }

    // --- GetSubtreeProjectionDigestAsync ---

    [Test]
    public async Task GetSubtreeProjectionDigestAsync_empty_node_returns_zero_aggregates()
    {
        var (grain, _, _) = CreateDigestGrain();

        var digest = await grain.GetSubtreeProjectionDigestAsync();

        Assert.That(digest.EntryCount, Is.Zero);
        Assert.That(digest.CheckpointOffset, Is.Zero);
        Assert.That(digest.Hash, Is.Not.Null);
        Assert.That(digest.Hash.Length, Is.EqualTo(16));
    }

    [Test]
    public async Task GetSubtreeProjectionDigestAsync_returns_same_bytes_across_repeated_calls()
    {
        var (grain, _, _) = CreateDigestGrain();
        await grain.OnChildDigestPublishedAsync(DigestChild0, new ChildDigestSnapshot
        {
            Hash = Bytes16(0xAB),
            EntryCount = 4,
            CheckpointOffset = 10,
        });

        var d1 = await grain.GetSubtreeProjectionDigestAsync();
        var d2 = await grain.GetSubtreeProjectionDigestAsync();

        Assert.That(d1.Hash, Is.EqualTo(d2.Hash));
        Assert.That(d1.EntryCount, Is.EqualTo(d2.EntryCount));
        Assert.That(d1.CheckpointOffset, Is.EqualTo(d2.CheckpointOffset));
    }

    // --- OnChildDigestPublishedAsync ---

    [Test]
    public async Task OnChildDigestPublishedAsync_folds_first_child_into_subtree_aggregate()
    {
        var (grain, state, _) = CreateDigestGrain();

        await grain.OnChildDigestPublishedAsync(DigestChild0, new ChildDigestSnapshot
        {
            Hash = Bytes16(0xAB),
            EntryCount = 7,
            CheckpointOffset = 42,
        });

        Assert.That(state.State.SubtreeProjectionHash, Is.EqualTo(Bytes16(0xAB)));
        Assert.That(state.State.SubtreeEntryCount, Is.EqualTo(7));
        Assert.That(state.State.SubtreeHighestCheckpointOffset, Is.EqualTo(42));
        Assert.That(state.State.ChildDigests.ContainsKey(DigestChild0), Is.True);
    }

    [Test]
    public async Task OnChildDigestPublishedAsync_two_children_xor_to_subtree_hash()
    {
        var (grain, state, _) = CreateDigestGrain();
        var a = Bytes16(0xAB);
        var b = Bytes16(0xCD);
        var expected = new byte[16];
        for (var i = 0; i < 16; i++) expected[i] = (byte)(a[i] ^ b[i]);

        await grain.OnChildDigestPublishedAsync(DigestChild0, new ChildDigestSnapshot
        {
            Hash = a, EntryCount = 3, CheckpointOffset = 100,
        });
        await grain.OnChildDigestPublishedAsync(DigestChild1, new ChildDigestSnapshot
        {
            Hash = b, EntryCount = 5, CheckpointOffset = 50,
        });

        Assert.That(state.State.SubtreeProjectionHash, Is.EqualTo(expected));
        Assert.That(state.State.SubtreeEntryCount, Is.EqualTo(8));
        // Max-reduced upward (not summed).
        Assert.That(state.State.SubtreeHighestCheckpointOffset, Is.EqualTo(100));
    }

    [Test]
    public async Task OnChildDigestPublishedAsync_republish_replaces_prior_contribution()
    {
        var (grain, state, _) = CreateDigestGrain();
        await grain.OnChildDigestPublishedAsync(DigestChild0, new ChildDigestSnapshot
        {
            Hash = Bytes16(0xAB), EntryCount = 3, CheckpointOffset = 10,
        });

        await grain.OnChildDigestPublishedAsync(DigestChild0, new ChildDigestSnapshot
        {
            Hash = Bytes16(0x99), EntryCount = 7, CheckpointOffset = 20,
        });

        // Prior contribution XOR'd out, new contribution XOR'd in: just 0x99.
        Assert.That(state.State.SubtreeProjectionHash, Is.EqualTo(Bytes16(0x99)));
        Assert.That(state.State.SubtreeEntryCount, Is.EqualTo(7));
        Assert.That(state.State.SubtreeHighestCheckpointOffset, Is.EqualTo(20));
    }

    [Test]
    public async Task OnChildDigestPublishedAsync_persists_state()
    {
        var (grain, state, _) = CreateDigestGrain();
        var writesBefore = state.WriteCount;

        await grain.OnChildDigestPublishedAsync(DigestChild0, new ChildDigestSnapshot
        {
            Hash = Bytes16(0x11), EntryCount = 1, CheckpointOffset = 1,
        });

        Assert.That(state.WriteCount, Is.GreaterThan(writesBefore));
    }

    [Test]
    public async Task OnChildDigestPublishedAsync_publishes_upward_when_parent_set()
    {
        var fakeState = new FakePersistentState<InternalNodeState>
        {
            State = { ParentId = DigestParent }
        };
        var (grain, _, factory) = CreateDigestGrain(fakeState);
        var parentStub = Substitute.For<IBPlusInternalGrain>();
        factory.GetGrain<IBPlusInternalGrain>(DigestParent).Returns(parentStub);

        await grain.OnChildDigestPublishedAsync(DigestChild0, new ChildDigestSnapshot
        {
            Hash = Bytes16(0x22), EntryCount = 2, CheckpointOffset = 5,
        });

        await parentStub.Received(1).OnChildDigestPublishedAsync(
            Arg.Any<GrainId>(),
            Arg.Any<ChildDigestSnapshot>());
    }

    [Test]
    public async Task OnChildDigestPublishedAsync_no_parent_skips_upward_publish()
    {
        var (grain, _, factory) = CreateDigestGrain();
        var parentStub = Substitute.For<IBPlusInternalGrain>();
        factory.GetGrain<IBPlusInternalGrain>(Arg.Any<GrainId>()).Returns(parentStub);

        await grain.OnChildDigestPublishedAsync(DigestChild0, new ChildDigestSnapshot
        {
            Hash = Bytes16(0x33), EntryCount = 1, CheckpointOffset = 1,
        });

        await parentStub.DidNotReceive().OnChildDigestPublishedAsync(
            Arg.Any<GrainId>(),
            Arg.Any<ChildDigestSnapshot>());
    }

    [Test]
    public async Task OnChildDigestPublishedAsync_null_child_hash_is_treated_as_zero()
    {
        var (grain, state, _) = CreateDigestGrain();

        await grain.OnChildDigestPublishedAsync(DigestChild0, new ChildDigestSnapshot
        {
            Hash = null, EntryCount = 0, CheckpointOffset = 0,
        });

        Assert.That(state.State.SubtreeProjectionHash, Is.EqualTo(new byte[16]));
        Assert.That(state.State.SubtreeEntryCount, Is.Zero);
    }

    [Test]
    public async Task OnChildDigestPublishedAsync_snapshot_cloned_into_state()
    {
        // The persisted ChildDigests slot must hold a snapshot that
        // is unaffected by mutations to the caller's hash buffer
        // after the publish call returns. The producer-side
        // PublishCurrentDigestAsync clones, but this assertion guards
        // the contract that the internal node treats the snapshot as
        // immutable storage.
        var (grain, state, _) = CreateDigestGrain();
        var mutable = Bytes16(0xAB);

        await grain.OnChildDigestPublishedAsync(DigestChild0, new ChildDigestSnapshot
        {
            Hash = mutable, EntryCount = 1, CheckpointOffset = 1,
        });

        // The state's snapshot Hash reference is the same byte[] passed in
        // (no clone in the aggregator), but the running SubtreeProjectionHash
        // is a separate buffer. Mutating `mutable` after the call must not
        // affect SubtreeProjectionHash.
        var snapshotBefore = (byte[])state.State.SubtreeProjectionHash!.Clone();
        mutable[0] = 0x00;
        Assert.That(state.State.SubtreeProjectionHash, Is.EqualTo(snapshotBefore),
            "running subtree hash must not retroactively follow caller's buffer mutations");
    }

    // --- GetChildDigestSnapshotAsync ---

    [Test]
    public async Task GetChildDigestSnapshotAsync_returns_current_aggregates()
    {
        var (grain, _, _) = CreateDigestGrain();
        await grain.OnChildDigestPublishedAsync(DigestChild0, new ChildDigestSnapshot
        {
            Hash = Bytes16(0x44), EntryCount = 3, CheckpointOffset = 99,
        });

        var snapshot = await grain.GetChildDigestSnapshotAsync();

        Assert.That(snapshot.Hash, Is.EqualTo(Bytes16(0x44)));
        Assert.That(snapshot.EntryCount, Is.EqualTo(3));
        Assert.That(snapshot.CheckpointOffset, Is.EqualTo(99));
    }

    [Test]
    public async Task GetChildDigestSnapshotAsync_returns_independent_clone()
    {
        var (grain, state, _) = CreateDigestGrain();
        await grain.OnChildDigestPublishedAsync(DigestChild0, new ChildDigestSnapshot
        {
            Hash = Bytes16(0x55), EntryCount = 1, CheckpointOffset = 1,
        });

        var snapshot = await grain.GetChildDigestSnapshotAsync();
        snapshot.Hash![0] = 0x00;

        Assert.That(state.State.SubtreeProjectionHash![0], Is.EqualTo((byte)0x55),
            "GetChildDigestSnapshotAsync must clone so callers cannot retroactively mutate state");
    }

    // --- SetParentAsync ---

    [Test]
    public async Task SetParentAsync_persists_parent_id()
    {
        var (grain, state, _) = CreateDigestGrain();

        await grain.SetParentAsync(DigestParent);

        Assert.That(state.State.ParentId, Is.EqualTo(DigestParent));
    }

    [Test]
    public async Task SetParentAsync_does_not_callback_into_new_parent()
    {
        // The internal-grain seeding contract is pull-based: SetParentAsync
        // is a pure persist with no reentrant callback into the new parent.
        // The parent pulls the child's snapshot via GetChildDigestSnapshotAsync
        // immediately afterward, which keeps the chain consistent without
        // deadlocking the non-reentrant internal grain.
        var (grain, _, factory) = CreateDigestGrain();
        var parentStub = Substitute.For<IBPlusInternalGrain>();
        factory.GetGrain<IBPlusInternalGrain>(DigestParent).Returns(parentStub);

        await grain.SetParentAsync(DigestParent);

        await parentStub.DidNotReceive().OnChildDigestPublishedAsync(
            Arg.Any<GrainId>(),
            Arg.Any<ChildDigestSnapshot>());
    }

    [Test]
    public async Task SetParentAsync_idempotent_on_same_parent()
    {
        var fakeState = new FakePersistentState<InternalNodeState>
        {
            State = { ParentId = DigestParent }
        };
        var (grain, state, _) = CreateDigestGrain(fakeState);
        var writesBefore = state.WriteCount;

        await grain.SetParentAsync(DigestParent);

        Assert.That(state.WriteCount, Is.EqualTo(writesBefore),
            "re-call with identical parent must not re-persist");
    }

    [Test]
    public async Task SetParentAsync_null_clears_slot()
    {
        var fakeState = new FakePersistentState<InternalNodeState>
        {
            State = { ParentId = DigestParent }
        };
        var (grain, state, _) = CreateDigestGrain(fakeState);

        await grain.SetParentAsync(null);

        Assert.That(state.State.ParentId, Is.Null);
    }

    // --- Crash-recovery / legacy-state behaviour ---

    [Test]
    public async Task GetSubtreeProjectionDigestAsync_backfills_missing_hash_slot()
    {
        // Simulate state that pre-dates the SubtreeProjectionHash slot:
        // the digest read must transparently treat the missing slot as
        // a 16-byte zero buffer for fold arithmetic.
        var legacyState = new FakePersistentState<InternalNodeState>
        {
            State = { SubtreeProjectionHash = null }
        };
        var (grain, _, _) = CreateDigestGrain(legacyState);

        var digest = await grain.GetSubtreeProjectionDigestAsync();

        Assert.That(digest.Hash, Is.Not.Null);
        Assert.That(digest.Hash.Length, Is.EqualTo(16));
        Assert.That(legacyState.State.SubtreeProjectionHash, Is.Not.Null);
    }

    // --- Drift regression: prior snapshot with null/wrong-length Hash ---

    [Test]
    public async Task OnChildDigestPublishedAsync_replaces_prior_entry_count_when_prior_hash_is_null()
    {
        // Repro for the post-v6.0.0 CI-only flake on
        // DigestCoalescingClusterIntegrationTests.DigestCoalescingWindow_eventually_publishes_aggregate_to_parent
        // ("expected 12, observed 15" - +1 per split).
        //
        // The pre-shipping ApplyChildSnapshotAsync gated the
        // EntryCount subtraction on the prior snapshot having a
        // well-formed (length-16) hash:
        //   if (hadPrior && prior.Hash is { Length: 16 } priorHash) {
        //       hash ^= priorHash;
        //       SubtreeEntryCount -= prior.EntryCount;   <-- skipped
        //                                                    when Hash
        //                                                    is null /
        //                                                    wrong length
        //   }
        //   SubtreeEntryCount += newSnapshot.EntryCount;
        //
        // Result: a child whose stored prior snapshot had a
        // null-or-wrong-length Hash (e.g. a default snapshot inserted
        // by a topology-rewrite seam, a legacy persisted state, or a
        // never-published-by-this-child slot) re-publishes with a
        // valid hash and the new count is ADDED to a stale count
        // that should have been subtracted. Each re-publish compounds
        // the drift.
        //
        // The fix re-derives SubtreeEntryCount from
        // state.State.ChildDigests on every apply (single source of
        // truth) instead of maintaining it incrementally. This test
        // is the deterministic repro: stash a prior snapshot with
        // EntryCount=4 and Hash=null directly into the persisted
        // dictionary, then publish a fresh snapshot with
        // EntryCount=7 - the resulting SubtreeEntryCount must be 7
        // (the new value), not 11 (4 + 7 from the skipped subtract)
        // and not 4 (no update at all).
        var (grain, state, _) = CreateDigestGrain();
        state.State.ChildDigests[DigestChild0] = new ChildDigestSnapshot
        {
            Hash = null,           // the load-bearing condition
            EntryCount = 4,
            CheckpointOffset = 0,
        };
        state.State.SubtreeEntryCount = 4;  // mirror the dictionary so the
                                            // invariant pre-condition holds.

        await grain.OnChildDigestPublishedAsync(DigestChild0, new ChildDigestSnapshot
        {
            Hash = Bytes16(0x42),
            EntryCount = 7,
            CheckpointOffset = 99,
        });

        Assert.That(state.State.SubtreeEntryCount, Is.EqualTo(7),
            "SubtreeEntryCount must reflect the new per-child sum (the table is the source of truth); "
            + "a skipped subtract on a null/wrong-length prior hash silently double-counts the prior entries.");
        Assert.That(state.State.SubtreeHighestCheckpointOffset, Is.EqualTo(99));
        Assert.That(state.State.ChildDigests[DigestChild0].Hash, Is.EqualTo(Bytes16(0x42)));
    }

    [Test]
    public async Task OnChildDigestPublishedAsync_replaces_prior_entry_count_when_prior_hash_has_wrong_length()
    {
        // Sibling case of the null-prior-hash test above: an existing
        // entry whose Hash is non-null but the wrong length (e.g.
        // legacy persisted state from an older hash algorithm) must
        // also have its EntryCount subtracted on re-publish.
        var (grain, state, _) = CreateDigestGrain();
        state.State.ChildDigests[DigestChild0] = new ChildDigestSnapshot
        {
            Hash = new byte[8],  // wrong length triggers the same skip
            EntryCount = 4,
            CheckpointOffset = 0,
        };
        state.State.SubtreeEntryCount = 4;  // mirror the dictionary.

        await grain.OnChildDigestPublishedAsync(DigestChild0, new ChildDigestSnapshot
        {
            Hash = Bytes16(0x42),
            EntryCount = 7,
            CheckpointOffset = 99,
        });

        Assert.That(state.State.SubtreeEntryCount, Is.EqualTo(7),
            "SubtreeEntryCount must reflect the new per-child sum even when the prior snapshot's "
            + "Hash has the wrong byte width; the entry-count is independent of hash-width gating.");
    }

    [Test]
    public async Task OnChildDigestPublishedAsync_subtree_entry_count_equals_sum_of_child_entry_counts()
    {
        // Strong invariant: after any apply,
        //   SubtreeEntryCount == sum over ChildDigests of EntryCount
        // Exercises a deliberate mix of valid / null / wrong-length
        // prior hashes so a future regression that re-introduces the
        // incremental shape (and silently skips a subtract on a
        // null/wrong-length prior) trips here even when each
        // individual test passes in isolation.
        var (grain, state, _) = CreateDigestGrain();

        // Seed three children with three different prior-hash shapes.
        state.State.ChildDigests[DigestChild0] = new ChildDigestSnapshot
        {
            Hash = Bytes16(0x11), EntryCount = 2, CheckpointOffset = 0,
        };
        state.State.ChildDigests[DigestChild1] = new ChildDigestSnapshot
        {
            Hash = null, EntryCount = 3, CheckpointOffset = 0,
        };
        var DigestChild2 = GrainId.Create("leaf", "digest-child-2");
        state.State.ChildDigests[DigestChild2] = new ChildDigestSnapshot
        {
            Hash = new byte[8], EntryCount = 5, CheckpointOffset = 0,
        };

        // Re-publish each in turn with a fresh count.
        await grain.OnChildDigestPublishedAsync(DigestChild0, new ChildDigestSnapshot
        {
            Hash = Bytes16(0x22), EntryCount = 4, CheckpointOffset = 1,
        });
        await grain.OnChildDigestPublishedAsync(DigestChild1, new ChildDigestSnapshot
        {
            Hash = Bytes16(0x33), EntryCount = 6, CheckpointOffset = 2,
        });
        await grain.OnChildDigestPublishedAsync(DigestChild2, new ChildDigestSnapshot
        {
            Hash = Bytes16(0x44), EntryCount = 8, CheckpointOffset = 3,
        });

        var expected = 4 + 6 + 8;
        Assert.That(state.State.SubtreeEntryCount, Is.EqualTo(expected),
            "SubtreeEntryCount must equal the sum of the current per-child EntryCount values; "
            + $"observed {state.State.SubtreeEntryCount}, expected {expected}");

        // Cross-check the invariant directly against the dictionary.
        long fromTable = 0;
        foreach (var kvp in state.State.ChildDigests)
            fromTable += kvp.Value.EntryCount;
        Assert.That(state.State.SubtreeEntryCount, Is.EqualTo(fromTable));
    }

    // --- PruneMovedChildDigests: internal-node split stale-row repro ---

    [Test]
    public async Task PruneMovedChildDigests_drops_moved_rows_and_recomputes_entry_count()
    {
        // An internal-node split hands half its children to a new
        // sibling. Before this fix the donor trimmed state.State.Children
        // but never pruned state.State.ChildDigests, so its
        // SubtreeEntryCount kept summing the moved children's counts
        // while the new sibling also counted them - a permanent
        // double-count across the chained fold. This is the deterministic
        // repro: fold three children in, prune two of them (as a split
        // would), and assert the donor's aggregate reflects only the
        // retained child.
        var (grain, state, _) = CreateDigestGrain();
        var child2 = GrainId.Create("leaf", "digest-child-2");

        await grain.OnChildDigestPublishedAsync(DigestChild0, new ChildDigestSnapshot
        {
            Hash = Bytes16(0x11), EntryCount = 2, CheckpointOffset = 5,
        });
        await grain.OnChildDigestPublishedAsync(DigestChild1, new ChildDigestSnapshot
        {
            Hash = Bytes16(0x22), EntryCount = 3, CheckpointOffset = 9,
        });
        await grain.OnChildDigestPublishedAsync(child2, new ChildDigestSnapshot
        {
            Hash = Bytes16(0x44), EntryCount = 7, CheckpointOffset = 2,
        });
        Assert.That(state.State.SubtreeEntryCount, Is.EqualTo(2 + 3 + 7),
            "pre-condition: all three children counted before the prune");

        var removed = grain.PruneMovedChildDigests(new[] { DigestChild1, child2 });

        Assert.That(removed, Is.True, "rows that exist must report as removed");
        Assert.That(state.State.ChildDigests.ContainsKey(DigestChild1), Is.False);
        Assert.That(state.State.ChildDigests.ContainsKey(child2), Is.False);
        Assert.That(state.State.ChildDigests.ContainsKey(DigestChild0), Is.True);
        Assert.That(state.State.SubtreeEntryCount, Is.EqualTo(2),
            "donor's SubtreeEntryCount must reflect only the retained child after the split prune");
        // The retained child's hash is the only surviving XOR contribution.
        Assert.That(state.State.SubtreeProjectionHash, Is.EqualTo(Bytes16(0x11)));
    }

    [Test]
    public async Task PruneMovedChildDigests_recomputes_max_reduced_checkpoint_offset()
    {
        // The checkpoint offset is max-reduced, not summed. Pruning the
        // child that held the maximum must drop the aggregate back to the
        // highest offset among the retained children.
        var (grain, state, _) = CreateDigestGrain();

        await grain.OnChildDigestPublishedAsync(DigestChild0, new ChildDigestSnapshot
        {
            Hash = Bytes16(0x11), EntryCount = 1, CheckpointOffset = 4,
        });
        await grain.OnChildDigestPublishedAsync(DigestChild1, new ChildDigestSnapshot
        {
            Hash = Bytes16(0x22), EntryCount = 1, CheckpointOffset = 42,
        });
        Assert.That(state.State.SubtreeHighestCheckpointOffset, Is.EqualTo(42));

        grain.PruneMovedChildDigests(new[] { DigestChild1 });

        Assert.That(state.State.SubtreeHighestCheckpointOffset, Is.EqualTo(4),
            "max-reduced checkpoint offset must drop to the highest retained child after the prune");
    }

    [Test]
    public async Task PruneMovedChildDigests_no_matching_rows_is_a_noop()
    {
        var (grain, state, _) = CreateDigestGrain();
        await grain.OnChildDigestPublishedAsync(DigestChild0, new ChildDigestSnapshot
        {
            Hash = Bytes16(0x11), EntryCount = 2, CheckpointOffset = 5,
        });
        var hashBefore = (byte[])state.State.SubtreeProjectionHash!.Clone();

        var removed = grain.PruneMovedChildDigests(new[] { DigestChild1 });

        Assert.That(removed, Is.False, "pruning ids that are not present must report no removal");
        Assert.That(state.State.SubtreeEntryCount, Is.EqualTo(2),
            "a no-op prune must not alter the aggregate");
        Assert.That(state.State.SubtreeProjectionHash, Is.EqualTo(hashBefore));
    }

    [Test]
    public void PruneMovedChildDigests_null_argument_throws()
    {
        var (grain, _, _) = CreateDigestGrain();

        Assert.That(
            () => grain.PruneMovedChildDigests(null!),
            Throws.TypeOf<ArgumentNullException>());
    }

    // --- ApplyChildSnapshotAsync ownership guard ---

    [Test]
    public async Task OnChildDigestPublishedAsync_ignores_snapshot_from_non_owned_child()
    {
        // Once a node has recorded its children, a digest publish from a
        // child it does not own (e.g. a child that was re-parented to a
        // new sibling during a split but still has an in-flight publish
        // targeting this former parent) must be rejected rather than
        // folded. Folding it would re-add the moved child's contribution
        // while the new sibling also counts it, double-counting the moved
        // subtree across the chained fold.
        var owned = GrainId.Create("leaf", "owned-child");
        var stranger = GrainId.Create("leaf", "stranger-child");
        var state = new FakePersistentState<InternalNodeState>();
        state.State.Children =
        [
            new ChildEntry { SeparatorKey = null, ChildId = owned },
        ];
        var (grain, _, _) = CreateDigestGrain(state);

        await grain.OnChildDigestPublishedAsync(owned, new ChildDigestSnapshot
        {
            Hash = Bytes16(0x11), EntryCount = 4, CheckpointOffset = 1,
        });
        await grain.OnChildDigestPublishedAsync(stranger, new ChildDigestSnapshot
        {
            Hash = Bytes16(0x22), EntryCount = 9, CheckpointOffset = 1,
        });

        Assert.That(state.State.ChildDigests.ContainsKey(stranger), Is.False,
            "a snapshot from a non-owned child must not create a row");
        Assert.That(state.State.SubtreeEntryCount, Is.EqualTo(4),
            "only the owned child contributes to the subtree aggregate");
    }

    [Test]
    public async Task OnChildDigestPublishedAsync_drops_stale_row_when_child_no_longer_owned()
    {
        // A child that was previously folded in, then removed from the
        // Children list (re-parented away), must have its lingering
        // ChildDigests row dropped on its next stale publish so the
        // aggregate self-heals back to the owned set.
        var owned = GrainId.Create("leaf", "owned-child");
        var moved = GrainId.Create("leaf", "moved-child");
        var state = new FakePersistentState<InternalNodeState>();
        state.State.Children =
        [
            new ChildEntry { SeparatorKey = null, ChildId = owned },
            new ChildEntry { SeparatorKey = "m", ChildId = moved },
        ];
        var (grain, _, _) = CreateDigestGrain(state);

        await grain.OnChildDigestPublishedAsync(owned, new ChildDigestSnapshot
        {
            Hash = Bytes16(0x11), EntryCount = 4, CheckpointOffset = 1,
        });
        await grain.OnChildDigestPublishedAsync(moved, new ChildDigestSnapshot
        {
            Hash = Bytes16(0x22), EntryCount = 5, CheckpointOffset = 1,
        });
        Assert.That(state.State.SubtreeEntryCount, Is.EqualTo(9),
            "pre-condition: both children counted while owned");

        // The child is handed to a new sibling: drop it from Children.
        state.State.Children =
        [
            new ChildEntry { SeparatorKey = null, ChildId = owned },
        ];

        // A stale publish from the now-moved child arrives.
        await grain.OnChildDigestPublishedAsync(moved, new ChildDigestSnapshot
        {
            Hash = Bytes16(0x22), EntryCount = 5, CheckpointOffset = 1,
        });

        Assert.That(state.State.ChildDigests.ContainsKey(moved), Is.False,
            "the stale row must be dropped on the next non-owned publish");
        Assert.That(state.State.SubtreeEntryCount, Is.EqualTo(4),
            "the aggregate must self-heal to the owned set");
    }

    // --- DigestPublishTimeout (parked upward publish) ---

    [Test]
    public async Task OnChildDigestPublishedAsync_parked_parent_publish_faults_with_timeout()
    {
        // A parent that never returns from OnChildDigestPublishedAsync
        // simulates a parent mid-mutation. The upward publish must fault
        // with a TimeoutException once the DigestPublishTimeout elapses,
        // rather than parking the holding turn (and the split gate)
        // forever.
        var options = new LatticeOptions { DigestPublishTimeout = TimeSpan.FromMilliseconds(50) };
        var fakeState = new FakePersistentState<InternalNodeState>
        {
            State = { ParentId = DigestParent }
        };
        var (grain, _, factory) = CreateDigestGrain(options, fakeState);
        var parentStub = Substitute.For<IBPlusInternalGrain>();
        parentStub.OnChildDigestPublishedAsync(Arg.Any<GrainId>(), Arg.Any<ChildDigestSnapshot>())
            .Returns(new TaskCompletionSource().Task); // never completes
        factory.GetGrain<IBPlusInternalGrain>(DigestParent).Returns(parentStub);

        Assert.That(async () => await grain.OnChildDigestPublishedAsync(DigestChild0, new ChildDigestSnapshot
        {
            Hash = Bytes16(0x22), EntryCount = 2, CheckpointOffset = 5,
        }), Throws.TypeOf<TimeoutException>());
    }

    [Test]
    public async Task OnChildDigestPublishedAsync_releases_gate_after_parked_publish_times_out()
    {
        // After a parked publish faults, the non-reentrant split gate must
        // be released so the next mutating turn on the activation can run.
        // A second publish with a parent that returns promptly proves the
        // gate is free.
        var options = new LatticeOptions { DigestPublishTimeout = TimeSpan.FromMilliseconds(50) };
        var fakeState = new FakePersistentState<InternalNodeState>
        {
            State = { ParentId = DigestParent }
        };
        var (grain, state, factory) = CreateDigestGrain(options, fakeState);
        var parkingParent = Substitute.For<IBPlusInternalGrain>();
        parkingParent.OnChildDigestPublishedAsync(Arg.Any<GrainId>(), Arg.Any<ChildDigestSnapshot>())
            .Returns(new TaskCompletionSource().Task);
        factory.GetGrain<IBPlusInternalGrain>(DigestParent).Returns(parkingParent);

        Assert.That(async () => await grain.OnChildDigestPublishedAsync(DigestChild0, new ChildDigestSnapshot
        {
            Hash = Bytes16(0x11), EntryCount = 3, CheckpointOffset = 1,
        }), Throws.TypeOf<TimeoutException>());

        // Swap in a parent that returns immediately and confirm the next
        // publish completes (the gate was released).
        var promptParent = Substitute.For<IBPlusInternalGrain>();
        promptParent.OnChildDigestPublishedAsync(Arg.Any<GrainId>(), Arg.Any<ChildDigestSnapshot>())
            .Returns(Task.CompletedTask);
        factory.GetGrain<IBPlusInternalGrain>(DigestParent).Returns(promptParent);

        await grain.OnChildDigestPublishedAsync(DigestChild1, new ChildDigestSnapshot
        {
            Hash = Bytes16(0x22), EntryCount = 4, CheckpointOffset = 2,
        });

        await promptParent.Received(1).OnChildDigestPublishedAsync(
            Arg.Any<GrainId>(), Arg.Any<ChildDigestSnapshot>());
        Assert.That(state.State.SubtreeEntryCount, Is.EqualTo(7),
            "both children's contributions persist; only the upward publish was abandoned");
    }

    [Test]
    public async Task OnChildDigestPublishedAsync_infinite_timeout_awaits_parent_unbounded()
    {
        // With DigestPublishTimeout = InfiniteTimeSpan the publish must be
        // awaited without a ceiling. A parent that completes after a short
        // delay (longer than any finite test deadline we would otherwise
        // set) is awaited to completion rather than abandoned.
        var options = new LatticeOptions { DigestPublishTimeout = Timeout.InfiniteTimeSpan };
        var fakeState = new FakePersistentState<InternalNodeState>
        {
            State = { ParentId = DigestParent }
        };
        var (grain, _, factory) = CreateDigestGrain(options, fakeState);
        var parentStub = Substitute.For<IBPlusInternalGrain>();
        var tcs = new TaskCompletionSource();
        parentStub.OnChildDigestPublishedAsync(Arg.Any<GrainId>(), Arg.Any<ChildDigestSnapshot>())
            .Returns(tcs.Task);
        factory.GetGrain<IBPlusInternalGrain>(DigestParent).Returns(parentStub);

        var publish = grain.OnChildDigestPublishedAsync(DigestChild0, new ChildDigestSnapshot
        {
            Hash = Bytes16(0x22), EntryCount = 2, CheckpointOffset = 5,
        });

        Assert.That(publish.IsCompleted, Is.False,
            "the publish must remain pending while the parent has not returned");
        tcs.SetResult();
        await publish; // completes without timing out

        await parentStub.Received(1).OnChildDigestPublishedAsync(
            Arg.Any<GrainId>(), Arg.Any<ChildDigestSnapshot>());
    }

    [Test]
    public async Task OnChildDigestPublishedAsync_recycles_deadline_across_sequential_publishes()
    {
        // The finite-timeout path reuses a single per-activation deadline
        // source across publishes (arm via CancelAfter, disarm via TryReset
        // after each non-fired publish) instead of allocating a fresh
        // CancellationTokenSource(timeout) per publish. Drive several
        // sequential successful publishes through that recycle path and
        // confirm every one lands at the parent and the cumulative subtree
        // aggregate is exact - i.e. a recycled (TryReset'd) source never
        // carries a stale deadline into the next publish nor drops one.
        var options = new LatticeOptions { DigestPublishTimeout = TimeSpan.FromSeconds(30) };
        var fakeState = new FakePersistentState<InternalNodeState>
        {
            State = { ParentId = DigestParent }
        };
        var (grain, state, factory) = CreateDigestGrain(options, fakeState);
        var promptParent = Substitute.For<IBPlusInternalGrain>();
        promptParent.OnChildDigestPublishedAsync(Arg.Any<GrainId>(), Arg.Any<ChildDigestSnapshot>())
            .Returns(Task.CompletedTask);
        factory.GetGrain<IBPlusInternalGrain>(DigestParent).Returns(promptParent);

        var children = new[]
        {
            GrainId.Create("leaf", "recycle-0"),
            GrainId.Create("leaf", "recycle-1"),
            GrainId.Create("leaf", "recycle-2"),
            GrainId.Create("leaf", "recycle-3"),
        };
        for (var i = 0; i < children.Length; i++)
        {
            await grain.OnChildDigestPublishedAsync(children[i], new ChildDigestSnapshot
            {
                Hash = Bytes16((byte)(0x10 + i)), EntryCount = i + 1, CheckpointOffset = i,
            });
        }

        await promptParent.Received(children.Length).OnChildDigestPublishedAsync(
            Arg.Any<GrainId>(), Arg.Any<ChildDigestSnapshot>());
        Assert.That(state.State.SubtreeEntryCount, Is.EqualTo(1 + 2 + 3 + 4),
            "every recycled-deadline publish folds its child's entry count exactly once");
    }
}
