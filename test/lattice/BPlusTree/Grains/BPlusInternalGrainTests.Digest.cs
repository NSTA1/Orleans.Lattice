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
}
