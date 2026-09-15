using Microsoft.Extensions.DependencyInjection;
using NUnit.Framework;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Runtime;
using Orleans.Serialization;
using Orleans.Storage;

namespace Orleans.Lattice.Tests.Storage;

/// <summary>
/// Covers the binary-persistence marking on <see cref="LeafNodeState"/>.
/// <para>
/// The remedy behind <see cref="ILatticeBinaryPersistedState"/> was applied to
/// <c>LeafSnapshotBlob</c> but not to this type, even though this row is read
/// and written on every leaf activation and carries the deliberately unbounded
/// unresolved-prepare ledger. These tests pin the marking, prove the binary
/// form loses none of the row's state, prove rows already written as JSON by an
/// earlier build still read, and measure the reduction against the JSON
/// serializer the silo actually used before the change rather than against a
/// stand-in.
/// </para>
/// </summary>
[TestFixture]
public sealed class LeafNodeStateBinaryPersistenceTests
{
    private ServiceProvider provider = null!;
    private Serializer serializer = null!;
    private IGrainStorageSerializer json = null!;
    private LatticeGrainStorageSerializer lattice = null!;

    [SetUp]
    public void SetUp()
    {
        var services = new ServiceCollection();
        services.AddSerializer();
        services.AddOptions();

        // The silo registers this as part of its JSON grain-storage wiring;
        // a bare serializer container does not, so register it here to get
        // the real JsonGrainStorageSerializer rather than a stand-in.
        services.AddSingleton<OrleansJsonSerializer>();

        this.provider = services.BuildServiceProvider();
        this.serializer = this.provider.GetRequiredService<Serializer>();
        this.json = ActivatorUtilities.CreateInstance<JsonGrainStorageSerializer>(this.provider);
        this.lattice = new LatticeGrainStorageSerializer(this.serializer, this.json);
    }

    [TearDown]
    public void TearDown() => this.provider?.Dispose();

    [Test]
    public void LeafNodeState_IsMarkedForBinaryPersistence()
    {
        Assert.That(
            LatticeGrainStorageSerializer.WritesBinary(typeof(LeafNodeState)),
            Is.True,
            "LeafNodeState must be written through the Orleans binary serializer");
    }

    [Test]
    public void LeafNodeState_IsWrittenWithTheBinaryMagicPrefix()
    {
        var payload = this.lattice.Serialize(NewPopulatedState()).ToArray();

        Assert.That(
            payload.Length,
            Is.GreaterThan(LatticeGrainStorageSerializer.BinaryMagic.Length),
            "sanity: the payload must be longer than the magic it is prefixed with");
        Assert.That(
            payload.AsSpan(0, LatticeGrainStorageSerializer.BinaryMagic.Length)
                .SequenceEqual(LatticeGrainStorageSerializer.BinaryMagic),
            Is.True,
            "a marked type must be written in the binary format, not as a JSON document");
    }

    /// <summary>
    /// The data-loss guard. The JSON serializer persists any public property
    /// whereas the binary serializer persists exactly the <c>[Id(n)]</c>
    /// members, so a member added later without an id would be silently
    /// dropped by this change. Every member is populated with a distinct,
    /// non-default value so that a dropped one cannot pass by coinciding with
    /// the default.
    /// </summary>
    [Test]
    public void LeafNodeState_BinaryRoundTripPreservesEveryPersistedMember()
    {
        var original = NewPopulatedState();

        var payload = this.lattice.Serialize(original);

        // The trip must actually have been binary, or this fixture would keep
        // its name while proving the JSON path preserves the members.
        Assert.That(
            payload.ToArray().AsSpan(0, LatticeGrainStorageSerializer.BinaryMagic.Length)
                .SequenceEqual(LatticeGrainStorageSerializer.BinaryMagic),
            Is.True,
            "the round trip under test must be the binary one");

        AssertMatchesPopulatedState(this.lattice.Deserialize<LeafNodeState>(payload));
    }

    /// <summary>
    /// Backward compatibility, which is what makes the marking safe to deploy
    /// with no migration: a row written as JSON by a build from before the
    /// marking carries no binary magic, so the reader must route it to the
    /// fallback and still return the state.
    /// </summary>
    [Test]
    public void LeafNodeState_RowWrittenAsJsonByAnEarlierBuildStillReads()
    {
        // Exactly what the pre-change silo would have left on disk.
        var legacy = this.json.Serialize(NewPopulatedState());

        Assert.That(
            legacy.ToArray().AsSpan(0, LatticeGrainStorageSerializer.BinaryMagic.Length)
                .SequenceEqual(LatticeGrainStorageSerializer.BinaryMagic),
            Is.False,
            "sanity: the legacy row must not carry the binary magic, or this proves nothing");

        AssertMatchesPopulatedState(this.lattice.Deserialize<LeafNodeState>(legacy));
    }

    /// <summary>
    /// The reduction this change exists for, measured on a row whose mass is
    /// the unresolved-prepare ledger. Both figures are measured, and each
    /// assertion references the other measurement rather than a fixed
    /// threshold, so the test cannot pass by agreeing with a constant it also
    /// supplies.
    /// </summary>
    [Test]
    public void LeafNodeState_WithLargeUnresolvedReplayWorkCostsFarLessThanTheJsonPath()
    {
        const int Entries = 4_000;
        var state = new LeafNodeState
        {
            TreeId = "tree",
            UnresolvedReplayWork = BuildLedger(Entries),
        };

        // Warm both paths so first-call setup is charged to neither.
        var warmup = new LeafNodeState { TreeId = "w", UnresolvedReplayWork = BuildLedger(1) };
        _ = this.lattice.Serialize(warmup);
        _ = this.json.Serialize(warmup);

        var before = GC.GetAllocatedBytesForCurrentThread();
        var binaryPayload = this.lattice.Serialize(state).ToMemory().Length;
        var binaryAllocated = GC.GetAllocatedBytesForCurrentThread() - before;

        before = GC.GetAllocatedBytesForCurrentThread();
        var jsonPayload = this.json.Serialize(state).ToMemory().Length;
        var jsonAllocated = GC.GetAllocatedBytesForCurrentThread() - before;

        TestContext.Out.WriteLine(
            $"entries={Entries} json: payload={jsonPayload} allocated={jsonAllocated}; " +
            $"binary: payload={binaryPayload} allocated={binaryAllocated}; " +
            $"payload ratio={(double)jsonPayload / binaryPayload:F2}x " +
            $"allocation ratio={(double)jsonAllocated / binaryAllocated:F2}x");

        Assert.Multiple(() =>
        {
            Assert.That(
                binaryPayload,
                Is.LessThan(jsonPayload),
                "the binary payload must be smaller than the JSON document it replaces");
            Assert.That(
                binaryAllocated,
                Is.LessThan(jsonAllocated),
                "the binary path must allocate less than the JSON path it replaces");
        });
    }

    /// <summary>
    /// The read path, which is the direction that fails first. Activation
    /// deserializes this row before any grain code runs, so the JSON
    /// fallback's contiguous UTF-16 materialisation is charged on every
    /// activation of every leaf. Measured through the production entry point
    /// in both arms, so the only difference is the format the row is stored
    /// in. Both figures are measured and each assertion references the other,
    /// so the test cannot pass by agreeing with a constant it also supplies.
    /// </summary>
    [Test]
    public void LeafNodeState_WithLargeUnresolvedReplayWorkCostsFarLessToReadThanTheJsonPath()
    {
        const int Entries = 4_000;
        var state = new LeafNodeState
        {
            TreeId = "tree",
            UnresolvedReplayWork = BuildLedger(Entries),
        };

        // The row exactly as each build leaves it on disk.
        var legacyRow = this.json.Serialize(state);
        var binaryRow = this.lattice.Serialize(state);

        // Warm both read paths so first-call setup is charged to neither.
        _ = this.lattice.Deserialize<LeafNodeState>(legacyRow);
        _ = this.lattice.Deserialize<LeafNodeState>(binaryRow);

        var before = GC.GetAllocatedBytesForCurrentThread();
        var fromJson = this.lattice.Deserialize<LeafNodeState>(legacyRow);
        var jsonAllocated = GC.GetAllocatedBytesForCurrentThread() - before;

        before = GC.GetAllocatedBytesForCurrentThread();
        var fromBinary = this.lattice.Deserialize<LeafNodeState>(binaryRow);
        var binaryAllocated = GC.GetAllocatedBytesForCurrentThread() - before;

        TestContext.Out.WriteLine(
            $"read entries={Entries} json: row={legacyRow.ToMemory().Length} allocated={jsonAllocated}; " +
            $"binary: row={binaryRow.ToMemory().Length} allocated={binaryAllocated}; " +
            $"allocation ratio={(double)jsonAllocated / binaryAllocated:F2}x");

        Assert.Multiple(() =>
        {
            // Sanity: both arms must actually have read the ledger, or an
            // arm that returned early would look cheap for the wrong reason.
            Assert.That(fromJson.UnresolvedReplayWork!, Has.Count.EqualTo(Entries));
            Assert.That(fromBinary.UnresolvedReplayWork!, Has.Count.EqualTo(Entries));
            Assert.That(
                binaryAllocated,
                Is.LessThan(jsonAllocated),
                "reading the binary row must allocate less than reading the JSON row it replaces");
        });
    }

    private static List<UnresolvedReplayWorkEntry> BuildLedger(int count)
    {
        var work = new List<UnresolvedReplayWorkEntry>(count);
        for (var i = 0; i < count; i++)
        {
            work.Add(new UnresolvedReplayWorkEntry(
                i % 4,
                i,
                new LatticeMutation
                {
                    TreeId = "tree",
                    Kind = MutationKind.Set,
                    Key = $"key-{i}",
                    Value = [1, 2, 3, 4],
                    Timestamp = new HybridLogicalClock { WallClockTicks = i, Counter = i % 7 },
                }));
        }

        return work;
    }

    /// <summary>
    /// Every <c>[Id(n)]</c> member set to a distinct non-default value.
    /// </summary>
    private static LeafNodeState NewPopulatedState() => new()
    {
        NextSibling = GrainId.Create("leaf", "next"),
        SplitState = SplitState.SplitInProgress,
        SplitKey = "split-key",
        SplitSiblingId = GrainId.Create("leaf", "sibling"),
        Clock = new HybridLogicalClock { WallClockTicks = 1234, Counter = 7 },
        Version = new VersionVector
        {
            Entries = { ["a"] = new HybridLogicalClock { WallClockTicks = 11, Counter = 1 } },
        },
        TreeId = "tree-id",
        PrevSibling = GrainId.Create("leaf", "prev"),
        OldNextSibling = GrainId.Create("leaf", "old-next"),
        LastCompactionVersion = new VersionVector
        {
            Entries = { ["b"] = new HybridLogicalClock { WallClockTicks = 22, Counter = 2 } },
        },
        ProjectionCheckpointOffset = 4242,
        ProjectionCheckpointOffsetAssigned = true,
        ProjectionHash = [9, 8, 7],
        ShardIndex = 5,
        LowKeyInclusive = "low",
        HighKeyExclusive = "high",
        MovedAwaySlots = [1, 3, 5],
        MovedAwayVirtualShardCount = 11,
        ParentId = GrainId.Create("internal", "parent"),
        ProjectionCheckpointOffsetsByPartition = [10, 20, 30],
        DigestPublishSequence = 77,
        UnresolvedReplayWork = BuildLedger(3),
        SnapshotLoadHintBytes = 9090,
    };

    private static void AssertMatchesPopulatedState(LeafNodeState restored)
    {
        Assert.That(restored, Is.Not.Null);
        Assert.Multiple(() =>
        {
            Assert.That(restored.NextSibling, Is.EqualTo(GrainId.Create("leaf", "next")));
            Assert.That(restored.SplitState, Is.EqualTo(SplitState.SplitInProgress));
            Assert.That(restored.SplitKey, Is.EqualTo("split-key"));
            Assert.That(restored.SplitSiblingId, Is.EqualTo(GrainId.Create("leaf", "sibling")));
            Assert.That(restored.Clock.WallClockTicks, Is.EqualTo(1234));
            Assert.That(restored.Clock.Counter, Is.EqualTo(7));
            Assert.That(restored.Version.Entries["a"].WallClockTicks, Is.EqualTo(11));
            Assert.That(restored.TreeId, Is.EqualTo("tree-id"));
            Assert.That(restored.PrevSibling, Is.EqualTo(GrainId.Create("leaf", "prev")));
            Assert.That(restored.OldNextSibling, Is.EqualTo(GrainId.Create("leaf", "old-next")));
            Assert.That(restored.LastCompactionVersion.Entries["b"].WallClockTicks, Is.EqualTo(22));
            Assert.That(restored.ProjectionCheckpointOffset, Is.EqualTo(4242));
            Assert.That(restored.ProjectionCheckpointOffsetAssigned, Is.True);
            Assert.That(restored.ProjectionHash, Is.EqualTo(new byte[] { 9, 8, 7 }));
            Assert.That(restored.ShardIndex, Is.EqualTo(5));
            Assert.That(restored.LowKeyInclusive, Is.EqualTo("low"));
            Assert.That(restored.HighKeyExclusive, Is.EqualTo("high"));
            Assert.That(restored.MovedAwaySlots, Is.EqualTo(new[] { 1, 3, 5 }));
            Assert.That(restored.MovedAwayVirtualShardCount, Is.EqualTo(11));
            Assert.That(restored.ParentId, Is.EqualTo(GrainId.Create("internal", "parent")));
            Assert.That(
                restored.ProjectionCheckpointOffsetsByPartition,
                Is.EqualTo(new long[] { 10, 20, 30 }));
            Assert.That(restored.DigestPublishSequence, Is.EqualTo(77));
            Assert.That(restored.SnapshotLoadHintBytes, Is.EqualTo(9090));
            Assert.That(restored.UnresolvedReplayWork, Is.Not.Null);
            Assert.That(restored.UnresolvedReplayWork!, Has.Count.EqualTo(3));
            Assert.That(restored.UnresolvedReplayWork![1].Offset, Is.EqualTo(1));
            Assert.That(restored.UnresolvedReplayWork![1].Mutation.Key, Is.EqualTo("key-1"));
        });
    }
}
