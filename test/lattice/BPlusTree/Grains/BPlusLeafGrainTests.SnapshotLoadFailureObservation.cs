using System.Collections.Concurrent;
using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Testing;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Coverage for the snapshot-load failure observation added for issue #2364.
/// <para>
/// The rehydrate seam swallows a failed snapshot load and returns <c>false</c>,
/// which is correct for availability and is deliberately left unchanged here.
/// What was wrong is that the decline was <b>silent</b>, and therefore
/// indistinguishable from the decline a leaf with no snapshot at all produces:
/// both return <c>false</c>, the activation then takes the <c>-1</c>
/// replay-start override and replays its whole readable WAL window, and the
/// cold-replay log line reports "no snapshot rehydrate" - true, and read by
/// every operator as "there was no snapshot". The failure population read as
/// zero at every rate of occurrence, which is not a weak measurement but no
/// measurement.
/// </para>
/// <para>
/// The <c>resource_exhausted</c> arm is the reason the deployed occurrence went
/// undiagnosed for hours. Under a container memory limit the .NET GC heap hard
/// limit is sized from the cgroup limit, so the process is never OOM-killed -
/// there is no restart, no exit code and no resource event. It throws
/// <see cref="OutOfMemoryException"/> inside the storage provider's deserialise
/// of the snapshot blob, and surfaces only as the provider's own
/// "Error reading grain state". A memory shortage presenting as a storage
/// fault, which sends an operator to investigate the wrong subsystem.
/// </para>
/// </summary>
public partial class BPlusLeafGrainTests
{
    private static string UniqueSnapshotLoadFailureTree() => $"snapshot-load-failure-{Guid.NewGuid():N}";

    private static ConcurrentBag<(long Value, KeyValuePair<string, object?>[] Tags)> CaptureSnapshotLoadFailures(
        string treeId,
        out IDisposable listener)
    {
        var records = new ConcurrentBag<(long, KeyValuePair<string, object?>[])>();
        listener = MeterListening.StartForInstrument(
            LatticeMetrics.LeafSnapshotLoadFailures,
            l => l.SetMeasurementEventCallback<long>((_, value, tags, _) =>
            {
                var copied = tags.ToArray();

                // This instrument is process-wide and other fixtures share it.
                // Filter to this test's own unique tree so a concurrent fixture
                // can neither satisfy nor weaken the assertions below.
                foreach (var tag in copied)
                {
                    if (tag.Key == LatticeMetrics.TagTree && (tag.Value as string) == treeId)
                    {
                        records.Add((value, copied));
                        return;
                    }
                }
            }));
        return records;
    }

    /// <summary>
    /// Builds a leaf whose snapshot storage grain throws <paramref name="failure"/>
    /// from <c>LoadAsync</c>, which is the shape a provider-side read failure
    /// takes at this seam.
    /// </summary>
    private static (BPlusLeafGrain Grain, FakePersistentState<LeafNodeState> State) CreateGrainWithFailingSnapshotLoad(
        string treeId,
        Exception failure)
    {
        var (grain, state, snapshotStub, _) = CreateGrainWithSnapshotAndCoordinator(
            preloadedSnapshot: null,
            persistedCheckpoint: 0L,
            walHead: 0L);

        state.State.TreeId = treeId;
        snapshotStub.LoadAsync(Arg.Any<CancellationToken>()).Returns<Task<LeafSnapshotBlob?>>(_ => throw failure);

        return (grain, state);
    }

    [Test]
    public async Task Snapshot_load_that_runs_out_of_memory_is_counted_as_resource_exhausted()
    {
        var treeId = UniqueSnapshotLoadFailureTree();

        // Wrapped, because that is how the fault actually arrives. The
        // allocation fails inside the provider's deserialiser, several frames
        // below this grain call, and Orleans surfaces a failure to read a
        // grain's persistent state as an activation failure carrying the
        // original as an inner exception. A classifier that tested only the
        // outermost type would report "faulted" for every real occurrence -
        // the exact wrong answer, not a missing one.
        var (grain, _) = CreateGrainWithFailingSnapshotLoad(
            treeId,
            new InvalidOperationException(
                "activation failed",
                new OutOfMemoryException("Exception of type 'System.OutOfMemoryException' was thrown.")));

        var records = CaptureSnapshotLoadFailures(treeId, out var listener);
        bool rehydrated;
        using (listener)
        {
            rehydrated = await grain.TryRehydrateFromSnapshotAsync(CancellationToken.None);
        }

        Assert.Multiple(() =>
        {
            Assert.That(rehydrated, Is.False,
                "The decline is deliberately unchanged: a failed load must not block the leaf coming "
                + "online. This fix makes the decline observable, it does not make it fatal.");
            Assert.That(records, Has.Count.EqualTo(1),
                "A failed snapshot load must be counted exactly once.");
        });

        var tags = records.Single().Tags;
        Assert.Multiple(() =>
        {
            Assert.That(
                tags.Select(t => t.Key),
                Is.EquivalentTo(new[]
                {
                    LatticeMetrics.TagTree,
                    LatticeMetrics.TagReason,
                    LatticeTenantLabel.TagTenant,
                }),
                "Exactly these three bounded tags - no leaf id, whose population is unbounded. The "
                + "tenant dimension is derived from the tree and so adds no cardinality of its own.");
            Assert.That(tags.Single(t => t.Key == LatticeMetrics.TagTree).Value, Is.EqualTo(treeId));
            Assert.That(
                tags.Single(t => t.Key == LatticeMetrics.TagReason).Value,
                Is.EqualTo(LatticeMetrics.SnapshotLoadFailureResourceExhausted.Value),
                "An OutOfMemoryException anywhere in the chain means the blob could not be materialised "
                + "within the available heap. Reporting it as an ordinary storage fault is what sent the "
                + "deployed investigation to the wrong subsystem.");
        });
    }

    [Test]
    public async Task Snapshot_load_that_fails_for_any_other_reason_is_counted_as_faulted()
    {
        var treeId = UniqueSnapshotLoadFailureTree();
        var (grain, _) = CreateGrainWithFailingSnapshotLoad(
            treeId,
            new InvalidOperationException("storage unreachable"));

        var records = CaptureSnapshotLoadFailures(treeId, out var listener);
        bool rehydrated;
        using (listener)
        {
            rehydrated = await grain.TryRehydrateFromSnapshotAsync(CancellationToken.None);
        }

        Assert.Multiple(() =>
        {
            Assert.That(rehydrated, Is.False);
            Assert.That(records, Has.Count.EqualTo(1));
        });

        Assert.That(
            records.Single().Tags.Single(t => t.Key == LatticeMetrics.TagReason).Value,
            Is.EqualTo(LatticeMetrics.SnapshotLoadFailureFaulted.Value),
            "The two arms call for opposite operator responses - raise the memory limit, versus "
            + "investigate the storage provider - so folding them together would undo the entire point "
            + "of the counter.");
    }

    [Test]
    public async Task A_leaf_with_no_snapshot_records_no_failure()
    {
        // The negative half of the distinction this counter exists to draw. A
        // genuine absence and a swallowed failure previously rendered
        // identically; if the absence also counted, they would still render
        // identically, merely as one rather than as zero.
        var treeId = UniqueSnapshotLoadFailureTree();
        var (grain, state, _, _) = CreateGrainWithSnapshotAndCoordinator(
            preloadedSnapshot: null,
            persistedCheckpoint: 0L,
            walHead: 0L);
        state.State.TreeId = treeId;

        var records = CaptureSnapshotLoadFailures(treeId, out var listener);
        bool rehydrated;
        using (listener)
        {
            rehydrated = await grain.TryRehydrateFromSnapshotAsync(CancellationToken.None);
        }

        Assert.Multiple(() =>
        {
            Assert.That(rehydrated, Is.False, "No snapshot means no rehydrate, exactly as before.");
            Assert.That(records, Is.Empty,
                "A leaf that simply has no snapshot has suffered no failure and must not be counted.");
        });
    }

    [Test]
    public void IsResourceExhaustion_finds_an_OutOfMemoryException_however_it_is_wrapped()
    {
        Assert.Multiple(() =>
        {
            Assert.That(BPlusLeafGrain.IsResourceExhaustion(new OutOfMemoryException()), Is.True,
                "The unwrapped case.");
            Assert.That(
                BPlusLeafGrain.IsResourceExhaustion(
                    new InvalidOperationException("outer", new OutOfMemoryException())),
                Is.True,
                "One level of wrapping, which is how Orleans surfaces a failed persistent-state read.");
            Assert.That(
                BPlusLeafGrain.IsResourceExhaustion(
                    new InvalidOperationException("a", new InvalidOperationException("b", new OutOfMemoryException()))),
                Is.True,
                "Arbitrary nesting depth.");
            Assert.That(
                BPlusLeafGrain.IsResourceExhaustion(
                    new AggregateException(new InvalidOperationException("x"), new OutOfMemoryException())),
                Is.True,
                "Every branch of an aggregate is walked, not only the first.");
        });
    }

    [Test]
    public void IsResourceExhaustion_is_false_for_an_ordinary_fault_and_for_null()
    {
        // The positive control for the assertions above: without this, a
        // classifier hard-wired to return true would pass every case in
        // IsResourceExhaustion_finds_an_OutOfMemoryException_however_it_is_wrapped
        // and the suite would still be green.
        Assert.Multiple(() =>
        {
            Assert.That(BPlusLeafGrain.IsResourceExhaustion(null), Is.False);
            Assert.That(BPlusLeafGrain.IsResourceExhaustion(new InvalidOperationException("plain")), Is.False);
            Assert.That(
                BPlusLeafGrain.IsResourceExhaustion(
                    new InvalidOperationException("a", new TimeoutException("b"))),
                Is.False);
            Assert.That(
                BPlusLeafGrain.IsResourceExhaustion(
                    new AggregateException(new InvalidOperationException("x"), new TimeoutException("y"))),
                Is.False);
        });
    }

    [Test]
    public void IsResourceExhaustion_terminates_on_a_cyclic_exception_chain()
    {
        // Runs on the activation path, where a hang is a worse outcome than a
        // missed classification. A hand-constructed cycle is the cheapest way
        // to prove the depth bound is load-bearing rather than decorative.
        var inner = new InvalidOperationException("inner");
        var outer = new InvalidOperationException("outer", inner);

        // Point the chain back at itself via reflection; the InnerException
        // setter is not public, so this reaches the field directly.
        var field = typeof(Exception).GetField(
            "_innerException",
            System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic);
        Assert.That(field, Is.Not.Null,
            "The private inner-exception field must be reachable for this control to construct a cycle. "
            + "If the runtime renames it, this test must be revisited, not deleted.");
        field!.SetValue(inner, outer);

        Assert.That(
            () => BPlusLeafGrain.IsResourceExhaustion(outer),
            Throws.Nothing,
            "A cyclic chain must terminate at the depth bound rather than recursing forever.");
        Assert.That(BPlusLeafGrain.IsResourceExhaustion(outer), Is.False);
    }
    /// <summary>
    /// A leaf whose identity is not Guid-keyed has no snapshot storage grain to
    /// address, so it must take the SAME arm as a leaf with no snapshot: decline
    /// silently, count nothing, and never reach the storage grain.
    /// <para>
    /// This is a regression guard on the observation seam itself rather than on
    /// the leaf. Resolving the Guid key throws for a non-Guid identity, so had
    /// that resolution stayed inside the observed try block the new counter
    /// would have recorded a snapshot LOAD failure for what is really a naming
    /// precondition. That would answer "did the snapshot store fail?" with
    /// evidence about grain identity - a worse outcome than the silence this
    /// change replaces, because it is a confident answer rather than an absent
    /// one, and issue #2364 exists precisely to stop one subsystem's fault
    /// being reported as another's.
    /// </para>
    /// </summary>
    [Test]
    public async Task A_leaf_that_is_not_guid_keyed_records_no_failure_and_never_asks_storage()
    {
        var treeId = UniqueSnapshotLoadFailureTree();

        var snapshotStub = Substitute.For<ILeafSnapshotStorageGrain>();
        snapshotStub.LoadAsync(Arg.Any<CancellationToken>()).Returns<Task<LeafSnapshotBlob?>>(
            _ => throw new InvalidOperationException(
                "The storage grain must never be asked for a leaf that cannot address one."));

        var grainFactory = Substitute.For<IGrainFactory>();
        grainFactory.GetGrain<ILeafSnapshotStorageGrain>(Arg.Any<Guid>()).Returns(snapshotStub);

        var sc = new ServiceCollection();
        sc.AddSingleton(Substitute.For<ICommitLogReader>());
        sc.AddSingleton(Substitute.For<ILeafCursorReporter>());
        var services = sc.BuildServiceProvider();

        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("leaf", "not-a-guid-key"));
        context.ActivationServices.Returns(services);

        var state = new FakePersistentState<LeafNodeState>();
        state.State.TreeId = treeId;

        var grain = new BPlusLeafGrain(
            context,
            state,
            grainFactory,
            TestOptionsResolver.Create(
                baseOptions: new LatticeOptions { MaterialiserCheckpointInterval = TimeSpan.Zero },
                maxLeafKeys: 128,
                shardCount: 1,
                factory: grainFactory),
            TestMutationObservers.NoObservers(),
            TestOriginClusterIdResolver.Default());

        var records = CaptureSnapshotLoadFailures(treeId, out var listener);
        bool rehydrated;
        using (listener)
        {
            rehydrated = await grain.TryRehydrateFromSnapshotAsync(CancellationToken.None);
        }

        Assert.Multiple(() =>
        {
            Assert.That(rehydrated, Is.False,
                "A leaf that cannot address a snapshot grain declines, exactly as one with no snapshot does.");
            Assert.That(records, Is.Empty,
                "A naming precondition is not a snapshot load failure and must not be counted as one.");
        });

        await snapshotStub.DidNotReceive().LoadAsync(Arg.Any<CancellationToken>());
    }
}
