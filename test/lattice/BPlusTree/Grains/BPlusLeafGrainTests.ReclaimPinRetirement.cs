using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;
using System.Text;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Regression fixture for issue #3101: an emptied leaf folded out by
/// <c>ShardRootGrain</c>'s reclaim path used to have its state cleared without
/// its materialiser cursor ever being deregistered, leaving a pin registered
/// against a leaf that no longer carries a tree id.
/// <para>
/// The orphaned pin is not merely untidy - it is unbounded WAL retention. The
/// WAL GC resolves it, activates the leaf, asks it to drive a checkpoint, and
/// gets <see cref="LeafStarvationDriveOutcome.NotDriven"/> back because the
/// cleared state has no tree id to drive against. The pin never moves, the WAL
/// trim point never passes it, and the cycle re-arms on a backoff forever. That
/// is what took the repocontext deployment's WAL to tens of gigabytes.
/// </para>
/// <para>
/// These tests pin the death seam that now mirrors the birth seam
/// (<c>SeedDurableMaterialiserBlockPinAsync</c>): every pin the leaf could have
/// registered is retired <em>while the tree id is still bound</em>, because once
/// the state is cleared the consumer id can no longer be derived at all.
/// </para>
/// </summary>
public partial class BPlusLeafGrainTests
{
    [Test]
    public async Task ClearGrainState_retires_the_materialiser_pin()
    {
        var (grain, reporter, _) = CreateGrainWithReporter();
        var projection = AsProjection(grain);

        // Register a real pin first, so the retirement below is removing
        // something rather than passing vacuously.
        projection.Apply(BuildSet("k1", Encoding.UTF8.GetBytes("v1"), hlcPhysical: 100));
        await projection.SetCheckpointOffsetAsync(1, default);
        await projection.FlushCheckpointAsync(default);
        await reporter.Received().ReportAsync(
            CursorTreeId, Arg.Any<string>(), Arg.Any<HybridLogicalClock>(), Arg.Any<CancellationToken>());

        await grain.ClearGrainStateAsync();

        await reporter.Received(1).UnregisterAsync(
            CursorTreeId,
            Arg.Is<string>(s => s.StartsWith(ILeafCursorReporter.MaterialiserConsumerIdPrefix)
                && s.Contains(CursorTreeId)
                && s.Contains(CursorReplicaId)),
            Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task ClearGrainState_retires_the_pin_before_the_state_is_cleared()
    {
        // The ordering is the whole fix. ResolveConsumerIdBase returns null once
        // TreeId is empty, so a retirement attempted after the clear could not
        // name the consumer it meant to retire and would silently do nothing -
        // which is indistinguishable, from outside, from the bug itself. Proven
        // here by capturing the tree id the reporter was actually handed: a
        // post-clear call could only have passed an empty one.
        string? observedTreeId = null;
        var reporter = Substitute.For<ILeafCursorReporter>();
        reporter.UnregisterAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(call =>
            {
                observedTreeId = call.ArgAt<string>(0);
                return Task.CompletedTask;
            });

        var (grain, _, state) = CreateGrainWithReporter(reporter: reporter);
        var projection = AsProjection(grain);
        projection.Apply(BuildSet("k1", Encoding.UTF8.GetBytes("v1"), hlcPhysical: 100));
        await projection.SetCheckpointOffsetAsync(1, default);
        await projection.FlushCheckpointAsync(default);

        await grain.ClearGrainStateAsync();

        Assert.Multiple(() =>
        {
            Assert.That(observedTreeId, Is.EqualTo(CursorTreeId),
                "the pin must be retired while the tree id is still bound, or the consumer id cannot be derived.");
            Assert.That(state.State.TreeId, Is.Null.Or.Empty,
                "the state must still end up cleared - retirement is an addition to the reclaim, not a replacement for it.");
        });
    }

    [Test]
    public async Task ClearGrainState_retires_one_pin_per_wal_partition()
    {
        // A partitioned WAL registers one materialiser cursor per partition, so
        // retiring only the unsuffixed base id would leave every other partition
        // pinned - the same unbounded retention, just narrower.
        const int Partitions = 4;
        var reporter = Substitute.For<ILeafCursorReporter>();
        var (grain, _, _) = CreateGrainWithReporter(reporter: reporter, walPartitions: Partitions);

        await grain.ClearGrainStateAsync();

        var retired = reporter.ReceivedCalls()
            .Where(c => c.GetMethodInfo().Name == nameof(ILeafCursorReporter.UnregisterAsync))
            .Select(c => (string)c.GetArguments()[1]!)
            .ToArray();

        Assert.Multiple(() =>
        {
            Assert.That(retired, Has.Length.EqualTo(Partitions));
            Assert.That(retired, Is.Unique,
                "each partition must be retired under its own consumer id, or the extra calls retire nothing.");
            for (var partition = 0; partition < Partitions; partition++)
            {
                var suffix = $"_{partition}";
                Assert.That(retired.Any(id => id.EndsWith(suffix, StringComparison.Ordinal)), Is.True,
                    $"partition {partition} must be retired, or its WAL prefix stays pinned.");
            }
        });
    }

    [Test]
    public async Task ClearGrainState_completes_when_the_reporter_faults()
    {
        // Reclaim runs post-commit, after the leaf has already been unlinked
        // from the tree. Letting a reporter fault escape would abort the clear
        // and strand an unlinked leaf holding state, which is strictly worse
        // than the retained WAL prefix the GC's orphan sweep can still retire.
        var reporter = Substitute.For<ILeafCursorReporter>();
        reporter.UnregisterAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromException(new InvalidOperationException("registry unavailable")));

        var (grain, _, state) = CreateGrainWithReporter(reporter: reporter);

        Assert.DoesNotThrowAsync(async () => await grain.ClearGrainStateAsync());
        Assert.That(state.State.TreeId, Is.Null.Or.Empty);
    }

    [Test]
    public async Task ClearGrainState_is_a_no_op_when_no_reporter_is_registered()
    {
        var reporter = Substitute.For<ILeafCursorReporter>();
        var (grain, _, state) = CreateGrainWithReporter(reporter: reporter, registerReporter: false);

        Assert.DoesNotThrowAsync(async () => await grain.ClearGrainStateAsync());

        await reporter.DidNotReceive().UnregisterAsync(
            Arg.Any<string>(), Arg.Any<string>(), Arg.Any<CancellationToken>());
        Assert.That(state.State.TreeId, Is.Null.Or.Empty);
    }

    [Test]
    public async Task ClearGrainState_does_not_retire_a_pin_it_could_never_have_registered()
    {
        // ResolveConsumerIdBase gates registration on a bound tree id, so a leaf
        // with no tree id has provably never pinned anything. Calling Unregister
        // for it would be a guess at a consumer id that does not exist.
        var (grain, reporter, _) = CreateGrainWithReporter(treeId: null);

        await grain.ClearGrainStateAsync();

        await reporter.DidNotReceive().UnregisterAsync(
            Arg.Any<string>(), Arg.Any<string>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task ClearGrainState_leaves_the_leaf_reading_as_having_no_durable_state()
    {
        // Fact-pinning for the reasoning in issue #3101: the WAL GC classifies a
        // blocking pin by reading the leaf's row directly and reporting
        // WalGcBlockingPinState.NoDurableState when !RecordExists. That arm is
        // separately pinned (see the blocking-pin classifier fixture); what is
        // pinned HERE is the other half of the join - that reclaim is what puts
        // a leaf into that state. Without this, "the orphan's row is already
        // gone" is an assumption about the provider rather than a tested
        // property, and the case for the sweep retiring the pin rather than
        // clearing state rests on it.
        var (grain, _, state) = CreateGrainWithReporter();
        Assert.That(state.RecordExists, Is.True, "the row must exist first, or the assertion below is vacuous.");

        await grain.ClearGrainStateAsync();

        Assert.That(state.RecordExists, Is.False,
            "a reclaimed leaf must read as absent, which is what leaves the GC nothing to clear.");
    }

    [Test]
    public async Task A_leaf_with_no_tree_id_persists_nothing_when_the_gc_touches_it()
    {
        // The GC re-touches an orphan on every backoff, forever. If any part of
        // that touch persisted state, the orphan would accrete storage for the
        // life of the deployment - a second unbounded-growth channel alongside
        // the WAL one. WriteCount is the direct instrument: the existing
        // drive-verdict test asserts the checkpoint offset does not advance,
        // which is a proxy a write could satisfy without advancing.
        var (grain, _, state) = CreateGrainWithReporter(treeId: null);

        await ActivateAsync(grain);
        var writesAfterActivation = state.WriteCount;

        for (var touch = 0; touch < 5; touch++)
        {
            var verdict = await grain.DriveStarvedCheckpointAsync();
            Assert.That(verdict, Is.EqualTo(LeafStarvationDriveOutcome.NotDriven),
                "the premise of the write assertion is that every touch takes the unseeded early return.");
        }

        Assert.Multiple(() =>
        {
            Assert.That(writesAfterActivation, Is.Zero,
                "activating an unseeded leaf must not persist - the replay declines its permit before any write.");
            Assert.That(state.WriteCount, Is.Zero,
                "and no number of GC touches may persist either, or an orphan grows storage without bound.");
        });
    }
}
