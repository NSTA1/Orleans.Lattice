using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Testing;
using Orleans.Lattice.Tests.Fakes;
using System.Diagnostics.Metrics;
using System.Text;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// A zero refusal count must be readable (issue #2834).
/// <para>
/// <see cref="LatticeMetrics.LeafBisectRefusals"/> settles the leaf-division
/// forfeiture the moment it reads non-zero. At zero it settles nothing: an
/// over-threshold leaf that never divided may have sought a division and
/// completed one, or may never have sought one at all, and those have opposite
/// bearing on whether the forfeiture is the cause. The refusal counter cannot
/// separate them, because it is only written when a division is actually
/// attempted.
/// </para>
/// <para>
/// <see cref="LatticeMetrics.LeafSplitAttempts"/> is the second signal that
/// makes the zero interpretable, and these arms pin the property the whole
/// thing rests on: <b>"no division was ever sought" must be a positive reading,
/// not an absence.</b> A counter exports nothing until its first <c>Add</c>, so
/// without priming that state is indistinguishable from "the instrument was
/// never reached" - which reproduces, one level up, the exact ambiguity the
/// counter exists to remove.
/// </para>
/// <para>
/// <b>Every arm drives a public grain method.</b> The counter's entire purpose
/// is to answer whether production ever sought a division, so a fixture that
/// reached <c>SplitIfNeededUnderGateAsync</c> by reflection would prove the
/// counter increments when the method is called and nothing about whether
/// anything calls it - leaving the instrument's own subject untested. That call
/// is gated behind <c>IsLeafOverCapacity</c> at both of its commit-path sites,
/// which is precisely the shape issue #2735 was raised about.
/// </para>
/// </summary>
public sealed class LeafSplitAttemptAccountingTests
{
    private sealed record Measurement(long Value, string Outcome);

    private static MeterListener ListenForAttempts(List<Measurement> sink)
        => MeterListening.StartForInstrument(
            LatticeMetrics.LeafSplitAttempts,
            l => l.SetMeasurementEventCallback<long>((_, value, tags, _) =>
            {
                var outcome = string.Empty;
                foreach (var tag in tags)
                {
                    if (tag.Key == LatticeMetrics.TagOutcome)
                    {
                        outcome = tag.Value?.ToString() ?? string.Empty;
                    }
                }

                lock (sink) sink.Add(new Measurement(value, outcome));
            }));

    private static string Key(int i) => $"k{i:D5}";

    private static LeafSnapshotRow[] Corpus(int rowCount)
    {
        var rows = new LeafSnapshotRow[rowCount];
        for (var i = 0; i < rowCount; i++)
        {
            rows[i] = new LeafSnapshotRow(
                Key(i),
                new LwwValue<byte[]>
                {
                    Value = new byte[64],
                    Timestamp = new HybridLogicalClock { WallClockTicks = 100L + i, Counter = i },
                });
        }

        return rows;
    }

    /// <summary>
    /// A leaf online from a snapshot, with a key bound the caller chooses so an
    /// arm can decide whether an ordinary write tips it over capacity.
    /// </summary>
    private static async Task<BPlusLeafGrain> RehydratedLeafAsync(int rowCount, int maxLeafKeys = 1_000_000)
    {
        var snapshotStub = Substitute.For<ILeafSnapshotStorageGrain>();
        snapshotStub.LoadAsync(Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<LeafSnapshotBlob?>(new LeafSnapshotBlob
            {
                SnapshotOffset = 25L,
                EncodedRows = LeafSnapshotCodec.Encode(Corpus(rowCount)),
                SnapshotOffsetsByPartition = [25L],
            }));

        var sibling = Substitute.For<IBPlusLeafGrain, IGrainBase>();
        var siblingContext = Substitute.For<IGrainContext>();
        siblingContext.GrainId.Returns(GrainId.Create("leaf", Guid.NewGuid().ToString()));
        ((IGrainBase)sibling).GrainContext.Returns(siblingContext);
        sibling.MergeEntriesAsync(Arg.Any<Dictionary<string, LwwValue<byte[]>>>())
            .Returns(Task.FromResult<SplitResult?>(null));
        sibling.InitializeSiblingAsync(Arg.Any<SiblingInitialization>()).Returns(Task.CompletedTask);
        sibling.SetCheckpointOffsetHintsAsync(Arg.Any<long[]>()).Returns(Task.CompletedTask);

        var grainFactory = Substitute.For<IGrainFactory>();
        grainFactory.GetGrain<ILeafSnapshotStorageGrain>(Arg.Any<Guid>()).Returns(snapshotStub);
        grainFactory.GetGrain<IBPlusLeafGrain>(Arg.Any<GrainId>()).Returns(sibling);
        grainFactory.GetGrain<IBPlusLeafGrain>(Arg.Any<Guid>()).Returns(sibling);

        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("leaf", Guid.NewGuid().ToString("N")));

        var state = new FakePersistentState<LeafNodeState>();
        state.State.TreeId = "tree-split-attempts";
        state.State.ShardIndex = 0;

        var grain = new BPlusLeafGrain(
            context,
            state,
            grainFactory,
            TestOptionsResolver.Create(
                baseOptions: new LatticeOptions
                {
                    WalPartitions = 1,
                    LeafPartialHydrationEnabled = true,
                    LeafHydrationResidentBytes = 4L * 1024,
                },
                maxLeafKeys: maxLeafKeys,
                shardCount: 1,
                factory: grainFactory),
            TestMutationObservers.NoObservers(),
            TestOriginClusterIdResolver.Default());

        Assert.That(
            await grain.TryRehydrateFromSnapshotAsync(CancellationToken.None),
            Is.True,
            "the leaf must come online from its snapshot");
        return grain;
    }

    // ---------------------------------------------------------------
    // The pair that matters. Neither arm means anything without the other:
    // the first shows the series EXISTS at zero, the second shows that
    // existence is caused by reaching the capture seam rather than by
    // activation, the fixture, or another test in the same process.
    // ---------------------------------------------------------------

    [Test]
    public async Task The_capture_seam_mints_every_outcome_at_zero_so_never_sought_is_readable()
    {
        var grain = await RehydratedLeafAsync(512);

        // A foreground write first, so the leaf unambiguously holds live data.
        // Without it the capture declines before it reaches the byte-overflow
        // check and mints nothing - which is itself the finding that the
        // reflected draft of this arm concealed, since calling the check
        // directly cannot observe a capture that never got there.
        await grain.SetAsync("zzz-live", Encoding.UTF8.GetBytes("v"));
        var before = grain.CacheForTest.Count;

        var measurements = new List<Measurement>();
        var declines = new List<string>();
        using (MeterListening.StartForInstrument(
            LatticeMetrics.LeafSnapshotCaptureDeclines,
            l => l.SetMeasurementEventCallback<long>((_, _, tags, _) =>
            {
                foreach (var tag in tags)
                {
                    if (tag.Key == LatticeMetrics.TagReason)
                    {
                        lock (declines) declines.Add(tag.Value?.ToString() ?? string.Empty);
                    }
                }
            })))
        using (ListenForAttempts(measurements))
        {
            // The public capture seam. Every snapshot-capture route reaches the
            // byte-overflow check through it, which is why the priming lives
            // there rather than at activation.
            await grain.CaptureSnapshotAsync();
        }

        // Precondition, established independently of the counter under test: a
        // capture that declines never reaches the priming site, so without this
        // an empty measurement list would be ambiguous between "the prime is
        // missing" and "the seam was never reached" - the very ambiguity this
        // fixture exists to eliminate.
        Assert.That(
            declines, Is.Empty,
            "the capture must actually proceed, or this arm measures nothing");

        // Established WITHOUT the counter: the leaf is structurally unchanged,
        // so nothing was divided. If the counter disagrees, the counter is
        // wrong - not this.
        Assert.That(grain.CacheForTest.Count, Is.EqualTo(before), "nothing may have been divided");

        Assert.Multiple(() =>
        {
            // The series EXISTS. This is the whole point: a reader can tell
            // "sought zero divisions" from "never looked".
            Assert.That(
                measurements.Select(m => m.Outcome).Distinct().OrderBy(o => o, StringComparer.Ordinal),
                Is.EquivalentTo(new[] { "already_under_capacity", "divided", "faulted", "gate_contended", "no_admissible_pivot" }),
                "every outcome must be minted, or the absent one is unreadable at zero");

            // And every one of them is a measured zero, not a count.
            Assert.That(
                measurements.Where(m => m.Value != 0),
                Is.Empty,
                "priming must not fabricate attempts that did not happen");
        });
    }

    [Test]
    public async Task A_leaf_that_never_reaches_the_capture_seam_mints_nothing_at_all()
    {
        // The control for the arm above. Without it, that arm cannot tell a
        // series minted BY THE SEAM from one minted by activation or by the
        // fixture - and "the series exists" would prove nothing about
        // reachability, which is the only property worth asserting.
        var grain = await RehydratedLeafAsync(512);

        var measurements = new List<Measurement>();
        using (ListenForAttempts(measurements))
        {
            Assert.That(grain.CacheForTest.Count, Is.EqualTo(512));
        }

        Assert.That(
            measurements,
            Is.Empty,
            "absence must remain distinguishable from a primed zero, or the prime proves nothing");
    }

    // ---------------------------------------------------------------
    // The outcomes, driven through the public write seam so the
    // IsLeafOverCapacity gate in front of the split is exercised rather
    // than bypassed.
    // ---------------------------------------------------------------

    [Test]
    public async Task A_division_reached_through_the_public_write_seam_is_counted_as_divided()
    {
        // maxLeafKeys is small, so an ordinary write tips the leaf over the
        // bound and the commit path's own IsLeafOverCapacity gate admits the
        // division - the wiring that can regress, and the reason this arm does
        // not reach for SplitIfNeededUnderGateAsync directly.
        var grain = await RehydratedLeafAsync(3, maxLeafKeys: 3);

        var measurements = new List<Measurement>();
        SplitResult? split;
        using (ListenForAttempts(measurements))
        {
            split = await grain.SetAsync("zzz-over", Encoding.UTF8.GetBytes("v"));
        }

        // Established without the counter: a division actually happened.
        Assert.That(split, Is.Not.Null, "precondition: the write must have driven a real division");

        Assert.Multiple(() =>
        {
            Assert.That(
                measurements.Where(m => m.Outcome == "divided").Sum(m => m.Value),
                Is.EqualTo(1),
                "a completed division must be counted exactly once");
            Assert.That(
                measurements.Where(m => m.Outcome == "gate_contended").Sum(m => m.Value),
                Is.Zero,
                "and must not also register as a refusal to evaluate");
        });
    }

    [Test]
    public async Task A_write_that_leaves_the_leaf_under_capacity_seeks_no_division()
    {
        var grain = await RehydratedLeafAsync(512);

        var measurements = new List<Measurement>();
        SplitResult? split;
        using (ListenForAttempts(measurements))
        {
            split = await grain.SetAsync("zzz-under", Encoding.UTF8.GetBytes("v"));
        }

        // Established without the counter.
        Assert.That(split, Is.Null, "precondition: an under-capacity leaf must not divide");

        Assert.That(
            measurements.Where(m => m.Outcome == "divided").Sum(m => m.Value),
            Is.Zero,
            "a write below the bound must never read as a division");
    }
}
