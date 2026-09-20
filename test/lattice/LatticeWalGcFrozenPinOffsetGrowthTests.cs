using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Regression tests for issue #3310: the consequence, at the GC seam, of a
/// durable leaf-materialiser pin whose <em>offset</em> stops restamping while
/// the leaf's live checkpoint keeps advancing.
/// <para>
/// This is the far end of the causal chain whose near end
/// <c>LeafCursorReporterShedCeilingTests</c> covers. There, the pin-shed window
/// in <see cref="WalMaterialiserPinPressure"/> suppresses the only write path
/// that carries an advancing <c>CheckpointOffset</c>, so the durable pin offset
/// freezes. Here, that frozen offset is taken as a given and the question is
/// what the GC does with it: the answer is that the offset floor pins the trim
/// point at the stale offset, the head runs away from it, and the gap - and so
/// the retained WAL - grows with no bound anywhere in the system.
/// </para>
/// <para>
/// Note carefully which floor binds. The leaf is <b>present</b> in the cursor
/// registry with a fresh cursor, so the HLC floor is governed by that fresher
/// in-memory value and advances normally; only the offset floor reads the stale
/// durable pin. That asymmetry is what reproduces the production signature on
/// <c>repo-context-vector-index</c>, where the durable checkpoint sat far
/// <em>above</em> a frozen coverage floor and the recorded trim-stop reason was
/// <c>offset_floor</c> and nothing else.
/// </para>
/// <para>
/// The fix is emphatically <b>not</b> to drop or weaken the floor. A floor that
/// released WAL it could not prove durable is issue #3300, the same seam failing
/// in the losing direction. The floor stays fail-closed; the bound is imposed
/// upstream, by forcing the suppressed pin report through so the floor has
/// something fresher to stand on. The second test below is that remedy observed
/// from the GC's side.
/// </para>
/// </summary>
[TestFixture]
public sealed class LatticeWalGcFrozenPinOffsetGrowthTests
{
    private const string Tree = "tree";
    private const string LeafConsumer = "_lattice_materialiser_tree_leaf-1";
    private const string Shipper = "shipper";

    /// <summary>Entries appended per wave; each wave is one GC pass.</summary>
    private const int WaveSize = 8;

    /// <summary>Number of waves. Three is enough to show a trend rather than a step.</summary>
    private const int Waves = 4;

    private readonly Dictionary<string, HybridLogicalClock> _durablePins =
        new(StringComparer.Ordinal);

    private readonly Dictionary<string, long> _durableOffsets =
        new(StringComparer.Ordinal);

    [SetUp]
    public void Reset()
    {
        _durablePins.Clear();
        _durableOffsets.Clear();
    }

    private static HybridLogicalClock Hlc(long ticks, int counter = 0) =>
        new() { WallClockTicks = ticks, Counter = counter };

    private static WalEntry Entry(long offset, HybridLogicalClock ts) => new()
    {
        Offset = offset,
        Mutation = new LatticeMutation
        {
            TreeId = Tree,
            Kind = MutationKind.Set,
            Key = $"k{offset}",
            Value = new byte[64],
            Timestamp = ts,
            OriginClusterId = "site-a",
        },
    };

    private static IOptionsMonitor<LatticeOptions> Monitor()
    {
        var monitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        var options = new LatticeOptions { WalPartitions = 1 };
        monitor.CurrentValue.Returns(options);
        monitor.Get(Arg.Any<string>()).Returns(options);
        return monitor;
    }

    /// <summary>
    /// Wires a pin grain whose reads project the <b>live</b> dictionaries, so a
    /// test can restamp the durable offset between GC passes exactly as a
    /// forced pin report would.
    /// </summary>
    private IServiceProvider Services(IWalStorageProvider provider)
    {
        var sc = new ServiceCollection();
        sc.AddSingleton(provider);

        var pinGrain = Substitute.For<IWalMaterialiserPinGrain>();
        pinGrain.GetPinsAsync().Returns(
            _ => Task.FromResult<IReadOnlyDictionary<string, HybridLogicalClock>>(
                new Dictionary<string, HybridLogicalClock>(_durablePins, StringComparer.Ordinal)));
        pinGrain.GetPinOffsetsAsync().Returns(
            _ => Task.FromResult<IReadOnlyDictionary<string, long>>(
                new Dictionary<string, long>(_durableOffsets, StringComparer.Ordinal)));

        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<IWalMaterialiserPinGrain>(Arg.Any<string>()).Returns(pinGrain);
        sc.AddSingleton(factory);

        return sc.BuildServiceProvider();
    }

    private static async Task<int> RetainedCountAsync(IWalStorageProvider provider)
    {
        var count = 0;
        await foreach (var _ in provider.ReadAsync(
            Tree, 0, fromOffsetExclusive: -1, maxEntries: 10_000, CancellationToken.None))
        {
            count++;
        }

        return count;
    }

    /// <summary>
    /// Appends one wave and advances every <em>live</em> cursor to its head, so
    /// the leaf's checkpoint is demonstrably moving. The durable pin offset is
    /// deliberately left alone - that is the defect under test.
    /// </summary>
    private static async Task AppendWaveAsync(
        InMemoryWalStorageProvider provider,
        InMemoryWalCursorRegistry registry,
        int wave)
    {
        var entries = new WalEntry[WaveSize];
        for (var i = 0; i < WaveSize; i++)
        {
            var offset = (wave * WaveSize) + i;
            entries[i] = Entry(offset, Hlc(100 + offset));
        }

        await provider.AppendBatchAsync(Tree, 0, entries, CancellationToken.None);

        var head = Hlc(100 + ((wave + 1) * WaveSize) - 1);
        await registry.ReportCursorAsync(Tree, Shipper, head);
        await registry.ReportCursorAsync(Tree, LeafConsumer, head);
    }

    [Test]
    public async Task A_pin_offset_that_stops_restamping_lets_retained_wal_grow_without_bound()
    {
        // The reproduction. The leaf's live checkpoint advances to the head on
        // every wave, but its DURABLE pin offset never restamps past 0 because
        // the write that would carry it is being shed upstream. The offset
        // floor is sound and therefore refuses to trim anything at or above
        // offset 0 - which is the entire log - so every appended byte is
        // retained for as long as the freeze lasts.
        var provider = new InMemoryWalStorageProvider();
        var registry = new InMemoryWalCursorRegistry();

        _durablePins[LeafConsumer] = Hlc(100);
        _durableOffsets[LeafConsumer] = 0;

        var sut = new LatticeWalGc(Services(provider), registry, Monitor());

        var retained = new List<int>();
        var trimmed = new List<long>();

        for (var wave = 0; wave < Waves; wave++)
        {
            await AppendWaveAsync(provider, registry, wave);
            var report = await sut.RunOnceAsync(Tree);
            trimmed.Add(report.EntriesTrimmed);
            retained.Add(await RetainedCountAsync(provider));
        }

        Assert.Multiple(() =>
        {
            Assert.That(trimmed.Skip(1), Is.All.Zero,
                "After the first pass clears the single entry AT the frozen floor there is nothing else at or below it, so no later pass reclaims anything at all.");

            Assert.That(trimmed[0], Is.EqualTo(1),
                "The checkpoint offset is scanned-THROUGH, so the entry at the floor itself is trimmable exactly once. Asserting this rather than eliding it keeps the test honest about the floor's inclusive semantics.");

            Assert.That(retained, Is.Ordered.Ascending.And.Unique,
                "Retention must be shown to GROW, pass over pass - a single large reading could be an ordinary backlog rather than an unbounded one.");

            Assert.That(retained[^1], Is.EqualTo((Waves * WaveSize) - 1),
                "Every entry ever appended above the frozen floor is still retained: the gap between that floor and the advancing head is the whole log, and nothing in the GC bounds it.");
        });

        // The point of the assertion above is the SHAPE, not the magnitude: the
        // run is bounded only by how long the test chooses to append. Nothing
        // observed here converges, and no ceiling elsewhere in the GC intervenes
        // - LatticeWalGc's byte ceiling is advisory and cannot force a trim past
        // a sound floor. That is why the bound has to be imposed upstream, on
        // the reporting path, rather than here.
        Assert.That(
            _durableOffsets[LeafConsumer],
            Is.Zero,
            "Sanity: the freeze is the test's premise - if something restamped the pin the growth result above would be vacuous.");
    }

    [Test]
    public async Task A_restamped_pin_offset_bounds_the_retained_wal_without_lowering_the_floor()
    {
        // The remedy observed from the GC's side. Identical to the test above
        // for the first half of the run; then the durable pin offset restamps
        // to the leaf's true checkpoint, exactly as a forced pin report makes it
        // do. The floor is not lowered, weakened, or bypassed - it is simply
        // given a fresher, still-provably-durable value to stand on, and the
        // retained log collapses to the genuine live tail.
        var provider = new InMemoryWalStorageProvider();
        var registry = new InMemoryWalCursorRegistry();

        _durablePins[LeafConsumer] = Hlc(100);
        _durableOffsets[LeafConsumer] = 0;

        var sut = new LatticeWalGc(Services(provider), registry, Monitor());

        for (var wave = 0; wave < Waves; wave++)
        {
            await AppendWaveAsync(provider, registry, wave);
            await sut.RunOnceAsync(Tree);
        }

        var retainedWhileFrozen = await RetainedCountAsync(provider);

        // The forced report. Its offset is the leaf's real scanned-through
        // checkpoint, which is what ResolveDurablePinForPartition clamps to
        // min(checkpoint, covered) before it is ever written - a forced report
        // cannot overstate durability, only deliver a true value sooner. It is
        // deliberately held short of the head: a leaf that has genuinely caught
        // up would not have been shedding, so restamping to the head would test
        // a state the defect cannot produce.
        var restampedTo = (Waves * WaveSize) - 4;
        _durableOffsets[LeafConsumer] = restampedTo;
        _durablePins[LeafConsumer] = Hlc(100 + restampedTo);

        var afterRestamp = await sut.RunOnceAsync(Tree);
        var retainedAfter = await RetainedCountAsync(provider);

        Assert.Multiple(() =>
        {
            Assert.That(afterRestamp.EntriesTrimmed, Is.GreaterThan(0),
                "Restamping the durable offset is what unblocks the trim; if it did not, the upstream bound would buy nothing.");

            Assert.That(retainedAfter, Is.LessThan(retainedWhileFrozen),
                "Retention must fall once coverage restamps - that is the bound.");

            Assert.That(retainedAfter, Is.GreaterThan(0),
                "The tail above the restamped floor is still retained. A trim that emptied the log would mean the floor had been dropped rather than advanced, which is the issue #3300 failure this fix must not introduce.");
        });
    }

    [Test]
    public async Task A_restamped_floor_never_trims_past_the_offset_it_was_restamped_to()
    {
        // The fail-closed guard, stated as its own assertion rather than left
        // implicit in the test above. A forced pin report changes WHEN a true
        // offset is published, never WHAT is published, so the survivors after
        // a forced restamp must be exactly the tail at and above that offset.
        var provider = new InMemoryWalStorageProvider();
        var registry = new InMemoryWalCursorRegistry();

        _durablePins[LeafConsumer] = Hlc(100);
        _durableOffsets[LeafConsumer] = 0;

        var sut = new LatticeWalGc(Services(provider), registry, Monitor());

        for (var wave = 0; wave < Waves; wave++)
        {
            await AppendWaveAsync(provider, registry, wave);
            await sut.RunOnceAsync(Tree);
        }

        const int RestampedTo = (Waves * WaveSize) / 2;
        _durableOffsets[LeafConsumer] = RestampedTo;
        _durablePins[LeafConsumer] = Hlc(100 + RestampedTo);

        await sut.RunOnceAsync(Tree);

        var survivors = new List<long>();
        await foreach (var entry in provider.ReadAsync(
            Tree, 0, fromOffsetExclusive: -1, maxEntries: 10_000, CancellationToken.None))
        {
            survivors.Add(entry.Offset);
        }

        Assert.That(survivors, Is.Not.Empty);
        Assert.That(survivors[0], Is.EqualTo((long)RestampedTo + 1),
            "The lowest surviving offset is the one immediately above the restamped floor: the checkpoint is scanned-through, so the trim stops AT the proven-durable point and not one entry beyond it.");
    }
}
