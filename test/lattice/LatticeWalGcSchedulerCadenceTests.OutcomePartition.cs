using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests;

/// <summary>
/// The pass-outcome partition of the WAL GC counter (issue #2850).
/// <para>
/// Before this, the outcome tag was selected by
/// <c>reclaimed ? ... : blocked ? ... : idle</c>, which makes <c>idle</c> a
/// catch-all: it absorbed a tree that evaluated a usable floor and found
/// nothing to trim, a tree with no consumer cursor at all, and any floor state
/// a later build might add. Those are not the same condition and they do not
/// warrant the same response - the first is health, the second is a tree whose
/// WAL cannot shrink because nobody has told the GC where the frontier is.
/// </para>
/// <para>
/// The cost of collapsing them is not cosmetic. Epic #2368 scores a release
/// criterion off this instrument, and reads <c>blocked = 0</c> as evidence of
/// reclamation. With a catch-all <c>idle</c>, <c>blocked = 0</c> only says that
/// one named predicate did not fire; a tree stuck at <c>no_consumer</c> forever
/// reports exactly the same shape as a healthy quiescent one. An affirmative
/// arm is what turns the absence of a negative into evidence of a positive.
/// </para>
/// <para>
/// Every test here drives the REAL <see cref="LatticeWalGc"/>, and establishes
/// its precondition from state the classifier does not read - the cursor
/// registry, the WAL provider's contents, the durable pin grain. Asserting an
/// arm against a hand-built report carrying the very floor state the arm is
/// derived from would only show the instrument agreeing with itself.
/// </para>
/// </summary>
public sealed partial class LatticeWalGcSchedulerCadenceTests
{
    private static HybridLogicalClock OutcomeHlc(long ticks) =>
        new() { WallClockTicks = ticks, Counter = 0 };

    private static WalEntry OutcomeEntry(string tree, long offset, HybridLogicalClock ts) => new()
    {
        Offset = offset,
        Mutation = new LatticeMutation
        {
            TreeId = tree,
            Kind = MutationKind.Set,
            Key = $"k{offset}",
            Value = new byte[] { 1 },
            Timestamp = ts,
            OriginClusterId = "site-a",
        },
    };

    private static IOptionsMonitor<LatticeOptions> SinglePartitionMonitor()
    {
        var monitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        var options = new LatticeOptions { WalPartitions = 1 };
        monitor.CurrentValue.Returns(options);
        monitor.Get(Arg.Any<string>()).Returns(options);
        return monitor;
    }

    /// <summary>
    /// A real collector over an in-memory WAL. <paramref name="durablePins"/> is
    /// null when no pin grain should be resolvable at all, which is the
    /// bare-service-provider shape a tree with no materialiser pins presents.
    /// </summary>
    private static ILatticeWalGc RealGc(
        IWalStorageProvider provider,
        IWalCursorRegistry registry,
        IReadOnlyDictionary<string, HybridLogicalClock>? durablePins = null)
    {
        var sc = new ServiceCollection();
        sc.AddSingleton(provider);

        if (durablePins is not null)
        {
            var pinGrain = Substitute.For<IWalMaterialiserPinGrain>();
            pinGrain.GetPinsAsync().Returns(Task.FromResult(durablePins));

            var factory = Substitute.For<IGrainFactory>();
            factory.GetGrain<IWalMaterialiserPinGrain>(Arg.Any<string>()).Returns(pinGrain);
            sc.AddSingleton(factory);
        }

        return new LatticeWalGc(sc.BuildServiceProvider(), registry, SinglePartitionMonitor());
    }

    /// <summary>
    /// Runs one scheduler pass over <paramref name="tree"/> against a real
    /// collector and returns the single outcome arm that advanced.
    /// </summary>
    private static async Task<(string Arm, InstrumentRecorder Passes)> OutcomeOfOnePassAsync(
        string tree,
        ILatticeWalGc gc)
    {
        var time = new VirtualTimeProvider();
        var passes = new InstrumentRecorder(LatticeMetrics.WalGcPasses, tree);
        var scheduler = CreateScheduler(FactoryWithTrees(tree), gc, Adaptive(), time);
        await StartAndRunFirstPassAsync(scheduler, time);
        await scheduler.StopAsync(CancellationToken.None);

        var advanced = passes.Counted;
        Assert.That(advanced, Has.Count.EqualTo(1),
            "a completed pass must advance exactly one outcome arm.");
        return ((advanced[0].Tag(LatticeMetrics.TagOutcome) as string)!, passes);
    }

    // ------------------------------------------- the arm the catch-all hid

    [Test]
    public async Task A_tree_with_no_consumer_cursor_reports_no_consumer_rather_than_idle()
    {
        // The defect of issue #2850, reproduced from independent state: a WAL
        // with entries in it, and a cursor registry nobody has reported to. The
        // GC has no frontier, so it evaluates no cursor predicate and trims
        // nothing. That is not idleness - the WAL cannot shrink at all - but
        // before the fix it was reported as `idle`.
        const string Tree = "walgc-outcome-no-consumer";
        var provider = new InMemoryWalStorageProvider();
        await provider.AppendBatchAsync(
            Tree, 0,
            new[] { OutcomeEntry(Tree, 0, OutcomeHlc(10)), OutcomeEntry(Tree, 1, OutcomeHlc(20)) },
            CancellationToken.None);

        var (arm, passes) = await OutcomeOfOnePassAsync(Tree, RealGc(provider, new InMemoryWalCursorRegistry()));
        using var _ = passes;

        // Independent corroboration that `idle` would have been a false
        // reading: the entries are all still there.
        var survivors = new List<long>();
        await foreach (var entry in provider.ReadAsync(Tree, 0, -1, 100, CancellationToken.None))
        {
            survivors.Add(entry.Offset);
        }

        Assert.Multiple(() =>
        {
            Assert.That(arm, Is.EqualTo("no_consumer"),
                "a tree with no reported cursor has its own arm: it is the state in which the WAL "
                + "cannot shrink for want of a frontier, and it must not be readable as health.");
            Assert.That(survivors, Is.EqualTo(new[] { 0L, 1L }),
                "and nothing was reclaimed, which is what makes reporting this as idle a lie "
                + "rather than a naming quibble.");
        });
    }

    [Test]
    public async Task Idle_now_means_a_floor_was_evaluated_and_nothing_was_eligible()
    {
        // The other side of the same partition, and the reason it is worth
        // having. After the fix `idle` carries a positive claim - a usable floor
        // existed and the WAL held nothing below it - so a reader can act on it.
        // The precondition is established by the registry (a consumer reported a
        // cursor) and by the provider (the WAL is empty), neither of which is
        // the classifier under test.
        const string Tree = "walgc-outcome-idle";
        var registry = new InMemoryWalCursorRegistry();
        await registry.ReportCursorAsync(Tree, "shipper", OutcomeHlc(30));

        var (arm, passes) = await OutcomeOfOnePassAsync(
            Tree, RealGc(new InMemoryWalStorageProvider(), registry));
        using var _ = passes;

        Assert.That(arm, Is.EqualTo("idle"),
            "a usable floor with nothing beneath it is the only thing idle may now mean.");
    }

    [Test]
    public async Task A_pass_that_reclaims_reports_reclaimed()
    {
        // The affirmative arm the epic's criterion actually reads, driven end to
        // end so its meaning is anchored to real reclamation rather than to a
        // report field a test set.
        const string Tree = "walgc-outcome-reclaimed";
        var provider = new InMemoryWalStorageProvider();
        await provider.AppendBatchAsync(
            Tree, 0,
            new[] { OutcomeEntry(Tree, 0, OutcomeHlc(10)) },
            CancellationToken.None);

        var registry = new InMemoryWalCursorRegistry();
        await registry.ReportCursorAsync(Tree, "shipper", OutcomeHlc(30));

        var (arm, passes) = await OutcomeOfOnePassAsync(Tree, RealGc(provider, registry));
        using var _ = passes;

        Assert.That(arm, Is.EqualTo("reclaimed"));
    }

    [Test]
    public async Task An_unusable_durable_pin_reports_blocked()
    {
        // Blocked stays blocked. The partition widens what `idle` excludes; it
        // must not move anything out of the arm the blocked-leaf remedy reads.
        const string Tree = "walgc-outcome-blocked";
        var provider = new InMemoryWalStorageProvider();
        await provider.AppendBatchAsync(
            Tree, 0,
            new[] { OutcomeEntry(Tree, 0, OutcomeHlc(10)) },
            CancellationToken.None);

        var registry = new InMemoryWalCursorRegistry();
        await registry.ReportCursorAsync(Tree, "shipper", OutcomeHlc(30));

        var pins = new Dictionary<string, HybridLogicalClock>(StringComparer.Ordinal)
        {
            [$"_lattice_materialiser_{Tree}_leaf-1"] = HybridLogicalClock.Zero,
        };

        var (arm, passes) = await OutcomeOfOnePassAsync(Tree, RealGc(provider, registry, pins));
        using var _ = passes;

        Assert.That(arm, Is.EqualTo("blocked"));
    }

    // ------------------------------------------------ the partition is total

    [Test]
    public async Task No_cursor_floor_state_other_than_Available_is_reported_as_idle()
    {
        // The structural half, and the invariant the issue is actually about:
        // `idle` must name exactly one state. Every other declared state -
        // including any a later build adds - has to land on an arm of its own,
        // or the catch-all is back and `blocked = 0` stops being evidence of
        // anything.
        //
        // This is the one test here that drives a synthetic report rather than
        // real collector state, and it has to: it enumerates the enum, and a
        // member that does not exist yet cannot be produced from a real WAL by
        // definition. It is asserting the mapping is total, not what any
        // particular state means - the fixtures above establish those from
        // independent state.
        var states = Enum.GetValues<WalGcCursorFloorState>();
        Assert.That(states, Is.Not.Empty, "the scan must not be vacuous.");

        foreach (var state in states)
        {
            var tree = $"walgc-outcome-total-{(int)state}";
            var gc = Substitute.For<ILatticeWalGc>();
            gc.RunOnceAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
                .Returns(_ => Task.FromResult(Report(entriesTrimmed: 0, cursorFloorState: state)));

            var (arm, passes) = await OutcomeOfOnePassAsync(tree, gc);
            using var _ = passes;

            Assert.That(arm, Is.EqualTo(ArmForState(state)),
                $"floor state {state} must report its own arm.");

            if (state != WalGcCursorFloorState.Available)
            {
                Assert.That(arm, Is.Not.EqualTo("idle"),
                    $"{state} must not be absorbed by idle: that is exactly the collapse that made "
                    + "blocked = 0 unreadable as evidence of reclamation.");
            }
        }
    }

    [Test]
    public async Task A_floor_state_this_build_does_not_name_reports_unclassified()
    {
        // The escape hatch, asserted rather than assumed. A value outside the
        // declared enum stands in for the member a future change adds, and it
        // must surface as its own arm rather than inheriting idle's meaning.
        const string Tree = "walgc-outcome-unclassified";
        var gc = Substitute.For<ILatticeWalGc>();
        gc.RunOnceAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromResult(Report(
                entriesTrimmed: 0,
                cursorFloorState: (WalGcCursorFloorState)99)));

        var (arm, passes) = await OutcomeOfOnePassAsync(Tree, gc);
        using var _ = passes;

        Assert.That(arm, Is.EqualTo("unclassified"));
    }

    /// <summary>
    /// The arm a given floor state must produce on a pass that reclaimed
    /// nothing.
    /// </summary>
    private static string ArmForState(WalGcCursorFloorState state) => state switch
    {
        WalGcCursorFloorState.Available => "idle",
        WalGcCursorFloorState.NoCursorReported => "no_consumer",
        WalGcCursorFloorState.BlockedByUnusablePin => "blocked",
        _ => "unclassified",
    };
}
