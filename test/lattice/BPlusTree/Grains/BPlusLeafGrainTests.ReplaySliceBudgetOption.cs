using System.Text.RegularExpressions;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Testing.Hygiene;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Regression coverage for issue #2898: the per-replay slice width was a
/// compiled constant, so the second of the two factors that set peak replay
/// memory was neither tunable nor derivable from configuration.
/// <para>
/// <b>What is deliberately NOT tested here, and why.</b> The issue proposed
/// promoting the duplicated constant to a shared scalar. That is the detector
/// that already failed: all three replay sites held a constant of the same
/// name <i>and the same value</i> for the whole period their <i>behaviour</i>
/// had diverged (issues #2742 through #2899), so every comparison of the two
/// values agreed while the defect was live. Nothing below compares a width
/// against a constant. The option is proved by driving the reader at a
/// non-default width and asserting the widths it actually <b>requests</b>, and
/// by inventorying the construction sites rather than counting them.
/// </para>
/// <para>
/// The behavioural arms and the two source gates are a pair and neither half
/// is sufficient. The behavioural arms prove a configured width changes what
/// the coordinator is asked for; the site gate proves every replay reads the
/// option rather than the default; the binding gate proves the reader's
/// fallback cannot drift from the option's default. Only together do they say
/// what is wanted - that one number governs every replay and can be moved.
/// </para>
/// </summary>
public partial class BPlusLeafGrainTests
{
    private const string BudgetOptionTreeId = "replay-slice-budget-option";

    private static ReplaySliceReader ConfiguredReader(
        PressuredReplayCoordinator coordinator, int budget) =>
        new(coordinator.Stub, BudgetOptionTreeId, partition: 0, initialBudget: budget);

    // ------------------------------------------------------- the option bites

    /// <summary>
    /// The first thing the option has to do: change the width the coordinator
    /// is actually asked for. The default arm is the positive control - the
    /// same harness, the same coordinator, the same call, and a different
    /// requested width - so a green here cannot be a harness that requests
    /// nothing.
    /// </summary>
    [Test]
    public async Task A_configured_width_is_the_width_the_coordinator_is_asked_for()
    {
        var configured = new PressuredReplayCoordinator(
            head: 9, sliceSize: 8, affordableBudget: 1024, PressuredReplayEntries(8));
        var control = new PressuredReplayCoordinator(
            head: 9, sliceSize: 8, affordableBudget: 1024, PressuredReplayEntries(8));

        await ConfiguredReader(configured, budget: 64)
            .ReadSliceAsync(-1, 8, onNarrowed: null, CancellationToken.None);

        // Positive control: identical harness, no configured width.
        await new ReplaySliceReader(control.Stub, BudgetOptionTreeId, partition: 0)
            .ReadSliceAsync(-1, 8, onNarrowed: null, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(
                configured.RequestedBudgets,
                Is.EqualTo(new[] { 64 }),
                "A reader given a configured width must ask the coordinator for that width. "
                + "Asserting on the width REQUESTED rather than on the reader's own field is the "
                + "point: a reader that stored the option and kept requesting 256 would satisfy any "
                + "check of its own state while the replay still drew the default.");
            Assert.That(
                control.RequestedBudgets,
                Is.EqualTo(new[] { LatticeOptions.DefaultWalReplaySliceBudget }),
                "Positive control. The same harness must observe the compiled default when no width "
                + "is configured, so the arm above is a measured difference and not a harness that "
                + "records whatever it is handed.");
            Assert.That(
                configured.RequestedBudgets,
                Is.Not.EqualTo(control.RequestedBudgets),
                "The two arms must differ, otherwise the test cannot distinguish an option that is "
                + "read from one that is ignored.");
        });
    }

    /// <summary>
    /// The widen-back ceiling has to follow the option, not the compiled
    /// default. This is the arm that fails if the option is plumbed into the
    /// starting width alone: such a reader starts at the configured width,
    /// narrows correctly under pressure, and then climbs back through it to
    /// 256 - so a host that lowered the width to fit its heap gets the default
    /// draw back on the first slice after any pressure blip, which is the worst
    /// possible moment for it.
    /// </summary>
    [Test]
    public async Task A_configured_width_is_also_the_ceiling_the_reader_widens_back_to()
    {
        var coordinator = new PressuredReplayCoordinator(
            head: 9, sliceSize: 8, affordableBudget: 16, PressuredReplayEntries(8));
        var reader = ConfiguredReader(coordinator, budget: 64);

        // Narrow: 64 refused, 16 affordable.
        await reader.ReadSliceAsync(-1, 8, onNarrowed: null, CancellationToken.None);

        // Pressure passes. Widen 16 -> 32 -> 64, then stay.
        coordinator.AffordableBudget = 1024;
        for (var i = 0; i < 3; i++)
            await reader.ReadSliceAsync(-1, 8, onNarrowed: null, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(
                coordinator.RequestedBudgets,
                Is.EqualTo(new[] { 64, 16, 32, 64, 64 }),
                "Width must be spent in quarters and recovered in doubles, and must plateau at the "
                + "CONFIGURED width. A reader that recovered towards the compiled default would "
                + "produce 64, 16, 32, 64, 128 here - identical for four reads and wrong on the "
                + "fifth, which is why this asserts the whole sequence rather than its last term.");
            Assert.That(
                coordinator.RequestedBudgets,
                Has.None.GreaterThan(64),
                "No read may ever be wider than the configured width. This is the operator's actual "
                + "guarantee: the number they set is an upper bound on the draw, not a starting "
                + "point the reader may climb past.");
        });
    }

    /// <summary>
    /// Narrowing depth is derived from the configured width rather than fixed,
    /// so a host that lowers the width also shortens the retry ladder instead
    /// of inheriting the default's five-step descent from a width it never
    /// requested.
    /// </summary>
    [Test]
    public void A_configured_width_sets_the_depth_of_the_narrowing_ladder()
    {
        var coordinator = new PressuredReplayCoordinator(
            head: 9, sliceSize: 8, affordableBudget: 0, PressuredReplayEntries(8));
        var reader = ConfiguredReader(coordinator, budget: 16);

        Assert.That(
            async () => await reader.ReadSliceAsync(-1, 8, onNarrowed: null, CancellationToken.None),
            Throws.InstanceOf<WalReadUnderPressureException>(),
            "Nothing is affordable, so the ladder must run to its floor and then propagate.");

        Assert.That(
            coordinator.RequestedBudgets,
            Is.EqualTo(new[] { 16, 4, 1 }),
            "Integer division by four floored at one, started from the CONFIGURED width: a 16-wide "
            + "reader has two narrowings to make, where the 256-wide default has four. A ladder of "
            + "256, 64, 16, 4, 1 here would mean the option reached neither the start nor the depth.");
    }

    /// <summary>
    /// A width below one cannot be served - a zero-width read returns nothing
    /// and the replay loop never advances - so the reader clamps to the
    /// narrowest legal read rather than throwing.
    /// <para>
    /// The clamp is unreachable through supported configuration, because the
    /// validator refuses the same values at startup. It is tested anyway, and
    /// the asymmetry is deliberate: the reader is constructed during activation
    /// on the recovery path, where throwing would convert a misconfiguration
    /// into an unreplayable partition holding the whole-tree WAL GC pin - the
    /// exact failure loop issues #2742 and #2867 exist to break. Refusing at
    /// configuration time and clamping at activation time are the correct
    /// behaviours for their respective seams, not a disagreement.
    /// </para>
    /// </summary>
    [TestCase(0)]
    [TestCase(-1)]
    [TestCase(int.MinValue)]
    public void A_configured_width_below_one_is_clamped_to_a_single_entry(int budget)
    {
        var coordinator = new PressuredReplayCoordinator(
            head: 9, sliceSize: 8, affordableBudget: 1024, PressuredReplayEntries(8));
        var reader = ConfiguredReader(coordinator, budget);

        Assert.That(
            reader.Budget,
            Is.EqualTo(1),
            "A width the reader cannot serve must become the narrowest one it can.");

        Assert.That(
            async () => await reader.ReadSliceAsync(-1, 8, onNarrowed: null, CancellationToken.None),
            Throws.Nothing,
            "Clamping must leave a working reader, not a reader that defers the failure to its "
            + "first read.");

        Assert.That(
            coordinator.RequestedBudgets,
            Is.EqualTo(new[] { 1 }),
            "The clamped width must be the width requested, not merely the width stored.");
    }

    // ----------------------------------------------------------- source gates

    /// <summary>
    /// The gate that makes the option's reach durable rather than momentary.
    /// <para>
    /// Plumbing the option into the three sites that exist today does nothing
    /// to stop a fourth being written that constructs the reader on the default
    /// - which would silently draw 256 on a host that had configured 32, and
    /// would look correct at every site anyone thought to read. This asserts
    /// the property from the other side: every construction in <c>src/</c>
    /// passes a width, and that width is the option.
    /// </para>
    /// <para>
    /// This inventories the sites rather than counting them. A count returns a
    /// reassuring small number whether or not the sites counted are the ones
    /// intended, and "uncovered sibling site" is the failure this whole family
    /// of issues is about - issue #2898 itself named two replay sites when
    /// there are three.
    /// </para>
    /// </summary>
    [Test]
    public void Every_replay_site_constructs_the_reader_with_the_configured_width()
    {
        var root = HygieneRepository.FindRepoRoot();
        var src = Path.Combine(root, "src");

        // Capture the whole argument list up to the closing parenthesis, so the
        // gate can see WHAT was passed and not merely that a construction
        // happened. Constructions span lines, hence Singleline.
        var construction = new Regex(
            @"new\s+ReplaySliceReader\s*\((?<args>[^;]*?)\)\s*;",
            RegexOptions.Compiled | RegexOptions.Singleline);

        var sites = new List<(string File, string Args)>();
        foreach (var file in HygieneRepository.EnumerateFiles(src, "*.cs"))
        {
            if (HygieneRepository.HasExcludedSegment(file))
                continue;

            foreach (Match m in construction.Matches(File.ReadAllText(file)))
                sites.Add((Path.GetFileName(file), m.Groups["args"].Value));
        }

        var files = sites.Select(s => s.File).Distinct().OrderBy(n => n, StringComparer.Ordinal).ToList();
        var onDefault = sites
            .Where(s => !s.Args.Contains(nameof(LatticeOptions.WalReplaySliceBudget), StringComparison.Ordinal))
            .Select(s => s.File)
            .OrderBy(n => n, StringComparer.Ordinal)
            .ToList();

        Assert.Multiple(() =>
        {
            // Anti-vacuity. A scan that matches nothing passes every assertion
            // below it, so a gate with no self-test silently becomes a no-op the
            // day its pattern, its root or its file filter stops matching - and
            // reports green while doing it.
            Assert.That(
                sites,
                Is.Not.Empty,
                "The scan matched no construction of ReplaySliceReader at all, so it cannot be "
                + "gating anything. Either the pattern, the repository root or the file filter has "
                + "stopped matching.");

            Assert.That(
                files,
                Is.EqualTo(new[]
                {
                    "BPlusLeafGrain.Activation.cs",
                    "BPlusLeafGrain.FrozenBaseline.cs",
                    "SnapshotLeafGrain.cs",
                }),
                "These are the three replay sites. A file listed here that is not one of them is a "
                + "new replay whose width this gate has not been told about; a missing one has "
                + "stopped replaying through the shared reader entirely.");

            Assert.That(
                onDefault,
                Is.Empty,
                "Every construction must pass LatticeOptions.WalReplaySliceBudget. A site left on "
                + "the default parameter compiles, runs, narrows and widens correctly, and quietly "
                + "draws the compiled 256 on a host that configured less - which is invisible "
                + "everywhere except in the memory the option was added to bound.");
        });
    }

    /// <summary>
    /// The reader's fallback default must be BOUND to the option's default, not
    /// equal to it.
    /// <para>
    /// An equality assertion between two independently declared constants is
    /// exactly the detector that failed between issues #2742 and #2899 - it
    /// passes for as long as nobody changes one of them, and it can be deleted.
    /// A compile-time binding cannot be deleted and cannot drift: changing the
    /// option's default changes the reader's, and removing it is a build error.
    /// So this gate asserts the binding <i>exists in source</i>, which is the
    /// only form the property takes at runtime - by construction the two are
    /// the same value, so comparing them would be a tautology that can never
    /// redden.
    /// </para>
    /// </summary>
    [Test]
    public void The_readers_fallback_width_is_bound_to_the_option_default_rather_than_repeating_it()
    {
        var root = HygieneRepository.FindRepoRoot();
        var reader = Path.Combine(root, "src", "lattice", "BPlusTree", "Grains", "ReplaySliceReader.cs");

        Assert.That(File.Exists(reader), Is.True, $"Expected the reader at {reader}.");

        var declaration = new Regex(
            @"const\s+int\s+InitialBudget\s*=\s*(?<rhs>[^;]+);",
            RegexOptions.Compiled);
        var match = declaration.Match(File.ReadAllText(reader));

        Assert.That(
            match.Success,
            Is.True,
            "The scan found no declaration of InitialBudget, so it cannot be gating anything - the "
            + "constant has been renamed or removed and this gate has silently become a no-op.");

        Assert.That(
            match.Groups["rhs"].Value.Trim(),
            Is.EqualTo($"{nameof(LatticeOptions)}.{nameof(LatticeOptions.DefaultWalReplaySliceBudget)}"),
            "The reader's fallback width must be a compile-time reference to the option's default, "
            + "never a repeated literal. Two constants holding 256 agree until one moves, and agree "
            + "silently while they disagree with behaviour - which is precisely how the slice width "
            + "drifted across three sites from issue #2742 to #2899 without a single test failing.");
    }
}
