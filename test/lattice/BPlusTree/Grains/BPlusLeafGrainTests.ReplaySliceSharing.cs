using System.Text.RegularExpressions;
using NSubstitute;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Testing.Hygiene;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Regression coverage for issue #2899: the narrow-and-retry that issue #2742
/// added to the activation-time replay reached only one of the three sites that
/// replay a WAL, so the two that were left behind are the two with the least
/// other protection.
/// <para>
/// <b>Why these tests assert behaviour and not a constant.</b> All three sites
/// held a slice-width constant named <c>ReplaySliceBudget</c> with the value
/// 256, and they still did after #2742 moved one of them onto a local that
/// narrows. So every comparison of the two <i>values</i> agreed, for the whole
/// period the <i>behaviour</i> was divergent, and the drift survived from #2742
/// to #2899 undetected. A scalar that can coincide is a weaker detector than
/// the thing itself, so nothing here compares widths between sites: the reader
/// is driven until it narrows, and the inventory gate is driven against the
/// call sites themselves.
/// </para>
/// <para>
/// The pair is deliberate and neither half is sufficient. The behavioural arms
/// prove the mechanism works; the inventory gate proves every site uses it.
/// Only together do they say what is actually wanted - that every WAL replay in
/// the library narrows under pressure - and only the gate extends that claim to
/// a fourth site nobody has written yet.
/// </para>
/// </summary>
public partial class BPlusLeafGrainTests
{
    private const string SharingTreeId = "replay-slice-sharing";

    private static ReplaySliceReader SharingReader(
        PressuredReplayCoordinator coordinator, int partition = 0) =>
        new(coordinator.Stub, SharingTreeId, partition);

    // --------------------------------------------------------- the mechanism

    /// <summary>
    /// The behaviour every site is now supposed to have: a read that is refused
    /// for memory pressure is retried narrower rather than propagating, and the
    /// caller gets its entries.
    /// </summary>
    [Test]
    public async Task The_shared_reader_narrows_and_retries_rather_than_propagating()
    {
        var coordinator = new PressuredReplayCoordinator(
            head: 8, sliceSize: 8, affordableBudget: 16, PressuredReplayEntries(8));
        var reader = SharingReader(coordinator);

        var slice = await reader.ReadSliceAsync(-1, 8, onNarrowed: null, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(
                coordinator.RequestedBudgets,
                Is.EqualTo(new[] { 256, 64, 16 }),
                "The reader must spend width in quarters until the read is affordable, rather than "
                + "abandoning the range at the first refusal.");
            Assert.That(coordinator.Refusals, Is.EqualTo(2), "Both over-wide reads must have been refused.");
            Assert.That(slice, Is.Not.Empty, "The narrowed read must still return the caller's entries.");
        });
    }

    /// <summary>
    /// Narrowing is bounded, not merely convergent. A width of one is the
    /// narrowest read there is, so there is no cheaper attempt to make and the
    /// failure must reach the caller for it to decide what an unreplayable
    /// partition means - which differs by site, and is why the reader rethrows
    /// rather than swallowing.
    /// </summary>
    [Test]
    public void The_shared_reader_gives_up_at_a_single_entry_instead_of_retrying_forever()
    {
        var coordinator = new PressuredReplayCoordinator(
            head: 8, sliceSize: 8, affordableBudget: 0, PressuredReplayEntries(8));
        var reader = SharingReader(coordinator);

        Assert.That(
            async () => await reader.ReadSliceAsync(-1, 8, onNarrowed: null, CancellationToken.None),
            Throws.InstanceOf<WalReadUnderPressureException>(),
            "Once the width reaches one there is nothing narrower to try, so the failure must propagate.");

        Assert.That(
            coordinator.RequestedBudgets,
            Is.EqualTo(new[] { 256, 64, 16, 4, 1 }),
            "Narrowing is integer division by four floored at one, so a 256-wide read can narrow at "
            + "most four times before giving up - a bounded retry, not an unbounded one.");
    }

    /// <summary>
    /// Width is spent, not surrendered. Without widening back, one pressure
    /// blip would pin the rest of a multi-million-entry range at a single entry
    /// per slice, which converges so slowly it is indistinguishable from the
    /// stall the narrowing exists to break.
    /// </summary>
    [Test]
    public async Task The_shared_reader_widens_back_once_the_pressure_passes()
    {
        var coordinator = new PressuredReplayCoordinator(
            head: 8, sliceSize: 1, affordableBudget: 16, PressuredReplayEntries(8));
        var reader = SharingReader(coordinator);

        await reader.ReadSliceAsync(-1, 8, onNarrowed: null, CancellationToken.None);
        var settled = coordinator.RequestedBudgets[^1];

        coordinator.AffordableBudget = 256;
        await reader.ReadSliceAsync(0, 8, onNarrowed: null, CancellationToken.None);
        var afterRecovery = coordinator.RequestedBudgets[^1];

        Assert.Multiple(() =>
        {
            Assert.That(
                settled,
                Is.EqualTo(16),
                "The first read must narrow until it reaches the affordable width.");
            Assert.That(
                afterRecovery,
                Is.GreaterThan(settled),
                "The next read must ASK the coordinator for more than the width pressure forced it "
                + "down to. Asserting on the width actually requested is the point: a reader that "
                + "widened an internal field but kept requesting the narrow width would satisfy any "
                + "check of its own state while the replay stayed crippled.");
        });
    }

    /// <summary>
    /// The callback is the only way a site can describe a narrowing in its own
    /// terms, and it must report the width that was actually adopted - a site
    /// logging the pre-narrowing width would tell an operator the opposite of
    /// what happened.
    /// </summary>
    [Test]
    public async Task The_shared_reader_reports_each_narrowing_with_the_width_it_adopted()
    {
        var coordinator = new PressuredReplayCoordinator(
            head: 8, sliceSize: 8, affordableBudget: 16, PressuredReplayEntries(8));
        var reader = SharingReader(coordinator);
        var reported = new List<int>();

        await reader.ReadSliceAsync(
            -1, 8, (_, narrowedTo) => reported.Add(narrowedTo), CancellationToken.None);

        Assert.That(
            reported,
            Is.EqualTo(new[] { 64, 16 }),
            "Each narrowing must be reported once, with the new width rather than the refused one.");
    }

    /// <summary>
    /// A failure that is not memory pressure is a real failure and must not be
    /// retried at any width - retrying it would turn a deterministic fault into
    /// five identical ones and bury the cause.
    /// </summary>
    [Test]
    public void The_shared_reader_does_not_narrow_for_a_failure_that_is_not_pressure()
    {
        var coordinator = new PressuredReplayCoordinator(
            head: 8, sliceSize: 8, affordableBudget: 256, PressuredReplayEntries(8));
        coordinator.Stub.ReadSliceAsync(
                Arg.Any<long>(), Arg.Any<long>(), Arg.Any<int>(), Arg.Any<CancellationToken>())
            .Returns<Task<IReadOnlyList<CommitLogSliceEntry>>>(
                _ => throw new InvalidOperationException("scripted: not a pressure fault."));
        var reader = SharingReader(coordinator);

        Assert.That(
            async () => await reader.ReadSliceAsync(-1, 8, onNarrowed: null, CancellationToken.None),
            Throws.InstanceOf<InvalidOperationException>(),
            "An ordinary read failure must reach the caller unchanged.");

        Assert.That(
            reader.Budget,
            Is.EqualTo(ReplaySliceReader.InitialBudget),
            "Width must be untouched by a fault that narrowing cannot help.");
    }

    // ------------------------------------------------------- the anti-drift gate

    /// <summary>
    /// The gate that makes the sharing durable rather than momentary.
    /// <para>
    /// Factoring the mechanism into <see cref="ReplaySliceReader"/> repairs the
    /// two sites that had drifted, but on its own it does nothing to stop a
    /// fourth site being written next month that calls the coordinator directly
    /// with a constant - which is exactly how the first divergence happened, and
    /// it went unnoticed for the whole of #2742's life. This asserts the
    /// property from the other side: the reader is the <b>only</b> place in
    /// <c>src/</c> that asks the coordinator for a slice.
    /// </para>
    /// <para>
    /// Note that this inventories the call sites rather than counting them. A
    /// count returns a reassuring small number whether or not the sites it
    /// counted are the ones intended, and "uncovered sibling site" is precisely
    /// the failure this issue is about - the issue itself named two sites when
    /// there were three.
    /// </para>
    /// </summary>
    [Test]
    public void The_slice_reader_is_the_only_place_in_src_that_reads_a_commit_log_slice()
    {
        var root = HygieneRepository.FindRepoRoot();
        var src = Path.Combine(root, "src");

        // Capture the RECEIVER, because after the repair the replay sites still
        // contain the text "ReadSliceAsync" - they call it on the reader. What
        // distinguishes a repaired site from a direct one is what it calls it
        // on, so a bare name match would report all four files and prove
        // nothing. Enforcing the receiver name alongside the routing is
        // deliberate rather than incidental: a new site that goes straight to
        // the coordinator will name its receiver for what it is, and the only
        // way to evade this gate is to call a coordinator "sliceReader".
        var call = new Regex(@"(?<recv>[\w\.]+)\.ReadSliceAsync\s*\(", RegexOptions.Compiled);

        // The interface that declares the method and the grain that implements
        // it are definitions, not call sites, so they are named individually
        // rather than pattern-excluded - an exclusion broad enough to cover
        // them would be broad enough to hide the next real site.
        var definitions = new[] { "ILeafReplayCoordinatorGrain.cs", "LeafReplayCoordinatorGrain.cs" };

        var callers = HygieneRepository.EnumerateFiles(src, "*.cs")
            .Where(f => !HygieneRepository.HasExcludedSegment(f))
            .Where(f => call.Matches(File.ReadAllText(f))
                .Any(m => m.Groups["recv"].Value != "sliceReader"))
            .Select(Path.GetFileName)
            .Where(n => !definitions.Contains(n))
            .OrderBy(n => n, StringComparer.Ordinal)
            .ToList();

        Assert.Multiple(() =>
        {
            // Anti-vacuity. A scan that matches nothing passes every assertion
            // below it, so a gate with no self-test silently becomes a no-op the
            // day its pattern, its root or its file filter stops matching - and
            // reports green while doing it.
            Assert.That(
                callers,
                Is.Not.Empty,
                "The scan matched no call site at all, so it cannot be gating anything. Either the "
                + "pattern, the repository root or the file filter has stopped matching.");

            Assert.That(
                callers,
                Is.EqualTo(new[] { "ReplaySliceReader.cs" }),
                "Every WAL replay must read its slices through ReplaySliceReader, which owns the "
                + "narrow-and-retry, the widening and the narrowing counter's priming as one thing. A "
                + "site listed here that is not the reader is a replay that cannot narrow under memory "
                + "pressure - which is the defect issue #2899 was raised for, at a new site.");
        });
    }
}
