using Orleans.Lattice.Vector.Persistence;
using Orleans.Lattice.Vector.Tests.Fakes;

namespace Orleans.Lattice.Vector.Tests.Persistence;

/// <summary>
/// The ingest slice's progress-armed extension (issue #4071): a slice that has
/// banked nothing at a budget boundary may be granted a bounded number of further
/// periods rather than ending starved.
/// <para>
/// <b>Why the elapsed-only bound was wrong, and only for one case.</b> The budget
/// is armed at the first wait, which correctly bounds a source that is slow,
/// stuck, or silent. It cannot distinguish any of those from a source that is
/// merely QUEUED - one whose leaves must first take a per-silo WAL replay permit.
/// There the slice cannot complete even one item inside the budget, banks
/// nothing, moves no cursor, and the next slice re-reads the identical range, so
/// the build does not converge while every component of it behaves as designed.
/// Measured on the deployment of issue #4071: a 24.6 s mean permit queue wait
/// against a 5 s slice budget, and 114 of 152 non-faulted ingest slices reporting
/// <c>progress=starved</c> against 38 that advanced.
/// </para>
/// <para>
/// <b>The same defect was found and fixed one layer up two issues earlier.</b>
/// Issue #3284 put the index OPEN in exactly this position and fixed it by arming
/// that deadline on progress rather than on elapsed time alone. Only the open
/// walk was fixed; the ingest slice was left on the elapsed-only bound. This
/// fixture is that mechanism's ingest counterpart.
/// </para>
/// <para>
/// <b>The real clock, deliberately, and this fixture paid for the lesson.</b>
/// <see cref="DurableVectorIndexStarvationSignalTests"/> already records that the
/// deadline exercised here is a TIMER rather than a sampled reading. A hand-driven
/// clock additionally cannot express what these fixtures assert: the production
/// code arms the timer a moment AFTER the source announces it is waiting - the
/// announcement happens inside the read, the arming happens in the caller once
/// that read is seen to be incomplete - so a fixture that advances a manual clock
/// on the announcement is racing an arming it has no barrier for. Built that way
/// this fixture passed in isolation and failed under the parallel load of the full
/// suite, which is the signature of a harness race rather than of the code under
/// test. Real timers fire on their own schedule whatever the arming order, so the
/// race disappears; the budget below is short and the margins around it wide, so
/// the run stays quick without becoming sensitive to how loaded the machine is.
/// </para>
/// <para>
/// <b>The fixtures are a PAIR and must be read as one.</b>
/// <see cref="An_unextended_slice_ends_on_its_first_budget_having_banked_nothing"/>
/// drives the identical source, budget and hold with the extension cap at zero
/// and proves the deadline really does fire - so it is the positive control that
/// stops <see cref="An_extended_slice_outlasts_a_wait_longer_than_one_budget"/>
/// passing vacuously against a timer that was never armed at all.
/// </para>
/// </summary>
[TestFixture]
public sealed class DurableVectorIndexSliceExtensionTests
{
    private const int Corpus = 12;

    /// <summary>
    /// One budget period. Short, so the run is quick, and the only real-time
    /// figure the assertions depend on.
    /// </summary>
    private static readonly TimeSpan Budget = TimeSpan.FromMilliseconds(250);

    /// <summary>
    /// How long the gate is held shut before the extended arm opens it.
    /// Comfortably more than one <see cref="Budget"/>, so an unextended slice has
    /// certainly been deadlined by then, and comfortably less than
    /// <see cref="ExtensionCap"/> periods, so an extended one has certainly not.
    /// </summary>
    private static readonly TimeSpan HeldFor = TimeSpan.FromMilliseconds(900);

    /// <summary>
    /// The extension cap the opted-in arm uses. Twenty periods is five seconds of
    /// slack against a nine-hundred millisecond hold, which is the margin that
    /// keeps this fixture insensitive to a loaded machine.
    /// </summary>
    private const int ExtensionCap = 20;

    /// <summary>
    /// The bound the fixture itself relies on, so a regression presents as a
    /// failing assertion rather than as a hung run.
    /// </summary>
    private static readonly TimeSpan Guard = TimeSpan.FromSeconds(30);

    private static DurableVectorIndexOptions Options(int maxExtensions)
    {
        var options = DurableIndexHarness.Options(ingestBatchSize: 4_096, maxItemsPerChunk: 64);
        options.IngestSliceBudget = Budget;
        options.MaxIngestSliceExtensions = maxExtensions;

        // The real clock: see the fixture remarks. A manual clock cannot express
        // the arming barrier these fixtures need.
        options.TimeProvider = TimeProvider.System;
        return options;
    }

    private static GatedVectorSource Gated()
    {
        var corpus = VectorCorpus.Clustered(Corpus, DurableIndexHarness.Dimensions, 4, seed: 11);
        var source = new GatedVectorSource(DurableIndexHarness.Dimensions);
        for (var i = 0; i < Corpus; i++)
        {
            source.Set(DurableIndexHarness.Id(i), corpus[i]);
        }

        return source;
    }

    /// <summary>
    /// Advances the build past the NotStarted transition, which counts the source
    /// without enumerating it, so that the next step is a real ingest slice.
    /// </summary>
    private static async Task<DurableVectorIndex> IngestingAsync(
        InMemoryVectorIndexStore store, GatedVectorSource source, DurableVectorIndexOptions options)
    {
        var index = await DurableVectorIndex.OpenAsync(store, source, options, VectorIndexLoadMode.Full);
        await index.BuildStepAsync();
        Assert.That(index.Progress.Phase, Is.EqualTo(VectorIndexBuildPhase.Ingesting));
        return index;
    }

    private static async Task WithinGuardAsync(Task work, string because)
    {
        var finished = await Task.WhenAny(work, Task.Delay(Guard));
        Assert.That(finished, Is.SameAs(work), because);
        await work;
    }

    [Test]
    public async Task An_unextended_slice_ends_on_its_first_budget_having_banked_nothing()
    {
        // THE POSITIVE CONTROL, and the pre-fix behaviour. Zero extensions is the
        // shipped default of DurableVectorIndexOptions.MaxIngestSliceExtensions, so
        // this also pins that the change is inert for every host that does not opt
        // in.
        var store = new InMemoryVectorIndexStore();
        var source = Gated();
        var index = await IngestingAsync(store, source, Options(maxExtensions: 0));

        var step = index.BuildStepAsync();
        await source.Waiting;

        // The gate is never opened, so the only thing that can end this slice is
        // its deadline.
        await WithinGuardAsync(step, "the deadline must end a slice that has banked nothing");

        var progress = index.Progress;
        Assert.Multiple(() =>
        {
            Assert.That(progress.VectorsIndexed, Is.Zero,
                "the gate was never opened, so nothing could be banked");
            Assert.That(progress.SlicesDeadlinedWithoutProgress, Is.EqualTo(1),
                "and the slice is reported starved - which is the defect when the source was "
                + "merely queued rather than dead, and is what the extension below prevents");
        });

        source.Release();
    }

    [Test]
    public async Task An_extended_slice_outlasts_a_wait_longer_than_one_budget()
    {
        // THE FIX. Same source, same budget, same hold as the control above - only
        // the extension cap differs - and the slice now survives a wait several
        // budgets long, so the read that was merely queued gets to land.
        var store = new InMemoryVectorIndexStore();
        var source = Gated();
        var index = await IngestingAsync(store, source, Options(maxExtensions: ExtensionCap));

        var step = index.BuildStepAsync();
        await source.Waiting;

        // Held shut for longer than one budget. The control fixture proves this
        // much alone ends an unextended slice, so surviving it is the property.
        await Task.Delay(HeldFor);
        source.Release();
        await WithinGuardAsync(step, "the released read must complete the slice");

        var progress = index.Progress;
        Assert.Multiple(() =>
        {
            Assert.That(progress.VectorsIndexed, Is.GreaterThan(0),
                "the slice the extension kept alive BANKED, which is the whole property: its cursor "
                + "moved, so the next slice resumes past it rather than re-reading the same range");
            Assert.That(progress.SlicesDeadlinedWithoutProgress, Is.Zero,
                "and nothing was reported starved, because nothing starved - whereas the control "
                + "fixture, given the identical hold with no extensions, reports exactly one");
        });

        // THE EXTENSION BUYS THE CHANCE TO START, NOT A LONGER SLICE. Once the
        // first item is banked the post-consumption elapsed sample is evaluated
        // against a clock already past the budget, so the slice ends promptly and
        // hands the turn back exactly as an unextended one would. The bound is
        // intact; what changed is that the slice had something to bank when it
        // ended.
        await WithinGuardAsync(index.BuildStepAsync(), "the build must resume past the banked cursor");

        Assert.That(index.Progress.VectorsIndexed, Is.EqualTo(Corpus),
            "and the following slice picks up the remainder, so the extension produced monotone "
            + "progress rather than a single lucky item");
    }

    [Test]
    public async Task Extensions_are_capped_so_a_genuinely_silent_source_is_still_reported_starved()
    {
        // The cap in its load-bearing direction. The extension must not be able to
        // suppress the signal it was added to stop being spurious: a source that
        // answers nothing at all still exhausts its extensions, still banks
        // nothing, and is still reported starved. Without this the fix would have
        // converted a loud stall into an unbounded slice, which is the coordinator
        // wedge of issue #3130.
        var store = new InMemoryVectorIndexStore();
        var source = Gated();
        var index = await IngestingAsync(store, source, Options(maxExtensions: 2));

        var step = index.BuildStepAsync();
        await source.Waiting;

        // Never released. Two extensions, then the boundary that fires regardless.
        await WithinGuardAsync(step, "the cap must end the slice even though nothing was banked");

        var progress = index.Progress;
        Assert.Multiple(() =>
        {
            Assert.That(progress.VectorsIndexed, Is.Zero, "the gate was never opened");
            Assert.That(progress.SlicesDeadlinedWithoutProgress, Is.EqualTo(1),
                "the starvation signal survives the fix, which is what keeps a real stall loud");
            Assert.That(progress.IsStarvedBySource, Is.True,
                "and the verdict the operator reads is unchanged for a source that truly answers nothing");
        });

        source.Release();
    }
}
