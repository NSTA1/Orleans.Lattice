using Orleans.Lattice.Vector.Persistence;
using Orleans.Lattice.Vector.Tests.Fakes;

namespace Orleans.Lattice.Vector.Tests.Persistence;

/// <summary>
/// Guards that a background build makes DURABLE progress on every step, so it
/// completes however little work an activation affords it.
/// <para>
/// A step that advances only in memory is not merely slow: if the activation
/// ends there, the next one reloads the state that preceded it and repeats the
/// same step forever. The build writes nothing of consequence, so it does not
/// present as a write-volume problem - it presents as an index that never
/// becomes queryable, which is why it survived the amplification work on issues
/// #2691 and #2763 untouched.
/// </para>
/// <para>
/// The regime this needs is an activation budget of one step or less, which is
/// exactly what a silo under permit starvation hands out: the acceptance rig for
/// issue #2782 reported 526 cold leaf activations cancelled while queued for a
/// permit. These are issues #2789 and #2791.
/// </para>
/// </summary>
[TestFixture]
public sealed class DurableVectorIndexActivationBudgetTests
{
    private const int Corpus = 500;

    /// <summary>
    /// Far above the roughly seven activations a healthy build of this corpus
    /// takes, and far below an unbounded loop, so a livelock is a failure rather
    /// than a hang.
    /// </summary>
    private const int ActivationCap = 100;

    /// <summary>
    /// Drives a build one activation at a time, each activation a genuine reload
    /// over the same store, affording it <paramref name="stepsPerActivation"/>
    /// steps before the index is thrown away.
    /// </summary>
    private static async Task<(bool Ready, int Activations)> DriveAsync(
        InMemoryVectorIndexStore store,
        ListVectorSource source,
        DurableVectorIndexOptions options,
        int stepsPerActivation)
    {
        for (var activations = 1; activations <= ActivationCap; activations++)
        {
            var index = await DurableIndexHarness.OpenAsync(store, source, options);
            if (index.Progress.Phase == VectorIndexBuildPhase.Ready)
            {
                return (true, activations);
            }

            for (var step = 0; step < stepsPerActivation; step++)
            {
                await index.BuildStepAsync();
                if (index.Progress.Phase == VectorIndexBuildPhase.Ready)
                {
                    break;
                }
            }
        }

        return (false, ActivationCap);
    }

    [Test]
    public async Task A_build_completes_when_every_activation_affords_a_single_step()
    {
        var store = new InMemoryVectorIndexStore();
        var (ready, activations) = await DriveAsync(
            store, DurableIndexHarness.Source(Corpus), DurableIndexHarness.Options(), stepsPerActivation: 1);

        Assert.Multiple(() =>
        {
            Assert.That(
                ready,
                Is.True,
                $"the build did not reach Ready in {ActivationCap} activations, so no amount of scheduling "
                + "would complete it: a step that advances only in memory is undone by the reload that "
                + "follows it");
            Assert.That(
                activations,
                Is.LessThan(ActivationCap / 2),
                "completing only just inside the cap would mean progress is being made a fraction of a "
                + "step at a time, which is a livelock the cap merely hides");
        });
    }

    [Test]
    public async Task A_one_step_budget_costs_no_more_write_volume_than_a_generous_one()
    {
        var lean = new InMemoryVectorIndexStore();
        var (leanReady, _) = await DriveAsync(
            lean, DurableIndexHarness.Source(Corpus), DurableIndexHarness.Options(), stepsPerActivation: 1);

        var generous = new InMemoryVectorIndexStore();
        var (generousReady, _) = await DriveAsync(
            generous, DurableIndexHarness.Source(Corpus), DurableIndexHarness.Options(), stepsPerActivation: 10);

        Assert.Multiple(() =>
        {
            Assert.That(leanReady && generousReady, Is.True, "both budgets must complete for the ratio to mean anything");

            // A build that restarts a phase per activation would show up here as
            // a multiple, not as a rounding difference.
            Assert.That(
                (double)lean.BytesWritten / generous.BytesWritten,
                Is.LessThan(1.5),
                $"a lean budget wrote {lean.BytesWritten} bytes against {generous.BytesWritten}, which is "
                + "repeated work rather than the same build spread over more activations");
        });
    }

    [Test]
    public async Task A_build_interrupted_before_its_first_checkpoint_resumes_instead_of_restarting()
    {
        var source = DurableIndexHarness.Source(Corpus);
        var options = DurableIndexHarness.Options();
        var store = new InMemoryVectorIndexStore();

        // One step takes the build from NotStarted to Ingesting and commits a
        // build state. No manifest exists yet: only an ingest checkpoint writes
        // one, and none has run.
        var index = await DurableIndexHarness.OpenAsync(store, source, options);
        await index.BuildStepAsync();
        Assert.That(index.Progress.Phase, Is.EqualTo(VectorIndexBuildPhase.Ingesting));
        Assert.That(
            await store.ReadAsync(VectorIndexStorageKeys.Manifest("vidx/")),
            Is.Null,
            "the arm is only meaningful while no manifest has been committed");
        Assert.That(
            await store.ReadAsync(VectorIndexStorageKeys.BuildState("vidx/")),
            Is.Not.Null,
            "the arm is only meaningful once a build state has been committed");

        var resumed = await DurableIndexHarness.OpenAsync(store, source, options);

        Assert.That(
            resumed.Progress.Phase,
            Is.EqualTo(VectorIndexBuildPhase.Ingesting),
            "a build state with no manifest is the state of EVERY build interrupted before its first "
            + "checkpoint; discarding it resets the build to NotStarted, so an activation that never "
            + "affords a second step restarts from nothing forever");
    }

    [Test]
    public async Task A_build_whose_manifest_is_missing_after_it_ingested_is_still_discarded()
    {
        var source = DurableIndexHarness.Source(Corpus);
        var options = DurableIndexHarness.Options();
        var store = new InMemoryVectorIndexStore();

        var index = await DurableIndexHarness.OpenAsync(store, source, options);
        await index.BuildStepAsync();
        await index.BuildStepAsync();
        Assert.That(
            index.Progress.VectorsIndexed,
            Is.GreaterThan(0),
            "the arm needs a build that has committed an ingest checkpoint");

        // The manifest that prefix was committed against is gone, so the chunks
        // it names cannot be validated. Adopting the build state here would
        // resume a cursor over a prefix nothing vouches for.
        Assert.That(store.Drop(VectorIndexStorageKeys.Manifest("vidx/")), Is.True);

        var resumed = await DurableIndexHarness.OpenAsync(store, source, options);

        Assert.That(
            resumed.Progress.Phase,
            Is.EqualTo(VectorIndexBuildPhase.NotStarted),
            "a build that has ingested has a populated key mapping, and a mapping with no manifest to "
            + "vouch for the chunks it names is exactly the partially trusted state the load contract "
            + "refuses");
    }

    [Test]
    public async Task A_build_state_claiming_vectors_the_key_mapping_cannot_vouch_for_is_discarded()
    {
        var source = DurableIndexHarness.Source(Corpus);
        var options = DurableIndexHarness.Options();
        var store = new InMemoryVectorIndexStore();

        var index = await DurableIndexHarness.OpenAsync(store, source, options);
        await index.BuildStepAsync();
        await index.BuildStepAsync();

        // Strip the store back to the build state alone. The key mapping is what
        // the emptiness test reads, so removing it leaves a store that LOOKS
        // unbuilt while the build state still claims a durable prefix - and that
        // claim is the one thing adoption must not take on trust.
        Assert.That(store.Drop(VectorIndexStorageKeys.Manifest("vidx/")), Is.True);
        foreach (var key in store.KeysWithPrefix(VectorIndexStorageKeys.KeyMapPrefix("vidx/")))
        {
            store.Drop(key);
        }

        store.Drop(VectorIndexStorageKeys.KeyWatermark("vidx/"));

        var record = await store.ReadAsync(VectorIndexStorageKeys.BuildState("vidx/"));
        Assert.That(record, Is.Not.Null, "the arm needs the build state to have survived");
        Assert.That(
            VectorIndexBuildState.TryReadRecord(record!, out var state) && state.Ingested > 0,
            Is.True,
            "the arm is vacuous unless the surviving build state really does claim durable vectors");

        var resumed = await DurableIndexHarness.OpenAsync(store, source, options);

        Assert.That(
            resumed.Progress.Phase,
            Is.EqualTo(VectorIndexBuildPhase.NotStarted),
            $"the build state claims {state.Ingested} durable vectors and a cursor at {state.Cursor}, and "
            + "nothing on the store can vouch for either. Adopting it resumes past a prefix that may not "
            + "exist, which silently omits every vector before the cursor");
    }

    [Test]
    public async Task A_persisting_phase_left_by_an_earlier_build_is_resumed_at_training()
    {
        var source = DurableIndexHarness.Source(Corpus);
        var options = DurableIndexHarness.Options();
        var store = new InMemoryVectorIndexStore();

        // Drive a real build until it has ingested the whole corpus and is
        // about to train. Everything the legacy record must account for -
        // durable chunks, the key mapping, the cursor - is genuinely on the
        // store at this point, which a hand-written state cannot fake.
        var building = await DurableIndexHarness.OpenAsync(store, source, options);
        for (var step = 0; step < ActivationCap; step++)
        {
            if (building.Progress.Phase == VectorIndexBuildPhase.Training)
            {
                break;
            }

            await building.BuildStepAsync();
        }

        Assert.That(
            building.Progress.Phase,
            Is.EqualTo(VectorIndexBuildPhase.Training),
            "the arm needs a build that has finished ingesting and not yet trained");

        // Now make it the record a binary that treated the training-to-persisting
        // boundary as an activation boundary would have left. This one does not,
        // so it cannot produce such a record itself - but it can still be handed
        // one by a store that a pre-fix binary wrote, and Persisting there names
        // a partitioning that no manifest on this store describes.
        var current = await store.ReadAsync(VectorIndexStorageKeys.BuildState("vidx/"));
        Assert.That(
            current is not null && VectorIndexBuildState.TryReadRecord(current, out _),
            Is.True,
            "the arm needs the build state the real build committed");

        VectorIndexBuildState.TryReadRecord(current!, out var committed);
        var legacy = committed with { Phase = VectorIndexBuildPhase.Persisting };
        await store.WriteAsync(
            [new KeyValuePair<string, byte[]>(VectorIndexStorageKeys.BuildState("vidx/"), legacy.ToRecord())]);

        var planted = await store.ReadAsync(VectorIndexStorageKeys.BuildState("vidx/"));
        Assert.That(
            planted is not null
                && VectorIndexBuildState.TryReadRecord(planted, out var plantedState)
                && plantedState.Phase == VectorIndexBuildPhase.Persisting
                && plantedState.Ingested == Corpus,
            Is.True,
            "the arm is vacuous unless a Persisting build state accounting for the whole corpus really is "
            + "on the store to be adopted");

        var index = await DurableIndexHarness.OpenAsync(store, source, options);

        Assert.That(
            index.Progress.Phase,
            Is.EqualTo(VectorIndexBuildPhase.Training),
            "resuming at Persisting would persist a partitioning that was never trained, because the "
            + "manifest the phase was written against is the pre-training one");

        await index.RunBuildAsync();

        Assert.That(
            index.Count,
            Is.EqualTo(Corpus),
            "a build handed a phase it cannot honour must still reach a complete index");
    }

    [Test]
    public async Task A_persisting_phase_with_nothing_banked_is_adopted_at_training()
    {
        var source = DurableIndexHarness.Source(Corpus);
        var options = DurableIndexHarness.Options();
        var store = new InMemoryVectorIndexStore();

        // The other Persisting case. This one reaches adoption down the
        // uncommitted-build path rather than the manifest path, which is a
        // separate normalisation site with its own resume semantics: there is no
        // manifest at all here, so the partitioning the phase names does not
        // exist even in its pre-training form.
        var legacy = new VectorIndexBuildState(
            0,
            VectorIndexBuildPhase.Persisting,
            0,
            Corpus,
            null);
        await store.WriteAsync(
            [new KeyValuePair<string, byte[]>(VectorIndexStorageKeys.BuildState("vidx/"), legacy.ToRecord())]);

        Assert.That(
            await store.ReadAsync(VectorIndexStorageKeys.Manifest("vidx/")),
            Is.Null,
            "the arm is only meaningful while no manifest exists, which is what sends the load down the "
            + "uncommitted-build path rather than the manifest one");

        var index = await DurableIndexHarness.OpenAsync(store, source, options);

        Assert.That(
            index.Progress.Phase,
            Is.EqualTo(VectorIndexBuildPhase.Training),
            "adopting Persisting here would persist a partitioning that was never trained, against a "
            + "manifest that was never written");

        await index.RunBuildAsync();

        Assert.That(
            index.Progress.Phase,
            Is.EqualTo(VectorIndexBuildPhase.Ready),
            "a build handed a phase it cannot honour must still converge rather than loop");
    }
}
