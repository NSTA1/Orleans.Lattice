using Microsoft.Extensions.Logging.Abstractions;
using Orleans.Lattice.Api.Mcp.RepoContext.Tests.Harness;
using Orleans.Lattice.Vector;
using Orleans.Lattice.Vector.Persistence;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Retrieval;

/// <summary>
/// The build tick's replay admission class (issue #4071): every phase of one
/// coordinator tick - the open walk, the ingest read, and the catch-up - is
/// classified <see cref="LatticeReplayAdmissionClass.Bulk"/>, not just the open.
/// <para>
/// <b>Why the narrower scope was wrong.</b> The class exists so that an O(corpus)
/// fan-out which activates cold leaves in bulk is turned away by a saturated
/// replay gate one full ceiling's worth of queue BEFORE that gate starts refusing
/// foreground reads. Issue #3284 introduced it and scoped it inside
/// <c>OpenAsync</c>, around the key walk alone. The ingest read is the phase that
/// walks the whole corpus, and it ran outside the scope entirely, so the majority
/// of the build's leaf activations took the wider Interactive bound and competed
/// with foreground reads for the gate.
/// </para>
/// <para>
/// <b>The classification is asserted from inside the read, not at the call
/// site.</b> Nothing in the handle's signature mentions the class; it flows
/// ambiently on <c>RequestContext</c> and is read by a leaf grain at the far end
/// of the call. A scope opened around the wrong region is therefore invisible
/// except to the code it fails to cover, which is exactly how this one survived
/// two issues. Observing it where the work happens answers "was this phase's work
/// classified Bulk?" instead of the weaker "was a scope opened somewhere?".
/// </para>
/// <para>
/// <b>The classification is asserted after a clean build, and that is not a
/// throwaway note.</b> These fixtures were briefly believed to be flaky under
/// parallel load and were nearly marked non-parallelizable on that theory. The
/// real cause was a stale binary: a source file restored with a preserved
/// timestamp left MSBuild believing the assembly was current, so the tests ran
/// against the pre-change build and observed
/// <see cref="LatticeReplayAdmissionClass.Interactive"/> exactly as they should
/// have. Serialising the fixture would have encoded a false explanation and fixed
/// nothing. If these ever report Interactive again, rebuild with
/// <c>--no-incremental</c> before theorising.
/// </para>
/// </summary>
[TestFixture]
public sealed class RepoContextAnnIndexBuildAdmissionTests
{
    private const string RepoId = "acme";

    private static readonly EmbeddingSpaceTag Space = new("test-model", 8, VectorNormalization.UnitL2);

    private CancellationToken Ct => TestContext.CurrentContext.CancellationToken;

    private static RepoContextAnnOptions Options() => new()
    {
        MinimumTrainingCount = 8,
        PartitionCount = 4,
        Probes = 4,
        FlushAfterUpdates = 1,
        IngestBatchSize = 64,
        MaxItemsPerChunk = 8,
    };

    private static (RepoContextAnnIndexHandle Handle, AdmissionObservingVectorSource Observed) Rig(int vectors)
    {
        var inner = new InMemoryRepoContextVectorSource(Space);
        for (var i = 0; i < vectors; i++)
        {
            var angle = 2d * Math.PI * i / vectors;
            var vector = new float[Space.Dimension];
            vector[0] = (float)Math.Cos(angle);
            vector[1] = (float)Math.Sin(angle);
            inner.Set($"vec-{i:D6}", RepoContextKeys.File(RepoId, $"src/File{i}.cs"), vector);
        }

        var observed = new AdmissionObservingVectorSource(inner);
        var handle = new RepoContextAnnIndexHandle(
            RepoId,
            Space,
            observed,
            new InMemoryVectorIndexStore(),
            Options(),
            RepoContextAnnIndexKeys.IndexPrefix(RepoId, Space),
            NullLogger.Instance);

        return (handle, observed);
    }

    [Test]
    public async Task A_bulk_scope_survives_an_await_in_this_process()
    {
        // A probe, not a product assertion. The core suite only ever reads the
        // ambient class synchronously inside its scope, so nothing until now
        // established that it survives an await in a plain test host. If this
        // fails, the seam - not this fixture - is what needs attention.
        using (LatticeReplayAdmissionContext.BeginBulkScope())
        {
            Assert.That(
                LatticeReplayAdmissionContext.Current,
                Is.EqualTo(LatticeReplayAdmissionClass.Bulk),
                "synchronously inside the scope");

            await Task.Yield();

            Assert.That(
                LatticeReplayAdmissionContext.Current,
                Is.EqualTo(LatticeReplayAdmissionClass.Bulk),
                "after a yield inside the scope");

            await Task.Delay(1);

            Assert.That(
                LatticeReplayAdmissionContext.Current,
                Is.EqualTo(LatticeReplayAdmissionClass.Bulk),
                "after a real delay inside the scope");
        }
    }

    [Test]
    public void The_ambient_class_outside_a_build_tick_is_interactive()
    {
        // The control for every assertion below. Bulk is not the process-wide
        // default, so observing it inside the tick is evidence the tick set it
        // rather than evidence the fixture could not have seen anything else.
        Assert.That(
            LatticeReplayAdmissionContext.Current,
            Is.EqualTo(LatticeReplayAdmissionClass.Interactive));
    }

    [Test]
    public async Task The_ingest_read_is_classified_bulk()
    {
        var built = Rig(24);
        using var handle = built.Handle;

        var progress = await handle.AdvanceAsync(phase: null, Ct);
        while (progress.Phase != VectorIndexBuildPhase.Ready)
        {
            progress = await handle.AdvanceAsync(phase: null, Ct);
        }

        Assert.Multiple(() =>
        {
            Assert.That(built.Observed.Enumerations, Is.Not.Empty,
                "the fixture proves nothing unless the corpus was actually read");
            Assert.That(built.Observed.Enumerations, Is.All.EqualTo(LatticeReplayAdmissionClass.Bulk),
                "every ingest read of the corpus must be classified Bulk; before this fix only the "
                + "open walk was scoped, so these reads took the wider Interactive bound and "
                + "competed with foreground reads for the replay permit gate");
            Assert.That(built.Observed.Counts, Is.Not.Empty);
            Assert.That(built.Observed.Counts, Is.All.EqualTo(LatticeReplayAdmissionClass.Bulk),
                "the corpus count is the same O(corpus) read by the same path and is classified with it");
        });
    }

    [Test]
    public async Task The_reconcile_tick_on_a_built_index_is_classified_bulk()
    {
        // The catch-up branch, reached by ticking an index that is ALREADY Ready.
        // That branch probes the store of record to learn whether it has moved on,
        // which is its own fan-out over the same leaves, and it runs at the end of
        // AdvanceAsync - so it is the phase furthest from the open walk the
        // original scope covered, and the one most likely to be left behind.
        var built = Rig(24);
        using var handle = built.Handle;

        var progress = await handle.AdvanceAsync(phase: null, Ct);
        while (progress.Phase != VectorIndexBuildPhase.Ready)
        {
            progress = await handle.AdvanceAsync(phase: null, Ct);
        }

        var before = built.Observed.All.Count;
        await handle.AdvanceAsync(phase: null, Ct);

        Assert.Multiple(() =>
        {
            Assert.That(built.Observed.All.Count, Is.GreaterThan(before),
                "the reconcile tick must actually have read the store of record; without this the "
                + "assertion below would pass against a tick that did nothing at all");
            Assert.That(
                built.Observed.All.Skip(before),
                Is.All.EqualTo(LatticeReplayAdmissionClass.Bulk),
                "every read the reconcile tick performs carries the tick's own classification, "
                + "because it is the same background build work over the same leaves");
        });
    }

    [Test]
    public async Task The_class_is_restored_once_the_tick_ends()
    {
        // The scope is a using inside AdvanceAsync, so it must not leak into
        // whatever the caller does next. A leaked Bulk class would quietly widen
        // the refusal of every later foreground read on this flow.
        var built = Rig(16);
        using var handle = built.Handle;

        await handle.AdvanceAsync(phase: null, Ct);

        Assert.That(
            LatticeReplayAdmissionContext.Current,
            Is.EqualTo(LatticeReplayAdmissionClass.Interactive),
            "the tick's scope is disposed with the tick, so the ambient class is restored");
    }
}
