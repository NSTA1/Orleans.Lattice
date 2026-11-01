using System.Reflection;
using System.Text;
using Orleans.Configuration;
using Orleans.Lattice.Backup;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Regression for issue #5506: a coordinated restore whose saga runs longer than
/// one grain call's response timeout must report the saga's real outcome. The
/// restore used to await the whole saga inside a single call to the coordinator
/// grain, so a saga that committed after that call timed out was reported as a
/// failed restore while every cluster had in fact been cut over.
/// <para>
/// The restore is driven through the <b>real</b> public restore entry point, the
/// <b>real</b> restore saga dispatcher and the <b>real</b> coordinator and
/// participant grains. The loopback control channel holds the saga in prepare for
/// longer than the silo's response timeout, which is shortened for that window
/// only, so the rest of the restore runs under the default.
/// </para>
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class CoordinatedRestoreSagaOutlastsResponseTimeoutTests
{
    private const string ReplTree = "saga-timeout-orders@site";
    private const string LocalTree = "saga-timeout-audit@site";
    private const string PostCutKey = "k/zzz-post-cut";

    /// <summary>The response timeout the saga is held past.</summary>
    private static readonly TimeSpan ShortResponseTimeout = TimeSpan.FromSeconds(3);

    /// <summary>The bound on every wait for a condition the restore must reach.</summary>
    private static readonly TimeSpan HangBound = TimeSpan.FromSeconds(60);

    private CoordinatedRestoreClusterFixture _fixture = null!;

    [SetUp]
    public void SetUp() => _fixture = new CoordinatedRestoreClusterFixture();

    [TearDown]
    public async Task TearDown() => await _fixture.DisposeAsync();

    [Test]
    public async Task A_restore_whose_saga_outlasts_the_response_timeout_reports_the_committed_outcome()
    {
        var setId = await ArrangeAdvancedSetAsync();

        var timeout = SiloResponseTimeout.Resolve(_fixture.SiloServices);
        var original = timeout.Current;
        var hold = _fixture.ControlChannel!.HoldPrepare();
        Task<IReadOnlyList<LatticeRestoreResult>>? restore = null;
        try
        {
            timeout.Set(ShortResponseTimeout);
            restore = _fixture.RestoreService.RestoreSetAsync(setId);
            await hold.Entered.Task.WaitAsync(HangBound);

            // The saga is in prepare and stays there for twice the response
            // timeout. A restore that waits on the saga inside one grain call has
            // had that call time out by now; one that follows the saga's own
            // progress is still waiting for it.
            await Task.Delay(ShortResponseTimeout * 2);
            Assert.That(restore.IsCompleted, Is.False,
                () => "the restore ended while its saga was still preparing: " + restore.Exception?.GetBaseException());
        }
        finally
        {
            timeout.Set(original);
            hold.Release.TrySetResult();
        }

        var results = await restore.WaitAsync(HangBound);

        Assert.That(
            results.Select(static r => r.TargetTreeId),
            Is.EquivalentTo(new[] { ReplTree, LocalTree }),
            "the restore reported the committed saga's result for both member trees");

        var repl = _fixture.GrainFactory.GetGrain<ILattice>(ReplTree);
        var local = _fixture.GrainFactory.GetGrain<ILattice>(LocalTree);
        await Assert.MultipleAsync(async () =>
        {
            Assert.That(await repl.GetAsync(PostCutKey), Is.Null, "the replicated member was cut over to the restored copy");
            Assert.That(await local.GetAsync(PostCutKey), Is.Null, "the local-only member was cut over to the restored copy");
            Assert.That(_fixture.ControlChannel!.Aborted, Is.Empty, "a committed saga never compensated");
        });
    }

    [Test]
    public async Task A_restore_whose_participant_prepare_outlasts_the_response_timeout_reports_the_committed_outcome()
    {
        // The shape F6 hit: the shadow build inside the participant's prepare outlasts
        // the coordinator's call to the participant, so that call times out while the
        // build carries on. The saga must still commit, and the restore must say so.
        var setId = await ArrangeAdvancedSetAsync(injectEngineFaults: true);
        var faults = _fixture.EngineFaults!;

        var timeout = SiloResponseTimeout.Resolve(_fixture.SiloServices);
        var original = timeout.Current;
        var hold = new CoordinatedRestoreClusterFixture.PrepareHold();
        faults.BuildHold = hold;
        Task<IReadOnlyList<LatticeRestoreResult>>? restore = null;
        try
        {
            timeout.Set(ShortResponseTimeout);
            restore = _fixture.RestoreService.RestoreSetAsync(setId);
            await hold.Entered.Task.WaitAsync(HangBound);

            // The build is held inside the participant's prepare for twice the
            // response timeout, so the coordinator's prepare call has timed out.
            await Task.Delay(ShortResponseTimeout * 2);
            Assert.That(restore.IsCompleted, Is.False,
                () => "the restore ended while its participant was still preparing: " + restore.Exception?.GetBaseException());
        }
        finally
        {
            timeout.Set(original);
            faults.BuildHold = null;
            hold.Release.TrySetResult();
        }

        var results = await restore.WaitAsync(HangBound);

        Assert.That(
            results.Select(static r => r.TargetTreeId),
            Is.EquivalentTo(new[] { ReplTree, LocalTree }),
            "the restore reported the committed saga's result");
        await AssertCutOverAndUnfencedAsync();
    }

    [Test]
    public async Task A_restore_whose_cutover_fails_unfences_writes_and_reports_the_outcome_once_it_cuts_over()
    {
        // The other shape F6 hit: the decided commit's cutover failed (there, a
        // redelivery authorized as anonymous) after the write fence engaged, and the
        // tree stayed write-fenced. A failed cutover must release local writes while
        // the commit is redelivered, and the restore must report what finally happened.
        var setId = await ArrangeAdvancedSetAsync(injectEngineFaults: true);
        var faults = _fixture.EngineFaults!;
        faults.FailCommits = true;

        var restore = _fixture.RestoreService.RestoreSetAsync(setId);
        await faults.FirstCommitFailure.Task.WaitAsync(HangBound);

        // While every cutover fails, the replicated member still takes writes.
        var repl = _fixture.GrainFactory.GetGrain<ILattice>(ReplTree);
        await WriteOnceUnfencedAsync(repl, "k/written-while-the-cutover-fails");
        Assert.That(restore.IsCompleted, Is.False,
            () => "a decided commit whose cutover fails is not a finished restore: " + restore.Exception?.GetBaseException());

        faults.FailCommits = false;
        var results = await restore.WaitAsync(HangBound);

        Assert.That(
            results.Select(static r => r.TargetTreeId),
            Is.EquivalentTo(new[] { ReplTree, LocalTree }),
            "the restore reported the saga's committed result once the cutover succeeded");
        Assert.That(faults.CommitFailures, Is.GreaterThan(0));
        await AssertCutOverAndUnfencedAsync();
    }

    /// <summary>
    /// Writes <paramref name="key"/> once the tree takes writes. A redelivered cutover
    /// re-engages the fence for its own short window, so a write that lands in one is
    /// retried; a tree left fenced never takes it and the bound fails the test.
    /// </summary>
    private static async Task WriteOnceUnfencedAsync(ILattice tree, string key)
    {
        using var bound = new CancellationTokenSource(HangBound);
        while (true)
        {
            try
            {
                await tree.SetAsync(key, Encoding.UTF8.GetBytes(key));
                return;
            }
            catch (LatticeWriteFencedException) when (!bound.IsCancellationRequested)
            {
                await Task.Delay(TimeSpan.FromMilliseconds(100), bound.Token);
            }
        }
    }

    /// <summary>Both members hold the restored cut, and the replicated member takes writes.</summary>
    private async Task AssertCutOverAndUnfencedAsync()
    {
        var repl = _fixture.GrainFactory.GetGrain<ILattice>(ReplTree);
        var local = _fixture.GrainFactory.GetGrain<ILattice>(LocalTree);
        await Assert.MultipleAsync(async () =>
        {
            Assert.That(await repl.GetAsync(PostCutKey), Is.Null, "the replicated member was cut over to the restored copy");
            Assert.That(await local.GetAsync(PostCutKey), Is.Null, "the local-only member was cut over to the restored copy");
            Assert.That(_fixture.ControlChannel!.Aborted, Is.Empty, "a committed saga never compensated");
        });

        Assert.That(async () => await repl.SetAsync("k/after-restore", Encoding.UTF8.GetBytes("after")),
            Throws.Nothing, "the restore left the tree's writes unfenced");
    }

    /// <summary>
    /// Deploys the dispatcher-driven cluster, seeds two member trees, captures them
    /// as one cross-tree consistent set, advances both past the cut, and returns
    /// the set id.
    /// </summary>
    private async Task<string> ArrangeAdvancedSetAsync(bool injectEngineFaults = false)
    {
        await _fixture.InitializeAsync(
            driveThroughDispatcher: true, replicatedTrees: [ReplTree], injectEngineFaults: injectEngineFaults);

        var repl = _fixture.GrainFactory.GetGrain<ILattice>(ReplTree);
        var local = _fixture.GrainFactory.GetGrain<ILattice>(LocalTree);
        for (var i = 0; i < 3; i++)
        {
            var key = $"k/{i:D3}";
            await repl.SetAsync(key, Encoding.UTF8.GetBytes(key));
            await local.SetAsync(key, Encoding.UTF8.GetBytes(key));
        }

        var set = await _fixture.Capture.CaptureSetAsync(new LatticeBackupSetCaptureRequest(
            "nightly",
            [BackupScopeSelector.WholeTree(ReplTree), BackupScopeSelector.WholeTree(LocalTree)],
            crossTreeConsistent: true));

        await repl.SetAsync(PostCutKey, Encoding.UTF8.GetBytes("repl-post"));
        await local.SetAsync(PostCutKey, Encoding.UTF8.GetBytes("local-post"));

        var setId = set.SetManifest.SetId;
        Assert.That(setId, Is.Not.Null, "a two-tree set carries a set id");
        return setId!;
    }

    /// <summary>
    /// The silo's runtime response timeout. The configured
    /// <c>SiloMessagingOptions.ResponseTimeout</c> is read once, when the runtime
    /// client is built, so shortening it for one window goes through the runtime
    /// client, which Orleans keeps internal. Grain calls sent from the silo take
    /// their deadline from this value when they are sent.
    /// </summary>
    private sealed class SiloResponseTimeout(object runtimeClient, MethodInfo get, MethodInfo set)
    {
        internal static SiloResponseTimeout Resolve(IServiceProvider services)
        {
            var type = typeof(MessagingOptions).Assembly.GetType("Orleans.Runtime.IRuntimeClient");
            Assert.That(type, Is.Not.Null,
                "Orleans.Runtime.IRuntimeClient was renamed or moved; update how this fixture shortens the timeout.");
            var get = type!.GetMethod("GetResponseTimeout", Type.EmptyTypes);
            var set = type.GetMethod("SetResponseTimeout", [typeof(TimeSpan)]);
            var client = services.GetService(type);
            Assert.Multiple(() =>
            {
                Assert.That(get, Is.Not.Null, "IRuntimeClient.GetResponseTimeout was renamed; update this fixture.");
                Assert.That(set, Is.Not.Null, "IRuntimeClient.SetResponseTimeout was renamed; update this fixture.");
                Assert.That(client, Is.Not.Null, "the silo no longer registers IRuntimeClient; update this fixture.");
            });
            return new SiloResponseTimeout(client!, get!, set!);
        }

        public TimeSpan Current => (TimeSpan)get.Invoke(runtimeClient, null)!;

        public void Set(TimeSpan timeout) => set.Invoke(runtimeClient, [timeout]);
    }
}
