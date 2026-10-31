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

    /// <summary>
    /// Deploys the dispatcher-driven cluster, seeds two member trees, captures them
    /// as one cross-tree consistent set, advances both past the cut, and returns
    /// the set id.
    /// </summary>
    private async Task<string> ArrangeAdvancedSetAsync()
    {
        await _fixture.InitializeAsync(driveThroughDispatcher: true, replicatedTrees: [ReplTree]);

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
