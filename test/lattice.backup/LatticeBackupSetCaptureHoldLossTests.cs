using System.Collections.Concurrent;
using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Hosting;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.TestingHost;

namespace Orleans.Lattice.Backup.Tests;

/// <summary>
/// A cross-tree backup set whose member registries lose the set's fence hold
/// between the drain and the decision gate. That is the observable effect of a
/// registry reactivation, which drops its in-memory capture holds: the gate's
/// acquire then creates a fresh hold under the same token, so the release
/// reports a valid lease, yet a cross-tree saga registered while no fence was
/// held. Under the #4485 fence this is the only way a registration enters the
/// capture window, and only the gated re-check and the post-capture
/// re-observation of <c>LatticeBackupCaptureService.CaptureFencedSetAsync</c> can
/// then refuse the attempt - the <c>Recheck</c> and <c>Validate</c> rows of
/// <c>spec/backup/RefinementCapture.md</c>. <c>spec/backup/BackupCapture.tla</c>
/// shows the two are mutually redundant, so one test pins the re-observation's
/// epoch clause alone and the other the pair together. A third pins the fence
/// itself (the <c>Fence</c> row): between the drain and the gate only the fence
/// refuses a new delegation. A fourth pins the set's lease validation (the
/// <c>Validate</c> row's release clause, issue #4441 F4): a member's gate lost
/// after the capture, while a single-tree saga decides on that member, must
/// discard the attempt, because only the release reports the loss.
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class LatticeBackupSetCaptureHoldLossTests
{
    private TestCluster _cluster = null!;

    private IServiceProvider SiloServices =>
        _cluster.Silos.OfType<InProcessSiloHandle>().First().SiloHost.Services;

    private IGrainFactory GrainFactory => _cluster.GrainFactory;

    private ILatticeBackupCaptureService Capture => SiloServices.GetRequiredService<ILatticeBackupCaptureService>();

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        var builder = new TestClusterBuilder(1);
        builder.AddSiloBuilderConfigurator<SiloConfigurator>();
        _cluster = builder.Build();
        await _cluster.DeployAsync();
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown()
    {
        HoldLossFilter.Reset();
        await _cluster.StopAllSilosAsync();
        await _cluster.DisposeAsync();
    }

    [SetUp]
    public void SetUp() => BackupInventoryRegistry.Instance.Reset();

    [TearDown]
    public void TearDown() => HoldLossFilter.Reset();

    [Test]
    public async Task A_cross_tree_write_completed_while_the_fence_was_lost_forces_a_second_attempt()
    {
        var (treeA, treeB, suffix) = await SeedAsync("lost-completed");

        // Attempt 1's gate: the fence is gone on both members, a cross-tree write
        // registers on both and completes, then the gate is taken afresh. Nothing
        // is in flight at the re-check, so only the moved registration epoch shows
        // the window was not quiet.
        HoldLossFilter.Arm(async (mode, ordinal, token, grainFactory) =>
        {
            if (mode != TxRegistryCaptureGateMode.Gate || ordinal != 1)
                return;
            await LoseFenceAsync(grainFactory, token, treeA, treeB);
            await grainFactory.SetManyAtomicAsync(
                new[]
                {
                    new LatticeTreeBatch(treeA, [new KeyValuePair<string, byte[]>("k", Bytes("new"))]),
                    new LatticeTreeBatch(treeB, [new KeyValuePair<string, byte[]>("k", Bytes("new"))]),
                },
                operationId: $"op-{suffix}");
        });

        var result = await CaptureSetAsync(treeA, treeB, suffix);

        Assert.Multiple(() =>
        {
            Assert.That(HoldLossFilter.Fired, Is.True, "the fence must have been lost inside the first attempt");
            Assert.That(result.SetManifest.Fence, Is.Not.Null);
            Assert.That(result.SetManifest.Fence!.Attempts, Is.EqualTo(2),
                "a registration while the fence was lost must discard the attempt, though nothing is in flight at its gate");
        });
    }

    [Test]
    public async Task A_delegation_registered_while_the_fence_was_lost_refuses_the_attempt()
    {
        var (treeA, treeB, suffix) = await SeedAsync("lost-live");
        var txid = Guid.NewGuid();

        // Attempt 1's gate: the fence is gone on both members and a cross-tree
        // saga registers its delegation on tree B, still undecided when the gate
        // is taken. Attempt 2's fence: the saga aborts, so the second drain passes.
        HoldLossFilter.Arm(async (mode, ordinal, token, grainFactory) =>
        {
            if (mode == TxRegistryCaptureGateMode.Gate && ordinal == 1)
            {
                await LoseFenceAsync(grainFactory, token, treeA, treeB);
                await TxRegistryRouting.GetRegistry(grainFactory, treeB, txid)
                    .RegisterExternalDecisionAuthorityAsync(txid, $"xop-lost-{suffix}");
            }
            else if (mode == TxRegistryCaptureGateMode.Fence && ordinal == 2)
            {
                await TxRegistryRouting.GetRegistry(grainFactory, treeB, txid).MarkAbortedAsync(txid);
            }
        });

        var result = await CaptureSetAsync(treeA, treeB, suffix);

        Assert.Multiple(() =>
        {
            Assert.That(HoldLossFilter.Fired, Is.True, "the fence must have been lost inside the first attempt");
            Assert.That(result.SetManifest.Fence, Is.Not.Null);
            Assert.That(result.SetManifest.Fence!.Attempts, Is.EqualTo(2),
                "a delegation live under the gate must discard the attempt");
        });
    }

    [Test]
    public async Task A_delegation_attempted_between_the_drain_and_the_gate_is_refused_by_the_set_fence()
    {
        var (treeA, treeB, suffix) = await SeedAsync("fenced");
        var txid = Guid.NewGuid();
        var refused = false;

        // Attempt 1, after its drain and before its gate: only the set's fence
        // stands between a new cross-tree saga and the capture window.
        HoldLossFilter.Arm(async (mode, ordinal, token, grainFactory) =>
        {
            if (mode != TxRegistryCaptureGateMode.Gate || ordinal != 1)
                return;
            var registry = TxRegistryRouting.GetRegistry(grainFactory, treeB, txid);
            try
            {
                await registry.RegisterExternalDecisionAuthorityAsync(txid, $"xop-fenced-{suffix}");
            }
            catch (TxDecisionGateRefusedException ex) when (ex.Refusal == TxDecisionGateRefusal.RegistrationFenced)
            {
                refused = true;
                return;
            }

            // Admitted: abort it so the capture can still finish and report.
            await registry.MarkAbortedAsync(txid);
        });

        var result = await CaptureSetAsync(treeA, treeB, suffix);

        Assert.Multiple(() =>
        {
            Assert.That(HoldLossFilter.Fired, Is.True, "the registration must have been attempted inside the first attempt");
            Assert.That(refused, Is.True, "the set's fence must refuse a new delegation between the drain and the gate");
            Assert.That(result.SetManifest.Fence!.Attempts, Is.EqualTo(1), "a refused registration leaves the window quiet");
        });
    }

    [Test]
    public async Task A_member_gate_lost_while_a_single_tree_saga_decides_on_it_forces_a_second_attempt()
    {
        var (treeA, treeB, suffix) = await SeedAsync("lost-gate");
        var decided = false;

        // Attempt 1, after every member was captured and before the step-6
        // release: tree A's gate is lost (a registry reactivation drops the hold),
        // so a single-tree saga on tree A records its decision and commits. The
        // cross-tree epoch does not move and nothing is in flight, so only the
        // release can report that the gate was not held for the whole attempt.
        HoldLossFilter.Arm(static (_, _, _, _) => Task.CompletedTask);
        HoldLossFilter.ArmFirstGateRelease(async (token, grainFactory) =>
        {
            await LoseFenceAsync(grainFactory, token, treeA);
            await grainFactory.GetGrain<ILattice>(treeA).SetManyAtomicAsync(
            [
                new KeyValuePair<string, byte[]>("k", Bytes("saga")),
                new KeyValuePair<string, byte[]>("k2", Bytes("saga")),
            ]);
            decided = true;
        });

        var result = await CaptureSetAsync(treeA, treeB, suffix);

        Assert.Multiple(() =>
        {
            Assert.That(HoldLossFilter.ReleaseFired, Is.True, "the gate must have been lost inside the first attempt");
            Assert.That(decided, Is.True, "the single-tree saga must have decided while the gate was lost");
            Assert.That(result.SetManifest.Fence, Is.Not.Null);
            Assert.That(result.SetManifest.Fence!.Attempts, Is.EqualTo(2),
                "a member gate lost inside the attempt must discard it, though the window looked quiet");
        });
    }

    private async Task<(string TreeA, string TreeB, string Suffix)> SeedAsync(string prefix)
    {
        var suffix = Guid.NewGuid().ToString("N");
        var treeA = $"{prefix}-a-{suffix}";
        var treeB = $"{prefix}-b-{suffix}";
        await GrainFactory.GetGrain<ILattice>(treeA).SetAsync("k", Bytes("old"));
        await GrainFactory.GetGrain<ILattice>(treeB).SetAsync("k", Bytes("old"));
        return (treeA, treeB, suffix);
    }

    private Task<LatticeBackupSetCaptureResult> CaptureSetAsync(string treeA, string treeB, string suffix) =>
        Capture.CaptureSetAsync(new LatticeBackupSetCaptureRequest(
            $"hold-loss-{suffix}",
            new[] { BackupScopeSelector.WholeTree(treeA), BackupScopeSelector.WholeTree(treeB) },
            crossTreeConsistent: true));

    private static async Task LoseFenceAsync(IGrainFactory grainFactory, Guid token, params string[] trees)
    {
        foreach (var tree in trees)
        {
            await TxRegistryFanOut.ReleaseCaptureGateAsync(
                grainFactory, tree, LatticeOptions.MaxTxRegistryShardCount, token);
        }
    }

    private static byte[] Bytes(string s) => Encoding.UTF8.GetBytes(s);

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.UseInMemoryReminderService();
            siloBuilder.AddLatticeBackup(o =>
            {
                o.MaxCrossTreeFenceAttempts = 5;
                o.CrossTreeFenceDrainTimeout = TimeSpan.FromSeconds(15);
                o.CrossTreeFencePollInterval = TimeSpan.FromMilliseconds(10);
            });
            siloBuilder.AddOutgoingGrainCallFilter<HoldLossFilter>();
        }
    }

    /// <summary>
    /// Silo-side outgoing filter that runs the armed action once per distinct
    /// capture-hold token and mode, before the first registry acquire carrying
    /// it, passing the 1-based ordinal of that token among the tokens seen in
    /// that mode (one token per capture attempt).
    /// </summary>
    private sealed class HoldLossFilter(IGrainFactory grainFactory) : IOutgoingGrainCallFilter
    {
        private static readonly object s_lock = new();
        private static readonly ConcurrentDictionary<(Guid, TxRegistryCaptureGateMode), Task> s_runs = new();
        private static readonly Dictionary<TxRegistryCaptureGateMode, int> s_ordinals = [];
        private static Func<TxRegistryCaptureGateMode, int, Guid, IGrainFactory, Task>? s_action;
        private static Func<Guid, IGrainFactory, Task>? s_releaseAction;
        private static Task? s_releaseRun;
        private static Guid s_firstGateToken;
        private static readonly AsyncLocal<bool> s_inReleaseAction = new();

        internal static bool Fired { get; private set; }

        internal static bool ReleaseFired { get; private set; }

        /// <summary>
        /// Runs <paramref name="action"/> once, before the first registry release
        /// carrying the first attempt's gate token: the set's step-6 release with
        /// validation, after every member was captured.
        /// </summary>
        internal static void ArmFirstGateRelease(Func<Guid, IGrainFactory, Task> action)
        {
            lock (s_lock)
            {
                s_releaseAction = action;
            }
        }

        internal static void Arm(Func<TxRegistryCaptureGateMode, int, Guid, IGrainFactory, Task> action)
        {
            lock (s_lock)
            {
                s_action = action;
            }
        }

        internal static void Reset()
        {
            lock (s_lock)
            {
                s_action = null;
                s_runs.Clear();
                s_ordinals.Clear();
                Fired = false;
                s_releaseAction = null;
                s_releaseRun = null;
                s_firstGateToken = Guid.Empty;
                ReleaseFired = false;
            }
        }

        public async Task Invoke(IOutgoingGrainCallContext context)
        {
            if (context.MethodName == nameof(ITxRegistryGrain.AcquireCaptureGateAsync)
                && context.Request.GetArgument(0) is Guid token
                && context.Request.GetArgument(1) is TxRegistryCaptureGateMode mode)
            {
                Task run;
                lock (s_lock)
                {
                    if (s_action is not { } action)
                    {
                        run = Task.CompletedTask;
                    }
                    else if (!s_runs.TryGetValue((token, mode), out run!))
                    {
                        var ordinal = s_ordinals[mode] = s_ordinals.GetValueOrDefault(mode) + 1;
                        if (mode == TxRegistryCaptureGateMode.Gate && ordinal == 1)
                        {
                            Fired = true;
                            s_firstGateToken = token;
                        }
                        run = action(mode, ordinal, token, grainFactory);
                        s_runs[(token, mode)] = run;
                    }
                }

                await run;
            }
            else if (context.MethodName == nameof(ITxRegistryGrain.ReleaseCaptureGateAsync)
                && !s_inReleaseAction.Value
                && context.Request.GetArgument(0) is Guid releaseToken)
            {
                Task release;
                lock (s_lock)
                {
                    if (s_releaseAction is not { } releaseAction
                        || releaseToken == Guid.Empty
                        || releaseToken != s_firstGateToken)
                    {
                        release = Task.CompletedTask;
                    }
                    else
                    {
                        if (s_releaseRun is null)
                        {
                            ReleaseFired = true;
                            s_releaseRun = RunReleaseActionAsync(releaseAction, releaseToken);
                        }

                        release = s_releaseRun;
                    }
                }

                await release;
            }

            await context.Invoke();
        }

        private async Task RunReleaseActionAsync(Func<Guid, IGrainFactory, Task> action, Guid token)
        {
            s_inReleaseAction.Value = true;
            await action(token, grainFactory);
        }
    }
}
