using System.Text;
using Orleans.Hosting;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.TestingHost;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// End-to-end coverage, on a real silo, of the saga decision gate a snapshot
/// capture holds (issue #4485):
/// <list type="bullet">
/// <item><description>
/// a capture whose gate is lost mid-capture (simulated by an outgoing-call
/// filter releasing it on the registry, the effect a lapse or a registry
/// reactivation has) is not accepted: the open retries with a fresh gate, and
/// fails closed with <see cref="LatticeTransactionOutcomeUnavailableException"/>
/// when it can never hold one;
/// </description></item>
/// <item><description>
/// a gate whose capture crashed and never released it lapses with its lease, so
/// a saga stalled behind it completes, and writes are never blocked by it;
/// </description></item>
/// <item><description>
/// a backup set's fence refuses a new cross-tree write by rolling it back on
/// every tree, so no sibling delegation is left in flight and the set's drain
/// terminates.
/// </description></item>
/// </list>
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class SnapshotDecisionGateIntegrationTests
{
    private TestCluster _cluster = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        var builder = new TestClusterBuilder(initialSilosCount: 1);
        builder.AddSiloBuilderConfigurator<SiloConfigurator>();
        _cluster = builder.Build();
        await _cluster.DeployAsync();
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown()
    {
        GateLossFilter.Reset();
        await _cluster.StopAllSilosAsync();
        await _cluster.DisposeAsync();
    }

    [TearDown]
    public void TearDown() => GateLossFilter.Reset();

    [Test]
    public async Task A_capture_that_loses_its_gate_is_retried_with_a_fresh_gate_and_then_accepted()
    {
        var treeId = $"gate-loss-once-{Guid.NewGuid():N}";
        var tree = _cluster.Client.GetGrain<ILattice>(treeId);
        await tree.SetAsync("a", Bytes("1"));
        await tree.SetAsync("b", Bytes("2"));
        GateLossFilter.Arm(treeId, losses: 1);

        var entries = await ReadSnapshotAsync(tree);

        Assert.Multiple(() =>
        {
            Assert.That(GateLossFilter.GatesSeen(treeId), Is.EqualTo(2), "the first attempt must be discarded and the open retried under a fresh gate");
            Assert.That(entries.Keys, Is.EquivalentTo(new[] { "a", "b" }));
        });
    }

    [Test]
    public async Task A_capture_that_can_never_hold_its_gate_fails_closed()
    {
        var treeId = $"gate-loss-always-{Guid.NewGuid():N}";
        var tree = _cluster.Client.GetGrain<ILattice>(treeId);
        await tree.SetAsync("a", Bytes("1"));
        GateLossFilter.Arm(treeId, losses: int.MaxValue);

        Assert.ThrowsAsync<LatticeTransactionOutcomeUnavailableException>(
            () => tree.OpenSnapshotEntryCursorAsync());
        Assert.That(GateLossFilter.GatesSeen(treeId), Is.EqualTo(SnapshotDecisionGateContext.MaxCaptureAttempts));
    }

    [Test]
    public async Task A_crashed_captures_gate_lapses_so_sagas_resume_and_writes_are_never_blocked()
    {
        var treeId = $"gate-crash-{Guid.NewGuid():N}";
        var tree = _cluster.Client.GetGrain<ILattice>(treeId);
        await tree.SetAsync("seed", Bytes("0"));

        // A capture that acquires the gate and then dies: nothing renews or
        // releases it.
        var lease = TimeSpan.FromSeconds(3);
        await TxRegistryFanOut.AcquireCaptureGateAsync(
            _cluster.Client, treeId, Guid.NewGuid(), TxRegistryCaptureGateMode.Gate, lease);

        await tree.SetAsync("plain", Bytes("w")).WaitAsync(TimeSpan.FromSeconds(10));
        var saga = tree.SetManyAtomicAsync([new("x", Bytes("1")), new("y", Bytes("1"))]);
        var completedUnderGate = await Task.WhenAny(saga, Task.Delay(TimeSpan.FromSeconds(1))) == saga;
        await saga.WaitAsync(TimeSpan.FromSeconds(60));

        Assert.Multiple(() =>
        {
            Assert.That(completedUnderGate, Is.False, "the saga's decision waits while the gate is live");
            Assert.That(saga.IsCompletedSuccessfully, Is.True, "the saga completes once the crashed capture's lease lapses");
        });
        Assert.That(Str(await tree.GetAsync("x")), Is.EqualTo("1"));
        Assert.That(Str(await tree.GetAsync("plain")), Is.EqualTo("w"));
    }

    [Test]
    public async Task A_fenced_tree_refuses_a_new_cross_tree_write_without_stranding_a_delegation()
    {
        // The backup set's drain terminates only because a cross-tree write
        // refused at its park on a fenced member rolls back on every tree
        // instead of retrying the park and holding its sibling's delegation open.
        var treeA = $"fence-a-{Guid.NewGuid():N}";
        var treeB = $"fence-b-{Guid.NewGuid():N}";
        await _cluster.Client.GetGrain<ILattice>(treeA).SetAsync("k", Bytes("pre"));
        await _cluster.Client.GetGrain<ILattice>(treeB).SetAsync("k", Bytes("pre"));
        var token = Guid.NewGuid();
        var highWater = await TxRegistryFanOut.AcquireCaptureGateAsync(
            _cluster.Client, treeB, token, TxRegistryCaptureGateMode.Fence, TimeSpan.FromSeconds(60));

        try
        {
            Assert.ThrowsAsync<InvalidOperationException>(() => _cluster.Client.SetManyAtomicAsync(
                [
                    new LatticeTreeBatch(treeA, [new("k", Bytes("post"))]),
                    new LatticeTreeBatch(treeB, [new("k", Bytes("post"))]),
                ],
                $"fenced-{Guid.NewGuid():N}").WaitAsync(TimeSpan.FromSeconds(60)));

            var inFlightA = await TxRegistryFanOut.ObserveCrossTreeInFlightAsync(_cluster.Client, treeA);
            var inFlightB = await TxRegistryFanOut.ObserveCrossTreeInFlightAsync(_cluster.Client, treeB);
            Assert.Multiple(() =>
            {
                Assert.That(inFlightA.InFlightCount, Is.Zero, "the sibling's delegation must not be left in flight");
                Assert.That(inFlightB.InFlightCount, Is.Zero);
            });
            Assert.That(Str(await _cluster.Client.GetGrain<ILattice>(treeA).GetAsync("k")), Is.EqualTo("pre"));
            Assert.That(Str(await _cluster.Client.GetGrain<ILattice>(treeB).GetAsync("k")), Is.EqualTo("pre"));
        }
        finally
        {
            await TxRegistryFanOut.ReleaseCaptureGateAsync(_cluster.Client, treeB, highWater, token);
        }
    }

    private static async Task<Dictionary<string, string?>> ReadSnapshotAsync(ILattice tree)
    {
        var cursorId = await tree.OpenSnapshotEntryCursorAsync();
        var result = new Dictionary<string, string?>(StringComparer.Ordinal);
        try
        {
            while (true)
            {
                var page = await tree.NextEntriesAsync(cursorId, 100);
                foreach (var (key, value) in page.Entries)
                    result[key] = Str(value);
                if (!page.HasMore) break;
            }
        }
        finally
        {
            await tree.CloseCursorAsync(cursorId);
        }

        return result;
    }

    private static byte[] Bytes(string s) => Encoding.UTF8.GetBytes(s);

    private static string? Str(byte[]? b) => b is null ? null : Encoding.UTF8.GetString(b);

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.UseInMemoryReminderService();
            siloBuilder.AddOutgoingGrainCallFilter<GateLossFilter>();
        }
    }

    /// <summary>
    /// Releases a capture's decision gate on the registry before its first shard
    /// capture runs, for a bounded number of distinct gates per tree - the
    /// observable effect of a lapsed lease or a reactivated registry.
    /// </summary>
    private sealed class GateLossFilter(IGrainFactory grainFactory) : IOutgoingGrainCallFilter
    {
        private static readonly Dictionary<string, int> s_losses = new(StringComparer.Ordinal);
        private static readonly Dictionary<string, HashSet<Guid>> s_seen = new(StringComparer.Ordinal);
        private static readonly object s_lock = new();

        internal static void Arm(string treeId, int losses)
        {
            lock (s_lock)
            {
                s_losses[treeId] = losses;
                s_seen[treeId] = [];
            }
        }

        internal static int GatesSeen(string treeId)
        {
            lock (s_lock)
            {
                return s_seen.TryGetValue(treeId, out var seen) ? seen.Count : 0;
            }
        }

        internal static void Reset()
        {
            lock (s_lock)
            {
                s_losses.Clear();
                s_seen.Clear();
            }
        }

        public async Task Invoke(IOutgoingGrainCallContext context)
        {
            if (context.MethodName == nameof(IShardRootGrain.CaptureGatedSnapshotBaselineAsync)
                && context.Request.GetArgument(1) is SnapshotDecisionGate gate)
            {
                var drop = false;
                lock (s_lock)
                {
                    if (s_seen.TryGetValue(gate.RegistryTreeId, out var seen) && seen.Add(gate.Token))
                    {
                        var remaining = s_losses[gate.RegistryTreeId];
                        if (remaining > 0)
                        {
                            s_losses[gate.RegistryTreeId] = remaining == int.MaxValue ? remaining : remaining - 1;
                            drop = true;
                        }
                    }
                }

                if (drop)
                {
                    await TxRegistryFanOut.ReleaseCaptureGateAsync(
                        grainFactory, gate.RegistryTreeId, LatticeOptions.MaxTxRegistryShardCount, gate.Token);
                }
            }

            await context.Invoke();
        }
    }
}
