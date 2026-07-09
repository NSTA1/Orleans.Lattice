using System.Collections.Concurrent;
using System.Text;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Backup.Tests.Chaos;

/// <summary>
/// Chaos coverage of the cross-tree atomic-write saga racing a shadow-cutover
/// restore of one of its participant trees. A cross-tree atomic write drives the
/// single-tree prepare-and-pause saga on every participant through the logical
/// alias; when a participant is cut over to a restored shadow mid-flight, the
/// retained physical tree refuses logical-alias traffic with the internal
/// staleness signal. The saga's deadline-bounded retry loops must absorb that
/// signal, re-resolve onto the shadow, and drive the batch to a clean terminal
/// decision rather than leaking the signal to the caller. This suite fires a
/// sustained stream of cross-tree atomic writes across two trees while one is
/// repeatedly cut over underneath the saga, and pins that no call surfaces the
/// staleness signal and that the participant stays atomically writable
/// afterward.
/// </summary>
/// <remarks>
/// Complements the deterministic prepare-side and terminal-broadcast
/// stale-routing retry regressions (which drive the retry loops with mocked
/// throws) by exercising the whole coordinator over a real restore that retires
/// a participant's physical identity mid-saga. The post-settle atomic write
/// proves the tree is not wedged: the saga self-healed onto the cut-over
/// identity and a fresh all-or-nothing batch still commits and is atomically
/// visible on both trees.
/// </remarks>
[TestFixture]
[NonParallelizable]
[Category("Chaos")]
public sealed class CrossTreeAtomicWriteAcrossCutoverChaosTests
{
    private RestoreClusterFixture _fixture = null!;

    [SetUp]
    public void SetUp() => _fixture = new RestoreClusterFixture();

    [TearDown]
    public async Task TearDown() => await _fixture.DisposeAsync();

    private static byte[] Bytes(string s) => Encoding.UTF8.GetBytes(s);
    private static string? Str(byte[]? v) => v is null ? null : Encoding.UTF8.GetString(v);

    private static bool IsRoutingLeak(Exception ex)
    {
        for (var e = ex; e is not null; e = e.InnerException!)
        {
            var name = e.GetType().Name;
            if (name.Contains("StaleTreeRouting", StringComparison.Ordinal)
                || name.Contains("StaleShardRouting", StringComparison.Ordinal))
                return true;
            if (e.InnerException is null) break;
        }
        return false;
    }

    // A sustained stream of cross-tree atomic writes across two trees, while one
    // participant is repeatedly cut over to a restored shadow underneath the
    // saga, must never leak the internal staleness signal to the caller, and the
    // participant must remain atomically writable afterward.
    [Test]
    public async Task Cross_tree_atomic_write_survives_a_participant_cutover_without_leaking_stale_routing()
    {
        await _fixture.InitializeAsync();

        const string treeA = "chaos-xtx-a";
        const string treeB = "chaos-xtx-b";
        var a = _fixture.GrainFactory.GetGrain<ILattice>(treeA);
        var b = _fixture.GrainFactory.GetGrain<ILattice>(treeB);

        // Seed both participants; capture the backup that each cutover reverts B
        // to (B's seed, without any of the in-flight tx keys).
        await a.SetAsync("seed", Bytes("a-seed"));
        await b.SetAsync("seed", Bytes("b-seed"));
        var backup = await _fixture.Capture.CaptureAsync(
            new LatticeBackupCaptureRequest("chaos-xtx", BackupScopeSelector.WholeTree(treeB)));

        var failures = new ConcurrentQueue<string>();
        var committed = 0;
        using var stop = new CancellationTokenSource();

        // Single writer loop: sequential cross-tree atomic writes (distinct
        // operation ids, distinct keys) so the only concurrency under test is the
        // saga versus the cutover, not saga-versus-saga contention.
        var writer = Task.Run(async () =>
        {
            var n = 0;
            while (!stop.IsCancellationRequested)
            {
                var key = $"tx-{n:D4}";
                var batches = new List<LatticeTreeBatch>
                {
                    new(treeA, new List<KeyValuePair<string, byte[]>> { new(key, Bytes($"a-{n}")) }),
                    new(treeB, new List<KeyValuePair<string, byte[]>> { new(key, Bytes($"b-{n}")) }),
                };
                try
                {
                    var outcome = await _fixture.GrainFactory.SetManyAtomicAsync(batches, $"xtx-op-{n}");
                    if (outcome == CrossTreeAtomicWriteOutcome.Committed) Interlocked.Increment(ref committed);
                }
                catch (Exception ex) when (IsRoutingLeak(ex))
                {
                    failures.Enqueue($"iteration {n}: leaked staleness signal {ex.GetType().Name}: {ex.Message}");
                }
                catch (Exception ex)
                {
                    failures.Enqueue($"iteration {n}: unexpected {ex.GetType().Name}: {ex.Message}");
                }
                n++;
            }
        });

        // Race the writer with repeated cutovers of B to the reverting backup.
        await Task.Delay(80);
        for (var i = 0; i < 3; i++)
        {
            var result = await _fixture.Restore.RestoreAsync(
                new LatticeRestoreRequest(backup.BackupId, treeB, mode: LatticeRestoreMode.ShadowCutover));
            Assert.That(result.Mode, Is.EqualTo(LatticeRestoreMode.ShadowCutover));
            await Task.Delay(80);
        }

        stop.Cancel();
        await writer;

        Assert.That(failures, Is.Empty,
            "The cross-tree saga must never leak the internal staleness signal or throw unexpectedly when a "
            + "participant is cut over mid-flight: " + string.Join(" | ", failures));
        Assert.That(committed, Is.GreaterThan(0),
            "At least one cross-tree atomic write must have committed across the race.");

        // The participant must not be wedged by the cutover: a fresh all-or-
        // nothing batch still commits and is atomically visible on both trees.
        var finalOutcome = await _fixture.GrainFactory.SetManyAtomicAsync(
            new List<LatticeTreeBatch>
            {
                new(treeA, new List<KeyValuePair<string, byte[]>> { new("final", Bytes("a-final")) }),
                new(treeB, new List<KeyValuePair<string, byte[]>> { new("final", Bytes("b-final")) }),
            },
            "xtx-op-final");

        Assert.That(finalOutcome, Is.EqualTo(CrossTreeAtomicWriteOutcome.Committed),
            "After the cutover storm the cross-tree saga must still commit a fresh batch.");
        Assert.Multiple(() =>
        {
            Assert.That(Str(a.GetAsync("final").Result), Is.EqualTo("a-final"),
                "Tree A must show the final atomic write.");
            Assert.That(Str(b.GetAsync("final").Result), Is.EqualTo("b-final"),
                "The cut-over tree B must show the final atomic write, proving it self-healed and is not wedged.");
        });
    }
}
