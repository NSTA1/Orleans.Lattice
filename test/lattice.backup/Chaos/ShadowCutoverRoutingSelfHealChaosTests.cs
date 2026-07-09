using System.Collections.Concurrent;
using System.Text;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Backup.Tests.Chaos;

/// <summary>
/// Chaos coverage of the stateless-worker routing self-heal fired by a
/// shadow-cutover restore. A shadow-cutover swaps the logical alias to a
/// freshly loaded shadow tree but leaves the previous physical tree in place
/// (so the restore can be reverted). Stateless-worker routing activations cache
/// the logical-to-physical alias for the lifetime of the activation, so without
/// a staleness signal a warm activation would keep serving pre-restore data
/// forever. The restore marks the retained tree's shards so logical-alias
/// traffic is refused and the routing tier re-resolves onto the shadow. This
/// suite warms many concurrent routing activations against the pre-cutover
/// identity, cuts over under sustained read load, and pins that every activation
/// self-heals onto the restored snapshot with no read ever surfacing the
/// internal staleness signal and none left permanently stale.
/// </summary>
/// <remarks>
/// Complements the deterministic single-cutover self-heal regression that landed
/// with the retained-redirect fix. The sustained concurrent readers ensure the
/// heal holds across many warm activations and that the staleness signal is
/// always caught internally (never thrown to a caller), and the repeated-cutover
/// case proves the heal re-arms for each successive restore rather than firing
/// only once.
/// </remarks>
[TestFixture]
[NonParallelizable]
[Category("Chaos")]
public sealed class ShadowCutoverRoutingSelfHealChaosTests
{
    private RestoreClusterFixture _fixture = null!;

    [SetUp]
    public void SetUp() => _fixture = new RestoreClusterFixture();

    [TearDown]
    public async Task TearDown() => await _fixture.DisposeAsync();

    private static byte[] Bytes(string s) => Encoding.UTF8.GetBytes(s);
    private static string? Str(byte[]? v) => v is null ? null : Encoding.UTF8.GetString(v);

    // Poll every key through the logical tree until each resolves to its
    // expected post-restore value (or null), bounded by a deadline. Returns the
    // first offending (key, observed) pair if the deadline elapses without full
    // convergence.
    private static async Task<(string Key, string? Observed)?> AwaitConvergedAsync(
        ILattice tree,
        IReadOnlyDictionary<string, string?> expected,
        TimeSpan timeout)
    {
        var deadline = DateTime.UtcNow + timeout;
        (string, string?) last = default;
        while (DateTime.UtcNow < deadline)
        {
            var converged = true;
            foreach (var (key, want) in expected)
            {
                var got = Str(await tree.GetAsync(key));
                if (!string.Equals(got, want, StringComparison.Ordinal))
                {
                    converged = false;
                    last = (key, got);
                    break;
                }
            }
            if (converged) return null;
            await Task.Delay(15);
        }
        return last;
    }

    // A single shadow-cutover restore over a tree with many warm routing
    // activations under sustained concurrent reads must self-heal every
    // activation onto the restored snapshot: no reader ever surfaces the
    // internal staleness signal, and no key is left resolving to pre-cutover
    // (live) content.
    [Test]
    public async Task Cutover_self_heals_all_routing_activations_under_sustained_concurrent_reads()
    {
        await _fixture.InitializeAsync();

        const string source = "chaos-heal-src";
        var sourceTree = _fixture.GrainFactory.GetGrain<ILattice>(source);
        var keys = Enumerable.Range(0, 20).Select(i => $"k-{i:D2}").ToArray();
        foreach (var k in keys) await sourceTree.SetAsync(k, Bytes($"backup-{k}"));
        var backup = await _fixture.Capture.CaptureAsync(
            new LatticeBackupCaptureRequest("chaos-heal", BackupScopeSelector.WholeTree(source)));

        // The live target diverges from the backup: the same keys carry 'live'
        // values, plus 'gone' keys absent from the backup. A routing activation
        // that failed to self-heal would keep serving these.
        const string target = "chaos-heal-live";
        var live = _fixture.GrainFactory.GetGrain<ILattice>(target);
        foreach (var k in keys) await live.SetAsync(k, Bytes($"live-{k}"));
        var gone = Enumerable.Range(0, 10).Select(i => $"gone-{i:D2}").ToArray();
        foreach (var k in gone) await live.SetAsync(k, Bytes($"live-{k}"));

        var liveValues = keys.Select(k => $"live-{k}").ToHashSet(StringComparer.Ordinal);
        var backupValues = keys.Select(k => $"backup-{k}").ToHashSet(StringComparer.Ordinal);

        // Sustained concurrent readers hammer the logical tree across the
        // cutover. Each warms a stateless-worker routing activation. A reader
        // must never surface the internal staleness signal and must only ever
        // observe a known live-or-backup value for an existing key.
        using var readerCts = new CancellationTokenSource();
        var failures = new ConcurrentQueue<string>();
        var readers = Enumerable.Range(0, 8).Select(_ => Task.Run(async () =>
        {
            try
            {
                while (!readerCts.IsCancellationRequested)
                {
                    var k = keys[Random.Shared.Next(keys.Length)];
                    var got = Str(await live.GetAsync(k));
                    if (got is null || (!liveValues.Contains(got) && !backupValues.Contains(got)))
                        failures.Enqueue($"reader observed unexpected value '{got}' for '{k}'");
                }
            }
            catch (Exception ex)
            {
                failures.Enqueue($"reader threw {ex.GetType().Name}: {ex.Message}");
            }
        })).ToArray();

        // Let the readers warm several activations before cutting over.
        await Task.Delay(100);

        var result = await _fixture.Restore.RestoreAsync(
            new LatticeRestoreRequest(backup.BackupId, target, mode: LatticeRestoreMode.ShadowCutover));
        Assert.That(result.Mode, Is.EqualTo(LatticeRestoreMode.ShadowCutover));

        // Keep reading briefly after the cutover so warm activations take at
        // least one more request and self-heal.
        await Task.Delay(150);
        readerCts.Cancel();
        await Task.WhenAll(readers);

        Assert.That(failures, Is.Empty,
            "No reader may surface the internal staleness signal or observe an out-of-universe value: "
            + string.Join(" | ", failures));

        var expected = keys.ToDictionary(k => k, k => (string?)$"backup-{k}");
        foreach (var k in gone) expected[k] = null;
        var offender = await AwaitConvergedAsync(live, expected, TimeSpan.FromSeconds(10));
        Assert.That(offender, Is.Null,
            $"Every routing activation must self-heal onto the restored snapshot; '{offender?.Key}' still resolved to '{offender?.Observed}'.");
    }

    // Repeated shadow-cutover restores to successively earlier point-in-times
    // must re-arm the routing self-heal each time: after each cutover every
    // routing activation converges onto that restore's snapshot, proving the
    // heal is not a one-shot.
    [Test]
    public async Task Repeated_cutovers_self_heal_routing_to_each_successive_snapshot()
    {
        await _fixture.InitializeAsync();

        const string source = "chaos-heal-multi-src";
        var sourceTree = _fixture.GrainFactory.GetGrain<ILattice>(source);

        // Earlier snapshot P1: a-00..a-04.
        var p1Keys = Enumerable.Range(0, 5).Select(i => $"a-{i:D2}").ToArray();
        foreach (var k in p1Keys) await sourceTree.SetAsync(k, Bytes($"p1-{k}"));
        var p1 = await _fixture.Capture.CaptureAsync(
            new LatticeBackupCaptureRequest("chaos-heal-p1", BackupScopeSelector.WholeTree(source)));

        // Later snapshot P2 adds a-05..a-09 (and overwrites the first five so
        // each snapshot is value-distinguishable).
        foreach (var k in p1Keys) await sourceTree.SetAsync(k, Bytes($"p2-{k}"));
        var p2Extra = Enumerable.Range(5, 5).Select(i => $"a-{i:D2}").ToArray();
        foreach (var k in p2Extra) await sourceTree.SetAsync(k, Bytes($"p2-{k}"));
        var p2Keys = p1Keys.Concat(p2Extra).ToArray();
        var p2 = await _fixture.Capture.CaptureAsync(
            new LatticeBackupCaptureRequest("chaos-heal-p2", BackupScopeSelector.WholeTree(source)));

        // Live target diverges from both snapshots and warms its routing.
        const string target = "chaos-heal-multi-live";
        var live = _fixture.GrainFactory.GetGrain<ILattice>(target);
        foreach (var k in p2Keys) await live.SetAsync(k, Bytes($"live-{k}"));
        var warm = Enumerable.Range(0, 6).Select(_ => Task.Run(async () =>
        {
            for (var i = 0; i < 20; i++)
                await live.GetAsync(p2Keys[Random.Shared.Next(p2Keys.Length)]);
        })).ToArray();
        await Task.WhenAll(warm);

        // Cutover to P2: every routing activation must converge to P2 values.
        await _fixture.Restore.RestoreAsync(
            new LatticeRestoreRequest(p2.BackupId, target, mode: LatticeRestoreMode.ShadowCutover));
        var expectP2 = p2Keys.ToDictionary(k => k, k => (string?)$"p2-{k}");
        Assert.That(await AwaitConvergedAsync(live, expectP2, TimeSpan.FromSeconds(10)), Is.Null,
            "Routing must self-heal onto the P2 snapshot after the first cutover.");

        // Re-warm, then cutover again to the earlier P1: routing must re-arm and
        // converge to P1, with a-05..a-09 now absent.
        var rewarm = Enumerable.Range(0, 6).Select(_ => Task.Run(async () =>
        {
            for (var i = 0; i < 20; i++)
                await live.GetAsync(p2Keys[Random.Shared.Next(p2Keys.Length)]);
        })).ToArray();
        await Task.WhenAll(rewarm);

        await _fixture.Restore.RestoreAsync(
            new LatticeRestoreRequest(p1.BackupId, target, mode: LatticeRestoreMode.ShadowCutover));
        var expectP1 = p1Keys.ToDictionary(k => k, k => (string?)$"p1-{k}");
        foreach (var k in p2Extra) expectP1[k] = null;
        Assert.That(await AwaitConvergedAsync(live, expectP1, TimeSpan.FromSeconds(10)), Is.Null,
            "The routing self-heal must re-arm for the second cutover and converge onto the earlier P1 snapshot.");
    }
}
