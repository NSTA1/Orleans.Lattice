using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Operations;
using Orleans.Serialization;

namespace Orleans.Lattice.Backup.Tests;

/// <summary>
/// The staged cross-tree-consistent set capture (issue #6067): the set's saga
/// decision gate covers only the member snapshot opens, so a cross-tree atomic
/// write issued while the members are being written is admitted - and is not in
/// the backup, whose content was fixed when the snapshots opened - and members
/// spilled to the durable baseline store under a zero memory budget are still
/// written whole.
/// </summary>
[Category("Integration")]
public sealed class LatticeBackupSetCaptureStagingTests
{
    private CaptureClusterFixture _fixture = null!;

    [SetUp]
    public void SetUp()
    {
        BackupInventoryRegistry.Instance.Reset();
        _fixture = new CaptureClusterFixture();
    }

    [TearDown]
    public Task TearDown() => _fixture.DisposeAsync();

    [Test]
    public async Task A_cross_tree_write_while_members_are_written_is_admitted_and_not_captured()
    {
        await _fixture.InitializeAsync();
        var suffix = Guid.NewGuid().ToString("N");
        var treeA = $"stage-a-{suffix}";
        var treeB = $"stage-b-{suffix}";
        await Tree(treeA).SetAsync("k", Bytes("old"));
        await Tree(treeB).SetAsync("k", Bytes("old"));

        var hook = new WritingPhaseHook(() => _fixture.GrainFactory.SetManyAtomicAsync(
            [
                new LatticeTreeBatch(treeA, [new KeyValuePair<string, byte[]>("k", Bytes("new"))]),
                new LatticeTreeBatch(treeB, [new KeyValuePair<string, byte[]>("k", Bytes("new"))]),
            ],
            operationId: $"op-{suffix}"));

        LatticeBackupSetCaptureResult result;
        using (LatticeOperationProgress.Enter(hook))
        {
            result = await _fixture.Capture.CaptureSetAsync(new LatticeBackupSetCaptureRequest(
                $"stage-{suffix}",
                [BackupScopeSelector.WholeTree(treeA), BackupScopeSelector.WholeTree(treeB)],
                crossTreeConsistent: true));
        }

        Assert.That(hook.Write, Is.Not.Null, "the write must have been issued during the write phase");
        await hook.Write!;
        var capturedA = await ValueAsync(result, treeA);
        var capturedB = await ValueAsync(result, treeB);

        var live = Encoding.UTF8.GetString((await Tree(treeA).GetAsync("k"))!);
        Assert.Multiple(() =>
        {
            Assert.That(result.SetManifest.Fence!.Attempts, Is.EqualTo(1));
            Assert.That(capturedA, Is.EqualTo("old"));
            Assert.That(capturedB, Is.EqualTo("old"));
            Assert.That(live, Is.EqualTo("new"), "the write was admitted, not refused by the gate");
        });
    }

    [Test]
    public async Task Members_spilled_under_a_zero_budget_are_written_whole()
    {
        await _fixture.InitializeAsync(pinnedEntryBudget: 0);
        var suffix = Guid.NewGuid().ToString("N");
        var trees = new[] { $"spill-a-{suffix}", $"spill-b-{suffix}", $"spill-c-{suffix}" };
        foreach (var tree in trees)
        {
            for (var i = 0; i < 25; i++)
            {
                await Tree(tree).SetAsync($"k{i:D2}", Bytes($"{tree}-{i}"));
            }
        }

        var result = await _fixture.Capture.CaptureSetAsync(new LatticeBackupSetCaptureRequest(
            $"spill-{suffix}",
            trees.Select(BackupScopeSelector.WholeTree).ToList(),
            crossTreeConsistent: true,
            pageSize: 4));

        foreach (var member in result.Members)
        {
            var entries = await DecodeAsync(member.Manifest);
            Assert.That(entries.Count(e => !e.IsTombstone), Is.EqualTo(25), member.Manifest.Scope.TreeId);
        }
    }

    [Test]
    public async Task Every_member_is_authorized_before_any_member_is_pinned()
    {
        // Regression (#6090 review): authorization, the size check and the WAL head
        // read ran per member under the set's decision gate, interleaved with the
        // snapshot opens. They now run for every member before the fence, so the
        // gate covers only the opens.
        await _fixture.InitializeAsync();
        var suffix = Guid.NewGuid().ToString("N");
        var trees = new[] { $"prep-a-{suffix}", $"prep-b-{suffix}", $"prep-c-{suffix}" };
        foreach (var tree in trees)
        {
            await Tree(tree).SetAsync("k", Bytes("v"));
        }

        var log = new List<string>();
        var capture = _fixture.CreateCaptureServiceWith(new BackupAccessAuthorizer(new LoggingAccessGate(log)));
        using (LatticeOperationProgress.Enter(new PhaseLog(log)))
        {
            await capture.CaptureSetAsync(new LatticeBackupSetCaptureRequest(
                $"prep-{suffix}", trees.Select(BackupScopeSelector.WholeTree).ToList(), crossTreeConsistent: true));
        }

        List<string> snapshot;
        lock (log)
        {
            snapshot = [.. log];
        }

        var firstPin = snapshot.IndexOf($"phase:{BackupOperationPhases.CapturingMembers}");
        Assert.That(firstPin, Is.GreaterThan(0), "the set reports its pin phase");
        foreach (var tree in trees)
        {
            var authorized = snapshot.IndexOf($"auth:{tree}");
            Assert.That(authorized, Is.InRange(0, firstPin - 1), $"{tree} must be authorized before the pin phase");
        }
    }

    private async Task<string?> ValueAsync(LatticeBackupSetCaptureResult result, string treeId)
    {
        var member = result.Members.Single(m => m.Manifest.Scope.TreeId == treeId);
        var entries = (await DecodeAsync(member.Manifest)).Where(e => e.Key == "k").ToList();
        return entries.Count == 0 || entries[0].IsTombstone ? null : Encoding.UTF8.GetString(entries[0].Value!);
    }

    private async Task<List<LwwEntry>> DecodeAsync(BackupManifest manifest)
    {
        var serializer = _fixture.SiloServices.GetRequiredService<Serializer>();
        var all = new List<LwwEntry>();
        foreach (var descriptor in manifest.ContentDescriptors)
        {
            await foreach (var chunk in _fixture.Sink.ReadArtifactAsync(descriptor.ArtifactId))
            {
                all.AddRange(serializer.Deserialize<LwwEntry[]>(chunk));
            }
        }

        return all;
    }

    private ILattice Tree(string treeId) => _fixture.GrainFactory.GetGrain<ILattice>(treeId);

    private static byte[] Bytes(string s) => Encoding.UTF8.GetBytes(s);

    private sealed class LoggingAccessGate(List<string> log) : ILatticeAccessGate
    {
        public ValueTask<LatticeAccessDecision> AuthorizeAsync(
            in LatticeAccessRequest request,
            CancellationToken cancellationToken = default)
        {
            lock (log)
            {
                log.Add($"auth:{request.TreeId}");
            }

            return new(LatticeAccessDecision.Allow());
        }
    }

    private sealed class PhaseLog(List<string> log) : ILatticeOperationProgress
    {
        public ValueTask ReportAsync(string phase, long completedUnits = 0, long? totalUnits = null, string? unitName = null)
        {
            lock (log)
            {
                log.Add($"phase:{phase}");
            }

            return ValueTask.CompletedTask;
        }
    }

    private sealed class WritingPhaseHook(Func<Task> write) : ILatticeOperationProgress
    {
        public Task? Write { get; private set; }

        public async ValueTask ReportAsync(string phase, long completedUnits = 0, long? totalUnits = null, string? unitName = null)
        {
            if (Write is null && phase == BackupOperationPhases.WritingMembers && completedUnits == 0)
            {
                // The gate is already released here, so the write must complete
                // before the members are written rather than being refused.
                Write = Task.Run(write);
                await Write;
            }
        }
    }
}
