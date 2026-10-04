using System.Text;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Operations;

namespace Orleans.Lattice.Backup.Tests;

/// <summary>
/// The cross-tree set capture's post-capture re-observation, end to end: a
/// cross-tree atomic write that registers and completes entirely inside one
/// capture window leaves the in-flight count at zero, and only the moved
/// registration epoch shows the window was not quiet. The capture must discard
/// that attempt and take a second one. Pins the call site
/// <c>LatticeBackupCaptureService.CaptureFencedSetAsync</c> routes through
/// <see cref="CrossTreeFenceWindow.IsStable"/> (the <c>Validate</c> row of
/// <c>spec/backup/RefinementCapture.md</c>), which a test of the core alone
/// cannot see.
/// </summary>
[Category("Integration")]
public sealed class LatticeBackupSetCaptureWindowTests
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
    public async Task A_cross_tree_write_completing_inside_the_capture_window_forces_a_second_attempt()
    {
        await _fixture.InitializeAsync();
        var suffix = Guid.NewGuid().ToString("N");
        var treeA = $"window-a-{suffix}";
        var treeB = $"window-b-{suffix}";
        await _fixture.GrainFactory.GetGrain<ILattice>(treeA).SetAsync("k", Bytes("old"));
        await _fixture.GrainFactory.GetGrain<ILattice>(treeB).SetAsync("k", Bytes("old"));

        // After the first member is captured - once - a cross-tree atomic write
        // registers on both trees and completes, all inside the window.
        var hook = new AfterFirstMemberHook(() => _fixture.GrainFactory.SetManyAtomicAsync(
            new[]
            {
                new LatticeTreeBatch(treeA, [new KeyValuePair<string, byte[]>("k", Bytes("new"))]),
                new LatticeTreeBatch(treeB, [new KeyValuePair<string, byte[]>("k", Bytes("new"))]),
            },
            operationId: $"op-{suffix}"));

        LatticeBackupSetCaptureResult result;
        using (LatticeOperationProgress.Enter(hook))
        {
            result = await _fixture.Capture.CaptureSetAsync(new LatticeBackupSetCaptureRequest(
                $"window-{suffix}",
                new[] { BackupScopeSelector.WholeTree(treeA), BackupScopeSelector.WholeTree(treeB) },
                crossTreeConsistent: true));
        }

        Assert.Multiple(() =>
        {
            Assert.That(hook.Fired, Is.True, "the write must have run inside the first attempt's window");
            Assert.That(result.SetManifest.Fence, Is.Not.Null);
            Assert.That(result.SetManifest.Fence!.Attempts, Is.EqualTo(2),
                "a registration inside the window must discard the attempt, though nothing is in flight when it closes");
        });
    }

    private static byte[] Bytes(string s) => Encoding.UTF8.GetBytes(s);

    private sealed class AfterFirstMemberHook(Func<Task> write) : ILatticeOperationProgress
    {
        public bool Fired { get; private set; }

        public async ValueTask ReportAsync(string phase, long completedUnits = 0, long? totalUnits = null, string? unitName = null)
        {
            if (!Fired && phase == BackupOperationPhases.CapturingMembers && completedUnits == 1)
            {
                Fired = true;
                await write();
            }
        }
    }
}
