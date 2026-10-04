using System.Text;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Operations;

namespace Orleans.Lattice.Backup.Tests;

/// <summary>
/// The cross-tree set capture's window, end to end: a cross-tree atomic write
/// that tries to start while the members are being captured is refused by the
/// set's decision gate, which refuses a new delegation as the fence does (issue
/// #4485), so it is rolled back once the capture releases the gate, the window
/// stays quiet, and the first attempt is accepted. What the fence alone refuses,
/// and what the re-check and the re-observation catch when a fence is lost, is
/// pinned by <see cref="LatticeBackupSetCaptureHoldLossTests"/>.
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
    public async Task A_cross_tree_write_starting_inside_the_capture_window_is_refused_and_rolled_back()
    {
        await _fixture.InitializeAsync();
        var suffix = Guid.NewGuid().ToString("N");
        var treeA = $"window-a-{suffix}";
        var treeB = $"window-b-{suffix}";
        await _fixture.GrainFactory.GetGrain<ILattice>(treeA).SetAsync("k", Bytes("old"));
        await _fixture.GrainFactory.GetGrain<ILattice>(treeB).SetAsync("k", Bytes("old"));

        // After the first member is captured - once - a cross-tree atomic write
        // tries to start on both trees, inside the window. It is not awaited
        // there: a refused write's compensating abort is a decision, which the
        // capture's gate holds back until the capture releases it.
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

        var refused = false;
        try
        {
            await hook.Write!;
        }
        catch (InvalidOperationException)
        {
            refused = true;
        }

        Assert.Multiple(() =>
        {
            Assert.That(hook.Fired, Is.True, "the write must have run inside the first attempt's window");
            Assert.That(refused, Is.True, "the set's gate must refuse a cross-tree write starting inside the window");
            Assert.That(result.SetManifest.Fence, Is.Not.Null);
            Assert.That(result.SetManifest.Fence!.Attempts, Is.EqualTo(1),
                "a refused write never registers, so the first attempt's window stays quiet");
        });

        var a = await _fixture.GrainFactory.GetGrain<ILattice>(treeA).GetAsync("k");
        var b = await _fixture.GrainFactory.GetGrain<ILattice>(treeB).GetAsync("k");
        Assert.Multiple(() =>
        {
            Assert.That(Encoding.UTF8.GetString(a ?? []), Is.EqualTo("old"), "a refused batch must be rolled back on tree A");
            Assert.That(Encoding.UTF8.GetString(b ?? []), Is.EqualTo("old"), "a refused batch must be rolled back on tree B");
        });
    }

    private static byte[] Bytes(string s) => Encoding.UTF8.GetBytes(s);

    private sealed class AfterFirstMemberHook(Func<Task> write) : ILatticeOperationProgress
    {
        public bool Fired { get; private set; }

        public Task? Write { get; private set; }

        public async ValueTask ReportAsync(string phase, long completedUnits = 0, long? totalUnits = null, string? unitName = null)
        {
            if (!Fired && phase == BackupOperationPhases.CapturingMembers && completedUnits == 1)
            {
                Fired = true;
                Write = Task.Run(write);

                // Long enough for the write to reach its registration, which the
                // gate refuses at once.
                await Task.WhenAny(Write, Task.Delay(TimeSpan.FromSeconds(2)));
            }
        }
    }
}
