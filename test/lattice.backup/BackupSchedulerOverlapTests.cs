using System.Text;

namespace Orleans.Lattice.Backup.Tests;

/// <summary>
/// Overlap-guard coverage for the backup scheduler grain: while a capture for a
/// scope is in flight, a second concurrent trigger for the same scope is skipped
/// (returns <c>null</c>) rather than starting an overlapping capture, and the
/// capture engine is invoked exactly once.
/// </summary>
[Category("Integration")]
public sealed class BackupSchedulerOverlapTests
{
    private const string Tree = "orders";

    private SchedulerClusterFixture _fixture = null!;

    [SetUp]
    public void SetUp() => _fixture = new SchedulerClusterFixture();

    [TearDown]
    public async Task TearDown() => await _fixture.DisposeAsync();

    [Test]
    public async Task TriggerFullAsync_skips_a_second_concurrent_capture_for_the_same_scope()
    {
        await _fixture.InitializeAsync(gated: true);
        var scope = BackupScopeSelector.WholeTree(Tree);
        var tree = _fixture.GrainFactory.GetGrain<ILattice>(Tree);
        await tree.SetAsync("k1", Encoding.UTF8.GetBytes("v1"));
        var scheduler = _fixture.Scheduler(scope);

        // Start the first capture and wait until it is parked inside the gate, so
        // the grain's in-flight guard is armed before the second trigger arrives.
        var first = scheduler.TriggerFullAsync(scope);
        await _fixture.Gate.Started.WaitAsync(TimeSpan.FromSeconds(30));

        // The second trigger interleaves while the first is still running.
        var secondResult = await scheduler.TriggerFullAsync(scope);

        _fixture.Gate.Release();
        var firstResult = await first;

        Assert.Multiple(() =>
        {
            Assert.That(firstResult, Is.Not.Null, "the in-flight capture should complete with a backup id");
            Assert.That(secondResult, Is.Null, "the overlapping trigger should be skipped");
            Assert.That(_fixture.Gate.Calls, Is.EqualTo(1), "the capture engine should run exactly once");
        });
    }
}
