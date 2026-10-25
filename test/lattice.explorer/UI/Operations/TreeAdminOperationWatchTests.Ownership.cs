using NSubstitute;
using Orleans.Lattice.Api.Operations;
using Orleans.Lattice.Api.TreeAdmin;
using Orleans.Lattice.Explorer.UI.Operations;
using static Orleans.Lattice.Explorer.Tests.UI.Operations.TreeAdminOperationScript;

namespace Orleans.Lattice.Explorer.Tests.UI.Operations;

public sealed partial class TreeAdminOperationWatchTests
{
    [TestCase(false)]
    [TestCase(true)]
    public async Task Start_answering_after_clear_or_disposal_does_not_resume_following(bool dispose)
    {
        using var watch = new TreeAdminOperationWatch(_time);
        var accepted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var start = watch.StartAsync(
            _script.Operations, TreeAdminOperationKinds.ViewRebuild, "old",
            async (id, ct) =>
            {
                var handle = await _script.Operations.StartViewRebuildAsync("old", id, ct);
                await accepted.Task;
                return handle;
            }, CancellationToken.None);

        if (dispose)
        {
            watch.Dispose();
        }
        else
        {
            watch.Clear();
        }

        accepted.SetResult();
        await start;

        Assert.Multiple(() =>
        {
            Assert.That(watch.OperationId, Is.Null);
            Assert.That(watch.IsRunning, Is.False);
            Assert.That(_time.ArmedTimers, Is.Zero);
        });
    }

    [Test]
    public async Task Resume_answering_after_a_new_start_does_not_replace_it()
    {
        var oldId = TreeAdminOperationIds.New(TreeAdminOperationKinds.ViewRebuild, "old");
        _script.Running(oldId, Status(TreeAdminOperationKinds.ViewRebuild, LatticeOperationState.Running));
        var listed = new TaskCompletionSource<LatticeOperationPage>(TaskCreationOptions.RunContinuationsAsynchronously);
        _script.Operations.ListOperationsAsync(Arg.Any<LatticeOperationListRequest>(), Arg.Any<CancellationToken>()).Returns(listed.Task);
        using var watch = new TreeAdminOperationWatch(_time);
        var resume = watch.ResumeAsync(_script.Operations, [(TreeAdminOperationKinds.ViewRebuild, "old")], CancellationToken.None);
        await watch.StartAsync(
            _script.Operations, TreeAdminOperationKinds.ViewRebuild, "current",
            (id, ct) => _script.Operations.StartViewRebuildAsync("current", id, ct), CancellationToken.None);
        var currentId = watch.OperationId;

        listed.SetResult(new LatticeOperationPage { Operations = _script.Listed.ToList() });
        var resumed = await resume;

        Assert.Multiple(() =>
        {
            Assert.That(resumed, Is.False);
            Assert.That(watch.OperationId, Is.EqualTo(currentId));
            Assert.That(watch.Target, Is.EqualTo("current"));
        });
    }

    [Test]
    public async Task Start_that_answers_after_cancellation_does_not_follow()
    {
        using var watch = new TreeAdminOperationWatch(_time);
        using var cancellation = new CancellationTokenSource();
        await cancellation.CancelAsync();

        Assert.That(async () => await watch.StartAsync(
            _script.Operations, TreeAdminOperationKinds.ViewRebuild, "old",
            (id, ct) => _script.Operations.StartViewRebuildAsync("old", id, ct), cancellation.Token),
            Throws.InstanceOf<OperationCanceledException>());
        Assert.Multiple(() =>
        {
            Assert.That(watch.OperationId, Is.Null);
            Assert.That(_time.ArmedTimers, Is.Zero);
        });
    }
}
