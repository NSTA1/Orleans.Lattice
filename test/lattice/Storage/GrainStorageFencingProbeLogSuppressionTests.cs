using Microsoft.Extensions.Logging;
using NUnit.Framework;

namespace Orleans.Lattice.Tests.Storage;

/// <summary>
/// Unit tests for <see cref="GrainStorageFencingProbeLogSuppression"/> (issue #5442):
/// the scope that marks the fencing probe's own asynchronous flow.
/// </summary>
[TestFixture]
public sealed class GrainStorageFencingProbeLogSuppressionTests
{
    [Test]
    public void ShouldSuppress_outside_a_scope_keeps_every_level()
    {
        foreach (var level in Enum.GetValues<LogLevel>())
        {
            Assert.That(GrainStorageFencingProbeLogSuppression.ShouldSuppress(level), Is.False, level.ToString());
        }
    }

    [TestCase(LogLevel.Trace, false)]
    [TestCase(LogLevel.Debug, false)]
    [TestCase(LogLevel.Information, false)]
    [TestCase(LogLevel.Warning, true)]
    [TestCase(LogLevel.Error, true)]
    [TestCase(LogLevel.Critical, true)]
    [TestCase(LogLevel.None, false)]
    public void ShouldSuppress_inside_a_scope_drops_warning_and_above_only(LogLevel level, bool suppressed)
    {
        using var scope = GrainStorageFencingProbeLogSuppression.Enter();

        Assert.That(GrainStorageFencingProbeLogSuppression.ShouldSuppress(level), Is.EqualTo(suppressed));
    }

    [Test]
    public void Dispose_ends_suppression_on_the_calling_flow()
    {
        var scope = GrainStorageFencingProbeLogSuppression.Enter();

        scope.Dispose();

        Assert.That(GrainStorageFencingProbeLogSuppression.ShouldSuppress(LogLevel.Error), Is.False);
        Assert.That(scope.IsOpen, Is.False);
    }

    [Test]
    public void Dispose_twice_is_harmless()
    {
        var scope = GrainStorageFencingProbeLogSuppression.Enter();

        scope.Dispose();
        scope.Dispose();

        Assert.That(GrainStorageFencingProbeLogSuppression.ShouldSuppress(LogLevel.Error), Is.False);
    }

    [Test]
    public async Task Dispose_ends_suppression_for_work_that_captured_the_flow_and_outlives_the_scope()
    {
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        Task<(bool During, bool After)> outliving;
        var scope = GrainStorageFencingProbeLogSuppression.Enter();
        try
        {
            outliving = Task.Run(async () =>
            {
                var during = GrainStorageFencingProbeLogSuppression.ShouldSuppress(LogLevel.Error);
                await release.Task;
                return (during, GrainStorageFencingProbeLogSuppression.ShouldSuppress(LogLevel.Error));
            });
            await Task.Delay(10);
        }
        finally
        {
            scope.Dispose();
        }

        release.SetResult();
        var (during, after) = await outliving;

        Assert.That(during, Is.True, "work started on the probe's flow is covered while the probe runs");
        Assert.That(after, Is.False, "and is no longer covered once the probe has finished");
    }

    [Test]
    public async Task Scope_does_not_cover_a_flow_that_did_not_start_inside_it()
    {
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var other = Task.Run(async () =>
        {
            await entered.Task;
            return GrainStorageFencingProbeLogSuppression.ShouldSuppress(LogLevel.Error);
        });

        using (GrainStorageFencingProbeLogSuppression.Enter())
        {
            entered.SetResult();
            Assert.That(await other, Is.False, "other work in the silo keeps its records while a probe runs");
        }
    }

    [Test]
    public void Nested_scopes_restore_the_enclosing_scope_on_dispose()
    {
        using var outer = GrainStorageFencingProbeLogSuppression.Enter();
        var inner = GrainStorageFencingProbeLogSuppression.Enter();

        inner.Dispose();

        Assert.That(GrainStorageFencingProbeLogSuppression.ShouldSuppress(LogLevel.Error), Is.True);
        outer.Dispose();
        Assert.That(GrainStorageFencingProbeLogSuppression.ShouldSuppress(LogLevel.Error), Is.False);
    }
}
