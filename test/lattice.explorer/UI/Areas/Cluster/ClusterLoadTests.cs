using Orleans.Lattice.Explorer.UI.Areas.Cluster;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Cluster;

[TestFixture]
public sealed class ClusterLoadTests
{
    [Test]
    public void Loading_has_neither_a_value_nor_an_error()
    {
        var load = ClusterLoad<object>.Loading;

        Assert.Multiple(() =>
        {
            Assert.That(load.IsLoading, Is.True);
            Assert.That(load.Value, Is.Null);
            Assert.That(load.Error, Is.Null);
            Assert.That(load.Denied, Is.False);
        });
    }

    [Test]
    public void Loaded_preserves_the_value_and_stops_loading()
    {
        var value = new object();
        var load = ClusterLoad<object>.Loaded(value);

        Assert.Multiple(() =>
        {
            Assert.That(load.Value, Is.SameAs(value));
            Assert.That(load.IsLoading, Is.False);
            Assert.That(load.Error, Is.Null);
            Assert.That(load.Denied, Is.False);
        });
    }

    [Test]
    public async Task RunAsync_passes_the_page_token_and_preserves_the_answer()
    {
        using var page = new CancellationTokenSource();
        var value = new object();
        var reply = new TaskCompletionSource<object>(TaskCreationOptions.RunContinuationsAsynchronously);
        CancellationToken received = default;
        var running = ClusterLoad<object>.RunAsync(token =>
        {
            received = token;
            return reply.Task;
        }, page.Token);

        Assert.That(running.IsCompleted, Is.False);
        reply.SetResult(value);
        var load = await running.WaitAsync(TimeSpan.FromSeconds(5));

        Assert.Multiple(() =>
        {
            Assert.That(received, Is.EqualTo(page.Token));
            Assert.That(load.Value, Is.SameAs(value));
            Assert.That(load.IsLoading, Is.False);
            Assert.That(load.Error, Is.Null);
            Assert.That(load.Denied, Is.False);
        });
    }

    [TestCase(false)]
    [TestCase(true)]
    public void RunAsync_page_cancellation_propagates_instead_of_becoming_an_error(bool synchronous)
    {
        using var page = new CancellationTokenSource();
        var reply = new TaskCompletionSource<object>(TaskCreationOptions.RunContinuationsAsynchronously);
        var cancelled = new OperationCanceledException(page.Token);
        var running = ClusterLoad<object>.RunAsync(_ =>
        {
            if (synchronous)
            {
                page.Cancel();
                throw cancelled;
            }

            return reply.Task;
        }, page.Token);

        page.Cancel();
        if (!synchronous)
        {
            reply.SetException(cancelled);
        }
        var thrown = Assert.ThrowsAsync<OperationCanceledException>(
            async () => await running.WaitAsync(TimeSpan.FromSeconds(5)));

        Assert.Multiple(() =>
        {
            Assert.That(thrown, Is.SameAs(cancelled));
            Assert.That(thrown!.CancellationToken, Is.EqualTo(page.Token));
            Assert.That(running.IsCanceled, Is.True);
        });
    }

    [TestCase(false)]
    [TestCase(true)]
    public async Task RunAsync_unrelated_cancellation_is_a_failed_read_not_page_cancellation(bool pageCanCancel)
    {
        using var page = new CancellationTokenSource();
        using var other = new CancellationTokenSource();
        other.Cancel();
        var pageToken = pageCanCancel ? page.Token : CancellationToken.None;

        var load = await ClusterLoad<object>.RunAsync(
            _ => Task.FromCanceled<object>(other.Token), pageToken);

        Assert.Multiple(() =>
        {
            Assert.That(pageToken.IsCancellationRequested, Is.False);
            Assert.That(load.IsLoading, Is.False);
            Assert.That(load.Value, Is.Null);
            Assert.That(load.Denied, Is.False);
            Assert.That(load.Error, Is.Not.Null.And.Not.Empty);
        });
    }

    [TestCase(false)]
    [TestCase(true)]
    public async Task RunAsync_denials_are_distinct_from_other_read_failures(bool denied)
    {
        Exception failure = denied
            ? new UnauthorizedAccessException("private permission details")
            : new InvalidOperationException("The read is unavailable.");

        var load = await ClusterLoad<object>.RunAsync(
            _ => Task.FromException<object>(failure), CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(load.Value, Is.Null);
            Assert.That(load.IsLoading, Is.False);
            Assert.That(load.Denied, Is.EqualTo(denied));
            Assert.That(load.Error, Is.EqualTo(denied ? ClusterFaults.Denied : failure.Message));
        });
    }

    [Test]
    public void Missing_values_and_read_delegates_are_rejected()
    {
        Assert.Multiple(() =>
        {
            Assert.That(() => ClusterLoad<object>.Loaded(null!),
                Throws.ArgumentNullException.With.Property("ParamName").EqualTo("value"));
            Assert.That(() => ClusterLoad<object>.RunAsync(null!, CancellationToken.None),
                Throws.ArgumentNullException.With.Property("ParamName").EqualTo("read"));
        });
    }
}
