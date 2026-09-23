using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests;

/// <summary>
/// Tests for the foreground lease the <c>search</c> and <c>context</c> tools hold
/// on the indexing pacer while they run (issue #3447): the lease is open for
/// exactly the duration of the operation, is released however the operation ends,
/// and is simply absent on a host that registers no pacer.
/// </summary>
[TestFixture]
public sealed class RepoContextToolHandlerForegroundTests
{
    private static RepoContextIndexingPacer Pacer() => new(
        new RepoContextIndexingOptions(),
        TimeProvider.System,
        NullLogger<RepoContextIndexingPacer>.Instance,
        memoryLoad: () => 0.1);

    [Test]
    public async Task RunInForegroundAsync_holds_a_lease_for_the_duration_of_the_operation()
    {
        var pacer = Pacer();
        var context = await RepoContextRequestContexts.CreateAsync(
            new ServiceCollection().AddSingleton(pacer).BuildServiceProvider());

        var during = await RepoContextToolHandlers.RunInForegroundAsync(
            context, () => Task.FromResult(pacer.Snapshot().ForegroundRequests));

        Assert.Multiple(() =>
        {
            Assert.That(during, Is.EqualTo(1), "The tool's own work runs inside the lease.");
            Assert.That(pacer.Snapshot().ForegroundRequests, Is.Zero, "The lease is released when the tool returns.");
        });
    }

    [Test]
    public async Task RunInForegroundAsync_faulted_operation_still_releases_the_lease()
    {
        var pacer = Pacer();
        var context = await RepoContextRequestContexts.CreateAsync(
            new ServiceCollection().AddSingleton(pacer).BuildServiceProvider());

        Assert.That(
            async () => await RepoContextToolHandlers.RunInForegroundAsync<int>(
                context, () => throw new InvalidOperationException("boom")),
            Throws.InvalidOperationException);
        Assert.That(pacer.Snapshot().ForegroundRequests, Is.Zero,
            "A failed tool call must not leave the pacer yielding to a request that is gone.");
    }

    [Test]
    public async Task RunInForegroundAsync_without_a_registered_pacer_just_runs_the_operation()
    {
        var context = await RepoContextRequestContexts.CreateAsync(new ServiceCollection().BuildServiceProvider());

        var result = await RepoContextToolHandlers.RunInForegroundAsync(context, () => Task.FromResult(42));

        Assert.That(result, Is.EqualTo(42));
    }
}
