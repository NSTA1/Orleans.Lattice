using System.Collections.Concurrent;
using System.Text;
using Orleans.Hosting;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Testing;
using Orleans.TestingHost;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// A resize must not report itself complete while it still holds the tree's
/// alias reservation (issue #4527). A completing resize persists its completion
/// before it releases the reservation, so a caller that deleted the tree the
/// moment <see cref="ILattice.IsResizeCompleteAsync"/> answered
/// <see langword="true"/> could be refused with "alias operation 'resize:...' is
/// in progress"; and a completion whose release failed left the reservation held
/// for good, refusing every delete and alias change of a tree that reported its
/// resize complete. The first release is failed here, which makes the window
/// deterministic: the resize must keep reporting not complete until a later
/// phase tick has released the reservation, and a delete issued as soon as it
/// reports complete must be admitted.
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class ResizeCompletionAliasReleaseIntegrationTests
{
    private TestCluster _cluster = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        var builder = new TestClusterBuilder(initialSilosCount: 1);
        builder.AddSiloBuilderConfigurator<SiloConfigurator>();
        _cluster = builder.Build();
        await _cluster.DeployAsync();
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown()
    {
        await _cluster.StopAllSilosAsync();
        await _cluster.DisposeAsync();
    }

    [Test]
    public async Task A_resize_whose_alias_release_failed_reports_complete_only_once_it_is_released()
    {
        var treeId = $"resize-release-{Guid.NewGuid():N}";
        var tree = _cluster.Client.GetGrain<ILattice>(treeId);
        for (var i = 0; i < 48; i++)
            await tree.SetAsync($"k{i:D3}", Encoding.UTF8.GetBytes($"v{i}"));

        var failed = FailFirstRelease.Arm(treeId);
        await tree.ResizeAsync(64, 64);
        await failed.Task.WaitAsync(TimeSpan.FromSeconds(60));

        await TestPoll.UntilAsync(() => tree.IsResizeCompleteAsync(), "the resize to complete",
            timeout: TimeSpan.FromSeconds(60));

        // Once it reports complete, a delete is admitted at once: no retry.
        await tree.DeleteTreeAsync();
        Assert.That(await _cluster.Client.GetGrain<ITreeDeletionGrain>(treeId).IsDeletedAsync(), Is.True);
    }

    /// <summary>
    /// Fails the first release of an armed tree's alias reservation
    /// (<see cref="ITreeDeletionGrain.EndAliasChangeAsync"/>) at the deletion
    /// grain, before it has any effect - the shape of a release interrupted by a
    /// transient fault or a silo stopping.
    /// </summary>
    private static class FailFirstRelease
    {
        private static readonly ConcurrentDictionary<string, TaskCompletionSource> Armed = new();

        public static TaskCompletionSource Arm(string treeId) =>
            Armed[treeId] = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        public static Task InvokeAsync(IIncomingGrainCallContext context)
        {
            if (context.InterfaceMethod?.Name == nameof(ITreeDeletionGrain.EndAliasChangeAsync)
                && context.TargetId.Type.ToString() == "treedeletion"
                && Armed.TryRemove(context.TargetId.Key.ToString()!, out var failed))
            {
                failed.TrySetResult();
                throw new TimeoutException("Injected: the alias reservation release did not complete.");
            }

            return context.Invoke();
        }
    }

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.UseInMemoryReminderService();
            siloBuilder.AddIncomingGrainCallFilter(FailFirstRelease.InvokeAsync);
        }
    }
}
