using System.Collections.Concurrent;
using System.Text;
using Orleans.Configuration;
using Orleans.Hosting;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Testing;
using Orleans.TestingHost;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Issue #3941, end to end on a real silo scheduler: <see cref="ILattice.PurgeTreeAsync"/>
/// must not be stopped part-way by the response timeout of the call that asked for
/// it, and the deletion status must report the purge in progress while it runs.
/// <para>
/// The slow shard is pinned deterministically: an incoming-call filter holds one
/// shard root's <c>PurgeAsync</c> until the test releases it - the shape of the
/// incident, where one shard's atomic walk held its activation for 43 s against a
/// 30 s response timeout. The cluster's response timeout is shortened so the hold
/// outlasts it in seconds rather than minutes. Before the fix the purge call timed
/// out, the status read queued behind the walk, and nothing reported the purge as
/// running.
/// </para>
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class TreePurgeAcceptThenPollIntegrationTests
{
    /// <summary>The response timeout the silo and client are given.</summary>
    private static readonly TimeSpan ResponseTimeout = TimeSpan.FromSeconds(4);

    /// <summary>How long a purge call or status read may take to answer while a shard is held.</summary>
    private static readonly TimeSpan PromptAnswer = TimeSpan.FromSeconds(10);

    private TestCluster _cluster = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        var builder = new TestClusterBuilder(initialSilosCount: 1);
        builder.AddSiloBuilderConfigurator<SiloConfigurator>();
        builder.AddClientBuilderConfigurator<ClientConfigurator>();
        _cluster = builder.Build();
        await _cluster.DeployAsync();
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown()
    {
        PurgeGate.ReleaseAll();
        await _cluster.StopAllSilosAsync();
        await _cluster.DisposeAsync();
    }

    [Test]
    public async Task A_purge_whose_shard_outlasts_the_response_timeout_is_accepted_reported_and_completed()
    {
        var treeId = $"purge-accept-{Guid.NewGuid():N}";
        var tree = _cluster.Client.GetGrain<ILattice>(treeId);
        await WriteAsync(tree);
        await tree.DeleteTreeAsync();

        var deletion = _cluster.Client.GetGrain<ITreeDeletionGrain>(treeId);
        var hold = PurgeGate.Arm(shardKey => shardKey == $"{treeId}/0");
        try
        {
            await AnswerPromptlyAsync(tree.PurgeTreeAsync(), "the purge call");
            await hold.Entered.Task.WaitAsync(PromptAnswer);

            // The shard is still held past the response timeout, yet the status
            // answers, and it says the purge is running rather than nothing at all.
            await Task.Delay(ResponseTimeout);
            var running = await AnswerPromptlyAsync(deletion.GetDeletionStatusAsync(), "the deletion status read");
            Assert.Multiple(() =>
            {
                Assert.That(running.PurgeInProgress, Is.True, "a purge whose shard is being walked is in progress");
                Assert.That(running.PurgeComplete, Is.False);
                Assert.That(running.CanRecover, Is.False);
                Assert.That(running.PurgeShardCount, Is.GreaterThan(0));
                Assert.That(running.PurgedShardCount, Is.Zero, "the held shard has not finished");
            });

            // The walk's timer tick holds the deletion coordinator for up to a
            // response timeout each time it re-asks the held shard, so a status
            // read that queued behind it would stall for seconds. Sampled across
            // more than one retry cycle, every read must still answer at once.
            for (var i = 0; i < 12; i++)
            {
                var sample = deletion.GetDeletionStatusAsync();
                try
                {
                    await sample.WaitAsync(TimeSpan.FromSeconds(1));
                }
                catch (TimeoutException)
                {
                    Assert.Fail("the deletion status read queued behind the purge walk instead of answering at once");
                }
                await Task.Delay(500);
            }

            // A retry while it runs is acknowledged, not refused or timed out.
            await AnswerPromptlyAsync(tree.PurgeTreeAsync(), "a retried purge call");
            Assert.That(hold.Release.Task.IsCompleted, Is.False, "precondition: the shard was held throughout");
        }
        finally
        {
            hold.Release.TrySetResult();
        }

        // Once the shard answers, the background walk finishes the rest of the
        // tree on its own - nobody has to keep re-issuing the purge.
        await TestPoll.UntilAsync(
            async () => (await deletion.GetDeletionStatusAsync()).PurgeComplete,
            "the accepted purge to complete",
            timeout: TimeSpan.FromSeconds(60));

        var done = await deletion.GetDeletionStatusAsync();
        Assert.Multiple(async () =>
        {
            Assert.That(done.PurgeInProgress, Is.False);
            Assert.That(done.PurgedShardCount, Is.EqualTo(done.PurgeShardCount));
            Assert.That(done.PurgeShardCount, Is.GreaterThan(0));
            Assert.That(await tree.TreeExistsAsync(), Is.False, "the purged tree is unregistered");
        });

        // A retry after completion reports the success rather than a failure.
        await AnswerPromptlyAsync(tree.PurgeTreeAsync(), "a purge call after completion");
    }

    [Test]
    public async Task An_aliased_tree_s_purge_is_accepted_while_its_copy_s_shard_is_held_and_completes()
    {
        var treeId = $"purge-accept-alias-{Guid.NewGuid():N}";
        var tree = _cluster.Client.GetGrain<ILattice>(treeId);
        await WriteAsync(tree);
        await tree.ResizeAsync(64, 64);
        await TestPoll.UntilAsync(
            () => tree.IsResizeCompleteAsync(),
            "the resize to finish",
            timeout: TimeSpan.FromSeconds(60));
        var registry = _cluster.Client.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        var copy = await registry.ResolveAsync(treeId);
        Assert.That(copy, Is.Not.EqualTo(treeId), "precondition: the resize aliased the tree to a copy");
        await tree.DeleteTreeAsync();

        var deletion = _cluster.Client.GetGrain<ITreeDeletionGrain>(treeId);
        var hold = PurgeGate.Arm(shardKey => shardKey == $"{copy}/0");
        try
        {
            await AnswerPromptlyAsync(tree.PurgeTreeAsync(), "the purge call");
            await hold.Entered.Task.WaitAsync(PromptAnswer);
            await Task.Delay(ResponseTimeout);

            var running = await AnswerPromptlyAsync(deletion.GetDeletionStatusAsync(), "the deletion status read");
            Assert.Multiple(() =>
            {
                Assert.That(running.PurgeInProgress, Is.True);
                Assert.That(running.PurgeComplete, Is.False);
                Assert.That(running.PurgeShardCount, Is.GreaterThan(0), "the logical status reports the copy's walk");
            });
        }
        finally
        {
            hold.Release.TrySetResult();
        }

        await TestPoll.UntilAsync(
            async () => (await deletion.GetDeletionStatusAsync()).PurgeComplete,
            "the accepted logical purge to complete",
            timeout: TimeSpan.FromSeconds(60));

        Assert.Multiple(async () =>
        {
            Assert.That(await registry.ExistsAsync(treeId), Is.False, "the logical tree is unregistered");
            Assert.That(await registry.ExistsAsync(copy), Is.False, "the copy is unregistered");
        });
        await AnswerPromptlyAsync(tree.PurgeTreeAsync(), "a purge call after completion");
    }

    private static async Task WriteAsync(ILattice tree)
    {
        for (var i = 0; i < 48; i++)
            await tree.SetAsync($"k{i:D3}", Encoding.UTF8.GetBytes($"v{i}"));
    }

    private static async Task AnswerPromptlyAsync(Task call, string what)
    {
        try
        {
            await call.WaitAsync(PromptAnswer);
        }
        catch (TimeoutException ex)
        {
            Assert.Fail($"{what} did not answer: {ex.Message}. It was bounded by the walk of a shard held past "
                + "the response timeout instead of being accepted and returned.");
        }
    }

    private static async Task<T> AnswerPromptlyAsync<T>(Task<T> call, string what)
    {
        try
        {
            return await call.WaitAsync(PromptAnswer);
        }
        catch (TimeoutException ex)
        {
            Assert.Fail($"{what} did not answer: {ex.Message}. It queued behind the walk of a shard held past "
                + "the response timeout.");
            throw;
        }
    }

    /// <summary>
    /// Holds <see cref="IShardRootGrain.PurgeAsync"/> on a matching shard root until
    /// the test releases it, keeping that shard's activation inside its purge walk.
    /// </summary>
    private sealed class PurgeGate : IIncomingGrainCallFilter
    {
        private static readonly ConcurrentBag<(Func<string, bool> Match, Hold Hold)> Holds = new();

        internal static Hold Arm(Func<string, bool> match)
        {
            var hold = new Hold();
            Holds.Add((match, hold));
            return hold;
        }

        internal static void ReleaseAll()
        {
            foreach (var (_, hold) in Holds) hold.Release.TrySetResult();
        }

        public async Task Invoke(IIncomingGrainCallContext context)
        {
            if (context.MethodName == nameof(IShardRootGrain.PurgeAsync)
                && context.InterfaceMethod?.DeclaringType == typeof(IShardRootGrain))
            {
                var key = context.TargetId.Key.ToString()!;
                foreach (var (match, hold) in Holds)
                {
                    if (!match(key)) continue;
                    hold.Entered.TrySetResult();
                    await hold.Release.Task;
                }
            }

            await context.Invoke();
        }
    }

    private sealed class Hold
    {
        public TaskCompletionSource Entered { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public TaskCompletionSource Release { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
    }

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.UseInMemoryReminderService();
            siloBuilder.Configure<SiloMessagingOptions>(o => o.ResponseTimeout = ResponseTimeout);
            siloBuilder.AddIncomingGrainCallFilter<PurgeGate>();
        }
    }

    private sealed class ClientConfigurator : IClientBuilderConfigurator
    {
        public void Configure(Microsoft.Extensions.Configuration.IConfiguration configuration, IClientBuilder clientBuilder)
        {
            clientBuilder.Configure<ClientMessagingOptions>(o => o.ResponseTimeout = ResponseTimeout);
        }
    }
}
