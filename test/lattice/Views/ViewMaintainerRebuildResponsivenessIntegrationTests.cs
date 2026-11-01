using System.Collections.Concurrent;
using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Configuration;
using Orleans.Hosting;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Views;
using Orleans.Runtime;
using Orleans.TestingHost;

namespace Orleans.Lattice.Tests.Views;

/// <summary>
/// A view maintainer heals a source physical-identity swap by rebuilding the view
/// from the new physical tree inside its drain turn, and that rebuild can run for
/// minutes on a real estate. While it runs the maintainer must still answer a
/// status read and acknowledge its keepalive reminder: before #5519 both queued
/// behind the rebuild, so the tree-administration view status timed out and the
/// reminder tick was never delivered.
/// <para>
/// Each test cuts its view's source over to a physical copy, starts the healing
/// drain, and holds the rebuild on its first call to the new generation tree, so
/// the maintainer is deterministically mid-rebuild when the read or the tick
/// arrives. The view's drain timer is parked, so the only drain is the one the
/// test starts.
/// </para>
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class ViewMaintainerRebuildResponsivenessIntegrationTests
{
    private const string StatusViewName = "rebuild-responsive-status";
    private const string StatusSourceTreeId = "rebuild-responsive-status-src";
    private const string TickViewName = "rebuild-responsive-tick";
    private const string TickSourceTreeId = "rebuild-responsive-tick-src";
    private const string KeepaliveReminderName = "view-maintainer-keepalive";

    /// <summary>
    /// The silo response timeout is set well above <see cref="AnswerBound"/>, so a
    /// call queued behind the held rebuild is observed as unanswered rather than
    /// released by a timeout fault inside the bound.
    /// </summary>
    private static readonly TimeSpan ResponseTimeout = TimeSpan.FromSeconds(60);

    private static readonly TimeSpan AnswerBound = TimeSpan.FromSeconds(10);
    private static readonly TimeSpan Barrier = TimeSpan.FromSeconds(30);

    private TestCluster _cluster = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        var builder = new TestClusterBuilder(1);
        builder.AddSiloBuilderConfigurator<SiloConfigurator>();
        builder.AddClientBuilderConfigurator<ClientConfigurator>();
        _cluster = builder.Build();
        await _cluster.DeployAsync();
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown()
    {
        await _cluster.StopAllSilosAsync();
        await _cluster.DisposeAsync();
    }

    private IServiceProvider SiloServices =>
        _cluster.Silos.OfType<InProcessSiloHandle>().First().SiloHost.Services;

    private static byte[] Value(string text) => Encoding.UTF8.GetBytes(text);

    [Test]
    public async Task Status_read_is_answered_while_a_source_swap_rebuild_holds_the_maintainer()
    {
        var gate = SiloServices.GetRequiredService<RebuildGate>().Arm(StatusViewName);
        var (view, drain) = await CutOverAndHoldTheHealAsync(StatusViewName, StatusSourceTreeId, gate);
        var maintainer = _cluster.Client.GetGrain<IViewMaintainerGrain>(StatusViewName);

        Task<ViewMaintainerStatus> status;
        bool answered;
        try
        {
            status = maintainer.GetStatusAsync();
            answered = await Task.WhenAny(status, Task.Delay(AnswerBound)) == status;
        }
        finally
        {
            gate.Release();
        }

        Assert.That(answered, Is.True,
            "a status read must be answered while the maintainer rebuilds, not queue behind the rebuild");
        Assert.That((await status).ActiveTreeId, Is.EqualTo(LatticeViewTrees.ComposeTreeId(StatusViewName)),
            "the status reports the generation still serving reads, because the rebuild has not swapped yet");
        Assert.That((await status).Lag, Is.GreaterThan(0L),
            "a view still bound to the retired copy has not caught up with the current one");
        await AssertTheHealCompletesAsync(view, drain);
    }

    [Test]
    public async Task Keepalive_tick_is_acknowledged_while_a_source_swap_rebuild_holds_the_maintainer()
    {
        var gate = SiloServices.GetRequiredService<RebuildGate>().Arm(TickViewName);
        var (view, drain) = await CutOverAndHoldTheHealAsync(TickViewName, TickSourceTreeId, gate);
        var reminderTarget = SiloServices.GetRequiredService<IGrainFactory>()
            .GetGrain<IViewMaintainerGrain>(TickViewName)
            .AsReference<IRemindable>();

        bool acknowledged;
        try
        {
            var tick = reminderTarget.ReceiveReminder(KeepaliveReminderName, new TickStatus());
            acknowledged = await Task.WhenAny(tick, Task.Delay(AnswerBound)) == tick;
        }
        finally
        {
            gate.Release();
        }

        Assert.That(acknowledged, Is.True,
            "a keepalive tick must be acknowledged while the maintainer rebuilds, not time out behind the rebuild");
        await AssertTheHealCompletesAsync(view, drain);
    }

    /// <summary>
    /// Binds the view to its unaliased source, moves the source's alias to a
    /// physical copy, and starts the drain that heals the swap; returns once that
    /// drain's rebuild is held on its first call to the new generation tree.
    /// </summary>
    private async Task<(ILatticeView View, Task<int> Drain)> CutOverAndHoldTheHealAsync(
        string viewName, string sourceTreeId, RebuildGate.Gate gate)
    {
        await _cluster.Client.GetGrain<ILattice>(sourceTreeId).SetAsync("k", Value("old"));
        var view = await SiloServices.GetRequiredService<ILatticeViewFactory>().GetAsync(viewName);
        Assert.That(view, Is.Not.Null);
        await view!.WaitForSourceHeadAsync(Barrier);
        Assert.That(await view.GetAsync("k"), Is.EqualTo(Value("old")),
            "precondition: the view is bound to its unaliased source");

        var copy = $"{sourceTreeId}-copy";
        await _cluster.Client.GetGrain<ILattice>(copy).SetAsync("k", Value("new"));
        await _cluster.Client.GetLatticeRegistry().SetAliasAsync(sourceTreeId, copy);

        var drain = _cluster.Client.GetGrain<IViewMaintainerGrain>(viewName).DrainAsync();
        var held = await Task.WhenAny(gate.Entered, Task.Delay(Barrier)) == gate.Entered;
        if (!held)
        {
            gate.Release();
        }

        Assert.That(held, Is.True,
            "precondition: the drain is rebuilding the view from the new physical source and is held mid-rebuild");
        return (view, drain);
    }

    private static async Task AssertTheHealCompletesAsync(ILatticeView view, Task<int> drain)
    {
        await drain.WaitAsync(Barrier);
        await view.WaitForSourceHeadAsync(Barrier);
        Assert.That(await view.GetAsync("k"), Is.EqualTo(Value("new")),
            "once released, the rebuild completes and the view serves the new physical source");
    }

    /// <summary>
    /// Silo-scoped gates, one per armed view, that hold every call to that view's
    /// first rebuild generation tree until the test releases it.
    /// </summary>
    internal sealed class RebuildGate
    {
        private readonly ConcurrentDictionary<string, Gate> _gates = new(StringComparer.Ordinal);

        public Gate Arm(string viewName) => _gates.GetOrAdd(viewName, static name => new Gate(name));

        /// <summary>Returns the gate holding <paramref name="treeId"/>, when it is an armed view's generation tree.</summary>
        public Gate? For(string treeId)
        {
            foreach (var gate in _gates.Values)
            {
                if (gate.Holds(treeId))
                {
                    return gate;
                }
            }

            return null;
        }

        internal sealed class Gate(string viewName)
        {
            // The first rebuild of a view that has never rebuilt targets generation 1,
            // and its first call to that generation is to the generation's own tree
            // grain, so holding exactly that grain holds the rebuild.
            private readonly string _rebuildTreeId = LatticeViewTrees.ComposeTreeId(viewName, 1);
            private readonly TaskCompletionSource _entered = new(TaskCreationOptions.RunContinuationsAsynchronously);
            private readonly TaskCompletionSource _released = new(TaskCreationOptions.RunContinuationsAsynchronously);

            public Task Entered => _entered.Task;

            public Task Released => _released.Task;

            public bool Holds(string treeId) => string.Equals(treeId, _rebuildTreeId, StringComparison.Ordinal);

            public void SignalEntered() => _entered.TrySetResult();

            public void Release() => _released.TrySetResult();
        }
    }

    /// <summary>Holds calls to an armed view's rebuild generation trees until released.</summary>
    private sealed class RebuildGateFilter(RebuildGate gates) : IIncomingGrainCallFilter
    {
        public async Task Invoke(IIncomingGrainCallContext context)
        {
            var gate = gates.For(context.TargetId.Key.ToString() ?? string.Empty);
            if (gate is not null)
            {
                gate.SignalEntered();
                await gate.Released;
            }

            await context.Invoke();
        }
    }

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.UseInMemoryReminderService();
            siloBuilder.Configure<SiloMessagingOptions>(o => o.ResponseTimeout = ResponseTimeout);
            siloBuilder.Services.AddSingleton<RebuildGate>();
            siloBuilder.AddIncomingGrainCallFilter<RebuildGateFilter>();
            siloBuilder.AddLatticeViews(views =>
            {
                views.AddView(StatusViewName, StatusSourceTreeId, new PredicateLatticeViewProjection());
                views.AddView(TickViewName, TickSourceTreeId, new PredicateLatticeViewProjection());
            });
            foreach (var viewName in new[] { StatusViewName, TickViewName })
            {
                siloBuilder.ConfigureLatticeView(viewName, o =>
                {
                    o.CoalesceWindow = TimeSpan.FromHours(1);
                    o.SourceIdentityBackstopInterval = TimeSpan.FromTicks(1);
                    o.OldGenerationReclaimGrace = TimeSpan.FromHours(2);
                });
            }
        }
    }

    private sealed class ClientConfigurator : IClientBuilderConfigurator
    {
        public void Configure(Microsoft.Extensions.Configuration.IConfiguration configuration, IClientBuilder clientBuilder) =>
            clientBuilder.Configure<ClientMessagingOptions>(o => o.ResponseTimeout = ResponseTimeout);
    }
}
