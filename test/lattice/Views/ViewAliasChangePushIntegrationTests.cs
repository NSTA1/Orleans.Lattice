using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Configuration;
using Orleans.Hosting;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Views;
using Orleans.TestingHost;

namespace Orleans.Lattice.Tests.Views;

/// <summary>
/// The tree registry pushes an alias change to a source tree's view maintainers
/// from inside its own non-reentrant <see cref="ILatticeRegistry.SetAliasAsync"/>
/// turn, after the new alias row is already committed. A maintainer whose drain
/// observes that committed row first rebuilds from the new physical tree in the
/// same turn, and the rebuild's fresh generation tree registers itself through the
/// non-interleaved <see cref="ILatticeRegistry.RegisterAsync"/>, which queues behind
/// the very <c>SetAliasAsync</c> turn awaiting the push. The push must therefore
/// never wait behind a maintainer turn, or the registry singleton deadlocks until a
/// response timeout breaks the cycle (#5279).
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class ViewAliasChangePushIntegrationTests
{
    private const string ViewName = "alias-push-count";
    private const string SourceTreeId = "alias-push-src";
    private const string PhysicalTreeId = "alias-push-physical";

    /// <summary>
    /// The silo response timeout - the only thing that breaks the cycle - is set
    /// well above <see cref="AliasCompletionBound"/>, so a deadlocked alias change
    /// is observed as one, not as a slow call the timeout eventually releases.
    /// </summary>
    private static readonly TimeSpan ResponseTimeout = TimeSpan.FromSeconds(60);

    private static readonly TimeSpan AliasCompletionBound = TimeSpan.FromSeconds(20);
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

    private static byte[] Value(string group) => Encoding.UTF8.GetBytes(group);

    [Test]
    public async Task SetAliasAsync_completes_while_a_source_view_maintainer_rebuild_awaits_the_registry()
    {
        var probe = SiloServices.GetRequiredService<AliasPushProbe>();
        await _cluster.Client.GetGrain<ILattice>(PhysicalTreeId).SetAsync("p", Value("physical"));
        await _cluster.Client.GetGrain<ILattice>(SourceTreeId).SetAsync("s", Value("logical"));
        var view = await SiloServices.GetRequiredService<ILatticeViewFactory>().GetAsync(ViewName);
        Assert.That(view, Is.Not.Null);
        await view!.WaitForSourceHeadAsync(Barrier);
        Assert.That(await view.GetAggregateInt64Async("logical"), Is.EqualTo(1L),
            "precondition: the maintainer is bound to the unaliased source");

        probe.Arm(SourceTreeId);
        var alias = _cluster.Client.GetLatticeRegistry().SetAliasAsync(SourceTreeId, PhysicalTreeId);
        var completed = await Task.WhenAny(alias, Task.Delay(AliasCompletionBound)) == alias;

        Assert.That(probe.RebuildRegistrationDispatched, Is.True,
            "precondition: when the alias change was pushed, the maintainer was rebuilding from the new "
            + "physical tree and had dispatched its generation tree's registry registration");
        Assert.That(completed, Is.True,
            "SetAliasAsync must not wait behind a view maintainer turn that itself awaits the registry");
        await alias;

        await probe.Drain!;
        await view.WaitForSourceHeadAsync(Barrier);
        Assert.Multiple(async () =>
        {
            Assert.That(await view.GetAggregateInt64Async("physical"), Is.EqualTo(1L), "the view rebuilt from the physical tree");
            Assert.That(await view.GetAggregateInt64Async("logical"), Is.Null, "the retired source no longer contributes");
        });
    }

    /// <summary>Shared, silo-scoped state between the test, the observer and the filter.</summary>
    internal sealed class AliasPushProbe
    {
        private volatile string? _armedSource;

        public TaskCompletionSource RegistrationDispatched { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public Task? Drain { get; set; }

        public bool RebuildRegistrationDispatched { get; set; }

        public bool IsArmed => _armedSource is not null;

        public void Arm(string sourceTreeId) => _armedSource = sourceTreeId;

        public bool IsArmedFor(string treeId) => string.Equals(_armedSource, treeId, StringComparison.Ordinal);
    }

    /// <summary>
    /// Runs before the view observer inside the registry's alias turn: starts a
    /// maintainer drain, which re-resolves the source (the backstop interval is a
    /// tick, so every drain does), finds the committed alias and rebuilds; then
    /// returns only once that rebuild has dispatched its generation tree's registry
    /// registration - the state in which the maintainer turn needs the registry.
    /// </summary>
    private sealed class RebuildRacingAliasObserver(IGrainFactory grainFactory, AliasPushProbe probe) : ITreeAliasObserver
    {
        public async Task OnTreeAliasChangedAsync(TreeAliasChange change, CancellationToken cancellationToken)
        {
            if (!probe.IsArmedFor(change.TreeId))
            {
                return;
            }

            probe.Drain = grainFactory.GetGrain<IViewMaintainerGrain>(ViewName).DrainAsync(CancellationToken.None);
            try
            {
                await probe.RegistrationDispatched.Task.WaitAsync(Barrier, cancellationToken);
                probe.RebuildRegistrationDispatched = true;
            }
            catch (TimeoutException)
            {
            }
        }
    }

    /// <summary>
    /// Signals when the armed maintainer's rebuild sends a registry registration for
    /// one of the view's own generation trees. Observes only; never delays a call.
    /// </summary>
    private sealed class GenerationRegistrationSignal(AliasPushProbe probe) : IOutgoingGrainCallFilter
    {
        public Task Invoke(IOutgoingGrainCallContext context)
        {
            if (probe.IsArmed
                && context.InterfaceMethod?.DeclaringType == typeof(ILatticeRegistry)
                && context.MethodName is nameof(ILatticeRegistry.RegisterAsync) or nameof(ILatticeRegistry.RegisterAndGetEntryAsync)
                && context.Request.GetArgumentCount() > 0
                && context.Request.GetArgument(0) is string treeId
                && treeId.Contains(ViewName, StringComparison.Ordinal))
            {
                probe.RegistrationDispatched.TrySetResult();
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
            siloBuilder.Configure<SiloMessagingOptions>(o => o.ResponseTimeout = ResponseTimeout);
            siloBuilder.Services.AddSingleton<AliasPushProbe>();

            // Registered before AddLatticeViews so the dispatcher runs it before the
            // view observer that pushes the change to the maintainer.
            siloBuilder.Services.AddSingleton<ITreeAliasObserver, RebuildRacingAliasObserver>();
            siloBuilder.AddOutgoingGrainCallFilter<GenerationRegistrationSignal>();
            siloBuilder.AddLatticeViews(views => views.AddAggregationView(
                ViewName,
                SourceTreeId,
                new AggregationLatticeViewProjection(AggregationKind.Count, static v => Encoding.UTF8.GetString(v), "v1")));
            // Every drain re-resolves the source, and every read resolves the active
            // generation, so neither a binding nor a read handle outlives the swap.
            siloBuilder.ConfigureLatticeView(ViewName, o =>
            {
                o.SourceIdentityBackstopInterval = TimeSpan.FromTicks(1);
                o.ReadHandleCacheTtl = TimeSpan.FromTicks(1);
            });
        }
    }

    private sealed class ClientConfigurator : IClientBuilderConfigurator
    {
        public void Configure(Microsoft.Extensions.Configuration.IConfiguration configuration, IClientBuilder clientBuilder) =>
            clientBuilder.Configure<ClientMessagingOptions>(o => o.ResponseTimeout = ResponseTimeout);
    }
}
