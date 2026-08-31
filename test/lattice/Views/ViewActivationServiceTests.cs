using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice.Views;

namespace Orleans.Lattice.Tests.Views;

/// <summary>
/// Unit tests for <see cref="ViewActivationService"/>: the hosted service that
/// registers every startup-declared view, re-hydrates every durably-registered
/// runtime view into the <see cref="IViewCatalog"/>, and then activates each view's
/// <see cref="IViewMaintainerGrain"/> on silo startup.
/// <para>
/// The behaviour under test is the retry loop. Hosted-service start can race ahead of
/// the silo becoming dispatch-ready, so both re-hydration and per-view activation are
/// retried with exponential backoff, individually, and a failure of one must never
/// strand the others. These tests pin the startup-wins-over-runtime precedence, the
/// per-view failure isolation, the "hydrate exactly once" contract, and the
/// cancellation seams that let a shutting-down silo exit the loop promptly.
/// </para>
/// </summary>
[TestFixture]
[Category("Unit")]
public sealed class ViewActivationServiceTests
{
    private sealed class StubProjection : ILatticeViewProjection
    {
        public string ProjectionVersion => "v1";

        public IEnumerable<ViewWrite> Project(LatticeMutation mutation) => [];
    }

    private sealed class RecordingCatalog : IViewCatalog
    {
        private readonly Dictionary<string, ViewRegistration> _registered = new(StringComparer.Ordinal);

        public List<string> RegisteredNames { get; } = [];

        public void Register(ViewRegistration registration)
        {
            RegisteredNames.Add(registration.ViewName);
            _registered[registration.ViewName] = registration;
        }

        public ViewRegistration? TryGet(string viewName) =>
            _registered.TryGetValue(viewName, out var r) ? r : null;

        public void Remove(string viewName) => _registered.Remove(viewName);

        public IReadOnlyCollection<ViewRegistration> All() => _registered.Values;
    }

    private static IServiceProvider Services() => new ServiceCollection().BuildServiceProvider();

    private static StartupViewRegistration Startup(string viewName, string sourceTreeId = "orders") =>
        new(viewName, sourceTreeId, _ => new StubProjection());

    private static RuntimeViewRegistration Runtime(string viewName, string sourceTreeId = "orders") => new()
    {
        ViewName = viewName,
        SourceTreeId = sourceTreeId,
        ProjectionTypeName = typeof(StubProjection).FullName!,
        ProjectionVersion = "v1",
    };

    private sealed class Harness : IAsyncDisposable
    {
        public required ViewActivationService Service { get; init; }

        public required RecordingCatalog Catalog { get; init; }

        public required IViewRegistryGrain Registry { get; init; }

        public required Dictionary<string, IViewMaintainerGrain> Maintainers { get; init; }

        public required IGrainFactory Factory { get; init; }

        /// <summary>
        /// Runs the loop to its natural completion (every maintainer activated and
        /// runtime views hydrated), failing rather than hanging if it never settles.
        /// </summary>
        public async Task RunToCompletionAsync()
        {
            await Service.StartAsync(CancellationToken.None);
            var execute = Service.ExecuteTask!;
            var finished = await Task.WhenAny(execute, Task.Delay(TimeSpan.FromSeconds(30)));
            Assert.That(finished, Is.SameAs(execute), "ViewActivationService did not settle within 30s.");
            await execute;
        }

        public async ValueTask DisposeAsync()
        {
            try
            {
                await Service.StopAsync(CancellationToken.None);
            }
            catch (OperationCanceledException)
            {
                // Expected when the loop is torn down mid-retry.
            }

            // Observe a cancelled/faulted execute task so it never surfaces as an
            // unobserved exception on the finalizer thread.
            if (Service.ExecuteTask is { } execute)
            {
                try
                {
                    await execute;
                }
                catch (OperationCanceledException)
                {
                }
            }

            Service.Dispose();
        }
    }

    private static Harness CreateHarness(
        IEnumerable<StartupViewRegistration>? startup = null,
        IEnumerable<RuntimeViewRegistration>? runtime = null,
        Action<IViewRegistryGrain>? configureRegistry = null,
        Action<Dictionary<string, IViewMaintainerGrain>>? configureMaintainers = null)
    {
        var registry = Substitute.For<IViewRegistryGrain>();
        registry.ListAsync().Returns(
            Task.FromResult<IReadOnlyList<RuntimeViewRegistration>>([.. runtime ?? []]));
        configureRegistry?.Invoke(registry);

        var maintainers = new Dictionary<string, IViewMaintainerGrain>(StringComparer.Ordinal);
        configureMaintainers?.Invoke(maintainers);

        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<IViewRegistryGrain>(IViewRegistryGrain.SingletonKey, Arg.Any<string?>())
            .Returns(registry);
        factory.GetGrain<IViewMaintainerGrain>(Arg.Any<string>(), Arg.Any<string?>())
            .Returns(call =>
            {
                var key = call.ArgAt<string>(0);
                if (!maintainers.TryGetValue(key, out var grain))
                {
                    grain = Substitute.For<IViewMaintainerGrain>();
                    grain.EnsureActiveAsync(Arg.Any<CancellationToken>()).Returns(Task.CompletedTask);
                    maintainers[key] = grain;
                }

                return grain;
            });

        var catalog = new RecordingCatalog();
        var service = new ViewActivationService(
            Services(),
            [.. startup ?? []],
            catalog,
            new RuntimeViewProjectionProviderCatalog([]),
            factory,
            Substitute.For<ILogger<ViewActivationService>>());

        return new Harness
        {
            Service = service,
            Catalog = catalog,
            Registry = registry,
            Maintainers = maintainers,
            Factory = factory,
        };
    }

    [Test]
    public async Task ExecuteAsync_completes_immediately_when_there_is_nothing_to_activate()
    {
        await using var harness = CreateHarness();

        await harness.RunToCompletionAsync();

        Assert.That(harness.Catalog.RegisteredNames, Is.Empty);
        await harness.Registry.Received(1).ListAsync();
    }

    [Test]
    public async Task ExecuteAsync_registers_and_activates_every_startup_view()
    {
        await using var harness = CreateHarness(startup: [Startup("by-owner"), Startup("by-status")]);

        await harness.RunToCompletionAsync();

        Assert.That(harness.Catalog.RegisteredNames, Is.EquivalentTo(new[] { "by-owner", "by-status" }));
        await harness.Maintainers["by-owner"].Received(1).EnsureActiveAsync(Arg.Any<CancellationToken>());
        await harness.Maintainers["by-status"].Received(1).EnsureActiveAsync(Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task ExecuteAsync_rehydrates_and_activates_a_durable_runtime_view()
    {
        await using var harness = CreateHarness(runtime: [Runtime("runtime-view")]);

        await harness.RunToCompletionAsync();

        Assert.That(harness.Catalog.RegisteredNames, Is.EqualTo(new[] { "runtime-view" }));
        await harness.Maintainers["runtime-view"].Received(1).EnsureActiveAsync(Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task ExecuteAsync_lets_a_startup_declaration_win_over_a_runtime_record_of_the_same_name()
    {
        await using var harness = CreateHarness(
            startup: [Startup("shared", sourceTreeId: "startup-tree")],
            runtime: [Runtime("shared", sourceTreeId: "runtime-tree")]);

        await harness.RunToCompletionAsync();

        // The durable runtime record is skipped entirely: registered once, from the
        // authoritative startup declaration.
        Assert.That(harness.Catalog.RegisteredNames, Is.EqualTo(new[] { "shared" }));
        Assert.That(harness.Catalog.TryGet("shared")!.SourceTreeId, Is.EqualTo("startup-tree"));
    }

    [Test]
    public async Task ExecuteAsync_skips_a_runtime_record_whose_projection_cannot_be_resolved()
    {
        var unresolvable = Runtime("broken") with { ProjectionTypeName = "No.Such.Projection.Type" };
        await using var harness = CreateHarness(runtime: [unresolvable, Runtime("healthy")]);

        await harness.RunToCompletionAsync();

        // A record that cannot be rebuilt stays dormant; its sibling still activates.
        Assert.That(harness.Catalog.RegisteredNames, Is.EqualTo(new[] { "healthy" }));
        Assert.That(harness.Maintainers.Keys, Is.EqualTo(new[] { "healthy" }));
    }

    [Test]
    public async Task ExecuteAsync_registers_a_duplicated_runtime_record_name_only_once()
    {
        await using var harness = CreateHarness(runtime: [Runtime("dupe"), Runtime("dupe")]);

        await harness.RunToCompletionAsync();

        await harness.Maintainers["dupe"].Received(1).EnsureActiveAsync(Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task ExecuteAsync_retries_rehydration_until_the_registry_read_succeeds()
    {
        var calls = 0;
        await using var harness = CreateHarness(configureRegistry: registry =>
            registry.ListAsync().Returns(_ =>
                ++calls == 1
                    ? throw new TimeoutException("silo not dispatch-ready")
                    : Task.FromResult<IReadOnlyList<RuntimeViewRegistration>>([Runtime("late")])));

        await harness.RunToCompletionAsync();

        Assert.That(calls, Is.EqualTo(2));
        Assert.That(harness.Catalog.RegisteredNames, Is.EqualTo(new[] { "late" }));
    }

    [Test]
    public async Task ExecuteAsync_rehydrates_exactly_once_even_when_activation_retries()
    {
        var attempts = 0;
        var maintainer = Substitute.For<IViewMaintainerGrain>();
        maintainer.EnsureActiveAsync(Arg.Any<CancellationToken>()).Returns(_ =>
            ++attempts == 1
                ? throw new TimeoutException("maintainer not ready")
                : Task.CompletedTask);

        await using var harness = CreateHarness(
            startup: [Startup("flaky")],
            configureMaintainers: m => m["flaky"] = maintainer);

        await harness.RunToCompletionAsync();

        Assert.That(attempts, Is.EqualTo(2), "activation should be retried after a transient failure");
        // The hydration pass is latched on first success and must not repeat.
        await harness.Registry.Received(1).ListAsync();
    }

    [Test]
    public async Task ExecuteAsync_isolates_a_failing_view_from_its_healthy_siblings()
    {
        var flakyAttempts = 0;
        var flaky = Substitute.For<IViewMaintainerGrain>();
        flaky.EnsureActiveAsync(Arg.Any<CancellationToken>()).Returns(_ =>
            ++flakyAttempts <= 2
                ? throw new InvalidOperationException("still starting")
                : Task.CompletedTask);

        var healthy = Substitute.For<IViewMaintainerGrain>();
        healthy.EnsureActiveAsync(Arg.Any<CancellationToken>()).Returns(Task.CompletedTask);

        await using var harness = CreateHarness(
            startup: [Startup("flaky"), Startup("healthy")],
            configureMaintainers: m =>
            {
                m["flaky"] = flaky;
                m["healthy"] = healthy;
            });

        await harness.RunToCompletionAsync();

        // The healthy view activates once and is dropped from the pending set, so the
        // retries driven by its failing sibling never re-activate it.
        await healthy.Received(1).EnsureActiveAsync(Arg.Any<CancellationToken>());
        Assert.That(flakyAttempts, Is.EqualTo(3));
    }

    [Test]
    public async Task ExecuteAsync_stops_promptly_while_a_view_is_still_failing_to_activate()
    {
        var maintainer = Substitute.For<IViewMaintainerGrain>();
        maintainer.EnsureActiveAsync(Arg.Any<CancellationToken>())
            .Throws(new InvalidOperationException("never ready"));

        var harness = CreateHarness(
            startup: [Startup("stuck")],
            configureMaintainers: m => m["stuck"] = maintainer);

        await harness.Service.StartAsync(CancellationToken.None);
        var execute = harness.Service.ExecuteTask!;

        // Let at least one failing pass run so the loop is parked in its backoff delay.
        await Task.Delay(150);
        Assert.That(execute.IsCompleted, Is.False);

        await harness.DisposeAsync();

        Assert.That(execute.IsCompleted, Is.True);
        await maintainer.ReceivedWithAnyArgs().EnsureActiveAsync(default);
    }

    [Test]
    public async Task ExecuteAsync_observes_cancellation_requested_before_the_first_pass()
    {
        await using var harness = CreateHarness(startup: [Startup("never-run")]);
        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();

        // ExecuteAsync is driven directly here so the already-cancelled token is the
        // stopping token on the very first loop iteration.
        Assert.ThrowsAsync<OperationCanceledException>(async () =>
            await InvokeExecuteAsync(harness.Service, cts.Token));

        Assert.That(harness.Maintainers, Is.Empty);
    }

    [Test]
    public async Task ExecuteAsync_registers_startup_views_in_the_catalog_before_activating_them()
    {
        RecordingCatalog? observed = null;
        var maintainer = Substitute.For<IViewMaintainerGrain>();

        var harness = CreateHarness(
            startup: [Startup("ordered")],
            configureMaintainers: m => m["ordered"] = maintainer);
        observed = harness.Catalog;
        maintainer.EnsureActiveAsync(Arg.Any<CancellationToken>()).Returns(_ =>
        {
            // The catalog binding must already exist: a maintainer that activates
            // resolves its source tree and projection from the catalog.
            Assert.That(observed!.TryGet("ordered"), Is.Not.Null);
            return Task.CompletedTask;
        });

        await using (harness)
        {
            await harness.RunToCompletionAsync();
        }

        await maintainer.Received(1).EnsureActiveAsync(Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task ExecuteAsync_rethrows_cancellation_raised_during_rehydration()
    {
        using var cts = new CancellationTokenSource();
        await using var harness = CreateHarness(configureRegistry: registry =>
            registry.ListAsync().Returns<Task<IReadOnlyList<RuntimeViewRegistration>>>(_ =>
            {
                // Model the silo shutting down mid-read: the registry call observes the
                // stopping token and cancels rather than failing transiently.
                cts.Cancel();
                throw new OperationCanceledException(cts.Token);
            }));

        // Shutdown cancellation must escape the "will retry" catch, otherwise the loop
        // would spin re-reading a registry on a stopping silo.
        Assert.ThrowsAsync<OperationCanceledException>(async () =>
            await InvokeExecuteAsync(harness.Service, cts.Token));
    }

    [Test]
    public async Task ExecuteAsync_rethrows_cancellation_raised_during_activation()
    {
        using var cts = new CancellationTokenSource();
        var maintainer = Substitute.For<IViewMaintainerGrain>();
        maintainer.EnsureActiveAsync(Arg.Any<CancellationToken>()).Returns(_ =>
        {
            cts.Cancel();
            throw new OperationCanceledException(cts.Token);
        });

        await using var harness = CreateHarness(
            startup: [Startup("stopping")],
            configureMaintainers: m => m["stopping"] = maintainer);

        Assert.ThrowsAsync<OperationCanceledException>(async () =>
            await InvokeExecuteAsync(harness.Service, cts.Token));
    }

    [Test]
    public async Task ExecuteAsync_treats_a_cancellation_unrelated_to_shutdown_as_a_retryable_failure()
    {
        // The exception filter keys on the *stopping* token, so a cancellation from an
        // unrelated source is a transient fault and must be retried, not rethrown.
        var attempts = 0;
        var maintainer = Substitute.For<IViewMaintainerGrain>();
        maintainer.EnsureActiveAsync(Arg.Any<CancellationToken>()).Returns(_ =>
            ++attempts == 1
                ? throw new OperationCanceledException(new CancellationToken(canceled: true))
                : Task.CompletedTask);

        await using var harness = CreateHarness(
            startup: [Startup("unrelated")],
            configureMaintainers: m => m["unrelated"] = maintainer);

        await harness.RunToCompletionAsync();

        Assert.That(attempts, Is.EqualTo(2));
    }

    /// <summary>
    /// Invokes the protected <see cref="Microsoft.Extensions.Hosting.BackgroundService.ExecuteAsync"/>
    /// directly so a test can supply an already-cancelled stopping token, which the
    /// hosted-service start path cannot express.
    /// </summary>
    private static Task InvokeExecuteAsync(ViewActivationService service, CancellationToken token)
    {
        var method = typeof(ViewActivationService).GetMethod(
            "ExecuteAsync",
            System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic)!;
        return (Task)method.Invoke(service, [token])!;
    }
}
