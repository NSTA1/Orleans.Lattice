using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Orleans.Hosting;
using Orleans.Lattice.Auth;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Membership;
using Orleans.Lattice.Testing;
using Orleans.TestingHost;

namespace Orleans.Lattice.Apps.Tests;

/// <summary>
/// End-to-end activation on a real single-silo cluster wired with the core lattice, membership,
/// authorization, and the apps add-on. Three in-image apps are already enabled when the silo
/// starts - one valid, one whose manifest is not even JSON, one whose role exceeds its consented
/// ceiling - and the silo must start regardless, with each app's outcome recorded by the
/// background startup reconcile.
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class AppActivationClusterTests
{
    private const string Resource = "app.manifest.json";

    private static readonly AppSlug Good = AppSlug.Parse("good-app");
    private static readonly AppSlug Invalid = AppSlug.Parse("invalid-app");
    private static readonly AppSlug Greedy = AppSlug.Parse("greedy-app");

    private static readonly SeededAppRegistryStore RegistryStore = new();

    private TestCluster _cluster = null!;

    private IServiceProvider Silo => _cluster.Silos.OfType<InProcessSiloHandle>().First().SiloHost.Services;

    private static string Manifest(AppSlug slug, string operations) => $$"""
        {
          "identity": { "slug": "{{slug}}", "version": "1.0.0" },
          "trees": [{ "name": "records", "softDeleteDuration": "1.00:00:00" }],
          "roles": [{ "name": "reader", "operations": {{operations}}, "scopes": [{ "tree": "records" }] }],
          "subscriptions": [],
          "mcpTools": []
        }
        """;

    private static AppRegistryRecord Enabled(AppSlug slug) => AppRegistryTestData.Record(AppRegistryLifecycleState.Enabled, slug: slug) with
    {
        RoleBindings = new[] { AppRoleBinding.Create("reader", "readers") },
    };

    [OneTimeSetUp]
    public async Task SetUpAsync()
    {
        RegistryStore.Seed(Enabled(Good));
        RegistryStore.Seed(Enabled(Invalid));
        RegistryStore.Seed(Enabled(Greedy));

        var builder = new TestClusterBuilder(1);
        builder.AddSiloBuilderConfigurator<SiloConfigurator>();
        _cluster = builder.Build();

        // The requirement under test: deploying the silo succeeds despite the broken apps.
        await _cluster.DeployAsync();
    }

    [OneTimeTearDown]
    public async Task TearDownAsync()
    {
        if (_cluster is not null)
        {
            await _cluster.StopAllSilosAsync();
            await _cluster.DisposeAsync();
        }
    }

    [Test, Order(1)]
    public async Task Silo_startup_is_unaffected_by_invalid_and_over_ceiling_manifests()
    {
        var pipeline = Silo.GetRequiredService<IAppActivationPipeline>();
        await TestPoll.UntilAsync(
            async () => await pipeline.GetStatusAsync(TenantId.Default, Good) is not null
                && await pipeline.GetStatusAsync(TenantId.Default, Invalid) is not null
                && await pipeline.GetStatusAsync(TenantId.Default, Greedy) is not null,
            "the startup reconcile records an outcome for every enabled app",
            timeout: TimeSpan.FromSeconds(60));

        var good = await pipeline.GetStatusAsync(TenantId.Default, Good);
        var invalid = await pipeline.GetStatusAsync(TenantId.Default, Invalid);
        var greedy = await pipeline.GetStatusAsync(TenantId.Default, Greedy);
        Assert.That(good!.LastOutcome.Succeeded, Is.True, () => string.Join("; ", good.LastOutcome.Diagnostics.Select(d => d.Message)));
        Assert.That(invalid!.LastOutcome.Failure, Is.EqualTo(AppActivationFailure.InvalidManifest));
        Assert.That(greedy!.LastOutcome.Failure, Is.EqualTo(AppActivationFailure.CeilingExceeded));

        // The silo keeps serving.
        Assert.That(_cluster.Silos, Has.Count.EqualTo(1));
        await _cluster.Client.GetGrain<ILattice>("plain-tree").TreeExistsAsync();
    }

    [Test, Order(2)]
    public async Task A_valid_app_is_activated_end_to_end()
    {
        var store = Silo.GetRequiredService<ILatticeAuthorizationPolicyStore>();
        var grains = Silo.GetRequiredService<IGrainFactory>();
        var tree = AppActivationTreeNames.LocalStructuralTree(Good, "records");

        var rules = new List<LatticeAuthorizationRule>();
        await foreach (var rule in store.ListRulesForTreeAsync(tree))
        {
            rules.Add(rule);
        }

        Assert.That(rules.Single().RuleId, Does.StartWith("app:good-app:reader:"));
        Assert.That(await grains.GetLatticeRegistry().ExistsAsync(tree), Is.True);
        Assert.That(Silo.GetRequiredService<Microsoft.Extensions.Options.IOptionsMonitor<LatticeOptions>>().Get(tree).SoftDeleteDuration,
            Is.EqualTo(TimeSpan.FromDays(1)));
    }

    [Test, Order(3)]
    public async Task Disable_and_uninstall_withdraw_rules_and_soft_delete_the_tree()
    {
        var pipeline = Silo.GetRequiredService<IAppActivationPipeline>();
        var store = Silo.GetRequiredService<ILatticeAuthorizationPolicyStore>();
        var grains = Silo.GetRequiredService<IGrainFactory>();
        var tree = AppActivationTreeNames.LocalStructuralTree(Good, "records");

        AppActivationOutcome disabled;
        AppActivationOutcome uninstalled;
        using (LatticeSystemOrigin.Enter())
        {
            disabled = await pipeline.DisableAsync(TenantId.Default, Good);
            uninstalled = await pipeline.UninstallAsync(TenantId.Default, Good);
        }

        Assert.That(disabled.Succeeded, Is.True, () => string.Join("; ", disabled.Diagnostics.Select(d => d.Message)));
        Assert.That(uninstalled.Succeeded, Is.True, () => string.Join("; ", uninstalled.Diagnostics.Select(d => d.Message)));
        Assert.That(uninstalled.State, Is.EqualTo(AppRegistryLifecycleState.Uninstalled));
        await foreach (var rule in store.ListRulesForTreeAsync(tree))
        {
            Assert.Fail($"Rule '{rule.RuleId}' survived uninstall.");
        }

        Assert.That(await grains.GetGrain<ITreeDeletionGrain>(tree).IsDeletedAsync(), Is.True);
    }

    [Test, Order(4)]
    public async Task An_external_client_calling_the_activation_grain_directly_is_denied()
    {
        var pipeline = Silo.GetRequiredService<IAppActivationPipeline>();
        var before = await pipeline.GetStatusAsync(TenantId.Default, Invalid);
        var grain = _cluster.Client.GetGrain<IAppActivationGrain>(AppRegistryTreeNames.ComposeKey(TenantId.Default, Invalid));

        // Even a forged system-origin marker is stripped at the trust boundary, so the anonymous
        // client is authorized as itself and refused before anything runs.
        using (LatticeSystemOrigin.Enter())
        {
            Assert.ThrowsAsync<LatticeAuthorizationDeniedException>(
                () => grain.ExecuteAsync(AppActivationOperation.Uninstall, TenantId.Default, Invalid));
        }

        var after = await pipeline.GetStatusAsync(TenantId.Default, Invalid);
        Assert.That(after!.LastOutcome.Operation, Is.EqualTo(before!.LastOutcome.Operation));
        Assert.That(after.LastOutcome.CompletedAtUtc, Is.EqualTo(before.LastOutcome.CompletedAtUtc));
        Assert.That(after.LastOutcome.State, Is.EqualTo(AppRegistryLifecycleState.Enabled));
    }

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.UseInMemoryReminderService();
            siloBuilder.AddLatticeMembership();
            siloBuilder.AddLatticeAuth();
            siloBuilder.AddLatticeApps()
                .AddLatticeApp(Good.Value, new FakeAppAssembly(Resource, Manifest(Good, "[\"Read\"]")), Resource)
                .AddLatticeApp(Invalid.Value, new FakeAppAssembly(Resource, "{ this is not json"), Resource)
                .AddLatticeApp(Greedy.Value, new FakeAppAssembly(Resource, Manifest(Greedy, "[\"Read\", \"Delete\"]")), Resource);
            siloBuilder.Services.Replace(ServiceDescriptor.Singleton<IAppRegistryStore>(RegistryStore));
        }
    }
}
