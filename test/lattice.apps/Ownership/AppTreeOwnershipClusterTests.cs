using Microsoft.Extensions.DependencyInjection;
using Orleans.Hosting;
using Orleans.Lattice.Auth;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Membership;
using Orleans.TestingHost;

namespace Orleans.Lattice.Apps.Tests;

/// <summary>
/// Tree ownership on a real single-silo cluster: the <c>sys-app-trees</c> ledger records claims at
/// install, a second install cannot take a tree another install owns, the ledger refuses user-origin
/// writes, core aliasing across an ownership boundary is denied while the resize alias passes, and
/// (the #3744 regression) uninstalling an app whose tree was resized soft-deletes the live
/// copy while re-enabling it recovers that same copy.
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class AppTreeOwnershipClusterTests
{
    private const string Resource = "app.manifest.json";
    private const string Records = "a/crm/records";
    private const string Legacy = "legacy-shared";

    private static readonly AppSlug Crm = AppSlug.Parse("crm");
    private static readonly AppSlug Rival = AppSlug.Parse("rival");
    private static readonly AppVersion V1 = AppVersion.Parse("1.0.0");

    private const string CrmManifest = """
        {
          "identity": { "slug": "crm", "version": "1.0.0" },
          "trees": [{ "name": "records" }, { "name": "legacy", "adoptedTreeId": "legacy-shared" }],
          "roles": [{ "name": "reader", "operations": ["Read"], "scopes": [{ "tree": "records" }] }],
          "subscriptions": [],
          "mcpTools": []
        }
        """;

    private const string RivalManifest = """
        {
          "identity": { "slug": "rival", "version": "1.0.0" },
          "trees": [{ "name": "old", "adoptedTreeId": "legacy-shared" }],
          "roles": [],
          "subscriptions": [],
          "mcpTools": []
        }
        """;

    private TestCluster _cluster = null!;

    private IServiceProvider Silo => _cluster.Silos.OfType<InProcessSiloHandle>().First().SiloHost.Services;

    private IGrainFactory Grains => Silo.GetRequiredService<IGrainFactory>();

    [OneTimeSetUp]
    public async Task SetUpAsync()
    {
        var builder = new TestClusterBuilder(1);
        builder.AddSiloBuilderConfigurator<SiloConfigurator>();
        _cluster = builder.Build();
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

    private static AppRegistryInstallRequest Request(AppSlug slug) => new()
    {
        Identity = new AppIdentity { Slug = slug, Version = V1 },
        Ceiling = AppCapabilityCeiling.Structural(LatticeOperation.Read),
        RoleBindings = slug == Crm ? [AppRoleBinding.Create("reader", "readers")] : [],
    };

    [Test, Order(1)]
    public async Task Install_records_structural_and_adopted_claims_in_the_ledger()
    {
        var registry = Silo.GetRequiredService<IAppRegistry>();
        var ledger = Silo.GetRequiredService<IAppTreeLedgerStore>();
        AppRegistryTransitionResult installed;
        AppActivationOutcome enabled;
        using (LatticeSystemOrigin.Enter())
        {
            installed = await registry.InstallAsync(Request(Crm));
            enabled = await Silo.GetRequiredService<IAppActivationPipeline>().EnableAsync(TenantId.Default, Crm);
        }

        Assert.That(installed.Succeeded, Is.True, installed.Message);
        Assert.That(enabled.Succeeded, Is.True, () => string.Join("; ", enabled.Diagnostics.Select(d => d.Message)));
        var structural = (await ledger.GetAsync(Records, CancellationToken.None)).Claim!;
        var adopted = (await ledger.GetAsync(Legacy, CancellationToken.None)).Claim!;
        Assert.That((structural.Slug, structural.Kind), Is.EqualTo((Crm, AppTreeClaimKind.Structural)));
        Assert.That((adopted.Slug, adopted.Kind), Is.EqualTo((Crm, AppTreeClaimKind.Adopted)));
    }

    [Test, Order(2)]
    public async Task A_second_install_adopting_the_same_tree_is_refused()
    {
        var registry = Silo.GetRequiredService<IAppRegistry>();

        AppRegistryTransitionResult rival;
        using (LatticeSystemOrigin.Enter())
        {
            rival = await registry.InstallAsync(Request(Rival));
        }

        Assert.That(rival.Error, Is.EqualTo(AppRegistryTransitionError.TreeOwnershipConflict));
        Assert.That(rival.Message, Is.EqualTo("Tree 'old' is owned by app 'crm'."));
        Assert.That(await registry.GetAsync(TenantId.Default, Rival), Is.Null);
    }

    [Test, Order(3)]
    public async Task The_ledger_refuses_user_origin_writes()
    {
        var ledger = Silo.GetRequiredService<IAppTreeLedgerStore>();
        var before = await ledger.GetAsync(Legacy, CancellationToken.None);

        Assert.CatchAsync<Exception>(() => _cluster.Client.GetGrain<ILattice>(AppRegistryTreeNames.TreeLedgerTree).SetAsync(Legacy, [1]));
        Assert.CatchAsync<Exception>(() => _cluster.Client.GetGrain<ILattice>(AppRegistryTreeNames.TreeLedgerTree).DeleteAsync(Legacy));

        var after = await ledger.GetAsync(Legacy, CancellationToken.None);
        Assert.That(after.Version, Is.EqualTo(before.Version));
        Assert.That(after.Claim, Is.EqualTo(before.Claim));
    }

    [Test, Order(4)]
    public async Task Uninstalling_a_resized_app_tree_soft_deletes_the_live_copy_and_re_enable_recovers_it()
    {
        var registry = Silo.GetRequiredService<IAppRegistry>();
        var pipeline = Silo.GetRequiredService<IAppActivationPipeline>();
        var ledger = Silo.GetRequiredService<IAppTreeLedgerStore>();
        var tree = Grains.GetGrain<ILattice>(Records);

        using (LatticeSystemOrigin.Enter())
        {
            await tree.SetAsync("before", [1]);
            var resize = Grains.GetGrain<ITreeResizeGrain>(Records);
            await resize.ResizeAsync(64, 64);
            await resize.RunResizePassAsync();
            Assert.That(await Grains.GetLatticeRegistry().ResolveAsync(Records), Is.Not.EqualTo(Records), "the resize aliased the tree");

            // Only the resized (live) copy holds this key.
            await tree.SetAsync("after", [2]);

            var uninstalled = await pipeline.UninstallAsync(TenantId.Default, Crm);
            Assert.That(uninstalled.Succeeded, Is.True, () => string.Join("; ", uninstalled.Diagnostics.Select(d => d.Message)));
        }

        Assert.That(await Grains.GetGrain<ITreeDeletionGrain>(Records).IsDeletedAsync(), Is.True);
        using (LatticeSystemOrigin.Enter())
        {
            Assert.ThrowsAsync<InvalidOperationException>(async () => await tree.GetAsync("after"), "the live resized copy is soft-deleted");
        }

        Assert.That((await ledger.GetAsync(Legacy, CancellationToken.None)).Claim!.Released, Is.True, "uninstall releases the adoption");
        Assert.That((await ledger.GetAsync(Records, CancellationToken.None)).Claim!.Released, Is.False, "the structural claim is held until purge");

        AppActivationOutcome reenabled;
        using (LatticeSystemOrigin.Enter())
        {
            var reinstalled = await registry.InstallAsync(Request(Crm));
            Assert.That(reinstalled.Succeeded, Is.True, reinstalled.Message);
            reenabled = await pipeline.EnableAsync(TenantId.Default, Crm);
        }

        Assert.That(reenabled.Succeeded, Is.True, () => string.Join("; ", reenabled.Diagnostics.Select(d => d.Message)));
        using (LatticeSystemOrigin.Enter())
        {
            Assert.That(await tree.GetAsync("after"), Is.EqualTo(new byte[] { 2 }), "the recovered copy is the live resized one");
            Assert.That(await tree.GetAsync("before"), Is.EqualTo(new byte[] { 1 }));
        }
    }

    [Test, Order(5)]
    public async Task Aliases_are_bounded_by_app_ownership_for_every_caller()
    {
        var registry = Grains.GetLatticeRegistry();
        var mine = $"mine-{Guid.NewGuid():N}";
        var elsewhere = $"elsewhere-{Guid.NewGuid():N}";
        using (LatticeSystemOrigin.Enter())
        {
            await Grains.GetGrain<ILattice>(mine).SetAsync("k", [1]);
            await Grains.GetGrain<ILattice>(elsewhere).SetAsync("k", [2]);

            // The owned tree is resized by now, so its live data is the derived copy it resolves to.
            var ownedCopy = await registry.ResolveAsync(Records);
            var into = Assert.ThrowsAsync<LatticeTreeOwnershipDeniedException>(() => registry.SetAliasAsync(mine, ownedCopy));
            var outOf = Assert.ThrowsAsync<LatticeTreeOwnershipDeniedException>(() => registry.SetAliasAsync(Records, elsewhere));
            Assert.That(into!.Reason, Does.Contain("would cross an app ownership boundary"));
            Assert.That(into.Reason, Does.Not.Contain("owned by app"), "the denial names only the caller's own ids, never the owning app");
            Assert.That(outOf, Is.Not.Null);

            await registry.SetAliasAsync(mine, elsewhere);
            Assert.That(await registry.ResolveAsync(mine), Is.EqualTo(elsewhere), "aliasing two unowned trees is unchanged");
            Assert.That(await registry.ResolveAsync(Records), Does.StartWith(Records + "/"), "the owned tree keeps its own derived backing");
        }
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
                .AddLatticeApp(Crm.Value, new FakeAppAssembly(Resource, CrmManifest), Resource)
                .AddLatticeApp(Rival.Value, new FakeAppAssembly(Resource, RivalManifest), Resource);
        }
    }
}
