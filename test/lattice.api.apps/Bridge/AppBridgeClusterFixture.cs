using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Orleans.Hosting;
using Orleans.Lattice.Apps;
using Orleans.Lattice.Apps.Tests;
using Orleans.Lattice.Auth;
using Orleans.Lattice.Membership;
using Orleans.Lattice.Testing;
using Orleans.TestingHost;

namespace Orleans.Lattice.Api.Apps.Tests.Bridge;

/// <summary>
/// A single-silo <see cref="TestCluster"/> wired with the core lattice, membership (with a deterministic test
/// authenticator), default-deny authorization, the apps add-on serving one in-image app with a UI, and the app
/// bridge facade. <see cref="InstallAsync"/> installs and enables the app through the real app-control facade as
/// the bootstrap administrator, so its role rules are compiled and persisted and its trees created exactly as in
/// production.
/// </summary>
/// <remarks>
/// With <c>tenant</c> set, the silo's active-tenant resolver is replaced by one that reads the ambient
/// <see cref="LatticeActiveTenantContext"/>, and every call is made inside that tenant, so the install, its
/// compiled rules, its trees and the bridge all use the tenant-composed <c>t/{tenant}/a/{slug}/{tree}</c> ids.
/// </remarks>
internal sealed class AppBridgeClusterFixture(TenantId? tenant)
{
    public const string BootstrapAdmin = "root-admin";
    public const string Slug = "notes-app";
    public const string Version = "1.0.0";
    public const string Viewers = "g-notes-viewers";
    public const string Editors = "g-notes-editors";
    public const string Editor = "erin";
    public const string Viewer = "victor";
    public const string Operator = "olga";

    private const string Resource = "app.manifest.json";

    public TestCluster Cluster { get; private set; } = null!;

    public IServiceProvider Silo => Cluster.Silos.OfType<InProcessSiloHandle>().First().SiloHost.Services;

    public ILatticeAppBridge Bridge => Silo.GetRequiredService<ILatticeAppBridge>();

    /// <summary>The tenant every call is made in, or null when tenancy is off.</summary>
    public TenantId? Tenant { get; } = tenant;

    /// <summary>The effective id of the app's <c>notes</c> tree.</summary>
    public string NotesTree => Tenant is { } t ? $"t/{t.Value}/a/{Slug}/notes" : $"a/{Slug}/notes";

    /// <summary>The install revision the app was enabled at.</summary>
    public long InstallRevision { get; private set; }

    public AppBridgeTarget Target(string tree = "notes") =>
        new() { AppSlug = Slug, InstallRevision = InstallRevision, LogicalTree = tree };

    public static string ManifestJson()
    {
        var entryDigest = UiTestManifests.Sha256(UiTestManifests.EntryBytes);
        var bundleDigest = AppUiBundle.ComputeBundleDigest(
            [new Orleans.Lattice.Apps.AppUiAsset { Path = UiTestManifests.EntryPath, MediaType = "text/html", Digest = entryDigest }]);
        return $$"""
            {
              "identity": { "slug": "{{Slug}}", "version": "{{Version}}" },
              "trees": [{ "name": "notes" }],
              "roles": [
                { "name": "viewer", "operations": ["Read", "RangeRead"], "scopes": [{ "tree": "notes" }] },
                { "name": "editor", "operations": ["Read", "RangeRead", "Write", "Delete"], "scopes": [{ "tree": "notes" }] }
              ],
              "subscriptions": [],
              "mcpTools": [],
              "ui": {
                "entry": "{{UiTestManifests.EntryPath}}",
                "assets": [{ "path": "{{UiTestManifests.EntryPath}}", "mediaType": "text/html", "digest": "{{entryDigest}}" }],
                "bundleDigest": "{{bundleDigest}}",
                "bridge": [
                  { "operation": "data.read" },
                  { "operation": "data.write", "trees": ["notes"] },
                  { "operation": "data.delete", "trees": ["notes"] }
                ],
                "minProtocol": 1
              }
            }
            """;
    }

    public async Task InitializeAsync()
    {
        var builder = new TestClusterBuilder(1);
        if (Tenant is null)
        {
            builder.AddSiloBuilderConfigurator<SiloConfigurator>();
        }
        else
        {
            builder.AddSiloBuilderConfigurator<TenantSiloConfigurator>();
        }

        Cluster = builder.Build();
        await Cluster.DeployAsync();
    }

    public async Task DisposeAsync()
    {
        if (Cluster is not null)
        {
            await Cluster.StopAllSilosAsync();
            await Cluster.DisposeAsync();
        }
    }

    /// <summary>Makes calls inside the returned scope as <paramref name="subject"/>, in the fixture's tenant.</summary>
    public IDisposable As(string subject) => new Scope(
        LatticeCredentialContext.Use(subject, scheme: BridgeTestCredentialAuthenticator.Scheme),
        Tenant is { } t ? LatticeActiveTenantContext.With(t) : null);

    /// <summary>
    /// Puts the editor and the operator's viewer role into their groups, then installs and enables the app as the
    /// bootstrap administrator, and waits until the bridge serves the editor.
    /// </summary>
    public async Task InstallAsync()
    {
        var directory = Silo.GetRequiredService<ILatticeMembershipDirectory>();
        using (LatticeSystemOrigin.Enter())
        {
            await directory.UpsertGroupAsync(new MembershipGroup(Viewers));
            await directory.UpsertGroupAsync(new MembershipGroup(Editors));
            await directory.AddMemberAsync(Editors, Editor);
            await directory.AddMemberAsync(Viewers, Viewer);
            await directory.AddMemberAsync(Viewers, Operator);
        }

        var control = Silo.GetRequiredService<ILatticeAppsControl>();
        using (As(BootstrapAdmin))
        {
            var installed = await control.InstallAsync(new AppInstallRequest
            {
                Slug = Slug,
                Version = Version,
                RoleBindings =
                [
                    new AppRoleBindingDescriptor { RoleName = "viewer", GroupId = Viewers },
                    new AppRoleBindingDescriptor { RoleName = "editor", GroupId = Editors },
                ],
                Ceiling = new AppCapabilityCeilingDescriptor
                {
                    AllowedOperations = LatticeOperation.Read | LatticeOperation.RangeRead | LatticeOperation.Write | LatticeOperation.Delete,
                },
            });
            Assert.That(installed.State, Is.EqualTo(AppLifecycleState.Installed));
            var enabled = await control.EnableAsync(Slug);
            Assert.That(enabled.State, Is.EqualTo(AppLifecycleState.Enabled));
        }

        // The operator's broad, non-app rights: an operator-authored rule allowing every data operation on the app's own tree.
        var store = Silo.GetRequiredService<ILatticeAuthorizationPolicyStore>();
        using (LatticeSystemOrigin.Enter())
        {
            await store.PutRuleAsync(new LatticeAuthorizationRule(
                "operator-everything",
                LatticeSubjectSelector.User(Operator),
                LatticeScope.Tree(NotesTree),
                LatticeOperation.Read | LatticeOperation.RangeRead | LatticeOperation.Write | LatticeOperation.Delete,
                LatticeEffect.Allow));
        }

        var projection = Silo.GetRequiredService<IAppRegistryProjection>();
        var own = Tenant ?? TenantId.Default;
        await TestPoll.UntilAsync(
            () => projection.Current.TryGet(own, AppSlug.Parse(Slug), out var record) && record.State == AppRegistryLifecycleState.Enabled,
            "the registry projection observes the enabled install",
            timeout: TimeSpan.FromSeconds(60));
        projection.Current.TryGet(own, AppSlug.Parse(Slug), out var enabledRecord);
        InstallRevision = enabledRecord!.Revision;

        // The compiled app rules reach the data-plane policy asynchronously; a probe read proves they have.
        await TestPoll.UntilAsync(
            async () =>
            {
                using (As(Editor))
                {
                    try
                    {
                        await Bridge.GetAsync(Target(), "probe");
                        return true;
                    }
                    catch (AppBridgeException)
                    {
                        return false;
                    }
                }
            },
            "the bridge serves the editor once the compiled rules are live",
            timeout: TimeSpan.FromSeconds(60));
    }

    /// <summary>Reads a key of an effective tree directly as the bootstrap administrator in the fixture's tenant, bypassing the bridge.</summary>
    public async Task<byte[]?> ReadRawAsync(string treeId, string key)
    {
        using (As(BootstrapAdmin))
        {
            return await Cluster.Client.GetGrain<ILattice>(treeId).GetAsync(key);
        }
    }

    /// <summary>Writes a key of an effective tree directly as the bootstrap administrator in the fixture's tenant, bypassing the bridge.</summary>
    public async Task WriteRawAsync(string treeId, string key, byte[] value)
    {
        using (As(BootstrapAdmin))
        {
            await Cluster.Client.GetGrain<ILattice>(treeId).SetAsync(key, value);
        }
    }

    private static void Configure(ISiloBuilder siloBuilder)
    {
        siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
        siloBuilder.ConfigureLattice(o => o.WalPartitions = 1);
        siloBuilder.UseInMemoryReminderService();
        siloBuilder.AddLatticeMembership();
        siloBuilder.Services.AddSingleton<ILatticeCredentialAuthenticator, BridgeTestCredentialAuthenticator>();
        siloBuilder.AddLatticeAuth(options =>
        {
            options.DefaultEffect = LatticeEffect.Deny;
            options.BootstrapAdministrators.Add(BootstrapAdmin);
        });
        siloBuilder.AddLatticeApps()
            .AddLatticeApp(Slug, new FakeAppAssembly(name => name switch
            {
                Resource => new MemoryStream(System.Text.Encoding.UTF8.GetBytes(ManifestJson())),
                _ when name.EndsWith("." + UiTestManifests.EntryPath, StringComparison.Ordinal) => new MemoryStream(UiTestManifests.EntryBytes),
                _ => null,
            }), Resource);
        siloBuilder.AddLatticeAppBridgeApi(options => options.RateLimitPermitLimit = 100_000);
    }

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder) => AppBridgeClusterFixture.Configure(siloBuilder);
    }

    private sealed class TenantSiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            AppBridgeClusterFixture.Configure(siloBuilder);
            siloBuilder.Services.Replace(ServiceDescriptor.Singleton<ITenantContextResolver, AmbientTenantContextResolver>());
        }
    }

    /// <summary>Resolves the ambient active tenant, or the default tenant outside any tenant scope.</summary>
    private sealed class AmbientTenantContextResolver : ITenantContextResolver
    {
        public ValueTask<TenantId> ResolveCurrentAsync(CancellationToken cancellationToken = default) =>
            new(LatticeActiveTenantContext.Current ?? TenantId.Default);

        public bool TryResolveCurrent(out TenantId tenant)
        {
            tenant = LatticeActiveTenantContext.Current ?? TenantId.Default;
            return true;
        }
    }

    private sealed class Scope(IDisposable credential, IDisposable? tenant) : IDisposable
    {
        public void Dispose()
        {
            tenant?.Dispose();
            credential.Dispose();
        }
    }
}
