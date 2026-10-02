using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Orleans.Hosting;
using Orleans.Lattice;
using Orleans.Lattice.Auth;
using Orleans.Lattice.Membership;
using Orleans.Lattice.Tenancy;
using Orleans.TestingHost;

namespace Orleans.Lattice.Api.TenantAdmin.Tests.Directory;

/// <summary>
/// A single-silo cluster composing auth (deny by default, with <c>root</c> as the
/// bootstrap platform operator), membership, tenancy with delegated tenant access
/// administration on or off, and the tenant-admin control API, for the tenant
/// directory facade's integration fixtures. The caller is an ambient subject set with
/// <see cref="As"/>; its groups are the membership directory's transitive closure, as
/// the real membership context resolves them.
/// </summary>
internal sealed class TenantDirectoryClusterFixture
{
    /// <summary>The bootstrap platform operator.</summary>
    public const string Operator = "root";

    private static readonly AsyncLocal<string?> CurrentCaller = new();

    private readonly bool _delegatedAccessEnabled;

    public TenantDirectoryClusterFixture(bool delegatedAccessEnabled) => _delegatedAccessEnabled = delegatedAccessEnabled;

    public TestCluster Cluster { get; private set; } = null!;

    public IServiceProvider Silo => Cluster.Silos.OfType<InProcessSiloHandle>().First().SiloHost.Services;

    public ILatticeTenantDirectoryAdmin Directory => Silo.GetRequiredService<ILatticeTenantDirectoryAdmin>();

    public ILatticeTenantAccessAdmin AccessAdmin => Silo.GetRequiredService<ILatticeTenantAccessAdmin>();

    public ITenantRegistry Registry => Silo.GetRequiredService<ITenantRegistry>();

    public ILatticeMembershipDirectory Membership => Silo.GetRequiredService<ILatticeMembershipDirectory>();

    public ILatticeAuthorizationPolicyStore Policy => Silo.GetRequiredService<ILatticeAuthorizationPolicyStore>();

    public ITenantPolicyEngine TenantPolicy => Silo.GetRequiredService<ITenantPolicyEngine>();

    public async Task InitializeAsync()
    {
        var builder = new TestClusterBuilder(1);
        builder.Properties["DelegatedAccess"] = _delegatedAccessEnabled.ToString();
        builder.AddSiloBuilderConfigurator<SiloConfigurator>();
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

    /// <summary>Makes <paramref name="subjectId"/> the caller until the scope is disposed.</summary>
    public static IDisposable As(string subjectId)
    {
        var previous = CurrentCaller.Value;
        CurrentCaller.Value = subjectId;
        return new Restore(previous);
    }

    /// <summary>Seeds a tenant administered by <paramref name="adminSubjects"/>, optionally with quotas.</summary>
    public async Task SeedTenantAsync(string tenantId, TenantQuotas? quotas = null, params string[] adminSubjects)
    {
        var record = TenantRecord.Create(
            TenantId.Parse(tenantId),
            TenantStatus.Active,
            quotas ?? TenantQuotas.Unbounded,
            TenantPlacement.Shared,
            new HybridLogicalClock { WallClockTicks = 1 },
            "seed");

        var ticks = 2L;
        foreach (var subject in adminSubjects)
        {
            record.AddAdminSubject(subject, new HybridLogicalClock { WallClockTicks = ticks++ }, "seed");
        }

        await Registry.PutAsync(record);
    }

    /// <summary>The caller's resolved groups, as the membership context would resolve them.</summary>
    public async Task<IReadOnlyCollection<string>> GroupsOfAsync(string subjectId)
    {
        using (LatticeSystemOrigin.Enter())
        {
            return await Membership.GroupsOfAsync(subjectId);
        }
    }

    private sealed class Restore(string? previous) : IDisposable
    {
        public void Dispose() => CurrentCaller.Value = previous;
    }

    /// <summary>Resolves the ambient caller with its transitive directory groups.</summary>
    private sealed class AmbientMembershipContext(ILatticeMembershipDirectory directory) : ILatticeMembershipContext
    {
        public async ValueTask<LatticeSubject> ResolveCurrentAsync(CancellationToken cancellationToken = default)
        {
            if (CurrentCaller.Value is not { } subjectId)
            {
                return LatticeSubject.Anonymous;
            }

            var groups = await directory.GroupsOfAsync(subjectId, cancellationToken);
            return new LatticeSubject(subjectId, groups);
        }
    }

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            var enabled = bool.Parse(siloBuilder.Configuration["DelegatedAccess"] ?? bool.FalseString);

            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.UseInMemoryReminderService();
            siloBuilder.AddLatticeMembership();
            siloBuilder.AddLatticeAuth(options =>
            {
                options.DefaultEffect = LatticeEffect.Deny;
                options.BootstrapAdministrators.Add(Operator);
            });
            siloBuilder.AddLatticeTenancy(options => options.DelegatedAccessAdministrationEnabled = enabled);
            siloBuilder.AddLatticeTenantAdminApi();
            siloBuilder.Services.Replace(ServiceDescriptor.Singleton<ILatticeMembershipContext>(
                sp => new AmbientMembershipContext(sp.GetRequiredService<ILatticeMembershipDirectory>())));
        }
    }
}
