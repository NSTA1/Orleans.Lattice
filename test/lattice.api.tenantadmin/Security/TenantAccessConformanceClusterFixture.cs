using System.Security.Claims;
using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.Primitives;
using Microsoft.IdentityModel.JsonWebTokens;
using Microsoft.IdentityModel.Tokens;
using Orleans.Hosting;
using Orleans.Lattice.Api.Auth;
using Orleans.Lattice.Apps;
using Orleans.Lattice.Auth;
using Orleans.Lattice.Membership;
using Orleans.Lattice.Tenancy;
using Orleans.TestingHost;

namespace Orleans.Lattice.Api.TenantAdmin.Tests.Security;

/// <summary>
/// The single-silo cluster the delegated tenant access conformance suite runs on:
/// membership, default-deny auth with one bootstrap platform operator and the
/// <c>Tree:*</c> tier enabled, tenancy with delegated tenant access administration
/// behind a runtime switch, the tenant-admin control API, the cluster auth facade,
/// and the apps add-on. Callers are identified the way production identifies them:
/// an ambient <see cref="LatticeCredential"/> resolved by the real membership context,
/// either through a test authenticator (whose token names the subject and may assert
/// groups) or through a JWT authenticator trusting an in-test signing key.
/// </summary>
internal sealed class TenantAccessConformanceClusterFixture
{
    /// <summary>The bootstrap platform operator.</summary>
    public const string Operator = "root";

    /// <summary>The credential scheme of the test authenticator.</summary>
    public const string TestScheme = "tenant-access-conformance";

    /// <summary>The credential scheme the JWT authenticator handles.</summary>
    public const string JwtScheme = "Bearer";

    private const string JwtIssuer = "https://issuer.tenant-access-conformance.test/";
    private const string JwtAudience = "tenant-access-conformance";
    private const string TestIssuer = "https://issuer.tenant-access-conformance-test/";

    /// <summary>Separates the subject from its asserted groups in a test credential token.</summary>
    private const char AssertedGroupsSeparator = '#';

    private static readonly SymmetricSecurityKey SigningKey =
        new(Encoding.UTF8.GetBytes("tenant-access-conformance-signing-key-0123456789"));

    private long _stamp = 100;

    /// <summary>
    /// The runtime switch for <see cref="LatticeTenancyOptions.DelegatedAccessAdministrationEnabled"/>.
    /// Static because the silo's service provider is built inside the test cluster;
    /// there is one cluster per fixture instance, and fixtures do not run in parallel.
    /// </summary>
    public static TenancyFlagSwitch Switch { get; } = new();

    public TestCluster Cluster { get; private set; } = null!;

    public IServiceProvider Silo => Cluster.Silos.OfType<InProcessSiloHandle>().First().SiloHost.Services;

    public ILatticeTenantDirectoryAdmin Directory => Silo.GetRequiredService<ILatticeTenantDirectoryAdmin>();

    public ILatticeTenantPolicyAdmin Policy => Silo.GetRequiredService<ILatticeTenantPolicyAdmin>();

    public ILatticeTenantAccessAdmin AccessAdmin => Silo.GetRequiredService<ILatticeTenantAccessAdmin>();

    public ILatticeTenantGrantAdmin GrantAdmin => Silo.GetRequiredService<ILatticeTenantGrantAdmin>();

    public ILatticeTenantAdmin TenantAdmin => Silo.GetRequiredService<ILatticeTenantAdmin>();

    public ILatticeAuthAdmin AuthAdmin => Silo.GetRequiredService<ILatticeAuthAdmin>();

    public IAppRegistry Apps => Silo.GetRequiredService<IAppRegistry>();

    public ITenantRegistry Registry => Silo.GetRequiredService<ITenantRegistry>();

    public ILatticeMembershipDirectory Membership => Silo.GetRequiredService<ILatticeMembershipDirectory>();

    public ILatticeAuthorizationPolicyStore Store => Silo.GetRequiredService<ILatticeAuthorizationPolicyStore>();

    public IGrainFactory Grains => Silo.GetRequiredService<IGrainFactory>();

    public async Task InitializeAsync()
    {
        Switch.Set(true);
        var builder = new TestClusterBuilder(1);
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

        Switch.Set(true);
    }

    /// <summary>
    /// Makes <paramref name="subjectId"/> the caller through the test authenticator,
    /// optionally asserting <paramref name="assertedGroups"/> the way an identity
    /// provider's group claim would.
    /// </summary>
    public static IDisposable As(string subjectId, params string[] assertedGroups) =>
        LatticeCredentialContext.Use(
            assertedGroups.Length == 0
                ? subjectId
                : subjectId + AssertedGroupsSeparator + string.Join(',', assertedGroups),
            scheme: TestScheme);

    /// <summary>
    /// Makes the bearer of a signed JWT for <paramref name="subjectId"/>, carrying
    /// <paramref name="groups"/> in its <c>groups</c> claim, the caller.
    /// </summary>
    public static IDisposable AsJwt(string subjectId, params string[] groups) =>
        LatticeCredentialContext.Use(MintToken(subjectId, groups), scheme: JwtScheme);

    /// <summary>Seeds an active tenant administered by <paramref name="admin"/>.</summary>
    public async Task<TenantId> SeedTenantAsync(string tenantId, string admin, TenantQuotas? quotas = null)
    {
        var tenant = TenantId.Parse(tenantId);
        var record = TenantRecord.Create(
            tenant,
            TenantStatus.Active,
            quotas ?? new TenantQuotas { MaxKeys = 1000 },
            TenantPlacement.Shared,
            NextStamp(),
            "seed");
        record.AddAdminSubject(admin, NextStamp(), "seed");
        using (LatticeSystemOrigin.Enter())
        {
            await Registry.PutAsync(record);
        }

        return tenant;
    }

    /// <summary>Adds <paramref name="subjectId"/> to a seeded tenant's admin set directly on its registry record.</summary>
    public async Task AddAdminAsync(TenantId tenant, string subjectId)
    {
        using (LatticeSystemOrigin.Enter())
        {
            var record = (await Registry.GetAsync(tenant))!;
            record.AddAdminSubject(subjectId, NextStamp(), "seed");
            await Registry.PutAsync(record);
        }
    }

    /// <summary>Upserts a cluster group (an identity-provider group, say) and its direct user members.</summary>
    public async Task SeedClusterGroupAsync(string groupId, params string[] members)
    {
        using (LatticeSystemOrigin.Enter())
        {
            await Membership.UpsertGroupAsync(new MembershipGroup(groupId));
            foreach (var member in members)
            {
                await Membership.AddMemberAsync(groupId, member);
            }
        }
    }

    /// <summary>Writes an operator (platform-layer) rule straight to the policy store.</summary>
    public async Task PutOperatorRuleAsync(
        string ruleId, LatticeSubjectSelector subject, LatticeScope scope, LatticeOperation operations, LatticeEffect effect)
    {
        using (LatticeSystemOrigin.Enter())
        {
            await Store.PutRuleAsync(new LatticeAuthorizationRule(ruleId, subject, scope, operations, effect));
        }
    }

    /// <summary>
    /// The real access gate's decision for the <b>ambient</b> caller: the credential
    /// is resolved by the real membership context (its directory groups, and its
    /// asserted groups through the tenant group claim filter), and
    /// <paramref name="activeTenant"/> is asserted the way the
    /// <c>lattice-active-tenant</c> header asserts it.
    /// </summary>
    public async Task<LatticeAccessDecision> DecideAsync(
        string? activeTenant, string treeId, LatticeOperation operation = LatticeOperation.Read, string? key = "k1")
    {
        var subject = await Silo.GetRequiredService<ILatticeMembershipContext>().ResolveCurrentAsync();
        var gate = Silo.GetRequiredService<ILatticeAccessGate>();
        var request = new LatticeAccessRequest(treeId, operation, subject, key);
        if (activeTenant is null)
        {
            return await gate.AuthorizeAsync(request);
        }

        using (LatticeActiveTenantContext.With(TenantId.Parse(activeTenant)))
        {
            return await gate.AuthorizeAsync(request);
        }
    }

    /// <summary>
    /// Whether the gate admits a point request by <paramref name="subjectId"/> (through
    /// the test authenticator) acting as <paramref name="activeTenant"/>.
    /// </summary>
    public async Task<bool> AllowsAsync(
        string subjectId, string? activeTenant, string treeId, LatticeOperation operation = LatticeOperation.Read, string key = "k1")
    {
        using (As(subjectId))
        {
            var decision = await DecideAsync(activeTenant, treeId, operation, key);
            return decision.Allowed && decision.KeyFilter is null;
        }
    }

    /// <summary>The policy tree's revision timeline for one rule, read under system origin.</summary>
    public async Task<IReadOnlyList<EntryRevision>> RuleHistoryAsync(string treeId, string ruleId)
    {
        var key = treeId + '\u001f' + ruleId;
        using (LatticeSystemOrigin.Enter())
        {
            var page = await Grains.GetGrain<ILattice>(LatticeAuthReservedTrees.PolicyTreeId)
                .ScanEntryHistoryAsync(key, fromHlc: null, toHlc: null, limit: 100, continuation: null);
            return page.Revisions;
        }
    }

    private HybridLogicalClock NextStamp() =>
        new() { WallClockTicks = Interlocked.Increment(ref _stamp) };

    private static string MintToken(string subject, IEnumerable<string> groups)
    {
        var claims = new List<Claim> { new("sub", subject) };
        foreach (var group in groups)
        {
            claims.Add(new Claim("groups", group));
        }

        var descriptor = new SecurityTokenDescriptor
        {
            Issuer = JwtIssuer,
            Audience = JwtAudience,
            Subject = new ClaimsIdentity(claims),
            Expires = DateTime.UtcNow.AddHours(1),
            SigningCredentials = new SigningCredentials(SigningKey, SecurityAlgorithms.HmacSha256),
        };

        return new JsonWebTokenHandler().CreateToken(descriptor);
    }

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.UseInMemoryReminderService();
            siloBuilder.AddLatticeMembership();
            siloBuilder.Services.AddSingleton<ILatticeCredentialAuthenticator, TestCredentialAuthenticator>();
            siloBuilder.AddLatticeJwtAuthenticator(options =>
            {
                options.Issuer = JwtIssuer;
                options.SchemeHint = JwtScheme;
                options.Audiences.Add(JwtAudience);
                options.SigningKeys.Add(SigningKey);
            });
            siloBuilder.AddLatticeAuth(options =>
            {
                options.DefaultEffect = LatticeEffect.Deny;
                options.BootstrapAdministrators.Add(Operator);
                options.AllTreesGrantsEnabled = true;
            });
            siloBuilder.AddLatticeTenancy(options => options.DelegatedAccessAdministrationEnabled = true);
            siloBuilder.Services.AddSingleton<IConfigureOptions<LatticeTenancyOptions>>(Switch);
            siloBuilder.Services.AddSingleton<IOptionsChangeTokenSource<LatticeTenancyOptions>>(Switch);
            siloBuilder.AddLatticeTenantAdminApi();
            siloBuilder.AddLatticeAuthApi();
            siloBuilder.AddLatticeApps(options => options.ReconcileOnStartup = false);
        }
    }

    /// <summary>
    /// Flips <see cref="LatticeTenancyOptions.DelegatedAccessAdministrationEnabled"/> at
    /// runtime through the options change-token path, which is how the tenancy add-on's
    /// live flag observes an operator reconfiguring it.
    /// </summary>
    public sealed class TenancyFlagSwitch : IConfigureOptions<LatticeTenancyOptions>, IOptionsChangeTokenSource<LatticeTenancyOptions>
    {
        private CancellationTokenSource _change = new();
        private volatile bool _enabled = true;

        public string Name => Options.DefaultName;

        public void Configure(LatticeTenancyOptions options) => options.DelegatedAccessAdministrationEnabled = _enabled;

        public IChangeToken GetChangeToken() => new CancellationChangeToken(_change.Token);

        public void Set(bool enabled)
        {
            if (_enabled == enabled)
            {
                return;
            }

            _enabled = enabled;
            Interlocked.Exchange(ref _change, new CancellationTokenSource()).Cancel();
        }
    }

    /// <summary>
    /// Resolves a test credential's token, <c>subject</c> or
    /// <c>subject#group1,group2</c>; the groups are carried as the principal's
    /// identity-provider-asserted groups, exactly as a token's group claim would be.
    /// </summary>
    private sealed class TestCredentialAuthenticator : ILatticeCredentialAuthenticator
    {
        public bool CanHandle(in LatticeCredential credential) =>
            string.Equals(credential.Scheme, TestScheme, StringComparison.Ordinal);

        public ValueTask<LatticePrincipal?> AuthenticateAsync(
            LatticeCredential credential, CancellationToken cancellationToken = default)
        {
            var token = credential.Token;
            var separator = token.IndexOf(AssertedGroupsSeparator);
            if (separator < 0)
            {
                return new(new LatticePrincipal(token, TestIssuer));
            }

            var groups = token[(separator + 1)..].Split(',', StringSplitOptions.RemoveEmptyEntries);
            return new(new LatticePrincipal(token[..separator], TestIssuer, assertedGroups: groups));
        }
    }
}
