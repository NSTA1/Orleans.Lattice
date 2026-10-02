using Grpc.Core;
using Grpc.Core.Interceptors;
using Grpc.Net.Client;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.Primitives;
using Orleans.Hosting;
using Orleans.Lattice.Api.TenantAdmin;
using Orleans.Lattice.Auth;
using Orleans.Lattice.Membership;
using Orleans.Lattice.Tenancy;
using Orleans.Lattice.Testing;
using Orleans.Serialization;
using Orleans.TestingHost;

namespace Orleans.Lattice.Api.TenantAdmin.Grpc.Tests;

/// <summary>
/// End-to-end coverage of the delegated tenant access RPCs over the real
/// facades: a single-silo cluster composes membership, default-deny auth with one
/// bootstrap operator, tenancy with delegated tenant access administration behind a
/// runtime switch, and the tenant-admin control API; the binding is mapped on an
/// in-memory <see cref="TestServer"/> over that silo's facades, and every RPC is
/// driven through the public <see cref="LatticeTenantAdminApiGrpcClient"/> with the
/// caller identified by its credential header. Proves each RPC end to end, each
/// fault mapping as the real facade raises it, the D13 caps travelling through
/// <c>SetTenantQuotas</c> into the posture, and that a host without the facades
/// answers <see cref="StatusCode.Unimplemented"/>.
/// </summary>
/// <remarks>
/// Owned by the epic coordinator's integration run; not exercised in the G1
/// unit-only pass. Each test seeds its own tenant, so the tests do not interfere.
/// </remarks>
[TestFixture]
[NonParallelizable]
[Category("Integration")]
public sealed class LatticeTenantAdminGrpcTenantAccessIntegrationTests
{
    private const string Operator = "root";
    private const string Admin = "alice";
    private const string Member = "bob";
    private const string Scheme = "TestToken";

    private static readonly TimeSpan Deadline = TimeSpan.FromSeconds(30);

    private TestCluster _cluster = null!;
    private IHost _host = null!;
    private IHost _bareHost = null!;

    private IServiceProvider Silo => _cluster.Silos.OfType<InProcessSiloHandle>().First().SiloHost.Services;

    [OneTimeSetUp]
    public async Task SetUp()
    {
        var builder = new TestClusterBuilder(1);
        builder.AddSiloBuilderConfigurator<SiloConfigurator>();
        _cluster = builder.Build();
        await _cluster.DeployAsync();
        _host = await StartHostAsync(withTenantAccess: true);
        _bareHost = await StartHostAsync(withTenantAccess: false);
    }

    [OneTimeTearDown]
    public async Task TearDown()
    {
        await _host.StopAsync();
        _host.Dispose();
        await _bareHost.StopAsync();
        _bareHost.Dispose();
        await _cluster.StopAllSilosAsync();
        await _cluster.DisposeAsync();
    }

    [SetUp]
    public void EnableTheFeature() => FlagSwitch.Instance.Set(true);

    [Test]
    public async Task A_tenant_admin_drives_the_whole_directory_over_the_wire()
    {
        var tenant = await SeedTenantAsync("grpc-directory");
        var client = ClientAs(Admin);

        var upserted = await client.UpsertGroupAsync(tenant, new TenantGroupDescriptor { Name = "readers", DisplayName = "Readers" });
        var fetched = await client.GetGroupAsync(tenant, "readers");
        var absent = await client.GetGroupAsync(tenant, "ghost");
        var groups = await client.ListGroupsAsync(tenant, new TenantAccessPageRequest());
        var edge = await client.AddGroupMemberAsync(tenant, "readers", Member);
        var members = await client.ListGroupMembersAsync(tenant, "readers");
        var entry = await client.AddMemberAsync(tenant, "readers", TenantSubjectKind.TenantGroup);
        var memberSet = await client.ListMembersAsync(tenant, new TenantAccessPageRequest());
        var resolution = await client.ResolveSubjectAsync(tenant, Member);

        Assert.Multiple(() =>
        {
            Assert.That(upserted.Name, Is.EqualTo("readers"));
            Assert.That(fetched?.DisplayName, Is.EqualTo("Readers"));
            Assert.That(absent, Is.Null);
            Assert.That(groups.Entries.Select(g => g.Name), Is.EqualTo(new[] { "readers" }));
            Assert.That(edge.Changed, Is.True);
            Assert.That(members, Is.EqualTo(new[] { new TenantGroupMember { MemberId = Member, Kind = TenantSubjectKind.User } }));
            Assert.That(entry.Changed, Is.True);
            Assert.That(memberSet.Entries, Does.Contain(new TenantMemberEntry { SubjectId = "readers", Kind = TenantSubjectKind.TenantGroup }));
            Assert.That(resolution.IsMember, Is.True, "bob is a member of the tenant through the readers group");
        });

        var removedEdge = await client.RemoveGroupMemberAsync(tenant, "readers", Member);
        var removedEntry = await client.RemoveMemberAsync(tenant, "readers", TenantSubjectKind.TenantGroup);
        var removal = await client.RemoveGroupAsync(tenant, "readers");

        Assert.Multiple(() =>
        {
            Assert.That(removedEdge.Changed, Is.True);
            Assert.That(removedEntry.Changed, Is.True);
            Assert.That(removal.Removed, Is.True);
        });
    }

    [Test]
    public async Task A_tenant_admin_drives_the_whole_policy_surface_over_the_wire()
    {
        var tenant = await SeedTenantAsync("grpc-policy");
        var client = ClientAs(Admin);
        await client.AddMemberAsync(tenant, Member);

        var put = await client.PutRuleAsync(tenant, new TenantRuleDraft
        {
            RuleId = "bob-read",
            SubjectId = Member,
            ScopeKind = TenantRuleScopeKind.Tree,
            TreeName = "orders",
            Operations = LatticeOperation.Read,
            Effect = LatticeEffect.Allow,
        });
        var got = await client.GetRuleAsync(tenant, "bob-read");
        var page = await client.ListRulesAsync(tenant, new TenantAccessPageRequest());

        Assert.Multiple(() =>
        {
            Assert.That(put.Layer, Is.EqualTo(TenantRuleLayer.Tenant));
            Assert.That(put.Editable, Is.True);
            Assert.That(got?.RuleId, Is.EqualTo("bob-read"));
            Assert.That(page.Entries.Select(r => r.RuleId), Does.Contain("bob-read"));
        });

        await TestPoll.UntilAsync(
            async () => (await client.ExplainAsync(tenant, Member, "orders", "k1", LatticeOperation.Read)).Allowed,
            "the tenant rule decides the explanation once it reaches the policy snapshot",
            Deadline);
        var explanation = await client.ExplainAsync(tenant, Member, "orders", "k1", LatticeOperation.Read);
        var effective = await client.EffectivePermissionsAsync(tenant, Member, "orders");
        var posture = await client.GetPostureAsync(tenant);

        Assert.Multiple(() =>
        {
            Assert.That(explanation.DecidingLayer, Is.EqualTo(TenantRuleLayer.Tenant));
            Assert.That(explanation.DecidingRuleId, Is.EqualTo("bob-read"));
            Assert.That(effective.Rules.Select(r => r.RuleId), Does.Contain("bob-read"));
            Assert.That(posture.Enabled, Is.True);
            Assert.That(posture.CallerIsTenantAdmin, Is.True);
            Assert.That(posture.TenantRules.Usage, Is.EqualTo(1));
        });

        Assert.That(await client.RemoveRuleAsync(tenant, "bob-read"), Is.True);
        Assert.That(await client.RemoveRuleAsync(tenant, "bob-read"), Is.False, "removing an absent rule is a no-op");
    }

    [Test]
    public async Task The_caps_set_through_SetTenantQuotas_reach_the_posture_and_are_enforced()
    {
        var tenant = await SeedTenantAsync("grpc-caps");
        var quotas = new TenantQuotasDescriptor { MaxGroups = 1, MaxMembershipEdges = 20, MaxMemberSubjects = 30, MaxTenantRules = 40 };

        var updated = await ClientAs(Operator).SetTenantQuotasAsync(tenant, quotas);
        var admin = ClientAs(Admin);
        var posture = await admin.GetPostureAsync(tenant);
        await admin.UpsertGroupAsync(tenant, new TenantGroupDescriptor { Name = "first" });
        var refused = Assert.ThrowsAsync<RpcException>(
            async () => await admin.UpsertGroupAsync(tenant, new TenantGroupDescriptor { Name = "second" }));

        Assert.Multiple(() =>
        {
            Assert.That(updated.Quotas.MaxGroups, Is.EqualTo(1));
            Assert.That(updated.Quotas.MaxTenantRules, Is.EqualTo(40));
            Assert.That(posture.Groups.Limit, Is.EqualTo(1));
            Assert.That(posture.MembershipEdges.Limit, Is.EqualTo(20));
            Assert.That(posture.MemberSubjects.Limit, Is.EqualTo(30));
            Assert.That(posture.TenantRules.Limit, Is.EqualTo(40));
            Assert.That(refused!.StatusCode, Is.EqualTo(StatusCode.ResourceExhausted));
            Assert.That(refused.Trailers.GetValue(LatticeTenantAdminGrpcService.QuotaDimensionTrailer), Is.Not.Null.And.Not.Empty);
        });
    }

    [Test]
    public async Task A_caller_who_is_not_a_tenant_admin_is_permission_denied()
    {
        var tenant = await SeedTenantAsync("grpc-denied");

        var ex = Assert.ThrowsAsync<RpcException>(async () => await ClientAs("mallory").ListGroupsAsync(tenant, new TenantAccessPageRequest()));

        Assert.That(ex!.StatusCode, Is.EqualTo(StatusCode.PermissionDenied));
    }

    [Test]
    public void The_reserved_default_tenant_is_a_failed_precondition()
    {
        var ex = Assert.ThrowsAsync<RpcException>(async () => await ClientAs(Operator).ListGroupsAsync(TenantId.DefaultId, new TenantAccessPageRequest()));

        Assert.That(ex!.StatusCode, Is.EqualTo(StatusCode.FailedPrecondition));
    }

    [Test]
    public async Task A_foreign_tenant_group_is_refused_as_an_invalid_argument()
    {
        var tenant = await SeedTenantAsync("grpc-confined");
        var client = ClientAs(Admin);
        await client.UpsertGroupAsync(tenant, new TenantGroupDescriptor { Name = "ops" });

        var ex = Assert.ThrowsAsync<RpcException>(
            async () => await client.AddGroupMemberAsync(tenant, "ops", "t/someone-else/ops", TenantSubjectKind.ClusterGroup));

        Assert.That(ex!.StatusCode, Is.EqualTo(StatusCode.InvalidArgument));
    }

    [Test]
    public async Task With_the_feature_off_every_call_but_the_posture_is_a_failed_precondition()
    {
        var tenant = await SeedTenantAsync("grpc-off");
        var client = ClientAs(Admin);
        FlagSwitch.Instance.Set(false);
        try
        {
            await TestPoll.UntilAsync(
                async () => !(await client.GetPostureAsync(tenant)).Enabled,
                "the posture reports the feature off once the switch flips",
                Deadline);

            var directory = Assert.ThrowsAsync<RpcException>(async () => await client.ListGroupsAsync(tenant, new TenantAccessPageRequest()));
            var policy = Assert.ThrowsAsync<RpcException>(async () => await client.ListRulesAsync(tenant, new TenantAccessPageRequest()));

            Assert.Multiple(() =>
            {
                Assert.That(directory!.StatusCode, Is.EqualTo(StatusCode.FailedPrecondition));
                Assert.That(policy!.StatusCode, Is.EqualTo(StatusCode.FailedPrecondition));
            });
        }
        finally
        {
            FlagSwitch.Instance.Set(true);
        }
    }

    [Test]
    public async Task A_host_without_the_tenant_access_facades_answers_unimplemented_and_serves_the_rest()
    {
        var tenant = await SeedTenantAsync("grpc-bare");
        var client = ClientAs(Admin, _bareHost);

        var directory = Assert.ThrowsAsync<RpcException>(async () => await client.GetGroupAsync(tenant, "ops"));
        var policy = Assert.ThrowsAsync<RpcException>(async () => await client.GetPostureAsync(tenant));
        var admins = await client.ListTenantAdminSubjectsAsync(tenant);

        Assert.Multiple(() =>
        {
            Assert.That(directory!.StatusCode, Is.EqualTo(StatusCode.Unimplemented));
            Assert.That(policy!.StatusCode, Is.EqualTo(StatusCode.Unimplemented));
            Assert.That(admins.Subjects, Does.Contain(Admin));
        });
    }

    // ---- composition -------------------------------------------------------

    private async Task<IHost> StartHostAsync(bool withTenantAccess)
    {
        var silo = Silo;
        return await new HostBuilder()
            .ConfigureWebHost(web =>
            {
                web.UseTestServer();
                web.ConfigureServices(services =>
                {
                    services.AddSerializer();
                    services.AddSingleton(silo.GetRequiredService<ILatticeTenantAdmin>());
                    services.AddSingleton(silo.GetRequiredService<ILatticeTenantSelfService>());
                    services.AddSingleton(silo.GetRequiredService<ILatticeTenantAccessAdmin>());
                    if (withTenantAccess)
                    {
                        services.AddSingleton(silo.GetRequiredService<ILatticeTenantDirectoryAdmin>());
                        services.AddSingleton(silo.GetRequiredService<ILatticeTenantPolicyAdmin>());
                    }

                    services.AddSingleton<ILatticeTenantAdminApiAuthorizer, AllowAllTenantAdminApiAuthorizer>();
                    services.AddLatticeTenantAdminApiGrpc(o => o.CredentialScheme = Scheme);
                });
                web.Configure(app =>
                {
                    app.UseRouting();
                    app.UseEndpoints(endpoints => endpoints.MapLatticeTenantAdminApiGrpc());
                });
            })
            .StartAsync();
    }

    private LatticeTenantAdminApiGrpcClient ClientAs(string subject, IHost? host = null)
    {
        host ??= _host;
        var testServer = host.GetTestServer();
        var channel = GrpcChannel.ForAddress(
            testServer.BaseAddress,
            new GrpcChannelOptions { HttpHandler = testServer.CreateHandler() });
        var invoker = channel.CreateCallInvoker().Intercept(metadata =>
        {
            metadata.Add("authorization", $"{Scheme} {subject}");
            return metadata;
        });
        return LatticeTenantAdminApiGrpcClient.Create(invoker, host.Services);
    }

    private async Task<string> SeedTenantAsync(string tenantId)
    {
        var record = TenantRecord.Create(
            TenantId.Parse(tenantId),
            TenantStatus.Active,
            TenantQuotas.Unbounded,
            TenantPlacement.Shared,
            new HybridLogicalClock { WallClockTicks = 1 },
            "seed");
        record.AddAdminSubject(Admin, new HybridLogicalClock { WallClockTicks = 2 }, "seed");
        using (LatticeSystemOrigin.Enter())
        {
            await Silo.GetRequiredService<ITenantRegistry>().PutAsync(record);
        }

        return tenantId;
    }

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.UseInMemoryReminderService();
            siloBuilder.AddLatticeMembership();
            siloBuilder.Services.AddSingleton<ILatticeCredentialAuthenticator, TokenAsSubjectAuthenticator>();
            siloBuilder.AddLatticeAuth(options =>
            {
                options.DefaultEffect = LatticeEffect.Deny;
                options.BootstrapAdministrators.Add(Operator);
            });
            siloBuilder.AddLatticeTenancy(options => options.DelegatedAccessAdministrationEnabled = true);
            siloBuilder.Services.AddSingleton<IConfigureOptions<LatticeTenancyOptions>>(FlagSwitch.Instance);
            siloBuilder.Services.AddSingleton<IOptionsChangeTokenSource<LatticeTenancyOptions>>(FlagSwitch.Instance);
            siloBuilder.AddLatticeTenantAdminApi();
        }
    }

    /// <summary>Flips the delegated tenant access flag at runtime through the options change-token path.</summary>
    private sealed class FlagSwitch : IConfigureOptions<LatticeTenancyOptions>, IOptionsChangeTokenSource<LatticeTenancyOptions>
    {
        public static FlagSwitch Instance { get; } = new();

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

    /// <summary>Resolves a test credential's token directly as the subject id.</summary>
    private sealed class TokenAsSubjectAuthenticator : ILatticeCredentialAuthenticator
    {
        public bool CanHandle(in LatticeCredential credential) =>
            string.Equals(credential.Scheme, Scheme, StringComparison.Ordinal);

        public ValueTask<LatticePrincipal?> AuthenticateAsync(
            LatticeCredential credential, CancellationToken cancellationToken = default) =>
            new(new LatticePrincipal(credential.Token, "https://issuer.tenant-access-grpc.test/"));
    }
}
