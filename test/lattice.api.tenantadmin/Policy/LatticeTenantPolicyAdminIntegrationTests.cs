using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.Primitives;
using Orleans.Hosting;
using Orleans.Lattice.Auth;
using Orleans.Lattice.Membership;
using Orleans.Lattice.Tenancy;
using Orleans.Lattice.Testing;
using Orleans.TestingHost;

namespace Orleans.Lattice.Api.TenantAdmin.Tests.Policy;

/// <summary>
/// End-to-end integration coverage for the tenant policy facade
/// (<see cref="ILatticeTenantPolicyAdmin"/>, issue #4166) over a real single-silo
/// cluster with the membership, auth (default-deny, one bootstrap operator) and
/// tenancy add-ons and delegated tenant access administration switched on. Callers
/// are identified through a test credential authenticator, and the data-plane
/// verdicts are read from the real <see cref="ILatticeAccessGate"/> under the
/// tenant's active-tenant assertion, so each test proves the whole path: the facade
/// writes a confined tenant-tier rule, the policy snapshot picks it up, and the gate
/// (tenant gate, operator layer, tenant layer) decides with it.
/// </summary>
/// <remarks>
/// <para>
/// Each test seeds its own tenant, so the tests do not interfere. Rule and
/// membership changes reach the compiled snapshots asynchronously, so a verdict is
/// polled against a deadline rather than read once. Every test first observes the
/// verdict its write changes, so it fails when the facade does not do its job.
/// </para>
/// <para>
/// Owned by the epic coordinator's integration run; not exercised in the F2
/// unit-only pass.
/// </para>
/// </remarks>
[TestFixture]
[Category("Integration")]
public sealed class LatticeTenantPolicyAdminIntegrationTests
{
    private const string Operator = "root";
    private const string Admin = "alice";
    private const string Member = "bob";
    private const string Scheme = "tenant-policy-test-scheme";

    private static readonly TimeSpan Deadline = TimeSpan.FromSeconds(30);

    private readonly PolicyClusterFixture _fixture = new();

    private ILatticeTenantPolicyAdmin Facade => _fixture.SiloServices.GetRequiredService<ILatticeTenantPolicyAdmin>();

    [OneTimeSetUp]
    public Task SetUp() => _fixture.InitializeAsync();

    [OneTimeTearDown]
    public Task TearDown() => _fixture.DisposeAsync();

    [SetUp]
    public void EnableTheFeature() => PolicyClusterFixture.Switch.Set(true);

    [Test]
    public async Task A_tenant_admin_grants_a_tenant_group_read_on_a_tree_and_a_member_reads_it()
    {
        var tenant = await SeedTenantAsync("grant-read");
        var group = $"t/{tenant.Value}/readers";
        await AddGroupMemberAsync(group, Member);
        await AddTenantMemberAsync(tenant, group);
        var tree = $"t/{tenant.Value}/orders";

        Assert.That(await ReadAllowedAsync(tenant, Member, tree), Is.False, "no rule grants the member anything yet");

        using (As(Admin))
        {
            var view = await Facade.PutRuleAsync(tenant.Value, new TenantRuleDraft
            {
                RuleId = "readers-read",
                SubjectId = "readers",
                SubjectKind = TenantSubjectKind.TenantGroup,
                ScopeKind = TenantRuleScopeKind.Tree,
                TreeName = "orders",
                Operations = LatticeOperation.Read,
                Effect = LatticeEffect.Allow,
            });
            Assert.That(view.RuleId, Is.EqualTo("readers-read"));
        }

        await TestPoll.UntilAsync(
            () => ReadAllowedAsync(tenant, Member, tree),
            "a member of the tenant group reads the tree once the tenant rule is in the snapshot",
            Deadline);

        using (As(Admin))
        {
            var explanation = await Facade.ExplainAsync(tenant.Value, Member, "orders", "k1", LatticeOperation.Read);
            Assert.Multiple(() =>
            {
                Assert.That(explanation.Allowed, Is.True);
                Assert.That(explanation.DecidingLayer, Is.EqualTo(TenantRuleLayer.Tenant));
                Assert.That(explanation.DecidingRuleId, Is.EqualTo("readers-read"));
            });
        }
    }

    [Test]
    public async Task A_tenant_wide_allow_grants_every_tenant_tree_but_not_the_tenants_app_trees()
    {
        var tenant = await SeedTenantAsync("wide");
        await AddTenantMemberAsync(tenant, Member);
        var orders = $"t/{tenant.Value}/orders";
        var invoices = $"t/{tenant.Value}/invoices";
        var appTree = $"t/{tenant.Value}/a/shop/items";

        Assert.That(await ReadAllowedAsync(tenant, Member, orders), Is.False);

        using (As(Admin))
        {
            await Facade.PutRuleAsync(tenant.Value, new TenantRuleDraft
            {
                RuleId = "everything",
                SubjectId = Member,
                ScopeKind = TenantRuleScopeKind.TenantWide,
                Operations = LatticeOperation.Read,
                Effect = LatticeEffect.Allow,
            });
        }

        await TestPoll.UntilAsync(
            () => ReadAllowedAsync(tenant, Member, orders),
            "the tenant-wide allow reaches the tenant's tree",
            Deadline);

        var invoicesAllowed = await ReadAllowedAsync(tenant, Member, invoices);
        var appTreeAllowed = await ReadAllowedAsync(tenant, Member, appTree);
        Assert.Multiple(() =>
        {
            Assert.That(invoicesAllowed, Is.True, "and every other tenant tree");
            Assert.That(appTreeAllowed, Is.False, "but never an app-owned tree");
        });

        using (As(Admin))
        {
            var explanation = await Facade.ExplainAsync(tenant.Value, Member, "a/shop/items", "k1", LatticeOperation.Read);
            Assert.Multiple(() =>
            {
                Assert.That(explanation.Allowed, Is.False);
                Assert.That(explanation.DecidingLayer, Is.Null, "no rule reaches the app tree; the default effect decides");
            });
        }
    }

    [Test]
    public async Task An_operator_deny_on_the_tree_wins_over_a_tenant_allow_and_explain_names_the_platform_layer()
    {
        var tenant = await SeedTenantAsync("guarded");
        await AddTenantMemberAsync(tenant, Member);
        var tree = $"t/{tenant.Value}/orders";

        using (As(Admin))
        {
            await Facade.PutRuleAsync(tenant.Value, new TenantRuleDraft
            {
                RuleId = "member-read",
                SubjectId = Member,
                ScopeKind = TenantRuleScopeKind.Tree,
                TreeName = "orders",
                Operations = LatticeOperation.Read,
                Effect = LatticeEffect.Allow,
            });
        }

        await TestPoll.UntilAsync(() => ReadAllowedAsync(tenant, Member, tree), "the tenant allow takes effect", Deadline);

        var store = _fixture.SiloServices.GetRequiredService<ILatticeAuthorizationPolicyStore>();
        using (As(Operator))
        {
            await store.PutRuleAsync(new LatticeAuthorizationRule(
                "op-deny-orders",
                LatticeSubjectSelector.User(Member),
                LatticeScope.Tree(tree),
                LatticeOperation.Read,
                LatticeEffect.Deny));
        }

        await TestPoll.UntilAsync(
            async () => !await ReadAllowedAsync(tenant, Member, tree),
            "the operator deny is final over the tenant allow",
            Deadline);

        using (As(Admin))
        {
            var explanation = await Facade.ExplainAsync(tenant.Value, Member, "orders", "k1", LatticeOperation.Read);
            Assert.Multiple(() =>
            {
                Assert.That(explanation.Allowed, Is.False);
                Assert.That(explanation.DecidingLayer, Is.EqualTo(TenantRuleLayer.Platform));
                Assert.That(explanation.DecidingRuleId, Is.EqualTo("op-deny-orders"));
                Assert.That(explanation.DecidingRule!.Origin, Is.EqualTo(TenantRuleOrigin.PlatformTree));
                Assert.That(explanation.MatchedRules.Select(r => r.RuleId), Is.EqualTo(new[] { "op-deny-orders", "member-read" }));
            });

            var listed = await Facade.ListRulesAsync(tenant.Value, new TenantAccessPageRequest());
            Assert.That(
                listed.Entries.Select(r => (r.RuleId, r.Editable)),
                Is.EquivalentTo(new[] { ("op-deny-orders", false), ("member-read", true) }));
        }
    }

    [Test]
    public async Task Rules_on_another_tenants_tree_a_system_tree_an_app_tree_or_with_platform_operations_are_refused()
    {
        var tenant = await SeedTenantAsync("confined");
        await SeedTenantAsync("neighbour", admin: "olivia");

        using (As(Admin))
        {
            Assert.Multiple(() =>
            {
                Assert.That(
                    () => Facade.PutRuleAsync("neighbour", Draft("on-b")),
                    Throws.TypeOf<LatticeAuthorizationDeniedException>(),
                    "an admin of one tenant cannot author rules on another tenant's trees");
                AssertConfined(Draft("on-sys") with { TreeName = "sys-audit" }, TenantAccessConfinementRule.RuleTree);
                AssertConfined(Draft("on-app") with { TreeName = "a/shop/items" }, TenantAccessConfinementRule.RuleTree);
                AssertConfined(Draft("telemetry") with { Operations = LatticeOperation.Telemetry }, TenantAccessConfinementRule.RuleOperations);
                AssertConfined(Draft("lifecycle") with { Operations = LatticeOperation.TreeLifecycle }, TenantAccessConfinementRule.RuleOperations);
                AssertConfined(
                    Draft("foreign-group") with { SubjectId = "t/neighbour/staff", SubjectKind = TenantSubjectKind.ClusterGroup },
                    TenantAccessConfinementRule.ForeignTenantGroup);
            });

            var listed = await Facade.ListRulesAsync(tenant.Value, new TenantAccessPageRequest());
            Assert.That(listed.Entries, Is.Empty, "nothing refused was written");
        }

        // The policy store's own guard is the backstop: a tenant-tier id is refused off
        // system origin, so the facade is the only way to write one.
        var store = _fixture.SiloServices.GetRequiredService<ILatticeAuthorizationPolicyStore>();
        using (As(Operator))
        {
            Assert.That(
                () => store.PutRuleAsync(new LatticeAuthorizationRule(
                    $"tenant:{tenant.Value}:direct",
                    LatticeSubjectSelector.User(Member),
                    LatticeScope.Tree($"t/{tenant.Value}/orders"),
                    LatticeOperation.Read,
                    LatticeEffect.Allow)),
                Throws.TypeOf<LatticeTenantOwnedRuleException>());
        }

        void AssertConfined(TenantRuleDraft draft, TenantAccessConfinementRule expected)
        {
            var ex = Assert.ThrowsAsync<TenantAccessConfinementException>(() => Facade.PutRuleAsync(tenant.Value, draft));
            Assert.That(ex!.Rule, Is.EqualTo(expected), draft.RuleId);
        }
    }

    [Test]
    public async Task The_MaxTenantRules_cap_set_through_the_quota_facade_refuses_a_rule_past_it()
    {
        var tenant = await SeedTenantAsync("capped");

        using (As(Operator))
        {
            var updated = await _fixture.SiloServices.GetRequiredService<ILatticeTenantAdmin>()
                .SetTenantQuotasAsync(tenant.Value, new TenantQuotasDescriptor { MaxKeys = 1000, MaxTenantRules = 2 });
            Assert.That(updated.Quotas.MaxTenantRules, Is.EqualTo(2), "the cap round-trips through the quota facade");
        }

        using (As(Admin))
        {
            await Facade.PutRuleAsync(tenant.Value, Draft("one"));
            await Facade.PutRuleAsync(tenant.Value, Draft("two"));

            var ex = Assert.ThrowsAsync<LatticeQuotaExceededException>(() => Facade.PutRuleAsync(tenant.Value, Draft("three")));
            Assert.That(ex!.Dimension, Is.EqualTo(TenantAccessCaps.TenantRulesDimension));

            var replaced = await Facade.PutRuleAsync(tenant.Value, Draft("one") with { Effect = LatticeEffect.Deny });
            Assert.That(replaced.Effect, Is.EqualTo(LatticeEffect.Deny), "replacing an existing rule at the cap is admitted");

            var posture = await Facade.GetPostureAsync(tenant.Value);
            Assert.Multiple(() =>
            {
                Assert.That(posture.TenantRules.Usage, Is.EqualTo(2));
                Assert.That(posture.TenantRules.Limit, Is.EqualTo(2));
            });
        }
    }

    [Test]
    public async Task Posture_answers_with_the_feature_on_and_off_and_the_other_operations_refuse_while_off()
    {
        var tenant = await SeedTenantAsync("posture");
        var group = $"t/{tenant.Value}/staff";
        await AddGroupMemberAsync(group, Member);

        using (As(Admin))
        {
            var on = await Facade.GetPostureAsync(tenant.Value);
            Assert.Multiple(() =>
            {
                Assert.That(on.Enabled, Is.True);
                Assert.That(on.CallerIsTenantAdmin, Is.True);
                Assert.That(on.CallerIsPlatformOperator, Is.False);
                Assert.That(on.Groups.Usage, Is.EqualTo(1), "the tenant's group is counted from the membership directory");
                Assert.That(on.Groups.Limit, Is.EqualTo(TenantQuotas.DefaultMaxGroups));
                Assert.That(on.MembershipEdges.Usage, Is.EqualTo(1));
                Assert.That(on.TenantRules.Limit, Is.EqualTo(TenantQuotas.DefaultMaxTenantRules));
            });
        }

        PolicyClusterFixture.Switch.Set(false);
        try
        {
            using (As(Admin))
            {
                var off = await Facade.GetPostureAsync(tenant.Value);
                Assert.That(off.Enabled, Is.False, "the posture is how a caller learns the feature is off");
                Assert.That(
                    () => Facade.PutRuleAsync(tenant.Value, Draft("while-off")),
                    Throws.TypeOf<TenantAccessAdministrationDisabledException>());
            }

            using (As("mallory"))
            {
                Assert.That(
                    () => Facade.GetPostureAsync(tenant.Value),
                    Throws.TypeOf<LatticeAuthorizationDeniedException>(),
                    "an unauthorized caller is denied even while the feature is off");
            }
        }
        finally
        {
            PolicyClusterFixture.Switch.Set(true);
        }
    }

    private static TenantRuleDraft Draft(string ruleId) => new()
    {
        RuleId = ruleId,
        SubjectId = Member,
        ScopeKind = TenantRuleScopeKind.Tree,
        TreeName = "orders",
        Operations = LatticeOperation.Read,
        Effect = LatticeEffect.Allow,
    };

    private static IDisposable As(string subject) => LatticeCredentialContext.Use(subject, scheme: Scheme);

    private async Task<TenantId> SeedTenantAsync(string tenantId, string admin = Admin)
    {
        var tenant = TenantId.Parse(tenantId);
        var record = TenantRecord.Create(
            tenant,
            TenantStatus.Active,
            new TenantQuotas { MaxKeys = 1000 },
            TenantPlacement.Shared,
            new HybridLogicalClock { WallClockTicks = 1 },
            "seed");
        record.AddAdminSubject(admin, new HybridLogicalClock { WallClockTicks = 2 }, "seed");
        using (LatticeSystemOrigin.Enter())
        {
            await _fixture.Registry.PutAsync(record);
        }

        return tenant;
    }

    private async Task AddTenantMemberAsync(TenantId tenant, string subjectOrGroupId)
    {
        using (LatticeSystemOrigin.Enter())
        {
            var record = (await _fixture.Registry.GetAsync(tenant))!;
            record.AddMemberSubject(subjectOrGroupId, new HybridLogicalClock { WallClockTicks = 10 }, "seed");
            await _fixture.Registry.PutAsync(record);
        }
    }

    private async Task AddGroupMemberAsync(string groupId, string memberId)
    {
        var directory = _fixture.SiloServices.GetRequiredService<ILatticeMembershipDirectory>();
        using (LatticeSystemOrigin.Enter())
        {
            await directory.UpsertGroupAsync(new MembershipGroup(groupId));
            await directory.AddMemberAsync(groupId, memberId);
        }
    }

    /// <summary>
    /// The real gate's verdict on a point read of <paramref name="treeId"/> by
    /// <paramref name="subjectId"/>, with the subject's groups resolved from the
    /// directory and the tenant asserted as the active tenant.
    /// </summary>
    private async Task<bool> ReadAllowedAsync(TenantId tenant, string subjectId, string treeId)
    {
        IReadOnlyCollection<string> groups;
        using (LatticeSystemOrigin.Enter())
        {
            groups = await _fixture.SiloServices.GetRequiredService<ILatticeMembershipDirectory>().GroupsOfAsync(subjectId);
        }

        var gate = _fixture.SiloServices.GetRequiredService<ILatticeAccessGate>();
        var request = new LatticeAccessRequest(treeId, LatticeOperation.Read, new LatticeSubject(subjectId, groups), "k1");
        using (LatticeActiveTenantContext.With(tenant))
        {
            var decision = await gate.AuthorizeAsync(request);
            return decision.Allowed && decision.KeyFilter is null;
        }
    }

    /// <summary>
    /// A single-silo cluster composing membership, default-deny auth with one
    /// bootstrap operator, tenancy with delegated tenant access administration behind
    /// a runtime switch, the tenant-admin control API, and a test credential
    /// authenticator that resolves a credential's token as its subject.
    /// </summary>
    private sealed class PolicyClusterFixture
    {
        public static TenancyFlagSwitch Switch { get; } = new();

        public TestCluster Cluster { get; private set; } = null!;

        public IServiceProvider SiloServices =>
            Cluster.Silos.OfType<InProcessSiloHandle>().First().SiloHost.Services;

        public ITenantRegistry Registry => SiloServices.GetRequiredService<ITenantRegistry>();

        public async Task InitializeAsync()
        {
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
                siloBuilder.Services.AddSingleton<IConfigureOptions<LatticeTenancyOptions>>(Switch);
                siloBuilder.Services.AddSingleton<IOptionsChangeTokenSource<LatticeTenancyOptions>>(Switch);
                siloBuilder.AddLatticeTenantAdminApi();
            }
        }
    }

    /// <summary>
    /// Flips <see cref="LatticeTenancyOptions.DelegatedAccessAdministrationEnabled"/> at
    /// runtime through the options change-token path, which is how the tenancy add-on's
    /// live flag observes an operator reconfiguring it.
    /// </summary>
    private sealed class TenancyFlagSwitch : IConfigureOptions<LatticeTenancyOptions>, IOptionsChangeTokenSource<LatticeTenancyOptions>
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

    /// <summary>Resolves a test credential's token directly as the subject id.</summary>
    private sealed class TokenAsSubjectAuthenticator : ILatticeCredentialAuthenticator
    {
        public bool CanHandle(in LatticeCredential credential) =>
            string.Equals(credential.Scheme, Scheme, StringComparison.Ordinal);

        public ValueTask<LatticePrincipal?> AuthenticateAsync(
            LatticeCredential credential, CancellationToken cancellationToken = default) =>
            new(new LatticePrincipal(credential.Token, "https://issuer.tenant-policy.test/"));
    }
}
