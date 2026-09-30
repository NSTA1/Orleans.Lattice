using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.Auth;
using Orleans.Lattice.Membership;

namespace Orleans.Lattice.Api.Auth.Tests;

/// <summary>
/// Unit coverage for <see cref="AuthPageRequest.ActiveTenantOnly"/> on
/// <see cref="ILatticeAuthAdmin.ListRulesAsync"/>: a rule listing narrowed to the
/// caller's resolved active tenant lists only the rules governing that tenant's
/// own trees, never another tenant's, a cluster-wide <c>Tree:*</c> rule or a
/// platform tree's; the default tenant's listing excludes every <c>t/</c> rule;
/// paging stays full; a denied assertion fails closed; and the unnarrowed
/// listing is unchanged.
/// </summary>
[TestFixture]
public sealed class LatticeAuthAdminTenantListingTests
{
    private static LatticeAuthorizationRule Rule(string ruleId, LatticeScope scope) =>
        new(ruleId, LatticeSubjectSelector.User("alice"), scope, LatticeOperation.Read, LatticeEffect.Allow);

    // In (tree id, rule id) catalogue order, as the store yields them.
    private static readonly LatticeAuthorizationRule[] Catalogue =
    [
        Rule("wide", LatticeScope.ClusterWide()),
        Rule("platform", LatticeScope.Tree("_lattice_auth_policy")),
        Rule("d1", LatticeScope.Tree("orders")),
        Rule("d2", LatticeScope.Tree("payments")),
        Rule("system", LatticeScope.Tree("sys-tenant-registry")),
        Rule("a1", LatticeScope.Tree("t/acme/orders")),
        Rule("a2", LatticeScope.Tree("t/acme/payments")),
        Rule("a3", LatticeScope.Prefix("t/acme/stock", "p")),
        Rule("g1", LatticeScope.Tree("t/globex/orders")),
    ];

    private static async IAsyncEnumerable<LatticeAuthorizationRule> Stream(LatticeAuthorizationRule[] rules)
    {
        foreach (var rule in rules)
        {
            yield return rule;
            await Task.Yield();
        }
    }

    private static ITenantContextResolver Resolving(TenantId tenant)
    {
        var resolver = Substitute.For<ITenantContextResolver>();
        resolver.TryResolveCurrent(out Arg.Any<TenantId>()).Returns(call =>
        {
            call[0] = tenant;
            return true;
        });
        return resolver;
    }

    private static LatticeAuthAdmin CreateAdmin(ITenantContextResolver? tenants)
    {
        var store = Substitute.For<ILatticeAuthorizationPolicyStore>();
        store.ListRulesAsync(Arg.Any<CancellationToken>()).Returns(_ => Stream(Catalogue));

        var authMonitor = Substitute.For<IOptionsMonitor<LatticeAuthOptions>>();
        authMonitor.CurrentValue.Returns(new LatticeAuthOptions());
        var membershipMonitor = Substitute.For<IOptionsMonitor<LatticeMembershipOptions>>();
        membershipMonitor.CurrentValue.Returns(new LatticeMembershipOptions());
        var identityMonitor = Substitute.For<IOptionsMonitor<LatticeIdentityDirectoryOptions>>();
        identityMonitor.CurrentValue.Returns(new LatticeIdentityDirectoryOptions());

        return new LatticeAuthAdmin(
            store,
            Substitute.For<ILatticeMembershipDirectory>(),
            new AllowAllAccessGate(),
            new AnonymousMembershipContext(),
            Substitute.For<ILatticeIdentityDirectory>(),
            [new AnonymousCredentialAuthenticator()],
            Options.Create(new LatticeApiAuthOptions()),
            authMonitor,
            membershipMonitor,
            identityMonitor,
            tenants);
    }

    private static async Task<List<string>> ListAllAsync(ILatticeAuthAdmin admin, AuthPageRequest request)
    {
        var ids = new List<string>();
        string? token = null;
        do
        {
            var page = await admin.ListRulesAsync(request with { PageToken = token });
            ids.AddRange(page.Entries.Select(rule => rule.RuleId));
            token = page.NextPageToken;
        }
        while (token is not null);

        return ids;
    }

    [Test]
    public async Task ListRulesAsync_narrowed_to_a_tenant_lists_only_its_own_rules()
    {
        var admin = CreateAdmin(Resolving(TenantId.Parse("acme")));

        var page = await admin.ListRulesAsync(new AuthPageRequest { ActiveTenantOnly = true });

        Assert.Multiple(() =>
        {
            Assert.That(page.Entries.Select(rule => rule.RuleId), Is.EqualTo(new[] { "a1", "a2", "a3" }));
            Assert.That(page.Tenant, Is.EqualTo("acme"));
            Assert.That(page.NextPageToken, Is.Null);
        });
    }

    [Test]
    public async Task ListRulesAsync_narrowed_to_the_default_tenant_lists_no_other_tenants_rule()
    {
        var admin = CreateAdmin(Resolving(TenantId.Default));

        var page = await admin.ListRulesAsync(new AuthPageRequest { ActiveTenantOnly = true });

        Assert.Multiple(() =>
        {
            Assert.That(page.Entries.Select(rule => rule.RuleId), Is.EqualTo(new[] { "d1", "d2" }));
            Assert.That(page.Tenant, Is.EqualTo(TenantId.DefaultId));
        });
    }

    [Test]
    public async Task ListRulesAsync_with_no_tenant_resolver_narrows_to_the_default_tenant()
    {
        var admin = CreateAdmin(tenants: null);

        var page = await admin.ListRulesAsync(new AuthPageRequest { ActiveTenantOnly = true });

        Assert.That(page.Entries.Select(rule => rule.RuleId), Is.EqualTo(new[] { "d1", "d2" }));
    }

    [Test]
    public async Task ListRulesAsync_narrowed_pages_are_full_and_page_through_every_owned_rule()
    {
        var admin = CreateAdmin(Resolving(TenantId.Parse("acme")));

        var first = await admin.ListRulesAsync(new AuthPageRequest { PageSize = 2, ActiveTenantOnly = true });
        var all = await ListAllAsync(admin, new AuthPageRequest { PageSize = 1, ActiveTenantOnly = true });

        Assert.Multiple(() =>
        {
            Assert.That(first.Entries.Select(rule => rule.RuleId), Is.EqualTo(new[] { "a1", "a2" }));
            Assert.That(first.NextPageToken, Is.Not.Null);
            Assert.That(all, Is.EqualTo(new[] { "a1", "a2", "a3" }));
        });
    }

    [Test]
    public async Task ListRulesAsync_unnarrowed_lists_the_whole_catalogue_with_no_tenant()
    {
        var admin = CreateAdmin(Resolving(TenantId.Parse("acme")));

        var page = await admin.ListRulesAsync(new AuthPageRequest());

        Assert.Multiple(() =>
        {
            Assert.That(page.Entries, Has.Count.EqualTo(Catalogue.Length));
            Assert.That(page.Tenant, Is.Null);
        });
    }

    [Test]
    public void ListRulesAsync_narrowed_under_a_denied_assertion_fails_closed()
    {
        var admin = CreateAdmin(Resolving(default));

        Assert.ThrowsAsync<LatticeTenantAccessDeniedException>(
            () => admin.ListRulesAsync(new AuthPageRequest { ActiveTenantOnly = true }));
    }

    [Test]
    public async Task ListRulesAsync_narrowed_resolves_the_tenant_asynchronously_when_it_is_not_warm()
    {
        var resolver = Substitute.For<ITenantContextResolver>();
        resolver.TryResolveCurrent(out Arg.Any<TenantId>()).Returns(false);
        resolver.ResolveCurrentAsync(Arg.Any<CancellationToken>()).Returns(new ValueTask<TenantId>(TenantId.Parse("globex")));
        var admin = CreateAdmin(resolver);

        var page = await admin.ListRulesAsync(new AuthPageRequest { ActiveTenantOnly = true });

        Assert.That(page.Entries.Select(rule => rule.RuleId), Is.EqualTo(new[] { "g1" }));
    }

    [TestCase("t/acme/orders", "acme", true)]
    [TestCase("t/acme/orders", "globex", false)]
    [TestCase("orders", "default", true)]
    [TestCase("orders", "acme", false)]
    [TestCase("t/acme/orders", "default", false)]
    [TestCase("*", "default", false)]
    [TestCase("_lattice_auth_policy", "default", false)]
    [TestCase("sys-tenant-registry", "default", false)]
    public void IsOwnedBy_decides_by_the_tenancy_ownership_grammar(string treeId, string tenant, bool owned)
    {
        var scope = treeId == LatticeScope.ClusterWideTreeId ? LatticeScope.ClusterWide() : LatticeScope.Tree(treeId);
        Assert.That(LatticeAuthAdmin.IsOwnedBy(Rule("r", scope), TenantId.Parse(tenant)), Is.EqualTo(owned));
    }

    private sealed class AllowAllAccessGate : ILatticeAccessGate
    {
        public ValueTask<LatticeAccessDecision> AuthorizeAsync(
            in LatticeAccessRequest request,
            CancellationToken cancellationToken = default) =>
            new(LatticeAccessDecision.Allow());
    }

    private sealed class AnonymousMembershipContext : ILatticeMembershipContext
    {
        public ValueTask<LatticeSubject> ResolveCurrentAsync(CancellationToken cancellationToken = default) =>
            new(LatticeSubject.Anonymous);

        public bool TryResolveCurrent(out LatticeSubject subject)
        {
            subject = LatticeSubject.Anonymous;
            return true;
        }
    }
}