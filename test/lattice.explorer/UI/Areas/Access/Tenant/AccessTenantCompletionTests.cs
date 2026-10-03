using Orleans.Lattice.Api.TenantAdmin;
using Orleans.Lattice.Auth;
using Orleans.Lattice.Explorer.UI.Areas.Access;
using Orleans.Lattice.Explorer.UI.Areas.Access.Tenant;
using Orleans.Lattice.Explorer.UI.Design.Components;
using Orleans.Lattice.Explorer.UI.Navigation;
using Orleans.Lattice.Explorer.UI.Navigation.Address;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Access.Tenant;

/// <summary>
/// Issue #4158: the tenant routes complete on the address line, and the picker's
/// sources never offer another tenant's group. From a tenant-rooted address whose
/// access administration is delegated to the caller, the tenant's own groups,
/// tenant-tier rules and sections complete, rooted at the tenant; otherwise the
/// completions are exactly as before.
/// </summary>
[TestFixture]
public sealed class AccessTenantCompletionTests
{
    private FakeAuthAdmin _admin = null!;
    private FakeTenantAccessFacades _facades = null!;
    private AccessCompletionSource _source = null!;
    private TenantAccessCatalog _tenantAccess = null!;

    [SetUp]
    public async Task SetUp()
    {
        _admin = new FakeAuthAdmin()
            .WithGroup("ops", "Operators")
            .WithRule(AccessTestContext.Rule("acme-orders", tree: "t/acme/orders"));
        _facades = new FakeTenantAccessFacades()
            .WithGroup("acme", "ops", "Operators")
            .WithGroup("acme", "eng")
            .WithGroup("globex", "ops-globex");
        _tenantAccess = new TenantAccessCatalog(_facades);
        _source = new AccessCompletionSource(new AccessCatalog(_admin), _tenantAccess);

        var enabled = _facades.Gate.Enabled;
        _facades.Gate.Enabled = true;
        await _facades.PolicyFake.PutRuleAsync("acme", new TenantRuleDraft
        {
            RuleId = "ops-read",
            SubjectId = "ops",
            SubjectKind = TenantSubjectKind.TenantGroup,
            ScopeKind = TenantRuleScopeKind.TenantWide,
            Operations = LatticeOperation.Read,
            Effect = LatticeEffect.Allow,
        });
        _facades.PolicyFake.SeedPlatformRule("acme", new TenantRuleView
        {
            RuleId = "ops-platform",
            Origin = TenantRuleOrigin.PlatformTree,
            SubjectId = "ops",
            TreeName = "orders",
            Operations = LatticeOperation.Read,
            Effect = LatticeEffect.Deny,
        });
        _facades.Gate.Enabled = enabled;
        _facades.Gate.Calls.Clear();
    }

    [Test]
    public async Task A_delegated_tenant_completes_its_own_groups_and_rules_rooted_at_it()
    {
        _facades.AsTenantAdmin();

        var completions = await CompleteAsync("ops", AddressQueryMode.Search);

        Assert.Multiple(() =>
        {
            Assert.That(completions.Select(completion => completion.Label), Is.EqualTo(new[] { "group:ops", "rule:ops-read" }));
            Assert.That(completions.Select(completion => completion.Target.Format()),
                Is.EqualTo(new[] { "/t/acme/access/groups/ops", "/t/acme/access/rules/ops-read" }));
            Assert.That(completions[0].Detail, Is.EqualTo("Tenant - Operators"));
            Assert.That(_admin.Calls, Is.Empty, "the cluster catalogue is not read for a delegated tenant");
        });
    }

    [Test]
    public async Task A_prefix_narrows_the_tenant_completions_to_one_kind()
    {
        _facades.AsTenantAdmin();

        var groups = await CompleteAsync("group:", AddressQueryMode.Search);
        var rules = await CompleteAsync("rule:", AddressQueryMode.Search);

        Assert.Multiple(() =>
        {
            Assert.That(groups.Select(completion => completion.Label), Is.EqualTo(new[] { "group:eng", "group:ops" }));
            Assert.That(rules.Select(completion => completion.Label), Is.EqualTo(new[] { "rule:ops-read" }), "a platform rule has no tenant rule page");
        });
    }

    [Test]
    public async Task A_raw_tenant_access_address_completes_its_sections_groups_and_rules()
    {
        _facades.AsTenantAdmin();

        var sections = await CompleteAsync("/t/acme/access/", AddressQueryMode.Address);
        var members = await CompleteAsync("/t/acme/access/m", AddressQueryMode.Address);
        var groups = await CompleteAsync("/t/acme/access/groups/en", AddressQueryMode.Address);
        var rules = await CompleteAsync("/t/acme/access/rules/", AddressQueryMode.Address);
        var elsewhere = await CompleteAsync("/t/globex/access/", AddressQueryMode.Address);

        Assert.Multiple(() =>
        {
            Assert.That(sections.Select(completion => completion.Detail), Is.EqualTo(new[] { "Groups", "Members", "Rules", "Explain" }));
            Assert.That(members.Select(completion => completion.Label), Is.EqualTo(new[] { "/t/acme/access/members" }));
            Assert.That(groups.Select(completion => completion.Label), Is.EqualTo(new[] { "group:eng" }));
            Assert.That(rules.Select(completion => completion.Label), Is.EqualTo(new[] { "rule:ops-read" }));
            Assert.That(elsewhere, Is.Empty, "another tenant's root completes nothing here");
        });
    }

    [Test]
    public async Task Another_tenants_group_never_completes()
    {
        _facades.AsTenantAdmin();

        var completions = await CompleteAsync("globex", AddressQueryMode.Search);

        Assert.That(completions, Is.Empty);
    }

    [Test]
    [TestCase(false)]
    [TestCase(true)]
    public async Task Without_delegation_the_tenant_completions_are_as_before(bool member)
    {
        if (member)
        {
            _facades.AsMember();
        }

        var completions = await CompleteAsync("o", AddressQueryMode.Search);
        var sections = await CompleteAsync("/t/acme/access/", AddressQueryMode.Address);

        Assert.Multiple(() =>
        {
            Assert.That(completions.Select(completion => completion.Label), Is.EqualTo(new[] { "rule:acme-orders" }));
            Assert.That(sections, Is.Empty);
        });
    }

    [Test]
    public async Task A_cluster_wide_address_never_asks_the_posture()
    {
        _facades.AsTenantAdmin();

        var completions = await _source.CompleteAsync(new AddressQuery("o", AddressQueryMode.Search, ExplorerAddress.Home), CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(completions.Select(completion => completion.Label), Does.Contain("group:ops"));
            Assert.That(_facades.Gate.Calls, Is.Empty);
        });
    }

    [Test]
    public async Task The_tenant_group_source_offers_only_the_named_tenants_groups()
    {
        _facades.AsTenantAdmin();
        var source = new TenantGroupSuggestionSource(_tenantAccess, "acme");

        var answer = await source.SuggestAsync("ops", 10, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(source.Tenant, Is.EqualTo("acme"));
            Assert.That(answer.Items.Select(item => (item.Value, item.Detail)), Is.EqualTo(new (string, string?)[] { ("ops", "Tenant - Operators") }));
        });
    }

    [Test]
    public async Task The_tenant_group_source_is_unavailable_when_the_groups_cannot_be_read()
    {
        _facades.AsTenantAdmin();
        _facades.ServesDirectory = false;

        var answer = await new TenantGroupSuggestionSource(_tenantAccess, "acme").SuggestAsync("o", 10, CancellationToken.None);

        Assert.That(answer.UnavailableReason, Is.EqualTo(TenantGroupSuggestionSource.UnavailableReason));
    }

    [Test]
    public async Task The_cluster_source_leaves_out_every_reserved_tenant_group_id()
    {
        var inner = new FixedSource(LtSuggestionSet.Of([new("t/acme/ops", "Acme"), new("eng", "Engineering"), new("t/x", null)], truncated: true));

        var answer = await new ClusterSubjectSuggestionSource(inner).SuggestAsync("e", 10, CancellationToken.None);
        var unavailable = await new ClusterSubjectSuggestionSource(new FixedSource(LtSuggestionSet.Unavailable("No directory."))).SuggestAsync("e", 10, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(answer.Items.Select(item => (item.Value, item.Detail)), Is.EqualTo(new (string, string?)[] { ("eng", "Cluster - Engineering") }));
            Assert.That(answer.Truncated, Is.True);
            Assert.That(unavailable.UnavailableReason, Is.EqualTo("No directory."));
            Assert.That(ClusterSubjectSuggestionSource.IsTenantGroupId("t/acme/ops"), Is.True);
            Assert.That(ClusterSubjectSuggestionSource.IsTenantGroupId("team/ops"), Is.False);
            Assert.That(ClusterSubjectSuggestionSource.IsTenantGroupId(null), Is.False);
        });
    }

    private async Task<IReadOnlyList<AddressCompletion>> CompleteAsync(string text, AddressQueryMode mode) =>
        await _source.CompleteAsync(new AddressQuery(text, mode, ExplorerAddress.ForArea("data").WithTenant("acme")), CancellationToken.None);

    private sealed class FixedSource(LtSuggestionSet answer) : ILtSuggestionSource
    {
        public ValueTask<LtSuggestionSet> SuggestAsync(string text, int limit, CancellationToken cancellationToken) => ValueTask.FromResult(answer);
    }
}
