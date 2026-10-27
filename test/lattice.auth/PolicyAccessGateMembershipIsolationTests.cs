using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Auth.Tests;

/// <summary>
/// Control-plane read isolation of the membership store (<c>sys-membership-*</c>). The
/// store holds every group and membership edge in the cluster, every tenant's tenant
/// groups included, so - exactly as for the tenant and app registries - neither a
/// data-plane default effect of allow nor a cluster-wide all-trees (<c>Tree:*</c>)
/// wildcard may expose it, while an explicit rule an operator scoped at the tree and a
/// bootstrap administrator still may. Direct in-process tests of the real
/// <see cref="PolicyAccessGate"/> over an in-memory policy store.
/// </summary>
[TestFixture]
[Category("Unit")]
public sealed class PolicyAccessGateMembershipIsolationTests
{
    private const string GroupsTree = "sys-membership-groups";
    private const string EdgesTree = "sys-membership-edges";
    private const string GroupsHistoryTree = "sys-membership-groups-history";

    private static LatticeAuthorizationRule Rule(LatticeScope scope, LatticeOperation operations, string subjectId) =>
        new("r", LatticeSubjectSelector.User(subjectId), scope, operations, LatticeEffect.Allow);

    private static LatticeAuthOptions WildcardOptions() => new()
    {
        DefaultEffect = LatticeEffect.Deny,
        AllTreesGrantsEnabled = true,
    };

    [TestCase(GroupsTree, LatticeOperation.Read)]
    [TestCase(EdgesTree, LatticeOperation.RangeRead)]
    [TestCase(GroupsHistoryTree, LatticeOperation.Read)]
    public async Task AuthorizeAsync_unmatched_membership_read_is_denied_even_under_default_allow(string treeId, LatticeOperation operation)
    {
        var harness = await AuthGateHarness.CreateAsync(new LatticeAuthOptions { DefaultEffect = LatticeEffect.Allow });
        var request = new LatticeAccessRequest(treeId, operation, new LatticeSubject("mallory"), "t/acme/admins");

        var decision = await harness.Gate.AuthorizeAsync(request);

        Assert.Multiple(() =>
        {
            Assert.That(decision.Allowed, Is.False, "the whole sys-membership-* prefix is control-plane isolated");
            Assert.That(decision.Reason, Does.Contain("Control-plane isolation"));
        });
    }

    [Test]
    public async Task AuthorizeAsync_anonymous_membership_scan_is_denied_under_default_allow()
    {
        var harness = await AuthGateHarness.CreateAsync(new LatticeAuthOptions { DefaultEffect = LatticeEffect.Allow });
        var request = new LatticeAccessRequest(EdgesTree, LatticeOperation.RangeRead, LatticeSubject.Anonymous);

        var decision = await harness.Gate.AuthorizeAsync(request);

        Assert.That(decision.Allowed, Is.False, "an anonymous caller must not dump every tenant's group membership");
    }

    [TestCase(LatticeOperation.Read)]
    [TestCase(LatticeOperation.RangeRead)]
    public async Task AuthorizeAsync_membership_denied_despite_cluster_wide_wildcard_grant(LatticeOperation operation)
    {
        var wildcard = Rule(LatticeScope.ClusterWide(), LatticeAuthOperations.All, "mallory");
        var harness = await AuthGateHarness.CreateAsync(WildcardOptions(), wildcard);
        var request = new LatticeAccessRequest(EdgesTree, operation, new LatticeSubject("mallory"), "t/acme/admins");

        var decision = await harness.Gate.AuthorizeAsync(request);

        Assert.That(decision.Allowed, Is.False, "a Tree:* wildcard never reaches the membership store");
    }

    [Test]
    public async Task AuthorizeAsync_the_same_wildcard_still_reaches_an_ordinary_tree()
    {
        var wildcard = Rule(LatticeScope.ClusterWide(), LatticeOperation.Read, "mallory");
        var harness = await AuthGateHarness.CreateAsync(WildcardOptions(), wildcard);
        var request = new LatticeAccessRequest("orders", LatticeOperation.Read, new LatticeSubject("mallory"), "k");

        var decision = await harness.Gate.AuthorizeAsync(request);

        Assert.That(decision.Allowed, Is.True, "over-exclusion guard: only the control plane is withheld");
    }

    [Test]
    public async Task AuthorizeAsync_membership_honours_an_explicit_matched_allow()
    {
        var explicitAllow = Rule(LatticeScope.Tree(GroupsTree), LatticeOperation.Read, "auditor");
        var harness = await AuthGateHarness.CreateAsync(new LatticeAuthOptions { DefaultEffect = LatticeEffect.Deny }, explicitAllow);
        var request = new LatticeAccessRequest(GroupsTree, LatticeOperation.Read, new LatticeSubject("auditor"), "t/acme/admins");

        var decision = await harness.Gate.AuthorizeAsync(request);

        Assert.That(decision.Allowed, Is.True, "an operator-scoped rule on the membership tree is the deliberate escape hatch");
    }

    [Test]
    public async Task AuthorizeAsync_bootstrap_administrator_may_read_the_membership_store()
    {
        var options = new LatticeAuthOptions
        {
            DefaultEffect = LatticeEffect.Deny,
            BootstrapAdministrators = new HashSet<string>(StringComparer.Ordinal) { "root" },
        };
        var harness = await AuthGateHarness.CreateAsync(options);
        var request = new LatticeAccessRequest(EdgesTree, LatticeOperation.RangeRead, new LatticeSubject("root"));

        var decision = await harness.Gate.AuthorizeAsync(request);

        Assert.That(decision.Allowed, Is.True);
    }

    [Test]
    public async Task HasAnyGrantAsync_membership_is_hidden_under_default_allow()
    {
        var harness = await AuthGateHarness.CreateAsync(new LatticeAuthOptions { DefaultEffect = LatticeEffect.Allow });

        var granted = await harness.Gate.HasAnyGrantAsync(GroupsTree, new LatticeSubject("mallory"), LatticeOperation.Read);

        Assert.That(granted, Is.False);
    }

    [Test]
    public async Task HasAnyGrantAsync_membership_is_not_surfaced_by_a_cluster_wide_wildcard_grant()
    {
        var wildcard = Rule(LatticeScope.ClusterWide(), LatticeOperation.Read, "mallory");
        var harness = await AuthGateHarness.CreateAsync(WildcardOptions(), wildcard);

        var granted = await harness.Gate.HasAnyGrantAsync(EdgesTree, new LatticeSubject("mallory"), LatticeOperation.Read);

        Assert.That(granted, Is.False);
    }

    [Test]
    public async Task HasAnyGrantAsync_membership_matched_allow_is_true()
    {
        var explicitAllow = Rule(LatticeScope.Tree(GroupsTree), LatticeOperation.Read, "auditor");
        var harness = await AuthGateHarness.CreateAsync(new LatticeAuthOptions { DefaultEffect = LatticeEffect.Deny }, explicitAllow);

        var granted = await harness.Gate.HasAnyGrantAsync(GroupsTree, new LatticeSubject("auditor"), LatticeOperation.Read);

        Assert.That(granted, Is.True);
    }
}
