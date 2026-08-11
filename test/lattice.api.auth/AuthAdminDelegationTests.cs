using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Api.Auth.Tests;

/// <summary>
/// Integration coverage for access-administration delegation through the control
/// facade (issue #1342). With
/// <see cref="LatticeAuthOptions.AccessAdministrationDelegationEnabled"/> set, a
/// bootstrap administrator may author a whole-tree <c>Admin</c> grant on the
/// reserved policy tree, and the delegated (non-bootstrap) subject then passes the
/// same facade admin authorization the bootstrap administrator does - it can author
/// and list rules, and Explain reflects the resolved allow. With the option off the
/// same authoring attempt fails closed at the store.
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class AuthAdminDelegationTests
{
    private const string PolicyTree = "sys-auth-policy";
    private const string DelegatedAdmin = "delegate-admin";

    private AuthAdminDelegationClusterFixture _fixture = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new AuthAdminDelegationClusterFixture();
        await _fixture.InitializeAsync();

        // The bootstrap administrator delegates access administration to a
        // non-bootstrap subject by authoring the one permitted delegation shape.
        using (AuthAdminDelegationClusterFixture.AsSubject(AuthAdminDelegationClusterFixture.BootstrapAdmin))
        {
            await _fixture.Admin.PutRuleAsync(new LatticeAuthorizationRule(
                "delegate-to-alice",
                LatticeSubjectSelector.User(DelegatedAdmin),
                LatticeScope.Tree(PolicyTree),
                LatticeOperation.Admin,
                LatticeEffect.Allow));
        }

        // Make the compiled snapshot observe the grant so enforcement honours it.
        await _fixture.RebuildPolicyAsync();
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown() => await _fixture.DisposeAsync();

    [Test]
    public void Delegated_admin_can_author_a_rule_through_the_facade()
    {
        using (AuthAdminDelegationClusterFixture.AsSubject(DelegatedAdmin))
        {
            Assert.That(
                async () => await _fixture.Admin.PutRuleAsync(new LatticeAuthorizationRule(
                    "delegated-write",
                    LatticeSubjectSelector.User("someone"),
                    LatticeScope.Tree("orders"),
                    LatticeOperation.Read,
                    LatticeEffect.Allow)),
                Throws.Nothing,
                "a delegated access administrator passes facade admin authorization");
        }
    }

    [Test]
    public void Delegated_admin_can_list_rules_through_the_facade()
    {
        using (AuthAdminDelegationClusterFixture.AsSubject(DelegatedAdmin))
        {
            Assert.That(
                async () => await _fixture.Admin.ListRulesAsync(new AuthPageRequest()),
                Throws.Nothing);
        }
    }

    [Test]
    public async Task Explain_reflects_the_delegated_admin_being_allowed_on_the_policy_tree()
    {
        using (AuthAdminDelegationClusterFixture.AsSubject(AuthAdminDelegationClusterFixture.BootstrapAdmin))
        {
            var explanation = await _fixture.Admin.ExplainAsync(
                DelegatedAdmin,
                LatticeOperation.Admin,
                LatticeScope.Tree(PolicyTree));

            Assert.That(explanation.Allowed, Is.True, "the resolved verdict must reflect the honoured delegation grant");
        }
    }

    [Test]
    public async Task Explain_surfaces_the_enabled_delegation_posture()
    {
        using (AuthAdminDelegationClusterFixture.AsSubject(AuthAdminDelegationClusterFixture.BootstrapAdmin))
        {
            var explanation = await _fixture.Admin.ExplainAsync(
                DelegatedAdmin,
                LatticeOperation.Admin,
                LatticeScope.Tree(PolicyTree));

            Assert.That(explanation.Posture.AccessAdministrationDelegationEnabled, Is.True,
                "explain must report delegation as enabled on a cluster where the flag is set");
        }
    }

    [Test]
    public async Task Access_model_surfaces_the_enabled_delegation_posture()
    {
        using (AuthAdminDelegationClusterFixture.AsSubject(AuthAdminDelegationClusterFixture.BootstrapAdmin))
        {
            var model = await _fixture.Admin.GetAccessModelAsync();

            Assert.That(model.AccessAdministrationDelegationEnabled, Is.True,
                "the access model must report delegation as enabled so the Explorer can badge it");
        }
    }

    [Test]
    public void A_non_delegated_subject_is_denied_admin_through_the_facade()
    {
        using (AuthAdminDelegationClusterFixture.AsSubject("outsider"))
        {
            Assert.That(
                async () => await _fixture.Admin.ListRulesAsync(new AuthPageRequest()),
                Throws.TypeOf<LatticeAuthorizationDeniedException>(),
                "reserved-namespace isolation still denies a subject without the delegation grant");
        }
    }
}
