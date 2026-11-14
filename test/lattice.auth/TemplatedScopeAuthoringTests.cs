using Microsoft.Extensions.Options;
using NSubstitute;

namespace Orleans.Lattice.Auth.Tests;

/// <summary>
/// Tests for authoring subject-relative (templated) scopes (issue #6061): the public
/// <see cref="LatticeScope.SubjectPlaceholder"/>, <see cref="LatticeScope.GroupPlaceholder(string)"/>
/// and <see cref="LatticeScope.IsTemplated"/> helpers, and the
/// <see cref="LatticeAuthorizationPolicyStore.PutRuleAsync"/> guard, which refuses an
/// unbounded placeholder, a template on a control-plane tree and a group placeholder
/// reaching another tenant's groups before anything is read or written.
/// </summary>
[TestFixture]
public sealed class TemplatedScopeAuthoringTests
{
    private static readonly LatticeSubjectSelector Members = LatticeSubjectSelector.Group("members");

    private IGrainFactory _grainFactory = null!;
    private ILattice _policy = null!;
    private LatticeAuthorizationPolicyStore _store = null!;

    [SetUp]
    public void SetUp()
    {
        _grainFactory = Substitute.For<IGrainFactory>();
        _policy = Substitute.For<ILattice>();
        _grainFactory.GetGrain<ILattice>(Arg.Any<string>(), Arg.Any<string?>()).Returns(_policy);
        var options = Substitute.For<IOptionsMonitor<LatticeAuthOptions>>();
        options.CurrentValue.Returns(new LatticeAuthOptions());
        var initializer = new AuthInitializer(_grainFactory, Substitute.For<IServiceProvider>(), options);
        _store = new LatticeAuthorizationPolicyStore(_grainFactory, initializer, options);
    }

    private static LatticeAuthorizationRule Rule(LatticeScope scope, string ruleId = "r") =>
        new(ruleId, Members, scope, LatticeOperation.Write, LatticeEffect.Allow);

    private void AssertRefused(LatticeAuthorizationRule rule, string because)
    {
        var ex = Assert.ThrowsAsync<ArgumentException>(() => _store.PutRuleAsync(rule));
        Assert.Multiple(() =>
        {
            Assert.That(ex!.Message, Does.Contain(because));
            Assert.That(_grainFactory.ReceivedCalls(), Is.Empty, "a refused write must not touch the policy tree");
        });
    }

    // ---- Public helpers -------------------------------------------------------

    [Test]
    public void GroupPlaceholder_builds_the_placeholder_text()
    {
        Assert.That(LatticeScope.GroupPlaceholder("grp:"), Is.EqualTo("{group:grp:}"));
    }

    [TestCase("")]
    [TestCase("a{b")]
    [TestCase("a}b")]
    [TestCase("a\u001fb")]
    public void GroupPlaceholder_rejects_an_invalid_prefix(string prefix)
    {
        Assert.Throws<ArgumentException>(() => LatticeScope.GroupPlaceholder(prefix));
    }

    [TestCase("{subject}/", ExpectedResult = true)]
    [TestCase("g/{group:grp:}/", ExpectedResult = true)]
    [TestCase("plain/", ExpectedResult = false)]
    [TestCase("{other}/", ExpectedResult = false)]
    [TestCase("{subject}", ExpectedResult = false)]
    public bool IsTemplated_reports_a_valid_template(string prefix) =>
        LatticeScope.Prefix("images", prefix).IsTemplated();

    [Test]
    public void IsTemplated_is_false_for_a_tree_scope_and_a_control_plane_tree()
    {
        Assert.Multiple(() =>
        {
            Assert.That(LatticeScope.Tree("images").IsTemplated(), Is.False);
            Assert.That(LatticeScope.Prefix("sys-auth-policy", "{subject}/").IsTemplated(), Is.False);
            Assert.That(LatticeScope.Prefix("_lattice_x", "{subject}/").IsTemplated(), Is.False);
        });
    }

    // ---- Store guard ---------------------------------------------------------

    [Test]
    public async Task PutRuleAsync_admits_a_valid_templated_rule()
    {
        var rule = Rule(LatticeScope.Prefix("images", "{subject}/"));

        await _store.PutRuleAsync(rule);

        await _policy.Received(1).SetAsync(
            "images" + AuthConstants.RuleKeySeparator + "r", Arg.Any<byte[]>(), Arg.Any<CancellationToken>());
    }

    [TestCase("{subject}", "delimiter")]
    [TestCase("x/{subject}", "delimiter")]
    [TestCase("{subject}{subject}/", "separated")]
    [TestCase("{group:grp:", "not closed")]
    [TestCase("{group:}/", "non-empty")]
    [TestCase("{subject}\u001f", "U+001F")]
    [TestCase("{subject}/{subject}/{subject}/{subject}/{subject}/{subject}/{subject}/{subject}/{subject}/", "at most 8")]
    public void PutRuleAsync_refuses_an_invalid_template(string prefix, string because)
    {
        AssertRefused(Rule(LatticeScope.Prefix("images", prefix)), because);
    }

    [Test]
    public void PutRuleAsync_refuses_an_invalid_templated_key()
    {
        AssertRefused(Rule(LatticeScope.Key("images", "profile/{subject}")), "delimiter");
    }

    [TestCase("sys-membership-edges")]
    [TestCase("sys-tenant-registry")]
    [TestCase("tag-colour")]
    public void PutRuleAsync_refuses_a_template_on_a_control_plane_tree(string tree)
    {
        AssertRefused(Rule(LatticeScope.Prefix(tree, "{subject}/")), "control-plane");
    }

    [TestCase("images", "{group:t/}/")]
    [TestCase("images", "{group:t}/")]
    [TestCase("t/contoso/orders", "{group:t/fabrikam/}/")]
    [TestCase("t/contoso/orders", "{group:t/contoso}/")]
    public void PutRuleAsync_refuses_a_group_placeholder_reaching_another_tenants_groups(string tree, string prefix)
    {
        AssertRefused(Rule(LatticeScope.Prefix(tree, prefix)), "tenant groups");
    }

    [Test]
    public async Task PutRuleAsync_admits_a_group_placeholder_confined_to_the_trees_tenant()
    {
        var rule = Rule(LatticeScope.Prefix("t/contoso/orders", "{group:t/contoso/}/"));

        await _store.PutRuleAsync(rule);

        await _policy.Received(1).SetAsync(
            "t/contoso/orders" + AuthConstants.RuleKeySeparator + "r", Arg.Any<byte[]>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public void PutRuleAsync_still_admits_a_literal_brace_that_is_not_a_placeholder()
    {
        Assert.DoesNotThrowAsync(() => _store.PutRuleAsync(Rule(LatticeScope.Prefix("images", "{other}"))));
    }
}
