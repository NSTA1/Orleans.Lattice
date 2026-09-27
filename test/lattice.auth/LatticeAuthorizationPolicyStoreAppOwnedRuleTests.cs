using Microsoft.Extensions.Options;
using NSubstitute;

namespace Orleans.Lattice.Auth.Tests;

/// <summary>
/// Unit tests for the app-owned rule write guard on
/// <see cref="LatticeAuthorizationPolicyStore"/>: a direct write or delete of a rule
/// id in the <see cref="LatticeAppRuleIds.Prefix"/> namespace is rejected with
/// <see cref="LatticeAppOwnedRuleException"/> unless the caller is already inside a
/// system-origin scope (the app compiler), while an ordinary rule id is unaffected.
/// The store is driven against substitute grains, so the tests also prove a
/// rejection issues no read or write at all.
/// </summary>
[TestFixture]
public sealed class LatticeAuthorizationPolicyStoreAppOwnedRuleTests
{
    private const string Tree = "orders";
    private const string AppRuleId = LatticeAppRuleIds.Prefix + "billing/reader/orders";
    private const string OrdinaryRuleId = "ops-orders-read";

    private IGrainFactory _grainFactory = null!;
    private ILattice _policy = null!;
    private LatticeAuthorizationPolicyStore _store = null!;

    [SetUp]
    public void SetUp()
    {
        _grainFactory = Substitute.For<IGrainFactory>();
        _policy = Substitute.For<ILattice>();
        _policy.DeleteAsync(Arg.Any<string>(), Arg.Any<CancellationToken>()).Returns(true);
        _policy.GetAsync(Arg.Any<string>(), Arg.Any<CancellationToken>()).Returns(Task.FromResult<byte[]?>(null));
        _grainFactory.GetGrain<ILattice>(Arg.Any<string>(), Arg.Any<string?>()).Returns(_policy);

        var options = Substitute.For<IOptionsMonitor<LatticeAuthOptions>>();
        options.CurrentValue.Returns(new LatticeAuthOptions());
        var initializer = new AuthInitializer(_grainFactory, Substitute.For<IServiceProvider>(), options);
        _store = new LatticeAuthorizationPolicyStore(_grainFactory, initializer, options);
    }

    private static LatticeAuthorizationRule Rule(string ruleId) =>
        new(ruleId, LatticeSubjectSelector.Group("billing-readers"), LatticeScope.Tree(Tree), LatticeOperation.Read, LatticeEffect.Allow);

    private static string Key(string ruleId) => Tree + AuthConstants.RuleKeySeparator + ruleId;

    [Test]
    public void PutRuleAsync_rejects_an_operator_write_to_an_app_owned_rule_id()
    {
        var ex = Assert.ThrowsAsync<LatticeAppOwnedRuleException>(() => _store.PutRuleAsync(Rule(AppRuleId)));

        Assert.That(ex!.RuleId, Is.EqualTo(AppRuleId));
        Assert.That(ex.ParamName, Is.EqualTo("rule"));
        Assert.That(_grainFactory.ReceivedCalls(), Is.Empty, "a rejected write must not touch the policy tree");
    }

    [Test]
    public async Task PutRuleAsync_leaves_an_operator_write_to_an_ordinary_rule_id_unaffected()
    {
        await _store.PutRuleAsync(Rule(OrdinaryRuleId));

        await _policy.Received(1).SetAsync(Key(OrdinaryRuleId), Arg.Any<byte[]>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task PutRuleAsync_admits_an_app_owned_rule_id_under_system_origin()
    {
        using (LatticeSystemOrigin.Enter())
        {
            await _store.PutRuleAsync(Rule(AppRuleId));
        }

        await _policy.Received(1).SetAsync(Key(AppRuleId), Arg.Any<byte[]>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task PutRuleAsync_rejects_an_app_owned_rule_id_after_the_system_origin_scope_is_disposed()
    {
        using (LatticeSystemOrigin.Enter())
        {
            await _store.PutRuleAsync(Rule(OrdinaryRuleId));
        }

        Assert.That(LatticeSystemOrigin.IsActive, Is.False);
        Assert.ThrowsAsync<LatticeAppOwnedRuleException>(() => _store.PutRuleAsync(Rule(AppRuleId)));
    }

    [Test]
    public async Task PutRuleAsync_store_internal_system_origin_does_not_leak_into_a_later_operator_write()
    {
        // The store enters its own system-origin scope around every write; that scope
        // must not be what the guard observes, or every operator write would pass.
        await _store.PutRuleAsync(Rule(OrdinaryRuleId));

        Assert.ThrowsAsync<LatticeAppOwnedRuleException>(() => _store.PutRuleAsync(Rule(AppRuleId)));
    }

    [TestCase("App:billing/reader")]
    [TestCase("app-billing-reader")]
    [TestCase("apps:billing")]
    [TestCase("x-app:billing")]
    public async Task PutRuleAsync_treats_a_prefix_lookalike_as_an_ordinary_rule_id(string ruleId)
    {
        await _store.PutRuleAsync(Rule(ruleId));

        await _policy.Received(1).SetAsync(Key(ruleId), Arg.Any<byte[]>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public void PutRuleAsync_still_rejects_a_reserved_scope_for_an_app_owned_rule_under_system_origin()
    {
        // System origin admits the app-owned id only; it does not bypass the
        // reserved-namespace authoring guard.
        var rule = new LatticeAuthorizationRule(
            AppRuleId,
            LatticeSubjectSelector.Group("billing-readers"),
            LatticeScope.Tree("sys-auth-audit"),
            LatticeOperation.Read,
            LatticeEffect.Allow);

        using (LatticeSystemOrigin.Enter())
        {
            var ex = Assert.ThrowsAsync(Is.InstanceOf<ArgumentException>(), () => _store.PutRuleAsync(rule));
            Assert.That(ex, Is.Not.InstanceOf<LatticeAppOwnedRuleException>());
        }
    }

    [Test]
    public void RemoveRuleAsync_rejects_an_operator_delete_of_an_app_owned_rule_id()
    {
        var ex = Assert.ThrowsAsync<LatticeAppOwnedRuleException>(() => _store.RemoveRuleAsync(Tree, AppRuleId));

        Assert.That(ex!.RuleId, Is.EqualTo(AppRuleId));
        Assert.That(ex.ParamName, Is.EqualTo("ruleId"));
        Assert.That(_grainFactory.ReceivedCalls(), Is.Empty, "a rejected delete must not read or write the policy tree");
    }

    [Test]
    public async Task RemoveRuleAsync_leaves_an_operator_delete_of_an_ordinary_rule_id_unaffected()
    {
        var removed = await _store.RemoveRuleAsync(Tree, OrdinaryRuleId);

        Assert.That(removed, Is.True);
        await _policy.Received(1).DeleteAsync(Key(OrdinaryRuleId), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task RemoveRuleAsync_admits_an_app_owned_rule_id_under_system_origin()
    {
        bool removed;
        using (LatticeSystemOrigin.Enter())
        {
            removed = await _store.RemoveRuleAsync(Tree, AppRuleId);
        }

        Assert.That(removed, Is.True);
        await _policy.Received(1).DeleteAsync(Key(AppRuleId), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task GetRuleAsync_reads_an_app_owned_rule_id_for_an_operator()
    {
        // Reads are not guarded: an operator may inspect compiler output.
        var rule = await _store.GetRuleAsync(Tree, AppRuleId);

        Assert.That(rule, Is.Null);
        await _policy.Received(1).GetAsync(Key(AppRuleId), Arg.Any<CancellationToken>());
    }
}
