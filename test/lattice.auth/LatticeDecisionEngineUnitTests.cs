using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Auth.Tests;

/// <summary>
/// Direct unit tests for <see cref="LatticeDecisionEngine"/> over an in-memory
/// policy store, focused on the structural "any grant" existence-hiding probe
/// (<c>HasAnyGrant</c>) and its argument guards. Exercising the engine here also
/// drives <see cref="PolicyEvaluator.HasAnyGrant"/> and
/// <see cref="CompiledTree.HasAnyResolvedAllow"/> across the whole-tree, exact-key,
/// and prefix scope tiers.
/// </summary>
[TestFixture]
public sealed class LatticeDecisionEngineUnitTests
{
    private static readonly LatticeSubject Alice = new("alice");

    private static LatticeAuthorizationRule Rule(
        LatticeScope scope,
        LatticeEffect effect = LatticeEffect.Allow) =>
        new("r", LatticeSubjectSelector.User("alice"), scope, LatticeOperation.Read, effect);

    private static async Task<LatticeDecisionEngine> EngineAsync(
        LatticeAuthOptions options,
        params LatticeAuthorizationRule[] rules) =>
        (await AuthGateHarness.CreateAsync(options, rules)).Engine;

    [Test]
    public async Task HasAnyGrant_default_allow_effect_returns_true_without_any_rule()
    {
        var engine = await EngineAsync(new LatticeAuthOptions { DefaultEffect = LatticeEffect.Allow });

        Assert.That(engine.HasAnyGrant(Alice, "app", LatticeOperation.Read), Is.True);
    }

    [Test]
    public async Task HasAnyGrant_default_allow_with_whole_tree_deny_returns_false()
    {
        var engine = await EngineAsync(
            new LatticeAuthOptions { DefaultEffect = LatticeEffect.Allow },
            Rule(LatticeScope.Tree("app"), LatticeEffect.Deny));

        Assert.That(
            engine.HasAnyGrant(Alice, "app", LatticeOperation.Read),
            Is.False,
            "a whole-tree deny removes the entire keyspace even under default-allow, so the existence probe must not out-reach the enforcement deny");
    }

    [Test]
    public async Task HasAnyGrant_default_allow_with_whole_tree_deny_and_prefix_allow_returns_true()
    {
        var engine = await EngineAsync(
            new LatticeAuthOptions { DefaultEffect = LatticeEffect.Allow },
            Rule(LatticeScope.Tree("app"), LatticeEffect.Deny),
            Rule(LatticeScope.Prefix("app", "pub/")));

        Assert.That(
            engine.HasAnyGrant(Alice, "app", LatticeOperation.Read),
            Is.True,
            "a prefix allow carve-out keeps some keys readable, so the tree stays visible despite the whole-tree deny");
    }

    [Test]
    public async Task HasAnyGrant_default_deny_with_all_trees_allow_and_whole_tree_deny_returns_false()
    {
        // The symmetric case of the default-allow whole-tree-deny probe above, on
        // the default-deny branch: enforcement gives the specific tree's own
        // whole-tree deny precedence over an all-trees allow (ResolveTiered tier 2
        // beats tier 3), so every key of "app" resolves deny - and the existence
        // probe must not out-reach that.
        var options = new LatticeAuthOptions
        {
            DefaultEffect = LatticeEffect.Deny,
            AllTreesGrantsEnabled = true,
        };
        var harness = await AuthGateHarness.CreateAsync(
            options,
            Rule(LatticeScope.ClusterWide()),
            Rule(LatticeScope.Tree("app"), LatticeEffect.Deny));

        var decision = await harness.Gate.AuthorizeAsync(
            new LatticeAccessRequest("app", LatticeOperation.Read, Alice, "k"));

        Assert.That(
            decision.Allowed,
            Is.False,
            "enforcement resolves the specific whole-tree deny over the all-trees allow");
        Assert.That(
            harness.Engine.HasAnyGrant(Alice, "app", LatticeOperation.Read),
            Is.False,
            "the existence probe must agree with enforcement: every key is denied, so the tree is not visible");
    }

    [Test]
    public async Task HasAnyGrant_default_deny_with_all_trees_allow_and_prefix_carve_out_returns_true()
    {
        // Regression guard against over-hiding: a prefix allow carve-out under the
        // whole-tree deny keeps some keys readable, so the tree must stay visible.
        var options = new LatticeAuthOptions
        {
            DefaultEffect = LatticeEffect.Deny,
            AllTreesGrantsEnabled = true,
        };
        var harness = await AuthGateHarness.CreateAsync(
            options,
            Rule(LatticeScope.ClusterWide()),
            Rule(LatticeScope.Tree("app"), LatticeEffect.Deny),
            Rule(LatticeScope.Prefix("app", "pub/")));

        Assert.That(
            harness.Engine.HasAnyGrant(Alice, "app", LatticeOperation.Read),
            Is.True,
            "a prefix allow carve-out keeps some keys readable, so the tree stays visible");
    }

    [Test]
    public async Task HasAnyGrant_default_deny_with_all_trees_allow_and_no_specific_rule_returns_true()
    {
        // Regression guard against over-hiding: a tree reachable only through the
        // all-trees tier must remain visible.
        var options = new LatticeAuthOptions
        {
            DefaultEffect = LatticeEffect.Deny,
            AllTreesGrantsEnabled = true,
        };
        var harness = await AuthGateHarness.CreateAsync(options, Rule(LatticeScope.ClusterWide()));

        Assert.That(
            harness.Engine.HasAnyGrant(Alice, "app", LatticeOperation.Read),
            Is.True,
            "a tree reachable only through the all-trees tier stays visible");
    }

    [Test]
    public async Task HasAnyGrant_all_trees_deny_hides_tree_despite_specific_allow()
    {
        // ResolveTiered tier 1: an all-trees deny wins outright over the specific
        // tree's own allow, so every key resolves deny. The probe consults only
        // "any resolved allow" on the specific tree and so reports the tree as
        // visible - out-reaching enforcement.
        var options = new LatticeAuthOptions
        {
            DefaultEffect = LatticeEffect.Deny,
            AllTreesGrantsEnabled = true,
        };
        var harness = await AuthGateHarness.CreateAsync(
            options,
            Rule(LatticeScope.ClusterWide(), LatticeEffect.Deny),
            Rule(LatticeScope.Tree("app")));

        var decision = await harness.Gate.AuthorizeAsync(
            new LatticeAccessRequest("app", LatticeOperation.Read, Alice, "k"));

        Assert.That(decision.Allowed, Is.False, "an all-trees deny wins outright at tier 1");
        Assert.That(
            harness.Engine.HasAnyGrant(Alice, "app", LatticeOperation.Read),
            Is.False,
            "the existence probe must agree with the tier 1 all-trees deny");
    }

    [Test]
    public async Task HasAnyGrant_all_trees_deny_hides_tree_under_default_allow()
    {
        // Same tier 1 deny, reached on the default-allow branch where the tree
        // carries no specific rules at all.
        var options = new LatticeAuthOptions
        {
            DefaultEffect = LatticeEffect.Allow,
            AllTreesGrantsEnabled = true,
        };
        var harness = await AuthGateHarness.CreateAsync(
            options,
            Rule(LatticeScope.ClusterWide(), LatticeEffect.Deny));

        var decision = await harness.Gate.AuthorizeAsync(
            new LatticeAccessRequest("app", LatticeOperation.Read, Alice, "k"));

        Assert.That(decision.Allowed, Is.False, "an all-trees deny wins outright at tier 1");
        Assert.That(
            harness.Engine.HasAnyGrant(Alice, "app", LatticeOperation.Read),
            Is.False,
            "the existence probe must agree with the tier 1 all-trees deny");
    }

    [Test]
    public async Task HasAnyGrant_whole_tree_allow_returns_true()
    {
        var engine = await EngineAsync(
            new LatticeAuthOptions { DefaultEffect = LatticeEffect.Deny },
            Rule(LatticeScope.Tree("app")));

        Assert.That(engine.HasAnyGrant(Alice, "app", LatticeOperation.Read), Is.True);
    }

    [Test]
    public async Task HasAnyGrant_exact_key_allow_returns_true()
    {
        var engine = await EngineAsync(
            new LatticeAuthOptions { DefaultEffect = LatticeEffect.Deny },
            Rule(LatticeScope.Key("app", "k1")));

        Assert.That(
            engine.HasAnyGrant(Alice, "app", LatticeOperation.Read),
            Is.True,
            "a single exact-key allow is a partial grant that keeps the tree visible");
    }

    [Test]
    public async Task HasAnyGrant_prefix_allow_returns_true()
    {
        var engine = await EngineAsync(
            new LatticeAuthOptions { DefaultEffect = LatticeEffect.Deny },
            Rule(LatticeScope.Prefix("app", "p/")));

        Assert.That(
            engine.HasAnyGrant(Alice, "app", LatticeOperation.Read),
            Is.True,
            "a prefix allow is a partial grant that keeps the tree visible");
    }

    [Test]
    public async Task HasAnyGrant_tree_with_only_a_deny_returns_false()
    {
        var engine = await EngineAsync(
            new LatticeAuthOptions { DefaultEffect = LatticeEffect.Deny },
            Rule(LatticeScope.Tree("app"), LatticeEffect.Deny));

        Assert.That(
            engine.HasAnyGrant(Alice, "app", LatticeOperation.Read),
            Is.False,
            "a tree that carries only a deny yields no resolved allow");
    }

    [Test]
    public async Task HasAnyGrant_empty_tree_id_throws()
    {
        var engine = await EngineAsync(new LatticeAuthOptions());

        Assert.That(
            () => engine.HasAnyGrant(Alice, string.Empty, LatticeOperation.Read),
            Throws.ArgumentException);
    }

    [Test]
    public async Task Evaluate_empty_tree_id_throws()
    {
        var engine = await EngineAsync(new LatticeAuthOptions());

        Assert.That(
            () => engine.Evaluate(Alice, string.Empty, LatticeOperation.Read),
            Throws.ArgumentException);
    }
}
