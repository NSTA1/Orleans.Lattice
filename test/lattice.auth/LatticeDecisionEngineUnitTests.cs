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
