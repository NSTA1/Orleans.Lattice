using System.Text;
using NSubstitute;

namespace Orleans.Lattice.Schema.Tests;

/// <summary>
/// Unit tests for the CRDT merge-result path: the pure
/// <see cref="LatticeSchemaMergeValidation"/> decision and the
/// <see cref="LatticeSchemaMergeObserver"/> that resolves the tree via the ambient
/// <see cref="LatticeSchemaMergeTree"/> scope. The observer never rejects or
/// transforms - it only annotates violations - so convergence is never blocked.
/// </summary>
public class LatticeSchemaMergeValidationTests
{
    private static byte[] Utf8(string s) => Encoding.UTF8.GetBytes(s);

    private static CompiledSchemaPolicy JsonPolicy() =>
        CompiledSchemaPolicy.Compile(new LatticeSchemaPolicy(new[] { LatticeSchemaRule.Json() }));

    private static LatticeMergeContext Merge(byte[] merged) =>
        new("k1", LatticeMergeMode.LwwRegister, localValue: null, incomingValue: null, merged);

    private static LatticeMergeContext MergeWithTree(string treeId, byte[] merged) =>
        new("k1", LatticeMergeMode.LwwRegister, localValue: null, incomingValue: null, merged, treeId);

    [Test]
    public void Evaluate_valid_merged_value_accepts()
    {
        var outcome = LatticeSchemaMergeValidation.Evaluate(JsonPolicy(), Merge(Utf8("{\"a\":1}")));
        Assert.That(outcome.Kind, Is.EqualTo(MergeOutcomeKind.Accept));
    }

    [Test]
    public void Evaluate_invalid_merged_value_accepts_with_event()
    {
        var outcome = LatticeSchemaMergeValidation.Evaluate(JsonPolicy(), Merge(Utf8("not json")));
        Assert.That(outcome.Kind, Is.EqualTo(MergeOutcomeKind.AcceptWithEvent));
        Assert.That(outcome.EventReason, Is.Not.Null.And.Not.Empty);
    }

    [Test]
    public void Evaluate_null_policy_throws()
    {
        Assert.That(
            () => LatticeSchemaMergeValidation.Evaluate(null!, Merge(Utf8("{}"))),
            Throws.ArgumentNullException);
    }

    [Test]
    public async Task Observer_without_ambient_scope_accepts()
    {
        var provider = Substitute.For<ILatticeSchemaPolicyProvider>();
        var observer = new LatticeSchemaMergeObserver(provider);

        var outcome = await observer.OnMergedAsync(Merge(Utf8("not json")), CancellationToken.None);

        Assert.That(outcome.Kind, Is.EqualTo(MergeOutcomeKind.Accept));
        await provider.DidNotReceive().GetCompiledPolicyAsync(Arg.Any<string>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task Observer_ungoverned_tree_accepts()
    {
        var provider = Substitute.For<ILatticeSchemaPolicyProvider>();
        provider.GetCompiledPolicyAsync("orders", Arg.Any<CancellationToken>())
            .Returns(new ValueTask<CompiledSchemaPolicy?>((CompiledSchemaPolicy?)null));
        var observer = new LatticeSchemaMergeObserver(provider);

        LatticeMergeOutcome outcome;
        using (LatticeSchemaMergeTree.Enter("orders"))
        {
            outcome = await observer.OnMergedAsync(Merge(Utf8("not json")), CancellationToken.None);
        }

        Assert.That(outcome.Kind, Is.EqualTo(MergeOutcomeKind.Accept));
    }

    [Test]
    public async Task Observer_governed_tree_valid_value_accepts()
    {
        var provider = Substitute.For<ILatticeSchemaPolicyProvider>();
        provider.GetCompiledPolicyAsync("orders", Arg.Any<CancellationToken>())
            .Returns(new ValueTask<CompiledSchemaPolicy?>(JsonPolicy()));
        var observer = new LatticeSchemaMergeObserver(provider);

        LatticeMergeOutcome outcome;
        using (LatticeSchemaMergeTree.Enter("orders"))
        {
            outcome = await observer.OnMergedAsync(Merge(Utf8("{\"a\":1}")), CancellationToken.None);
        }

        Assert.That(outcome.Kind, Is.EqualTo(MergeOutcomeKind.Accept));
    }

    [Test]
    public async Task Observer_governed_tree_invalid_value_accepts_with_event()
    {
        var provider = Substitute.For<ILatticeSchemaPolicyProvider>();
        provider.GetCompiledPolicyAsync("orders", Arg.Any<CancellationToken>())
            .Returns(new ValueTask<CompiledSchemaPolicy?>(JsonPolicy()));
        var observer = new LatticeSchemaMergeObserver(provider);

        LatticeMergeOutcome outcome;
        using (LatticeSchemaMergeTree.Enter("orders"))
        {
            outcome = await observer.OnMergedAsync(Merge(Utf8("not json")), CancellationToken.None);
        }

        Assert.That(outcome.Kind, Is.EqualTo(MergeOutcomeKind.AcceptWithEvent));
    }

    [Test]
    public void MergeTree_scope_restores_previous_value_on_dispose()
    {
        Assert.That(LatticeSchemaMergeTree.Current, Is.Null);
        using (LatticeSchemaMergeTree.Enter("orders"))
        {
            Assert.That(LatticeSchemaMergeTree.Current, Is.EqualTo("orders"));
        }

        Assert.That(LatticeSchemaMergeTree.Current, Is.Null);
    }

    [Test]
    public void MergeTree_enter_null_or_empty_throws()
    {
        Assert.That(() => LatticeSchemaMergeTree.Enter(null!), Throws.InstanceOf<ArgumentException>());
        Assert.That(() => LatticeSchemaMergeTree.Enter(string.Empty), Throws.ArgumentException);
    }

    [Test]
    public async Task Observer_resolves_tree_from_context_treeId_without_ambient_scope()
    {
        // The core merge seam stamps ctx.TreeId; the observer must resolve the policy
        // from it directly, with no ambient scope entered (the production path).
        var provider = Substitute.For<ILatticeSchemaPolicyProvider>();
        provider.GetCompiledPolicyAsync("orders", Arg.Any<CancellationToken>())
            .Returns(new ValueTask<CompiledSchemaPolicy?>(JsonPolicy()));
        var observer = new LatticeSchemaMergeObserver(provider);

        Assert.That(LatticeSchemaMergeTree.Current, Is.Null); // no ambient scope
        var outcome = await observer.OnMergedAsync(MergeWithTree("orders", Utf8("not json")), CancellationToken.None);

        Assert.That(outcome.Kind, Is.EqualTo(MergeOutcomeKind.AcceptWithEvent));
        await provider.Received(1).GetCompiledPolicyAsync("orders", Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task Observer_context_treeId_valid_value_accepts()
    {
        var provider = Substitute.For<ILatticeSchemaPolicyProvider>();
        provider.GetCompiledPolicyAsync("orders", Arg.Any<CancellationToken>())
            .Returns(new ValueTask<CompiledSchemaPolicy?>(JsonPolicy()));
        var observer = new LatticeSchemaMergeObserver(provider);

        var outcome = await observer.OnMergedAsync(MergeWithTree("orders", Utf8("{\"a\":1}")), CancellationToken.None);

        Assert.That(outcome.Kind, Is.EqualTo(MergeOutcomeKind.Accept));
    }

    [Test]
    public async Task Observer_context_treeId_takes_precedence_over_ambient_scope()
    {
        // ctx.TreeId ("orders") wins over the ambient scope ("other"): the observer
        // must consult the policy for the context's tree, not the ambient one.
        var provider = Substitute.For<ILatticeSchemaPolicyProvider>();
        provider.GetCompiledPolicyAsync("orders", Arg.Any<CancellationToken>())
            .Returns(new ValueTask<CompiledSchemaPolicy?>(JsonPolicy()));
        var observer = new LatticeSchemaMergeObserver(provider);

        LatticeMergeOutcome outcome;
        using (LatticeSchemaMergeTree.Enter("other"))
        {
            outcome = await observer.OnMergedAsync(MergeWithTree("orders", Utf8("not json")), CancellationToken.None);
        }

        Assert.That(outcome.Kind, Is.EqualTo(MergeOutcomeKind.AcceptWithEvent));
        await provider.Received(1).GetCompiledPolicyAsync("orders", Arg.Any<CancellationToken>());
        await provider.DidNotReceive().GetCompiledPolicyAsync("other", Arg.Any<CancellationToken>());
    }
}
