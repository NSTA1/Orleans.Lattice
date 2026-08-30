using Microsoft.Extensions.Options;
using NSubstitute;

namespace Orleans.Lattice.Schema.Tests;

/// <summary>
/// Unit tests for <see cref="LatticeSchemaPolicyProvider"/>: cached resolution,
/// the ungoverned-tree null sentinel, reserved-tree short-circuit, cache
/// invalidation (explicit and via the mutation observer), and the strict flag.
/// </summary>
public class LatticeSchemaPolicyProviderTests
{
    private static LatticeSchemaPolicyProvider CreateProvider(
        ILatticeSchemaPolicyStore store, bool strict = false)
    {
        var options = Options.Create(new LatticeSchemaEnforcementOptions { StrictIngest = strict });
        return new LatticeSchemaPolicyProvider(store, options);
    }

    private static LatticeSchemaPolicy JsonPolicy() =>
        new(new[] { LatticeSchemaRule.Json() });

    [Test]
    public async Task GetCompiledPolicyAsync_governed_tree_returns_compiled_policy()
    {
        var store = Substitute.For<ILatticeSchemaPolicyStore>();
        store.GetPolicyAsync("orders", Arg.Any<CancellationToken>()).Returns(JsonPolicy());
        var provider = CreateProvider(store);

        var compiled = await provider.GetCompiledPolicyAsync("orders");

        Assert.That(compiled, Is.Not.Null);
        Assert.That(compiled!.RuleCount, Is.EqualTo(1));
    }

    [Test]
    public async Task GetCompiledPolicyAsync_ungoverned_tree_returns_null()
    {
        var store = Substitute.For<ILatticeSchemaPolicyStore>();
        store.GetPolicyAsync("orders", Arg.Any<CancellationToken>()).Returns((LatticeSchemaPolicy?)null);
        var provider = CreateProvider(store);

        Assert.That(await provider.GetCompiledPolicyAsync("orders"), Is.Null);
    }

    [Test]
    public async Task GetCompiledPolicyAsync_caches_after_first_load()
    {
        var store = Substitute.For<ILatticeSchemaPolicyStore>();
        store.GetPolicyAsync("orders", Arg.Any<CancellationToken>()).Returns(JsonPolicy());
        var provider = CreateProvider(store);

        _ = await provider.GetCompiledPolicyAsync("orders");
        _ = await provider.GetCompiledPolicyAsync("orders");

        await store.Received(1).GetPolicyAsync("orders", Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task GetCompiledPolicyAsync_caches_null_sentinel_for_ungoverned_tree()
    {
        var store = Substitute.For<ILatticeSchemaPolicyStore>();
        store.GetPolicyAsync("orders", Arg.Any<CancellationToken>()).Returns((LatticeSchemaPolicy?)null);
        var provider = CreateProvider(store);

        _ = await provider.GetCompiledPolicyAsync("orders");
        _ = await provider.GetCompiledPolicyAsync("orders");

        await store.Received(1).GetPolicyAsync("orders", Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task GetCompiledPolicyAsync_reserved_tree_short_circuits_without_store()
    {
        var store = Substitute.For<ILatticeSchemaPolicyStore>();
        var provider = CreateProvider(store);

        Assert.That(await provider.GetCompiledPolicyAsync("sys-schema-policy"), Is.Null);
        await store.DidNotReceive().GetPolicyAsync(Arg.Any<string>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task Invalidate_forces_reload_on_next_get()
    {
        var store = Substitute.For<ILatticeSchemaPolicyStore>();
        store.GetPolicyAsync("orders", Arg.Any<CancellationToken>()).Returns(JsonPolicy());
        var provider = CreateProvider(store);

        _ = await provider.GetCompiledPolicyAsync("orders");
        provider.Invalidate("orders");
        _ = await provider.GetCompiledPolicyAsync("orders");

        await store.Received(2).GetPolicyAsync("orders", Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task OnMutationAsync_policy_tree_write_evicts_affected_tree()
    {
        var store = Substitute.For<ILatticeSchemaPolicyStore>();
        store.GetPolicyAsync("orders", Arg.Any<CancellationToken>()).Returns(JsonPolicy());
        var provider = CreateProvider(store);

        _ = await provider.GetCompiledPolicyAsync("orders");
        await provider.OnMutationAsync(
            new LatticeMutation { TreeId = "sys-schema-policy", Key = "orders" }, CancellationToken.None);
        _ = await provider.GetCompiledPolicyAsync("orders");

        await store.Received(2).GetPolicyAsync("orders", Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task OnMutationAsync_policy_tree_write_with_empty_key_does_not_evict()
    {
        var store = Substitute.For<ILatticeSchemaPolicyStore>();
        store.GetPolicyAsync("orders", Arg.Any<CancellationToken>()).Returns(JsonPolicy());
        var provider = CreateProvider(store);

        _ = await provider.GetCompiledPolicyAsync("orders");
        await provider.OnMutationAsync(
            new LatticeMutation { TreeId = "sys-schema-policy", Key = string.Empty }, CancellationToken.None);
        _ = await provider.GetCompiledPolicyAsync("orders");

        await store.Received(1).GetPolicyAsync("orders", Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task OnMutationAsync_unrelated_tree_write_does_not_evict()
    {
        var store = Substitute.For<ILatticeSchemaPolicyStore>();
        store.GetPolicyAsync("orders", Arg.Any<CancellationToken>()).Returns(JsonPolicy());
        var provider = CreateProvider(store);

        _ = await provider.GetCompiledPolicyAsync("orders");
        await provider.OnMutationAsync(
            new LatticeMutation { TreeId = "orders", Key = "some-key" }, CancellationToken.None);
        _ = await provider.GetCompiledPolicyAsync("orders");

        await store.Received(1).GetPolicyAsync("orders", Arg.Any<CancellationToken>());
    }

    [Test]
    public void StrictIngestEnabled_reflects_options()
    {
        var store = Substitute.For<ILatticeSchemaPolicyStore>();
        Assert.That(CreateProvider(store, strict: true).StrictIngestEnabled, Is.True);
        Assert.That(CreateProvider(store, strict: false).StrictIngestEnabled, Is.False);
    }

    [Test]
    public void Constructor_null_arguments_throw()
    {
        var store = Substitute.For<ILatticeSchemaPolicyStore>();
        Assert.That(
            () => new LatticeSchemaPolicyProvider(null!, Options.Create(new LatticeSchemaEnforcementOptions())),
            Throws.ArgumentNullException);
        Assert.That(
            () => new LatticeSchemaPolicyProvider(store, null!),
            Throws.ArgumentNullException);
    }
}
