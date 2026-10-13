using NSubstitute;
using Orleans.Lattice.Api.TenantAdmin;
using Orleans.Lattice.Explorer.UI.Areas.Access.Tenant;
using Orleans.Lattice.Explorer.UI.Areas.Access.Tenant.Rules;
using Orleans.Lattice.Explorer.UI.Design.Components;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Access.Tenant.Rules;

/// <summary>Tenant-local rule id suggestions, including unavailable reads and cancellation.</summary>
[TestFixture]
public sealed class TenantRuleIdSuggestionSourceTests
{
    private ILatticeTenantPolicyAdmin _policy = null!;
    private TenantAccessCatalog _catalog = null!;
    private TenantRuleIdSuggestionSource _source = null!;

    [SetUp]
    public void SetUp()
    {
        _policy = Substitute.For<ILatticeTenantPolicyAdmin>();
        _catalog = new TenantAccessCatalog(new FakeTenantAccessFacades { PolicyOverride = _policy });
        _source = new TenantRuleIdSuggestionSource(_catalog, "acme");
    }

    [Test]
    public void Constructor_and_query_reject_null_arguments()
    {
        Assert.Multiple(() =>
        {
            Assert.That(() => new TenantRuleIdSuggestionSource(null!, "acme"), Throws.ArgumentNullException);
            Assert.That(() => new TenantRuleIdSuggestionSource(_catalog, null!), Throws.ArgumentNullException);
            Assert.That(async () => await _source.SuggestAsync(null!, 10, CancellationToken.None), Throws.ArgumentNullException);
            Assert.That(_source.Tenant, Is.EqualTo("acme"));
        });
    }

    [Test]
    public async Task SuggestAsync_lists_only_tenant_rules_with_their_scope_descriptions()
    {
        Answer(
            Rule("tenant-wide", TenantRuleLayer.Tenant, TenantRuleScopeKind.TenantWide),
            Rule("operator", TenantRuleLayer.Platform, TenantRuleScopeKind.Tree, "orders"),
            Rule("orders-read", TenantRuleLayer.Tenant, TenantRuleScopeKind.Tree, "orders"),
            Rule("prefix-read", TenantRuleLayer.Tenant, TenantRuleScopeKind.Prefix, "orders", "eu/"),
            Rule("key-read", TenantRuleLayer.Tenant, TenantRuleScopeKind.Key, "orders", "o-1"));
        using var cancellation = new CancellationTokenSource();

        var result = await _source.SuggestAsync("", 10, cancellation.Token);

        Assert.Multiple(() =>
        {
            Assert.That(result.IsAvailable, Is.True);
            Assert.That(result.Truncated, Is.False);
            Assert.That(result.Items, Is.EqualTo(new[]
            {
                new LtSuggestion("tenant-wide", "every tree in this tenant"),
                new LtSuggestion("orders-read", "orders"),
                new LtSuggestion("prefix-read", "orders prefix eu/"),
                new LtSuggestion("key-read", "orders key o-1"),
            }));
        });
        await _policy.Received(1).ListRulesAsync("acme",
            Arg.Is<TenantAccessPageRequest>(request => request.PageSize == TenantAccessPageRequest.MaxPageSize),
            cancellation.Token);
    }

    [Test]
    public async Task SuggestAsync_ranks_exact_then_prefix_then_substring_and_reports_truncation()
    {
        Answer(
            Rule("xread", TenantRuleLayer.Tenant),
            Rule("reader", TenantRuleLayer.Tenant),
            Rule("read", TenantRuleLayer.Tenant),
            Rule("READ", TenantRuleLayer.Tenant),
            Rule("write", TenantRuleLayer.Tenant));

        var bounded = await _source.SuggestAsync("read", 2, CancellationToken.None);
        var all = await _source.SuggestAsync("read", 10, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(bounded.Items.Select(item => item.Value), Is.EqualTo(new[] { "read", "reader" }));
            Assert.That(bounded.Truncated, Is.True);
            Assert.That(all.Items.Select(item => item.Value), Is.EqualTo(new[] { "read", "reader", "READ", "xread" }));
            Assert.That(all.Truncated, Is.False);
        });
    }

    [TestCase(0)]
    [TestCase(-1)]
    public async Task SuggestAsync_nonpositive_limit_returns_no_suggestions(int limit)
    {
        Answer(Rule("read", TenantRuleLayer.Tenant));

        var result = await _source.SuggestAsync("", limit, CancellationToken.None);

        Assert.That(result, Is.SameAs(LtSuggestionSet.Empty));
    }

    [Test]
    public async Task SuggestAsync_no_tenant_rules_is_available_without_matches()
    {
        Answer(Rule("platform", TenantRuleLayer.Platform));

        var result = await _source.SuggestAsync("", 10, CancellationToken.None);

        Assert.That(result, Is.SameAs(LtSuggestionSet.Empty));
    }

    [Test]
    public async Task SuggestAsync_catalog_invalidation_removes_stale_id_clashes()
    {
        Answer(Rule("removed", TenantRuleLayer.Tenant));
        var first = await _source.SuggestAsync("removed", 10, CancellationToken.None);
        Answer(Rule("added", TenantRuleLayer.Tenant));
        _catalog.Invalidate();

        var second = await _source.SuggestAsync("", 10, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(first.Find("removed"), Is.Not.Null);
            Assert.That(second.Find("removed"), Is.Null);
            Assert.That(second.Find("added"), Is.Not.Null);
        });
        await _policy.Received(2).ListRulesAsync("acme", Arg.Any<TenantAccessPageRequest>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task SuggestAsync_failed_read_is_unavailable_and_the_next_read_can_recover()
    {
        _policy.ListRulesAsync("acme", Arg.Any<TenantAccessPageRequest>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromException<TenantRulePage>(new TimeoutException()));

        var unavailable = await _source.SuggestAsync("", 10, CancellationToken.None);
        Answer(Rule("read", TenantRuleLayer.Tenant));
        var recovered = await _source.SuggestAsync("", 10, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(unavailable.IsAvailable, Is.False);
            Assert.That(unavailable.UnavailableReason, Is.EqualTo(TenantRuleIdSuggestionSource.UnavailableReason));
            Assert.That(unavailable.Items, Is.Empty);
            Assert.That(recovered.IsAvailable, Is.True);
            Assert.That(recovered.Find("read"), Is.Not.Null);
        });
    }

    [Test]
    public async Task SuggestAsync_missing_policy_is_unavailable()
    {
        var source = new TenantRuleIdSuggestionSource(
            new TenantAccessCatalog(new FakeTenantAccessFacades { ServesPolicy = false }), "acme");

        var result = await source.SuggestAsync("", 10, CancellationToken.None);

        Assert.That(result.UnavailableReason, Is.EqualTo(TenantRuleIdSuggestionSource.UnavailableReason));
    }

    [Test]
    public void SuggestAsync_caller_cancellation_is_propagated()
    {
        using var cancellation = new CancellationTokenSource();
        _policy.ListRulesAsync("acme", Arg.Any<TenantAccessPageRequest>(), cancellation.Token)
            .Returns(_ =>
            {
                cancellation.Cancel();
                return Task.FromCanceled<TenantRulePage>(cancellation.Token);
            });

        var exception = Assert.CatchAsync<OperationCanceledException>(
            async () => await _source.SuggestAsync("", 10, cancellation.Token));

        Assert.That(exception!.CancellationToken, Is.EqualTo(cancellation.Token));
    }

    [Test]
    public async Task SuggestAsync_upstream_cancellation_without_caller_cancellation_is_unavailable()
    {
        _policy.ListRulesAsync("acme", Arg.Any<TenantAccessPageRequest>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromException<TenantRulePage>(new OperationCanceledException()));

        var result = await _source.SuggestAsync("", 10, CancellationToken.None);

        Assert.That(result.UnavailableReason, Is.EqualTo(TenantRuleIdSuggestionSource.UnavailableReason));
    }

    private void Answer(params TenantRuleView[] rules) =>
        _policy.ListRulesAsync("acme", Arg.Any<TenantAccessPageRequest>(), Arg.Any<CancellationToken>())
            .Returns(new TenantRulePage { Entries = rules });

    private static TenantRuleView Rule(
        string id, TenantRuleLayer layer, TenantRuleScopeKind scope = TenantRuleScopeKind.TenantWide,
        string? tree = null, string? keyOrPrefix = null) =>
        new() { RuleId = id, Layer = layer, ScopeKind = scope, TreeName = tree, KeyOrPrefix = keyOrPrefix };
}
