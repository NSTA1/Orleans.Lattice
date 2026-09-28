using Orleans.Lattice.Auth;
using static Orleans.Lattice.Apps.Tests.SubscriptionTestData;

namespace Orleans.Lattice.Apps.Tests;

[TestFixture]
public sealed class AppSubscriptionCompilerTests
{
    private static readonly AppTreeOwnerSnapshot BillingOwnsInvoices =
        AppTreeOwnerSnapshot.Create([new("a/billing/invoices", Billing)]);

    [Test]
    public void Compile_covered_cross_app_subscription_is_denied_when_the_observed_app_is_not_an_installed_owner()
    {
        var manifest = Manifest(Subscription("invoices", "invoices", Billing));

        var withoutSnapshot = AppSubscriptionCompiler.Compile(manifest, TenantId.Default, Ceiling(TreeException("a/billing/invoices")));
        var wrongOwner = AppSubscriptionCompiler.Compile(manifest, TenantId.Default, Ceiling(TreeException("a/billing/invoices")),
            AppTreeOwnerSnapshot.Create([new("a/billing/invoices", Notes)]));

        Assert.That(withoutSnapshot.Succeeded, Is.False);
        Assert.That(withoutSnapshot.Subscriptions, Is.Empty);
        var denial = withoutSnapshot.Denials.Single();
        Assert.That(denial.ObservedApp, Is.EqualTo(Billing));
        Assert.That(denial.Message, Does.Contain("not installed as the owner"));
        Assert.That(wrongOwner.Succeeded, Is.False);
    }

    [Test]
    public void Compile_own_and_adopted_subscriptions_ignore_the_owner_snapshot()
    {
        var manifest = Manifest(Subscription("docs-feed", "docs"));

        var result = AppSubscriptionCompiler.Compile(manifest, TenantId.Default, Ceiling(), AppTreeOwnerSnapshot.None);

        Assert.That(result.Succeeded, Is.True);
    }

    [Test]
    public void Compile_own_tree_subscription_needs_no_exception_entry()
    {
        var manifest = Manifest(Subscription("docs-feed", "docs"), Subscription("self-named", "audit", Notes, "log/"));

        var result = AppSubscriptionCompiler.Compile(manifest, TenantId.Default, Ceiling());

        Assert.That(result.Succeeded, Is.True);
        Assert.That(result.Denials, Is.Empty);
        Assert.That(result.Subscriptions, Has.Count.EqualTo(2));
        var docs = result.Subscriptions[0];
        Assert.Multiple(() =>
        {
            Assert.That(docs.Tenant, Is.EqualTo(TenantId.Default));
            Assert.That(docs.App, Is.EqualTo(Notes));
            Assert.That(docs.Name, Is.EqualTo("docs-feed"));
            Assert.That(docs.ObservedApp, Is.EqualTo(Notes));
            Assert.That(docs.Tree, Is.EqualTo("docs"));
            Assert.That(docs.LocalTreeId, Is.EqualTo("a/notes/docs"));
            Assert.That(docs.TreeId, Is.EqualTo("a/notes/docs"));
            Assert.That(docs.KeyPrefix, Is.Null);
            Assert.That(docs.IsCrossApp, Is.False);
            Assert.That(result.Subscriptions[1].TreeId, Is.EqualTo("a/notes/audit"));
            Assert.That(result.Subscriptions[1].KeyPrefix, Is.EqualTo("log/"));
            Assert.That(result.Subscriptions[1].IsCrossApp, Is.False);
        });
    }

    [Test]
    public void Compile_cross_app_subscription_is_permitted_when_an_exception_covers_it()
    {
        var manifest = Manifest(Subscription("invoices", "invoices", Billing));

        var result = AppSubscriptionCompiler.Compile(manifest, TenantId.Default, Ceiling(TreeException("a/billing/invoices")), BillingOwnsInvoices);

        Assert.That(result.Succeeded, Is.True);
        var subscription = result.Subscriptions.Single();
        Assert.Multiple(() =>
        {
            Assert.That(subscription.ObservedApp, Is.EqualTo(Billing));
            Assert.That(subscription.TreeId, Is.EqualTo("a/billing/invoices"));
            Assert.That(subscription.IsCrossApp, Is.True);
        });
    }

    [Test]
    public void Compile_cross_app_subscription_without_an_exception_is_denied_naming_the_observed_app()
    {
        var manifest = Manifest(Subscription("docs-feed", "docs"), Subscription("invoices", "invoices", Billing));

        var result = AppSubscriptionCompiler.Compile(manifest, TenantId.Default, Ceiling());

        Assert.That(result.Succeeded, Is.False);
        Assert.That(result.Subscriptions, Is.Empty, "a denial fails the whole subscription activation");
        var denial = result.Denials.Single();
        Assert.Multiple(() =>
        {
            Assert.That(denial.SubscriptionName, Is.EqualTo("invoices"));
            Assert.That(denial.ObservedApp, Is.EqualTo(Billing));
            Assert.That(denial.Scope, Is.EqualTo(new LatticeScope(LatticeScopeKind.Tree, "a/billing/invoices")));
            Assert.That(denial.Message, Does.Contain("'billing'"));
            Assert.That(denial.Message, Does.Contain("a/billing/invoices"));
        });
    }

    [Test]
    public void Compile_exception_on_a_different_tree_does_not_cover()
    {
        var manifest = Manifest(Subscription("invoices", "invoices", Billing));

        var result = AppSubscriptionCompiler.Compile(manifest, TenantId.Default, Ceiling(TreeException("a/billing/other")));

        Assert.That(result.Succeeded, Is.False);
    }

    [TestCase(LatticeScopeKind.Prefix, "inv/", "inv/2026/", true)]
    [TestCase(LatticeScopeKind.Prefix, "inv/2026/", "inv/", false)]
    [TestCase(LatticeScopeKind.Key, "inv/", "inv/", false)]
    [TestCase(LatticeScopeKind.Tree, null, "inv/", true)]
    public void Compile_prefix_subscription_follows_the_role_compiler_coverage_rule(
        LatticeScopeKind exceptionKind, string? exceptionKey, string subscriptionPrefix, bool covered)
    {
        var manifest = Manifest(Subscription("invoices", "invoices", Billing, subscriptionPrefix));
        var exception = new LatticeScope(exceptionKind, "a/billing/invoices", exceptionKey);

        var result = AppSubscriptionCompiler.Compile(manifest, TenantId.Default, Ceiling(exception), BillingOwnsInvoices);

        Assert.That(result.Succeeded, Is.EqualTo(covered));
        if (!covered)
            Assert.That(result.Denials.Single().Scope, Is.EqualTo(new LatticeScope(LatticeScopeKind.Prefix, "a/billing/invoices", subscriptionPrefix)));
    }

    [Test]
    public void Compile_prefix_exception_does_not_cover_a_whole_tree_subscription()
    {
        var manifest = Manifest(Subscription("invoices", "invoices", Billing));

        var result = AppSubscriptionCompiler.Compile(
            manifest, TenantId.Default, Ceiling(new LatticeScope(LatticeScopeKind.Prefix, "a/billing/invoices", "inv/")));

        Assert.That(result.Succeeded, Is.False);
    }

    [Test]
    public void Compile_composes_the_install_tenant_and_matches_tenant_local_exceptions()
    {
        var manifest = Manifest(Subscription("docs-feed", "docs"), Subscription("invoices", "invoices", Billing));

        var result = AppSubscriptionCompiler.Compile(manifest, Acme, Ceiling(TreeException("a/billing/invoices")),
            AppTreeOwnerSnapshot.Create([new("t/acme/a/billing/invoices", Billing)]));

        Assert.That(result.Succeeded, Is.True);
        Assert.That(result.Subscriptions.Select(s => s.TreeId), Is.EqualTo(new[] { "t/acme/a/notes/docs", "t/acme/a/billing/invoices" }));
        Assert.That(result.Subscriptions.Select(s => s.LocalTreeId), Is.EqualTo(new[] { "a/notes/docs", "a/billing/invoices" }));
        Assert.That(result.Subscriptions.All(s => s.Tenant == Acme), Is.True);
    }

    [Test]
    public void Compile_tenant_qualified_exception_never_matches()
    {
        var manifest = Manifest(Subscription("invoices", "invoices", Billing));

        var result = AppSubscriptionCompiler.Compile(manifest, Acme, Ceiling(TreeException("t/acme/a/billing/invoices")));

        Assert.That(result.Succeeded, Is.False);
    }

    [Test]
    public void Compile_adopted_tree_subscription_requires_an_exception_like_a_role_scope()
    {
        var manifest = Manifest(Notes, [Tree("legacy", adopted: "legacy-notes")], Subscription("legacy-feed", "legacy"));

        var denied = AppSubscriptionCompiler.Compile(manifest, TenantId.Default, Ceiling());
        var permitted = AppSubscriptionCompiler.Compile(manifest, TenantId.Default, Ceiling(TreeException("legacy-notes")));

        Assert.That(denied.Succeeded, Is.False);
        Assert.That(denied.Denials.Single().ObservedApp, Is.EqualTo(Notes));
        Assert.That(denied.Denials.Single().Message, Does.Contain("legacy-notes"));
        Assert.That(permitted.Succeeded, Is.True);
        Assert.That(permitted.Subscriptions.Single().TreeId, Is.EqualTo("legacy-notes"));
        Assert.That(permitted.Subscriptions.Single().IsCrossApp, Is.False);
    }

    [Test]
    public void Compile_manifest_without_subscriptions_succeeds_empty()
    {
        var result = AppSubscriptionCompiler.Compile(Manifest(), TenantId.Default, Ceiling());

        Assert.That(result.Succeeded, Is.True);
        Assert.That(result.Subscriptions, Is.Empty);
        Assert.That(result.Denials, Is.Empty);
    }

    [Test]
    public void Compile_rejects_null_and_uninitialised_arguments()
    {
        var manifest = Manifest();

        Assert.Throws<ArgumentNullException>(() => AppSubscriptionCompiler.Compile(null!, TenantId.Default, Ceiling()));
        Assert.Throws<ArgumentNullException>(() => AppSubscriptionCompiler.Compile(manifest, TenantId.Default, null!));
        Assert.Throws<ArgumentException>(() => AppSubscriptionCompiler.Compile(manifest, default, Ceiling()));
        Assert.Throws<ArgumentException>(() => AppSubscriptionCompiler.Compile(
            manifest with { Identity = new() { Slug = default, Version = V1 } }, TenantId.Default, Ceiling()));
        Assert.Throws<ArgumentException>(() => AppSubscriptionCompiler.Compile(
            manifest with { Subscriptions = [null!] }, TenantId.Default, Ceiling()));
    }

    [Test]
    public void ScopeCoverage_ignores_null_exceptions_and_is_not_a_wildcard()
    {
        var requested = new LatticeScope(LatticeScopeKind.Tree, "a/billing/invoices");

        Assert.That(AppSubscriptionScopeCoverage.IsCovered(requested, [null!]), Is.False);
        Assert.That(AppSubscriptionScopeCoverage.IsCovered(requested, [new LatticeScope(LatticeScopeKind.Tree, LatticeScope.ClusterWideTreeId)]), Is.False);
        Assert.That(AppSubscriptionScopeCoverage.IsCovered(requested, [TreeException("a/billing/invoices")]), Is.True);
        Assert.That(AppSubscriptionScopeCoverage.IsCovered(
            new LatticeScope(LatticeScopeKind.Key, "a/billing/invoices", "k"),
            [new LatticeScope(LatticeScopeKind.Key, "a/billing/invoices", "k")]), Is.True);
    }

    [Test]
    public void Denial_is_a_value_record()
    {
        var scope = TreeException("a/billing/invoices");
        var denial = new AppSubscriptionDenial("invoices", Billing, scope, "m");

        Assert.That(denial, Is.EqualTo(new AppSubscriptionDenial("invoices", Billing, scope, "m")));
        Assert.That(denial with { Message = "x" }, Is.Not.EqualTo(denial));
    }
}
