namespace Orleans.Lattice.Apps.Tests;

/// <summary>Unit tests for <see cref="AppTreeOwnershipGuard"/>: the ledger's alias decision mapped onto the core seam.</summary>
[TestFixture]
public sealed class AppTreeOwnershipGuardTests
{
    private InMemoryAppTreeLedgerStore _ledger = null!;
    private FakeAppTreeFacts _facts = null!;
    private AppTreeOwnershipGuard _guard = null!;

    [SetUp]
    public void SetUp()
    {
        _ledger = new InMemoryAppTreeLedgerStore();
        _facts = new FakeAppTreeFacts();
        _facts.Registered.Add(AppRegistryTreeNames.TreeLedgerTree);
        _guard = new AppTreeOwnershipGuard(AppRegistryTestData.CreateLedger(new InMemoryAppRegistryStore(), _ledger, _facts));
        _ledger.Seed("a/crm/contacts", new AppTreeClaim
        {
            Tenant = TenantId.Default,
            Slug = AppSlug.Parse("crm"),
            Publisher = "first-party",
            Kind = AppTreeClaimKind.Structural,
        });
    }

    [Test]
    public async Task Allows_a_derived_copy_of_the_owned_tree()
    {
        var decision = await _guard.AuthorizeAliasAsync("a/crm/contacts", "a/crm/contacts/resized/op", "a/crm/contacts");

        Assert.That(decision.Allowed, Is.True);
        Assert.That(decision.Reason, Is.Null);
    }

    [Test]
    public async Task Allows_two_unowned_trees()
    {
        var decision = await _guard.AuthorizeAliasAsync("left", "right", null);

        Assert.That(decision.Allowed, Is.True);
    }

    [Test]
    public async Task Denies_an_alias_into_an_owned_tree_with_a_reason_naming_only_the_callers_ids()
    {
        var decision = await _guard.AuthorizeAliasAsync("mine", "a/crm/contacts", null);

        Assert.That(decision.Allowed, Is.False);
        Assert.That(decision.Reason, Is.EqualTo(
            "Aliasing tree 'mine' to 'a/crm/contacts' would cross an app ownership boundary: the tree and the target are not owned by the same app install."));
    }

    [Test]
    public void A_ledger_failure_propagates()
    {
        _ledger.FailGet = true;

        Assert.ThrowsAsync<InvalidOperationException>(async () => await _guard.AuthorizeAliasAsync("mine", "a/crm/contacts", null));
    }

    [Test]
    public void Constructor_rejects_a_null_ledger()
    {
        Assert.That(() => new AppTreeOwnershipGuard(null!), Throws.ArgumentNullException);
    }
}
