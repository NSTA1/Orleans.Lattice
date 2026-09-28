namespace Orleans.Lattice.Apps.Tests;

public sealed partial class AppTreeOwnershipLedgerTests
{
    [Test]
    public async Task ReleaseAsync_releases_only_the_owners_live_claims()
    {
        Install(BillingOwner);
        await ClaimAsync(CrmOwner, Adopted("mine", "mine"));
        await ClaimAsync(BillingOwner, Adopted("theirs", "theirs"));

        await _ledger.ReleaseAsync(CrmOwner, ["mine", "theirs", "absent"], CancellationToken.None);

        Assert.That(_store.Peek("mine")!.Released, Is.True);
        Assert.That(_store.Peek("theirs")!.Released, Is.False);
        Assert.That(_store.Peek("absent"), Is.Null);
    }

    [Test]
    public async Task ReleaseAdoptedAsync_releases_adopted_claims_not_kept_and_never_structural_ones()
    {
        await ClaimAsync(CrmOwner, Adopted("kept", "kept"), Adopted("dropped", "dropped"), Structural(Crm, "contacts"));

        await _ledger.ReleaseAdoptedAsync(CrmOwner, new HashSet<string>(StringComparer.Ordinal) { "kept" }, CancellationToken.None);

        Assert.That(_store.Peek("kept")!.Released, Is.False);
        Assert.That(_store.Peek("dropped")!.Released, Is.True);
        Assert.That(_store.Peek("a/crm/contacts")!.Released, Is.False, "structural claims are held until purge");
    }

    [Test]
    public async Task A_released_claim_can_be_claimed_again()
    {
        await ClaimAsync(CrmOwner, Adopted("legacy", "legacy"));
        await _ledger.ReleaseAdoptedAsync(CrmOwner, keep: null, CancellationToken.None);

        var conflict = await ClaimAsync(BillingOwner, Adopted("legacy", "legacy"));

        Assert.That(conflict, Is.Null);
        Assert.That(_store.Peek("legacy")!.Slug, Is.EqualTo(Billing));
    }

    [Test]
    public async Task DescribeAsync_reports_every_conflict_without_writing()
    {
        Install(BillingOwner);
        await ClaimAsync(BillingOwner, Adopted("shared", "shared"));
        _facts.Registered.Add("a/crm/squatted");
        var writes = _store.AppliedWrites;

        var conflicts = await _ledger.DescribeAsync(
            CrmOwner, [Structural(Crm, "fresh"), Adopted("shared", "shared"), Structural(Crm, "squatted")], CancellationToken.None);

        Assert.That(conflicts.Select(c => (c.TreeName, c.Reason)), Is.EqualTo(new[]
        {
            ("shared", AppTreeOwnershipConflictReason.OwnedByAnotherApp),
            ("squatted", AppTreeOwnershipConflictReason.PreExistingUnownedTree),
        }));
        Assert.That(_store.AppliedWrites, Is.EqualTo(writes));
    }

    [Test]
    public async Task DescribeAsync_is_empty_for_the_owner_itself()
    {
        await ClaimAsync(CrmOwner, Structural(Crm, "contacts"));
        _facts.Registered.Add("a/crm/contacts");

        var conflicts = await _ledger.DescribeAsync(CrmOwner, [Structural(Crm, "contacts")], CancellationToken.None);

        Assert.That(conflicts, Is.Empty);
    }

    [Test]
    public async Task ResolveCrossAppOwnersAsync_returns_only_installed_owners_of_the_named_app()
    {
        Install(BillingOwner);
        await ClaimAsync(BillingOwner, Structural(Billing, "invoices"));
        var manifest = Manifest(Crm, Tree("contacts")) with
        {
            Roles =
            [
                new AppRoleDeclaration
                {
                    Name = "peer",
                    Operations = LatticeOperation.Read,
                    Scopes =
                    [
                        new AppScopeTemplate { Tree = "invoices", App = Billing },
                        new AppScopeTemplate { Tree = "unclaimed", App = Billing },
                        new AppScopeTemplate { Tree = "contacts" },
                    ],
                },
            ],
            Subscriptions = [new AppSubscriptionDeclaration { Name = "feed", Tree = "invoices", App = Billing }],
        };

        var owners = await _ledger.ResolveCrossAppOwnersAsync(manifest, Default, CancellationToken.None);

        Assert.That(owners.Count, Is.EqualTo(1));
        Assert.That(owners.IsOwnedBy("a/billing/invoices", Billing), Is.True);
        Assert.That(owners.IsOwnedBy("a/billing/unclaimed", Billing), Is.False);
    }

    [Test]
    public async Task ResolveCrossAppOwnersAsync_drops_an_owner_that_was_uninstalled()
    {
        Install(BillingOwner);
        await ClaimAsync(BillingOwner, Structural(Billing, "invoices"));
        _facts.Registered.Add("a/billing/invoices");
        Install(BillingOwner, AppRegistryLifecycleState.Uninstalled);
        var manifest = Manifest(Crm) with
        {
            Subscriptions = [new AppSubscriptionDeclaration { Name = "feed", Tree = "invoices", App = Billing }],
        };

        var owners = await _ledger.ResolveCrossAppOwnersAsync(manifest, Default, CancellationToken.None);

        Assert.That(owners, Is.SameAs(AppTreeOwnerSnapshot.None));
    }

    [Test]
    public async Task EvaluateAliasAsync_allows_everything_while_the_ledger_tree_does_not_exist()
    {
        await ClaimAsync(CrmOwner, Structural(Crm, "contacts"));

        var denial = await _ledger.EvaluateAliasAsync("mine", "a/crm/contacts", null, CancellationToken.None);

        Assert.That(denial, Is.Null, "no ledger tree means no claims could have been written through the real store");
    }

    [Test]
    public async Task EvaluateAliasAsync_allows_aliases_between_two_unowned_trees()
    {
        _facts.Registered.Add(AppRegistryTreeNames.TreeLedgerTree);

        Assert.That(await _ledger.EvaluateAliasAsync("left", "right", null, CancellationToken.None), Is.Null);
    }

    [Test]
    public async Task EvaluateAliasAsync_allows_a_resize_of_an_owned_tree_through_its_derivation()
    {
        _facts.Registered.Add(AppRegistryTreeNames.TreeLedgerTree);
        await ClaimAsync(CrmOwner, Structural(Crm, "contacts"));

        var denial = await _ledger.EvaluateAliasAsync("a/crm/contacts", "a/crm/contacts/resized/op-1", "a/crm/contacts", CancellationToken.None);

        Assert.That(denial, Is.Null);
    }

    [TestCase("mine", "a/crm/contacts", null, Description = "an unowned tree into an owned one")]
    [TestCase("a/crm/contacts", "elsewhere", null, Description = "an owned tree out to an unowned one")]
    [TestCase("mine", "a/crm/contacts/resized/op-1", "a/crm/contacts", Description = "into a derived copy of an owned tree")]
    [TestCase("a/crm/contacts", "a/billing/invoices", null, Description = "between two owners")]
    public async Task EvaluateAliasAsync_refuses_an_alias_that_crosses_an_ownership_boundary(string logical, string physical, string? derivedFrom)
    {
        _facts.Registered.Add(AppRegistryTreeNames.TreeLedgerTree);
        Install(BillingOwner);
        await ClaimAsync(CrmOwner, Structural(Crm, "contacts"));
        await ClaimAsync(BillingOwner, Structural(Billing, "invoices"));

        var denial = await _ledger.EvaluateAliasAsync(logical, physical, derivedFrom, CancellationToken.None);

        Assert.That(denial, Does.Contain("would cross an app ownership boundary"));
    }

    [Test]
    public async Task EvaluateAliasAsync_treats_a_released_or_uninstalled_adopted_claim_as_unowned()
    {
        _facts.Registered.Add(AppRegistryTreeNames.TreeLedgerTree);
        Install(CrmOwner);
        await ClaimAsync(CrmOwner, Adopted("legacy", "legacy"));
        Assert.That(await _ledger.EvaluateAliasAsync("mine", "legacy", null, CancellationToken.None), Is.Not.Null);

        Install(CrmOwner, AppRegistryLifecycleState.Uninstalled);

        Assert.That(await _ledger.EvaluateAliasAsync("mine", "legacy", null, CancellationToken.None), Is.Null);
    }

    [Test]
    public void Constructor_and_arguments_are_validated()
    {
        Assert.That(() => new AppTreeOwnershipLedger(null!, _facts, _registry), Throws.ArgumentNullException);
        Assert.That(() => new AppTreeOwnershipLedger(_store, null!, _registry), Throws.ArgumentNullException);
        Assert.That(() => new AppTreeOwnershipLedger(_store, _facts, null!), Throws.ArgumentNullException);
        Assert.That(() => AppTreeOwnershipLedger.Plan(null!, Default), Throws.ArgumentNullException);
        Assert.That(() => _ledger.EvaluateAliasAsync("", "p", null, CancellationToken.None), Throws.ArgumentException);
        Assert.That(() => _ledger.EvaluateAliasAsync("l", "", null, CancellationToken.None), Throws.ArgumentException);
    }
}
