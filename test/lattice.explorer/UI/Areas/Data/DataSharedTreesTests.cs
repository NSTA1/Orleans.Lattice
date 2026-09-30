using Orleans.Lattice.Api.TenantAdmin;
using Orleans.Lattice.Explorer.UI.Areas.Data;
using Orleans.Lattice.Explorer.UI.Transport;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Data;

/// <summary>
/// How a tenant's received grants become its shared Data rows: only an approved
/// grant shares anything, only a scope inside the granting tenant's own trees is
/// listed, a prefix is one prefix row, and an address under a shared tree or
/// prefix resolves against the grant that covers it.
/// </summary>
[TestFixture]
public sealed class DataSharedTreesTests
{
    private static readonly HashSet<string> NoOwned = new(StringComparer.Ordinal);

    [Test]
    public void Only_an_approved_grant_to_this_tenant_is_listed_marked_with_its_owner_and_access()
    {
        var report = Report(
            Grant("acme", "t/acme/orders", TenantGrantLifecycleState.Active),
            Grant("acme", "t/acme/offered", TenantGrantLifecycleState.Pending),
            Grant("acme", "t/acme/rejected", TenantGrantLifecycleState.Rejected),
            Grant("acme", "t/acme/revoked", TenantGrantLifecycleState.Revoked),
            Grant("acme", "t/acme/elsewhere", TenantGrantLifecycleState.Active, grantee: "initech"));

        var result = DataSharedTrees.Build(report, "globex", NoOwned);

        var entry = result.Entries.Single();
        Assert.Multiple(() =>
        {
            Assert.That(entry.LogicalId, Is.EqualTo("t/acme/orders"));
            Assert.That(entry.StateId, Is.EqualTo("t/acme/orders"), "the granted id is read as it is, never re-rooted into globex");
            Assert.That(entry.Tenant, Is.EqualTo("globex"), "the address stays under the grantee's root");
            Assert.That(entry.Kind, Is.EqualTo(DataTreeKind.Tree));
            Assert.That(entry.IsShared, Is.True);
            Assert.That(entry.SharedBy, Is.EqualTo("acme"));
            Assert.That(entry.SharedText, Is.EqualTo("Shared by acme"));
            Assert.That(entry.AccessText, Is.EqualTo("Read only"));
            Assert.That(entry.KindText, Is.EqualTo("Shared tree"));
            Assert.That(entry.Address.Format(), Is.EqualTo("/t/globex/data/t/acme/orders"));
            Assert.That(result.Unreadable, Is.Zero);
        });
    }

    [Test]
    public void A_scope_outside_the_granting_tenants_trees_is_counted_not_listed()
    {
        var report = Report(
            Grant("acme", "orders", TenantGrantLifecycleState.Active),
            Grant("acme", "t/initech/orders", TenantGrantLifecycleState.Active),
            Grant("acme", "t/acme//orders", TenantGrantLifecycleState.Active),
            Grant("acme", "t/acme/sys-secrets", TenantGrantLifecycleState.Active),
            Grant("acme", "t/acme", TenantGrantLifecycleState.Active));

        var result = DataSharedTrees.Build(report, "globex", NoOwned);

        Assert.That(result.Entries, Is.Empty);
        Assert.That(result.Unreadable, Is.EqualTo(5));
    }

    [Test]
    public void A_self_grant_or_one_that_allows_nothing_is_ignored()
    {
        var report = Report(
            Grant("globex", "t/globex/orders", TenantGrantLifecycleState.Active),
            Grant("acme", "t/acme/orders", TenantGrantLifecycleState.Active, access: TenantGrantAccess.None));

        Assert.That(DataSharedTrees.Build(report, "globex", NoOwned).Entries, Is.Empty);
    }

    [Test]
    public void A_prefix_grant_is_one_prefix_row_and_rows_sort_by_id()
    {
        var report = Report(
            Grant("initech", "t/initech/stock", TenantGrantLifecycleState.Active, access: TenantGrantAccess.ReadWrite),
            Grant("acme", "t/acme/archive/", TenantGrantLifecycleState.Active),
            Grant("acme", "t/acme/", TenantGrantLifecycleState.Active, access: TenantGrantAccess.Write));

        var entries = DataSharedTrees.Build(report, "globex", NoOwned).Entries;

        Assert.Multiple(() =>
        {
            Assert.That(entries.Select(entry => entry.LogicalId), Is.EqualTo(new[] { "t/acme/", "t/acme/archive/", "t/initech/stock" }));
            Assert.That(entries.Select(entry => entry.Kind), Is.EqualTo(new[] { DataTreeKind.Prefix, DataTreeKind.Prefix, DataTreeKind.Tree }));
            Assert.That(entries.Select(entry => entry.AccessText), Is.EqualTo(new[] { "Write only", "Read only", "Read and write" }));
            Assert.That(entries[1].KindText, Is.EqualTo("Shared prefix"));
            Assert.That(entries[1].Address.Format(), Is.EqualTo("/t/globex/data"), "a prefix names no tree, so it has no workspace");
        });
    }

    [Test]
    public void A_shared_id_that_an_owned_tree_already_takes_is_dropped_and_a_repeated_scope_is_one_row()
    {
        var report = Report(
            Grant("acme", "t/acme/orders", TenantGrantLifecycleState.Active),
            Grant("acme", "t/acme/stock", TenantGrantLifecycleState.Active),
            Grant("acme", "t/acme/stock", TenantGrantLifecycleState.Active, access: TenantGrantAccess.Write));

        var entries = DataSharedTrees.Build(report, "globex", new HashSet<string>(["t/acme/orders"], StringComparer.Ordinal)).Entries;

        Assert.That(entries.Single().LogicalId, Is.EqualTo("t/acme/stock"));
        Assert.That(entries.Single().SharedAccess, Is.EqualTo(TenantGrantAccess.ReadWrite));
    }

    [Test]
    public void Cover_finds_the_shared_tree_or_the_grant_that_covers_a_tree_below_it()
    {
        var entries = DataSharedTrees.Build(
            Report(
                Grant("acme", "t/acme/orders", TenantGrantLifecycleState.Active),
                Grant("acme", "t/acme/archive/", TenantGrantLifecycleState.Active, access: TenantGrantAccess.ReadWrite)),
            "globex",
            NoOwned).Entries;

        var exact = DataSharedTrees.Cover(entries, "t/acme/orders");
        var child = DataSharedTrees.Cover(entries, "t/acme/orders/2024");
        var underPrefix = DataSharedTrees.Cover(entries, "t/acme/archive/2023");

        Assert.Multiple(() =>
        {
            Assert.That(exact, Is.SameAs(entries.Single(entry => entry.LogicalId == "t/acme/orders")));
            Assert.That((child!.LogicalId, child.StateId, child.Kind, child.SharedBy), Is.EqualTo(("t/acme/orders/2024", "t/acme/orders/2024", DataTreeKind.Tree, "acme")));
            Assert.That((underPrefix!.LogicalId, underPrefix.SharedAccess), Is.EqualTo(("t/acme/archive/2023", TenantGrantAccess.ReadWrite)));
            Assert.That(DataSharedTrees.Cover(entries, "t/acme/orders-archive"), Is.Null, "a sibling that shares a leading substring is not covered");
            Assert.That(DataSharedTrees.Cover(entries, "t/acme/archive"), Is.Null, "the prefix itself names no tree");
            Assert.That(DataSharedTrees.Cover(entries, "t/acme/archive/"), Is.Null);
            Assert.That(DataSharedTrees.Cover(entries, "t/acme/secret"), Is.Null);
            Assert.That(DataSharedTrees.Cover([], "t/acme/orders"), Is.Null);
        });
    }

    [Test]
    public void TryDescribeScope_reads_trees_and_prefixes_in_the_granting_tenant_only()
    {
        Assert.Multiple(() =>
        {
            Assert.That(DataSharedTrees.TryDescribeScope("acme", "t/acme/orders", out var tree, out var treeIsPrefix), Is.True);
            Assert.That((tree, treeIsPrefix), Is.EqualTo(("t/acme/orders", false)));
            Assert.That(DataSharedTrees.TryDescribeScope("acme", "t/acme/a/crm/", out var prefix, out var isPrefix), Is.True);
            Assert.That((prefix, isPrefix), Is.EqualTo(("t/acme/a/crm/", true)));
            Assert.That(DataSharedTrees.TryDescribeScope("acme", null, out _, out _), Is.False);
            Assert.That(DataSharedTrees.TryDescribeScope("acme", "t/acme/_lattice_x", out _, out _), Is.False);
            Assert.That(DataSharedTrees.TryDescribeScope("acme", "t/acmecorp/orders", out _, out _), Is.False);
            Assert.That(() => DataSharedTrees.TryDescribeScope("", "t/acme/orders", out _, out _), Throws.ArgumentException);
        });
    }

    [Test]
    public void The_access_text_and_the_notes_are_fixed_sentences()
    {
        Assert.Multiple(() =>
        {
            Assert.That(DataSharedTrees.AccessText(TenantGrantAccess.None), Is.EqualTo("No access"));
            Assert.That(DataSharedTrees.NotOfferedNote("globex"), Does.Contain("tenant globex are not listed"));
            Assert.That(DataSharedTrees.NoteFor(new UnauthorizedAccessException("secret detail"), "globex"), Is.EqualTo("Trees other tenants share with tenant globex are not listed: you cannot list its grants. Only its own trees are shown."));
            Assert.That(DataSharedTrees.NoteFor(new NotSupportedException(), "globex"), Is.EqualTo(DataSharedTrees.NotOfferedNote("globex")));
            Assert.That(DataSharedTrees.NoteFor(new ShellTransportException("boom", isTransient: true, new InvalidOperationException()), "globex"), Does.EndWith("Refresh to try again.").And.Not.Contain("boom"));
            Assert.That(DataSharedTrees.UnreadableNote(1), Does.StartWith("1 approved grant names"));
            Assert.That(DataSharedTrees.UnreadableNote(2), Does.StartWith("2 approved grants name"));
            Assert.That(() => DataSharedTrees.Build(null!, "globex", NoOwned), Throws.ArgumentNullException);
            Assert.That(() => DataSharedTrees.Cover(null!, "t/acme/orders"), Throws.ArgumentNullException);
        });
    }

    private static TenantGrantReport Report(params TenantGrantDescriptor[] received) => new()
    {
        TenantId = "globex",
        Issued = [],
        Received = received,
    };

    private static TenantGrantDescriptor Grant(
        string granter,
        string scope,
        TenantGrantLifecycleState state,
        TenantGrantAccess access = TenantGrantAccess.Read,
        string grantee = "globex") => new()
        {
            GranterTenantId = granter,
            GranteeTenantId = grantee,
            Scope = scope,
            Operations = access,
            State = state,
            GrantId = $"{granter}:{scope}",
        };
}
