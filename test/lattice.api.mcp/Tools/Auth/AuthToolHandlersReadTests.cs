using NSubstitute;
using Orleans.Lattice.Api.Auth;
using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Api.Mcp.Tests;

/// <summary>
/// Unit tests for the <see cref="AuthToolHandlers"/> read and removal adapters the
/// sibling <see cref="AuthToolHandlersTests"/> does not drive: the single-record
/// group and rule reads, the three paged catalog reads (which assemble an
/// <see cref="AuthPageRequest"/> from two loose arguments), and the two removal
/// edges. Each proves the handler marshals its tool-call arguments into the
/// facade's model type and forwards the call verbatim, re-implementing no read,
/// paging, or authorization logic of its own.
/// </summary>
/// <remarks>
/// The paged reads matter most: <c>pageSize</c> and <c>pageToken</c> arrive as
/// separate tool arguments and are folded into one request object, so a handler
/// that dropped the cursor would silently restart every page from the beginning
/// while still returning well-formed results. Deterministic against a substituted
/// facade - no cluster.
/// </remarks>
[TestFixture]
public sealed class AuthToolHandlersReadTests
{
    private static ILatticeAuthAdmin Admin() => Substitute.For<ILatticeAuthAdmin>();

    [Test]
    public async Task GetGroupAsync_forwards_the_group_id()
    {
        var admin = Admin();
        var expected = new AuthGroup { GroupId = "ops", DisplayName = "Operations" };
        admin.GetGroupAsync("ops", Arg.Any<CancellationToken>()).Returns(expected);

        var result = await AuthToolHandlers.GetGroupAsync(admin, "ops", CancellationToken.None);

        Assert.That(result, Is.SameAs(expected));
    }

    [Test]
    public async Task GetGroupAsync_passes_an_absent_group_through_as_null()
    {
        var admin = Admin();
        admin.GetGroupAsync("missing", Arg.Any<CancellationToken>()).Returns((AuthGroup?)null);

        var result = await AuthToolHandlers.GetGroupAsync(admin, "missing", CancellationToken.None);

        Assert.That(result, Is.Null, "An absent group is a null read, never a fault.");
    }

    [Test]
    public async Task ListGroupsAsync_folds_the_page_size_and_cursor_into_one_request()
    {
        var admin = Admin();
        var expected = new AuthGroupPage { Entries = [new AuthGroup { GroupId = "ops" }], NextPageToken = "next" };
        admin.ListGroupsAsync(Arg.Any<AuthPageRequest>(), Arg.Any<CancellationToken>()).Returns(expected);

        var result = await AuthToolHandlers.ListGroupsAsync(admin, 25, "cursor", CancellationToken.None);

        Assert.That(result, Is.SameAs(expected));
        await admin.Received(1).ListGroupsAsync(
            Arg.Is<AuthPageRequest>(r => r.PageSize == 25 && r.PageToken == "cursor"),
            Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task ListGroupsAsync_defaults_to_an_unbounded_first_page()
    {
        var admin = Admin();
        admin.ListGroupsAsync(Arg.Any<AuthPageRequest>(), Arg.Any<CancellationToken>())
            .Returns(new AuthGroupPage());

        await AuthToolHandlers.ListGroupsAsync(admin);

        await admin.Received(1).ListGroupsAsync(
            Arg.Is<AuthPageRequest>(r => r.PageSize == 0 && r.PageToken == null),
            Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task GetRuleAsync_forwards_the_tree_and_rule_id()
    {
        var admin = Admin();
        var expected = new LatticeAuthorizationRule(
            ruleId: "r1",
            subject: LatticeSubjectSelector.User("alice"),
            scope: LatticeScope.Tree("orders"),
            operations: LatticeOperation.Read,
            effect: LatticeEffect.Allow);
        admin.GetRuleAsync("orders", "r1", Arg.Any<CancellationToken>()).Returns(expected);

        var result = await AuthToolHandlers.GetRuleAsync(admin, "orders", "r1", CancellationToken.None);

        Assert.That(result, Is.SameAs(expected));
    }

    [Test]
    public async Task GetRuleAsync_passes_an_absent_rule_through_as_null()
    {
        var admin = Admin();
        admin.GetRuleAsync("orders", "missing", Arg.Any<CancellationToken>())
            .Returns((LatticeAuthorizationRule?)null);

        var result = await AuthToolHandlers.GetRuleAsync(admin, "orders", "missing", CancellationToken.None);

        Assert.That(result, Is.Null);
    }

    [Test]
    public async Task ListRulesAsync_folds_the_page_size_and_cursor_into_one_request()
    {
        var admin = Admin();
        var expected = new AuthRulePage { NextPageToken = "next" };
        admin.ListRulesAsync(Arg.Any<AuthPageRequest>(), Arg.Any<CancellationToken>()).Returns(expected);

        var result = await AuthToolHandlers.ListRulesAsync(admin, 10, "cursor", CancellationToken.None);

        Assert.That(result, Is.SameAs(expected));
        await admin.Received(1).ListRulesAsync(
            Arg.Is<AuthPageRequest>(r => r.PageSize == 10 && r.PageToken == "cursor"),
            Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task ListRulesForTreeAsync_forwards_the_tree_alongside_the_page_request()
    {
        var admin = Admin();
        var expected = new AuthRulePage();
        admin.ListRulesForTreeAsync("orders", Arg.Any<AuthPageRequest>(), Arg.Any<CancellationToken>())
            .Returns(expected);

        var result = await AuthToolHandlers.ListRulesForTreeAsync(admin, "orders", 5, "cursor", CancellationToken.None);

        Assert.That(result, Is.SameAs(expected));
        await admin.Received(1).ListRulesForTreeAsync(
            "orders",
            Arg.Is<AuthPageRequest>(r => r.PageSize == 5 && r.PageToken == "cursor"),
            Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task RemoveGroupAsync_forwards_the_group_id()
    {
        var admin = Admin();

        await AuthToolHandlers.RemoveGroupAsync(admin, "ops", CancellationToken.None);

        await admin.Received(1).RemoveGroupAsync("ops", Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task RemoveMemberAsync_forwards_both_ends_of_the_membership_edge()
    {
        var admin = Admin();

        await AuthToolHandlers.RemoveMemberAsync(admin, "ops", "alice", CancellationToken.None);

        await admin.Received(1).RemoveMemberAsync("ops", "alice", Arg.Any<CancellationToken>());
    }

    [Test]
    public void Every_read_and_removal_handler_rejects_a_null_facade()
    {
        Assert.Multiple(() =>
        {
            Assert.That(() => AuthToolHandlers.ListGroupsAsync(null!), Throws.ArgumentNullException);
            Assert.That(() => AuthToolHandlers.ListGroupMembersAsync(null!, "g"), Throws.ArgumentNullException);
            Assert.That(() => AuthToolHandlers.ListSubjectGroupsAsync(null!, "m"), Throws.ArgumentNullException);
            Assert.That(() => AuthToolHandlers.GetRuleAsync(null!, "t", "r"), Throws.ArgumentNullException);
            Assert.That(() => AuthToolHandlers.ListRulesAsync(null!), Throws.ArgumentNullException);
            Assert.That(() => AuthToolHandlers.ListRulesForTreeAsync(null!, "t"), Throws.ArgumentNullException);
            Assert.That(() => AuthToolHandlers.RemoveGroupAsync(null!, "g"), Throws.ArgumentNullException);
            Assert.That(() => AuthToolHandlers.RemoveMemberAsync(null!, "g", "m"), Throws.ArgumentNullException);
            Assert.That(() => AuthToolHandlers.AddMemberAsync(null!, "g", "m"), Throws.ArgumentNullException);
            Assert.That(
                () => AuthToolHandlers.EffectivePermissionsAsync(null!, "s"),
                Throws.ArgumentNullException);
        });
    }
}
