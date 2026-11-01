using Bunit;
using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice.Api.Schema;
using Orleans.Lattice.Api.State;
using Orleans.Lattice.Api.TreeAdmin;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;
using Orleans.Lattice.Explorer.UI.Areas.Data;
using Orleans.Lattice.Explorer.UI.Design.Components;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Data;

/// <summary>
/// A tree's lifecycle on its Data page (issue #5477): Delete tree... and Recover tree... are
/// drawn only for a caller the tree-administration probe lets manage the tree's lifecycle,
/// run only after the typed confirmation, call the facade with the logical tree id, and
/// report a refusal in the Explorer's words. A view, a shared tree and a caller without the
/// grant see no lifecycle control.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class DataTreeLifecycleTests : DataTestContext
{
    private const string Tree = "factory-history";

    private IReadOnlyList<string> Toasts => Services.GetRequiredService<LtToastService>().Toasts.Select(toast => toast.Message).ToArray();

    [Test]
    public void A_caller_without_the_lifecycle_grant_sees_no_delete()
    {
        Client.WithTree(Tree, keys: 1);
        AdministeredTrees.Add(Tree);

        var cut = RenderAt("data/" + Tree);

        cut.WaitUntil(() => Assert.That(cut.Find("h1").TextContent, Is.EqualTo(Tree)));
        Admin.Received().ProbeCapabilitiesAsync(Tree, Arg.Any<CancellationToken>());
        Assert.That(cut.FindAll("[data-lt-data-tree-lifecycle]"), Is.Empty, "administering a tree is not managing its lifecycle");
    }

    [Test]
    public void A_view_offers_no_lifecycle_even_to_a_caller_who_may_manage_its_source()
    {
        Client.WithTree("orders");
        Client.Views.Add(new ViewStateSummary { ViewName = "by-status", SourceTreeId = "orders" });
        Client.Entries["view-by-status"] = new(StringComparer.Ordinal);
        GrantLifecycle();

        var cut = RenderAt("data/view-by-status");

        cut.WaitUntil(() => Assert.That(cut.Find("h1").TextContent, Is.EqualTo("by-status")));
        Assert.That(cut.FindAll("[data-lt-data-tree-lifecycle]"), Is.Empty);
    }

    [Test]
    public void A_probe_fault_draws_nothing()
    {
        Client.WithTree(Tree, keys: 1);
        Admin.ProbeCapabilitiesAsync(Arg.Any<string>(), Arg.Any<CancellationToken>()).ThrowsAsync(new InvalidOperationException("down"));

        var cut = RenderAt("data/" + Tree);

        cut.WaitUntil(() => Assert.That(cut.Find("h1").TextContent, Is.EqualTo(Tree)));
        Assert.That(cut.FindAll("[data-lt-data-tree-lifecycle]"), Is.Empty);
    }

    [Test]
    public void Delete_runs_only_after_the_confirmation_and_then_offers_recovery()
    {
        Client.WithTree(Tree, keys: 1);
        GrantLifecycle();
        var deadline = new DateTimeOffset(2026, 11, 7, 12, 0, 0, TimeSpan.Zero);
        Admin.DeleteTreeAsync(Tree, Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(new TreeDeletionStatus { TreeId = Tree, IsDeleted = true, CanRecover = true, RecoveryDeadlineUtc = deadline }));
        var cut = RenderAt("data/" + Tree);
        cut.WaitUntil(() => Assert.That(cut.FindAll("[data-lt-data-tree-lifecycle=delete]"), Has.Count.EqualTo(1)));
        Assert.That(cut.Find("[data-lt-data-tree-lifecycle=delete]").TextContent.Trim(), Is.EqualTo("Delete tree..."));

        cut.Find("[data-lt-data-tree-lifecycle=delete]").Click();
        cut.WaitUntil(() => Assert.That(cut.Find(".lt-dialog h2").TextContent, Is.EqualTo("Delete this tree?")));
        Admin.DidNotReceiveWithAnyArgs().DeleteTreeAsync(default!, default);

        Confirm(cut, Tree);

        cut.WaitUntil(() =>
        {
            Assert.That(Toasts, Is.EqualTo(new[] { $"Tree {Tree} deleted. It can be recovered until its window closes." }));
            Assert.That(cut.Find("[data-lt-tree-deleted]").GetAttribute("data-lt-tree-deleted"), Is.EqualTo("recoverable"));
            Assert.That(cut.Find("[data-lt-tree-deleted] h2").TextContent, Is.EqualTo($"{Tree} is deleted"));
            Assert.That(cut.FindAll("[role=tab]"), Is.Empty, "a deleted tree serves no reads, so its tabs give way");
            Assert.That(cut.FindAll("[data-lt-data-tree-lifecycle=delete]"), Is.Empty);
            Assert.That(cut.FindAll("[data-lt-data-tree-lifecycle=recover]"), Has.Count.EqualTo(1));
        });
        Admin.Received(1).DeleteTreeAsync(Tree, Arg.Any<CancellationToken>());
    }

    [Test]
    public void Recover_brings_the_tabs_back()
    {
        Client.WithTree(Tree, keys: 1);
        GrantLifecycle();
        Admin.DeleteTreeAsync(Tree, Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(new TreeDeletionStatus { TreeId = Tree, IsDeleted = true, CanRecover = true }));
        Admin.RecoverTreeAsync(Tree, Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(new TreeDeletionStatus { TreeId = Tree }));
        var cut = RenderAt("data/" + Tree);
        cut.WaitUntil(() => Assert.That(cut.FindAll("[data-lt-data-tree-lifecycle=delete]"), Has.Count.EqualTo(1)));
        cut.Find("[data-lt-data-tree-lifecycle=delete]").Click();
        Confirm(cut, Tree);
        cut.WaitUntil(() => Assert.That(cut.FindAll("[data-lt-data-tree-lifecycle=recover]"), Has.Count.EqualTo(1)));

        cut.Find("[data-lt-data-tree-lifecycle=recover]").Click();
        cut.WaitUntil(() => Assert.That(cut.Find(".lt-dialog h2").TextContent, Is.EqualTo("Recover this tree?")));
        Confirm(cut, Tree);

        cut.WaitUntil(() =>
        {
            Assert.That(Toasts, Does.Contain($"Tree {Tree} recovered."));
            Assert.That(cut.FindAll("[data-lt-tree-deleted]"), Is.Empty);
            Assert.That(cut.FindAll("[role=tab]").Select(tab => tab.TextContent), Does.Contain("Keys"));
            Assert.That(cut.FindAll("[data-lt-data-tree-lifecycle=delete]"), Has.Count.EqualTo(1));
        });
        Admin.Received(1).RecoverTreeAsync(Tree, Arg.Any<CancellationToken>());
    }

    [Test]
    public void A_refused_delete_is_said_in_the_explorers_words_and_the_tree_stays()
    {
        Client.WithTree(Tree, keys: 1);
        GrantLifecycle();
        Admin.DeleteTreeAsync(Tree, Arg.Any<CancellationToken>()).ThrowsAsync(new UnauthorizedAccessException("TreeLifecycle on factory-history"));
        var cut = RenderAt("data/" + Tree);
        cut.WaitUntil(() => Assert.That(cut.FindAll("[data-lt-data-tree-lifecycle=delete]"), Has.Count.EqualTo(1)));

        cut.Find("[data-lt-data-tree-lifecycle=delete]").Click();
        Confirm(cut, Tree);

        cut.WaitUntil(() => Assert.That(Toasts, Is.EqualTo(new[] { "You do not have permission to delete this tree." })));
        Assert.Multiple(() =>
        {
            Assert.That(cut.FindAll("[data-lt-tree-deleted]"), Is.Empty);
            Assert.That(cut.FindAll("[role=tab]"), Is.Not.Empty);
            Assert.That(cut.Markup, Does.Not.Contain("TreeLifecycle on"), "the facade's detail never reaches the page");
        });
    }

    [Test]
    public void A_soft_deleted_tree_opened_again_shows_its_deletion_and_the_way_back()
    {
        Client.Trees.Add(FakeStateClient.Tree(Tree, lifecycle: TreeLifecycleState.SoftDeleted));
        Client.Entries[Tree] = new(StringComparer.Ordinal);
        GrantLifecycle();
        Admin.GetTreeDeletionStatusAsync(Tree, Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(new TreeDeletionStatus { TreeId = Tree, IsDeleted = true, CanRecover = true }));

        var cut = RenderAt("data/" + Tree);

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find("[data-lt-tree-deleted]").GetAttribute("data-lt-tree-deleted"), Is.EqualTo("recoverable"));
            Assert.That(cut.FindAll("[data-lt-data-tree-lifecycle=recover]"), Has.Count.EqualTo(1));
            Assert.That(cut.FindAll("[role=tab]"), Is.Empty);
        });
    }

    [TestCase(true, true, "recovered until")]
    [TestCase(true, false, "until the cluster purges it")]
    [TestCase(false, false, "can no longer be recovered")]
    public void The_deleted_text_says_whether_and_until_when_the_tree_can_be_recovered(bool canRecover, bool deadline, string expected)
    {
        var status = new TreeDeletionStatus
        {
            TreeId = Tree,
            IsDeleted = true,
            CanRecover = canRecover,
            RecoveryDeadlineUtc = deadline ? new DateTimeOffset(2026, 11, 7, 12, 0, 0, TimeSpan.Zero) : null,
        };

        Assert.That(DataTreePage.DeletedText(status), Does.Contain(expected));
    }

    private void GrantLifecycle() =>
        Admin.ProbeCapabilitiesAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(call => Task.FromResult(new LatticeTreeAdminCapabilities
            {
                TreeId = call.Arg<string>(),
                CanManageTreeLifecycle = true,
                Schema = new LatticeSchemaCapabilities { TreeId = call.Arg<string>() },
            }));
}
