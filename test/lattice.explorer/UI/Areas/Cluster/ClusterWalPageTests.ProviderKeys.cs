using Bunit;
using Microsoft.AspNetCore.Components;
using Microsoft.AspNetCore.Components.Web;
using NSubstitute;
using Orleans.Lattice.Api.TreeAdmin;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;
using Orleans.Lattice.Explorer.UI.Design.Components;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Cluster;

public sealed partial class ClusterWalPageTests
{
    [Test]
    public async Task A_typed_key_is_accepted_when_the_audit_lists_no_provider_keys()
    {
        Admin.AuditWalPlacementAsync(TreeId, Arg.Any<CancellationToken>()).Returns(
            Audit(5, "blob-x") with { KnownProviderKeys = [] });

        await PlanTypedKeyAsync("blob-b", "no provider keys");
    }

    [Test]
    public async Task A_key_known_only_to_another_silo_is_accepted_and_planned()
    {
        Admin.PlanWalMoveAsync(TreeId, 1, "remote-only", Arg.Any<CancellationToken>()).Returns(new TreeWalMovePlan
        {
            TreeId = TreeId, Partition = 1, FromProviderKey = "blob-x", ToProviderKey = "remote-only",
            EntriesToCopy = 1200, TargetResolvableOnThisSilo = false, PlacementVersion = 5,
        });

        await PlanTypedKeyAsync("remote-only", "This silo does not list this key");

        var plan = RenderAt("/cluster/wal?tree=orders&partition=1&target=remote-only");
        plan.WaitUntil(() => Assert.That(plan.Find("[aria-labelledby='lt-cluster-wal-plan']").TextContent, Does.Contain("remote-only")
            .And.Contain("No: confirm the key resolves on every silo before moving")));
        await Admin.Received(1).PlanWalMoveAsync(TreeId, 1, "remote-only", Arg.Any<CancellationToken>());
    }

    private async Task PlanTypedKeyAsync(string target, string advisory)
    {
        UseTrees(Tree(TreeId));
        var cut = RenderAt("/cluster/wal?tree=orders");
        await cut.FireAsync(page => Button(page, "Plan a move...").ClickAsync(new MouseEventArgs()));
        cut.WaitUntil(() => Assert.That(cut.Find(".lt-dialog input").GetAttribute("value"), Is.EqualTo(TreeId)));

        await cut.FireAsync(page => page.FindAll(".lt-dialog input")[1].InputAsync(new ChangeEventArgs { Value = "1" }));
        await cut.FireAsync(page => page.FindAll(".lt-dialog input")[2].InputAsync(new ChangeEventArgs { Value = target }));
        var field = cut.FindComponents<LtComboBox>().Single(box => box.Instance.Label == "Target provider key");
        await cut.InvokeAsync(() => field.Instance.ConfirmAsync());
        cut.WaitUntil(() =>
        {
            Assert.That(field.Markup, Does.Contain(advisory));
            Assert.That(field.Find("input").HasAttribute("aria-invalid"), Is.False);
            var describedBy = field.Find("input").GetAttribute("aria-describedby")!.Split(' ');
            Assert.That(describedBy.Any(id => field.Find("#" + id).TextContent.Contains(advisory, StringComparison.Ordinal)), Is.True);
        });

        await cut.FireAsync(page => page.Find(".lt-dialog form").SubmitAsync());
        cut.WaitUntil(() => Assert.That(Navigation.Uri, Does.EndWith($"/cluster/wal?tree=orders&partition=1&target={target}")));
    }
}
