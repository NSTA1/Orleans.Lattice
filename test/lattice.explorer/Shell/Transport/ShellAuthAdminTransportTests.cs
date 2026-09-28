using Orleans.Lattice.Api.Auth;
using Orleans.Lattice.Auth;
using Orleans.Lattice.Membership;

namespace Orleans.Lattice.Explorer.Tests.Shell.Transport;

/// <summary>The Shell's <see cref="ILatticeAuthAdmin"/> transport adapter.</summary>
[TestFixture]
public sealed class ShellAuthAdminTransportTests : ShellTransportAdapterContractTests<ILatticeAuthAdmin>
{
    private const string Service = "/orleans.lattice.api.auth/";

    private static readonly LatticeAuthorizationRule Rule =
        new("r1", LatticeSubjectSelector.User("alice"), LatticeScope.Tree("orders"), LatticeOperation.Read, LatticeEffect.Allow);

    internal override IEnumerable<ShellTransportCall<ILatticeAuthAdmin>> Calls() =>
    [
        new("UpsertGroupAsync", Service + "UpsertGroup", (f, ct) => f.UpsertGroupAsync(new AuthGroup { GroupId = "admins" }, ct)),
        new("GetGroupAsync", Service + "GetGroup", (f, ct) => f.GetGroupAsync("admins", ct)),
        new("RemoveGroupAsync", Service + "RemoveGroup", (f, ct) => f.RemoveGroupAsync("admins", ct)),
        new("ListGroupsAsync", Service + "ListGroups", (f, ct) => f.ListGroupsAsync(new AuthPageRequest(), ct)),
        new("AddMemberAsync", Service + "AddMember", (f, ct) => f.AddMemberAsync("admins", "alice", MembershipMemberKind.User, ct)),
        new("RemoveMemberAsync", Service + "RemoveMember", (f, ct) => f.RemoveMemberAsync("admins", "alice", ct)),
        new("ListGroupMembersAsync", Service + "ListGroupMembers", (f, ct) => f.ListGroupMembersAsync("admins", ct)),
        new("ListSubjectGroupsAsync", Service + "ListSubjectGroups", (f, ct) => f.ListSubjectGroupsAsync("alice", ct)),
        new("PutRuleAsync", Service + "PutRule", (f, ct) => f.PutRuleAsync(Rule, ct)),
        new("GetRuleAsync", Service + "GetRule", (f, ct) => f.GetRuleAsync("orders", "r1", ct)),
        new("RemoveRuleAsync", Service + "RemoveRule", (f, ct) => f.RemoveRuleAsync("orders", "r1", ct)),
        new("ListRulesAsync", Service + "ListRules", (f, ct) => f.ListRulesAsync(new AuthPageRequest(), ct)),
        new("ListRulesForTreeAsync", Service + "ListRulesForTree", (f, ct) => f.ListRulesForTreeAsync("orders", new AuthPageRequest(), ct)),
        new("ExplainAsync", Service + "Explain", (f, ct) => f.ExplainAsync("alice", LatticeOperation.Read, LatticeScope.Tree("orders"), LatticeSubjectSelectorKind.User, ct)),
        new("EffectivePermissionsAsync", Service + "EffectivePermissions", (f, ct) => f.EffectivePermissionsAsync("alice", LatticeSubjectSelectorKind.Group, ct)),
        new("SearchDirectoryAsync", Service + "SearchDirectory", (f, ct) => f.SearchDirectoryAsync(new DirectorySearchRequest { Term = "al" }, ct)),
        new("ResolveDirectoryPrincipalAsync", Service + "ResolveDirectoryPrincipal", (f, ct) => f.ResolveDirectoryPrincipalAsync("alice", ct)),
        new("GetAccessModelAsync", Service + "GetAccessModel", (f, ct) => f.GetAccessModelAsync(ct)),
    ];

    [Test]
    public void Argument_guards_run_before_any_call()
    {
        using var circuit = new ShellTransportCircuit();
        var admin = circuit.Resolve<ILatticeAuthAdmin>();

        Assert.Multiple(() =>
        {
            Assert.That(() => admin.UpsertGroupAsync(null!), Throws.ArgumentNullException);
            Assert.That(() => admin.GetGroupAsync(string.Empty), Throws.ArgumentException);
            Assert.That(() => admin.RemoveGroupAsync(string.Empty), Throws.ArgumentException);
            Assert.That(() => admin.ListGroupsAsync(null!), Throws.ArgumentNullException);
            Assert.That(() => admin.AddMemberAsync("g", string.Empty), Throws.ArgumentException);
            Assert.That(() => admin.RemoveMemberAsync(string.Empty, "m"), Throws.ArgumentException);
            Assert.That(() => admin.ListGroupMembersAsync(string.Empty), Throws.ArgumentException);
            Assert.That(() => admin.ListSubjectGroupsAsync(string.Empty), Throws.ArgumentException);
            Assert.That(() => admin.PutRuleAsync(null!), Throws.ArgumentNullException);
            Assert.That(() => admin.GetRuleAsync("t", string.Empty), Throws.ArgumentException);
            Assert.That(() => admin.RemoveRuleAsync(string.Empty, "r"), Throws.ArgumentException);
            Assert.That(() => admin.ListRulesAsync(null!), Throws.ArgumentNullException);
            Assert.That(() => admin.ListRulesForTreeAsync("t", null!), Throws.ArgumentNullException);
            Assert.That(() => admin.ExplainAsync("s", LatticeOperation.Read, null!), Throws.ArgumentNullException);
            Assert.That(() => admin.EffectivePermissionsAsync(string.Empty), Throws.ArgumentException);
            Assert.That(() => admin.SearchDirectoryAsync(null!), Throws.ArgumentNullException);
            Assert.That(() => admin.ResolveDirectoryPrincipalAsync(string.Empty), Throws.ArgumentException);
            Assert.That(circuit.Peer.Requests, Is.Empty);
        });
    }

    [Test]
    public async Task A_group_page_arrives_intact()
    {
        using var circuit = new ShellTransportCircuit();
        var admin = circuit.Resolve<ILatticeAuthAdmin>();
        var page = new AuthGroupPage { Entries = [new AuthGroup { GroupId = "admins" }], NextPageToken = "next" };
        circuit.Peer.Respond(Service + "ListGroups", page);
        circuit.Peer.AnswerWithSuccess();

        var result = await admin.ListGroupsAsync(new AuthPageRequest());

        Assert.Multiple(() =>
        {
            Assert.That(result.Entries.Select(group => group.GroupId), Is.EqualTo(new[] { "admins" }));
            Assert.That(result.NextPageToken, Is.EqualTo("next"));
        });
    }
}
