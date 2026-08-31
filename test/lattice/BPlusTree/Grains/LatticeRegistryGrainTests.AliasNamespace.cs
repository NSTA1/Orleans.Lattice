using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Regression coverage for the alias namespace-escalation guard on
/// <c>LatticeRegistryGrain.SetAliasAsync</c>.
/// <para>
/// An alias transplants a logical tree identity onto another tree's physical
/// shards. Every data-plane access gate lives on the <c>ILattice</c> facade and
/// is evaluated against the <em>logical</em> id before the alias is resolved,
/// and the physical shard and leaf grains enforce no policy of their own.
/// Before this guard existed, a caller holding admin rights on any ordinary
/// tree could point it at <c>sys-</c>-prefixed authorization, membership or
/// tenant-registry state, at the internal <c>_lattice_</c> namespace, or at
/// another tenant's trees, and then read and rewrite that state through the
/// facade - the gates would only ever see the ordinary logical id.
/// </para>
/// <para>
/// The guard is namespace-preserving rather than a flat deny-list so that the
/// internal maintenance flows which derive the physical id from the logical one
/// (resize, schema remediation, backup shadow restore) keep working for system
/// and tenant trees alike.
/// </para>
/// </summary>
public partial class LatticeRegistryGrainTests
{
    private const string OrdinaryTree = "my-tree";
    private const string SystemDataTree = "sys-auth-policy";

    [Test]
    public void SetAliasAsync_refuses_an_ordinary_tree_aliased_onto_the_system_data_namespace()
    {
        var (grain, tree) = CreateGrain();
        tree.GetAsync(Arg.Any<string>()).Returns(Task.FromResult<byte[]?>(null));

        Assert.ThrowsAsync<ArgumentException>(
            () => grain.SetAliasAsync(OrdinaryTree, SystemDataTree));
    }

    [Test]
    public async Task SetAliasAsync_writes_nothing_when_it_refuses_an_escalating_target()
    {
        var (grain, tree) = CreateGrain();
        tree.GetAsync(Arg.Any<string>()).Returns(Task.FromResult<byte[]?>(null));

        Assert.ThrowsAsync<ArgumentException>(
            () => grain.SetAliasAsync(OrdinaryTree, SystemDataTree));

        await tree.DidNotReceive().SetAsync(Arg.Any<string>(), Arg.Any<byte[]>());
    }

    [Test]
    public void SetAliasAsync_refuses_a_target_in_the_internal_lattice_namespace()
    {
        var (grain, tree) = CreateGrain();
        tree.GetAsync(Arg.Any<string>()).Returns(Task.FromResult<byte[]?>(null));

        Assert.ThrowsAsync<ArgumentException>(
            () => grain.SetAliasAsync(OrdinaryTree, LatticeConstants.RegistryTreeId));
    }

    [Test]
    public void SetAliasAsync_refuses_an_alias_that_crosses_a_tenant_boundary()
    {
        var (grain, tree) = CreateGrain();
        tree.GetAsync(Arg.Any<string>()).Returns(Task.FromResult<byte[]?>(null));

        Assert.ThrowsAsync<ArgumentException>(
            () => grain.SetAliasAsync("t/acme/orders", "t/contoso/orders"));
    }

    [Test]
    public void SetAliasAsync_refuses_an_untenanted_tree_aliased_into_a_tenant_namespace()
    {
        var (grain, tree) = CreateGrain();
        tree.GetAsync(Arg.Any<string>()).Returns(Task.FromResult<byte[]?>(null));

        Assert.ThrowsAsync<ArgumentException>(
            () => grain.SetAliasAsync(OrdinaryTree, "t/acme/orders"));
    }

    [Test]
    public async Task SetAliasAsync_allows_a_system_tree_aliased_within_its_own_namespace()
    {
        // The resize and remediation flows derive the physical id from the
        // logical one, so a system tree's maintenance target stays legal.
        var (grain, tree) = CreateGrain();
        tree.GetAsync(Arg.Any<string>()).Returns(Task.FromResult<byte[]?>(null));

        await grain.SetAliasAsync(SystemDataTree, $"{SystemDataTree}/resized/op1");

        await tree.Received().SetAsync(SystemDataTree, Arg.Any<byte[]>());
    }

    [Test]
    public async Task SetAliasAsync_allows_a_tenant_tree_aliased_within_its_own_tenant()
    {
        var (grain, tree) = CreateGrain();
        tree.GetAsync(Arg.Any<string>()).Returns(Task.FromResult<byte[]?>(null));

        await grain.SetAliasAsync("t/acme/orders", "t/acme/orders/resized/op1");

        await tree.Received().SetAsync("t/acme/orders", Arg.Any<byte[]>());
    }

    [Test]
    public async Task SetAliasAsync_allows_a_system_origin_caller_to_target_the_system_data_namespace()
    {
        // Library-internal maintenance paths are gated at their own entry point
        // and legitimately repoint trees across namespaces.
        var (grain, tree) = CreateGrain();
        tree.GetAsync(Arg.Any<string>()).Returns(Task.FromResult<byte[]?>(null));

        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            await grain.SetAliasAsync(OrdinaryTree, SystemDataTree);
        }

        await tree.Received().SetAsync(OrdinaryTree, Arg.Any<byte[]>());
    }

    [Test]
    public async Task SetAliasAsync_still_allows_an_ordinary_target()
    {
        var (grain, tree) = CreateGrain();
        tree.GetAsync(Arg.Any<string>()).Returns(Task.FromResult<byte[]?>(null));

        await grain.SetAliasAsync(OrdinaryTree, "physical-tree");

        await tree.Received().SetAsync(OrdinaryTree, Arg.Any<byte[]>());
    }

    [Test]
    public void SetAliasAsync_refusal_precedes_the_single_level_indirection_check()
    {
        // The escalation guard must not be reachable only after an unrelated
        // validation passes: a reserved target that is itself aliased is still
        // refused as an escalation, and either way nothing is written.
        var (grain, tree) = CreateGrain();
        var targetEntry = new TreeRegistryEntry { PhysicalTreeId = "another-tree" };
        var targetBytes = System.Text.Json.JsonSerializer.SerializeToUtf8Bytes(targetEntry);
        tree.GetAsync(SystemDataTree).Returns(Task.FromResult<byte[]?>(targetBytes));

        Assert.ThrowsAsync<ArgumentException>(
            () => grain.SetAliasAsync(OrdinaryTree, SystemDataTree));
    }
}
