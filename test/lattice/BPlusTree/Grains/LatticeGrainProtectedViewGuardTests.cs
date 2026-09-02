using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Lattice.Views;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Security regression coverage that <b>every</b> structural whole-tree verb on
/// <see cref="ILattice"/> refuses a materialised-view tree. A view is maintained
/// from its source tree and is documented as read-only through the public
/// surface; <c>LatticeGrain.ThrowIfProtectedView()</c> is the sole mechanism
/// enforcing that, so a single lifecycle verb that omits it is a complete bypass
/// of the protection for that verb.
/// <para>
/// The verbs are asserted as a set rather than one at a time on purpose: the
/// defect this fixture exists to catch is <em>asymmetry</em> - one verb quietly
/// omitting a guard its semantic siblings all carry - which a per-verb test can
/// never see. <see cref="ILattice.ReshardAsync"/> was such a verb.
/// </para>
/// </summary>
[TestFixture]
public class LatticeGrainProtectedViewGuardTests
{
    private const string ViewTreeId = LatticeConstants.ViewTreePrefix + "orders-by-customer";

    private static ILattice CreateGrain(string treeId)
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("lattice", treeId));

        var grainFactory = Substitute.For<IGrainFactory>();
        var optionsMonitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        optionsMonitor.Get(Arg.Any<string>()).Returns(new LatticeOptions());

        var registry = Substitute.For<ILatticeRegistry>();
        grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).Returns(registry);
        registry.ResolveAsync(Arg.Any<string>()).Returns(c => Task.FromResult(c.Arg<string>()));
        registry.GetShardMapAsync(Arg.Any<string>()).Returns(Task.FromResult<ShardMap?>(null));
        registry.GetEntryAsync(Arg.Any<string>()).Returns(Task.FromResult<TreeRegistryEntry?>(
            new TreeRegistryEntry { MaxLeafKeys = 128, MaxInternalChildren = 128, ShardCount = 4 }));

        var optionsResolver = TestOptionsResolver.ForFactory(grainFactory);
        var services = Substitute.For<IServiceProvider>();
        return new LatticeGrain(
            context, grainFactory, optionsMonitor, optionsResolver, services, NullLogger<LatticeGrain>.Instance);
    }

    /// <summary>
    /// The structural whole-tree verbs, each named so a failure identifies the
    /// verb that lost its guard. Every one of these changes the physical shape or
    /// existence of the tree it is called on.
    /// </summary>
    private static IEnumerable<TestCaseData> StructuralVerbs()
    {
        yield return new TestCaseData((Func<ILattice, Task>)(t => t.ReshardAsync(8))).SetName("ReshardAsync");
        yield return new TestCaseData((Func<ILattice, Task>)(t => t.ResizeAsync(256, 256))).SetName("ResizeAsync");
        yield return new TestCaseData((Func<ILattice, Task>)(t => t.UndoResizeAsync())).SetName("UndoResizeAsync");
        yield return new TestCaseData((Func<ILattice, Task>)(t => t.DeleteTreeAsync())).SetName("DeleteTreeAsync");
        yield return new TestCaseData((Func<ILattice, Task>)(t => t.PurgeTreeAsync())).SetName("PurgeTreeAsync");
        yield return new TestCaseData((Func<ILattice, Task>)(t => t.RecoverTreeAsync())).SetName("RecoverTreeAsync");
    }

    [TestCaseSource(nameof(StructuralVerbs))]
    public void A_structural_verb_refuses_a_materialised_view_tree(Func<ILattice, Task> verb)
    {
        var ex = Assert.ThrowsAsync<InvalidOperationException>(() => verb(CreateGrain(ViewTreeId)));

        Assert.That(ex!.Message, Does.Contain("materialised view"),
            "the rejection is the protected-view guard, not an unrelated failure");
    }

    [TestCaseSource(nameof(StructuralVerbs))]
    public void A_structural_verb_is_not_fenced_inside_an_authorised_view_scope(Func<ILattice, Task> verb)
    {
        // The view maintainer legitimately restructures the view it owns, and runs
        // inside an authorised view scope. The guard must fence the public surface,
        // not the maintainer.
        using var scope = ViewWriteContext.BeginScope();

        InvalidOperationException? protectedView = null;
        try
        {
            verb(CreateGrain(ViewTreeId)).GetAwaiter().GetResult();
        }
        catch (InvalidOperationException ex) when (ex.Message.Contains("materialised view", StringComparison.Ordinal))
        {
            protectedView = ex;
        }
        catch
        {
            // Any other failure is the substituted infrastructure, not the guard.
        }

        Assert.That(protectedView, Is.Null, "an authorised view write is not fenced by the protected-view guard");
    }

    [Test]
    public void A_structural_verb_on_an_ordinary_tree_is_not_fenced()
    {
        // Negative control: the guard keys on the view prefix alone, so an ordinary
        // tree is untouched by it.
        InvalidOperationException? protectedView = null;
        try
        {
            CreateGrain("app-orders").ReshardAsync(8).GetAwaiter().GetResult();
        }
        catch (InvalidOperationException ex) when (ex.Message.Contains("materialised view", StringComparison.Ordinal))
        {
            protectedView = ex;
        }
        catch
        {
            // Any other failure is the substituted infrastructure, not the guard.
        }

        Assert.That(protectedView, Is.Null);
    }
}
