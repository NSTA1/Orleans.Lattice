using System.Reflection;
using Orleans.Lattice.Api.TreeAdmin;

namespace Orleans.Lattice.Api.Abstractions.Tests;

/// <summary>
/// Exercises the default implementation of
/// <see cref="ILatticeTreeAdmin.CreateViewAsync"/>, the runtime materialised-view
/// creation member added to the tree-administration contract after it had
/// shipped.
/// </summary>
/// <remarks>
/// <para>
/// The member is a default interface method so that an implementer written
/// against the earlier contract keeps compiling. That backwards compatibility is
/// only worth anything if the default is <em>loud</em>: an implementation that
/// cannot create a view must refuse, because silently answering a default
/// <c>TreeViewStatus</c> would report a view that was never created.
/// </para>
/// <para>
/// Nothing else in the repository reaches that body. Every production implementer
/// overrides the member, so a compiled legacy implementer is the only caller that
/// inherits the default at all.
/// </para>
/// </remarks>
[TestFixture]
public sealed class TreeAdminViewDefaultMemberTests
{
    [Test]
    public void CreateViewAsync_is_a_default_member_so_a_legacy_implementer_still_compiles()
    {
        var declared = typeof(ILatticeTreeAdmin).GetMethod(nameof(ILatticeTreeAdmin.CreateViewAsync));

        Assert.That(declared, Is.Not.Null);
        Assert.That(
            declared!.IsAbstract,
            Is.False,
            "CreateViewAsync must stay a default interface member. Were it abstract, every pre-existing "
            + "implementer would break, and the refusal asserted below would become unreachable because "
            + "the generated implementer would supply a body of its own.");
    }

    [Test]
    public void CreateViewAsync_refuses_on_an_implementer_that_does_not_support_runtime_view_creation()
    {
        var legacyType = LegacyContractImplementer.Compile(typeof(ILatticeTreeAdmin));
        var legacy = (ILatticeTreeAdmin)Activator.CreateInstance(legacyType)!;

        Assert.That(
            legacyType.GetMethod(nameof(ILatticeTreeAdmin.CreateViewAsync), BindingFlags.Public | BindingFlags.Instance),
            Is.Null,
            "the generated implementer must inherit the default rather than declare its own, or this proves nothing");

        var error = Assert.Throws<NotSupportedException>(
            () => { _ = legacy.CreateViewAsync("daily", "orders", "rollup", [1, 2, 3]); });

        Assert.Multiple(() =>
        {
            // The discriminator between the two bodies that could have run: the
            // generated stub throws NotImplementedException, the contract's own
            // default throws NotSupportedException. Asserting the type is what
            // pins which of them executed.
            Assert.That(error, Is.Not.InstanceOf<NotImplementedException>());
            Assert.That(
                error!.Message,
                Does.Contain("view creation"),
                "the refusal must name the missing capability so an operator can act on it");
        });
    }

    [Test]
    public void CreateViewAsync_refuses_before_it_can_report_a_view_that_was_never_created()
    {
        var legacy = (ILatticeTreeAdmin)LegacyContractImplementer.CreateInstance(typeof(ILatticeTreeAdmin));

        // A refusal, not a faulted task and not a default-valued status: the
        // caller must be unable to mistake "unsupported" for "created".
        Assert.That(
            () => { _ = legacy.CreateViewAsync("daily", "orders", "rollup", []); },
            Throws.InstanceOf<NotSupportedException>(),
            "an empty payload must refuse for the same reason a populated one does - the implementation "
            + "cannot create views at all, so no argument shape reaches a success path");
    }
}
