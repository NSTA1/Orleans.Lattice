using Bunit;
using Microsoft.AspNetCore.Components;
using Microsoft.AspNetCore.Components.Rendering;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Explorer.Shell.Design.Slots;

namespace Orleans.Lattice.Explorer.Tests.Shell.Design.Slots;

/// <summary>
/// The chrome slot contract: a closed set of names, contributions registered
/// against them, and an outlet that renders a slot's contributions in order and
/// nothing at all for an empty slot - so the navigation and session chrome meet
/// without either referencing the other.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class ShellSlotTests : ShellDesignTestContext
{
    [Test]
    public void The_slot_names_are_the_three_the_epic_declares()
    {
        Assert.Multiple(() =>
        {
            Assert.That(ShellSlotNames.HeaderIdentity, Is.EqualTo("header.identity"));
            Assert.That(ShellSlotNames.HeaderConnection, Is.EqualTo("header.connection"));
            Assert.That(ShellSlotNames.OverlaySession, Is.EqualTo("overlay.session"));
            Assert.That(ShellSlotNames.All, Is.EquivalentTo(new[] { "header.identity", "header.connection", "overlay.session" }));
        });
    }

    [Test]
    [TestCase("header.identity", true)]
    [TestCase("Header.Identity", false)]
    [TestCase("header.identity ", false)]
    [TestCase("footer", false)]
    [TestCase(null, false)]
    public void Only_a_declared_name_is_known(string? name, bool known)
    {
        Assert.That(ShellSlotNames.IsKnown(name), Is.EqualTo(known));
    }

    [Test]
    public void A_contribution_to_an_undeclared_slot_is_rejected()
    {
        Assert.Multiple(() =>
        {
            Assert.That(() => new ShellSlot<FirstProbe>("header.idenity"), Throws.ArgumentException);
            Assert.That(() => new ServiceCollection().AddShellSlot<FirstProbe>("footer"), Throws.ArgumentException);
        });
    }

    [Test]
    public void A_contribution_records_its_slot_order_and_component()
    {
        IShellSlot slot = new ShellSlot<FirstProbe>(ShellSlotNames.HeaderIdentity, order: 5);

        Assert.Multiple(() =>
        {
            Assert.That(slot.Name, Is.EqualTo(ShellSlotNames.HeaderIdentity));
            Assert.That(slot.Order, Is.EqualTo(5));
            Assert.That(slot.ComponentType, Is.EqualTo(typeof(FirstProbe)));
        });
    }

    [Test]
    public void Contributing_the_same_component_to_the_same_slot_twice_registers_it_once()
    {
        var services = new ServiceCollection()
            .AddShellSlot<FirstProbe>(ShellSlotNames.HeaderIdentity)
            .AddShellSlot<FirstProbe>(ShellSlotNames.HeaderIdentity)
            .AddShellSlot<FirstProbe>(ShellSlotNames.OverlaySession);

        var slots = services.BuildServiceProvider().GetServices<IShellSlot>().ToArray();

        Assert.That(slots.Select(slot => slot.Name), Is.EquivalentTo(new[] { ShellSlotNames.HeaderIdentity, ShellSlotNames.OverlaySession }));
    }

    [Test]
    public void Registering_into_a_null_collection_is_rejected()
    {
        Assert.That(() => ((IServiceCollection)null!).AddShellSlot<FirstProbe>(ShellSlotNames.HeaderIdentity), Throws.ArgumentNullException);
    }

    [Test]
    public void The_outlet_renders_a_slots_contributions_in_order_and_no_others()
    {
        Services
            .AddShellSlot<SecondProbe>(ShellSlotNames.HeaderIdentity, order: 10)
            .AddShellSlot<FirstProbe>(ShellSlotNames.HeaderIdentity, order: 10)
            .AddShellSlot<ThirdProbe>(ShellSlotNames.HeaderIdentity, order: -1)
            .AddShellSlot<FirstProbe>(ShellSlotNames.OverlaySession);

        var cut = Render<ShellSlotOutlet>(p => p.Add(x => x.Name, ShellSlotNames.HeaderIdentity));

        Assert.That(
            cut.FindAll("[data-probe]").Select(element => element.GetAttribute("data-probe")),
            Is.EqualTo(new[] { "third", "first", "second" }),
            "lower order first, ties broken by type name");
    }

    [Test]
    public void An_empty_slot_renders_nothing_at_all()
    {
        Services.AddShellSlot<FirstProbe>(ShellSlotNames.OverlaySession);

        var cut = Render<ShellSlotOutlet>(p => p.Add(x => x.Name, ShellSlotNames.HeaderConnection));

        Assert.That(cut.Markup, Is.Empty);
    }

    [Test]
    public void The_outlet_rejects_an_undeclared_slot()
    {
        Assert.That(() => Render<ShellSlotOutlet>(p => p.Add(x => x.Name, "header.idenity")), Throws.ArgumentException);
    }

    /// <summary>A slot contribution that marks where it rendered.</summary>
    public sealed class FirstProbe : ComponentBase
    {
        /// <inheritdoc />
        protected override void BuildRenderTree(RenderTreeBuilder builder) => Probe(builder, "first");
    }

    /// <summary>A second contribution, ordered after <see cref="FirstProbe"/> by type name.</summary>
    public sealed class SecondProbe : ComponentBase
    {
        /// <inheritdoc />
        protected override void BuildRenderTree(RenderTreeBuilder builder) => Probe(builder, "second");
    }

    /// <summary>A third contribution, ordered first by its lower order.</summary>
    public sealed class ThirdProbe : ComponentBase
    {
        /// <inheritdoc />
        protected override void BuildRenderTree(RenderTreeBuilder builder) => Probe(builder, "third");
    }

    private static void Probe(RenderTreeBuilder builder, string name)
    {
        builder.OpenElement(0, "span");
        builder.AddAttribute(1, "data-probe", name);
        builder.CloseElement();
    }
}
