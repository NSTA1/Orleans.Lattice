using Orleans.Lattice.Explorer.Plugins;

namespace Orleans.Lattice.Explorer.Tests.Plugins;

/// <summary>
/// The applicability half of the plugin contract: the flag set a selection
/// plugin declares, its conversion from the single kind a selection actually is,
/// and the descriptor's own applicability test.
/// <para>
/// This is what lets one selection kind resolve to a different plugin set rather
/// than being special-cased by the host, so its edges - an unrecognised kind, an
/// empty set - are the ones worth pinning.
/// </para>
/// </summary>
[TestFixture]
public sealed class ExplorerPluginSelectionKindsTests
{
    [Test]
    public void The_default_flag_set_is_none_so_an_unset_value_selects_nothing()
    {
        Assert.That(default(ExplorerPluginSelectionKinds), Is.EqualTo(ExplorerPluginSelectionKinds.None));
    }

    [Test]
    public void All_is_exactly_the_three_kinds()
    {
        Assert.That(
            ExplorerPluginSelectionKinds.All,
            Is.EqualTo(
                ExplorerPluginSelectionKinds.Tree
                | ExplorerPluginSelectionKinds.View
                | ExplorerPluginSelectionKinds.TagIndex));
    }

    [TestCase(ExplorerPluginSelectionKind.Tree, ExplorerPluginSelectionKinds.Tree)]
    [TestCase(ExplorerPluginSelectionKind.View, ExplorerPluginSelectionKinds.View)]
    [TestCase(ExplorerPluginSelectionKind.TagIndex, ExplorerPluginSelectionKinds.TagIndex)]
    public void ToFlag_pairs_each_kind_with_its_own_flag(
        ExplorerPluginSelectionKind kind,
        ExplorerPluginSelectionKinds expected)
    {
        Assert.That(kind.ToFlag(), Is.EqualTo(expected));
    }

    [Test]
    public void ToFlag_maps_an_unrecognised_kind_to_none_rather_than_throwing()
    {
        // A host reading a selection projected by a newer package degrades to
        // rendering no plugin instead of faulting the shell.
        Assert.That(((ExplorerPluginSelectionKind)99).ToFlag(), Is.EqualTo(ExplorerPluginSelectionKinds.None));
    }

    [Test]
    public void Includes_is_true_only_for_a_declared_kind()
    {
        var kinds = ExplorerPluginSelectionKinds.Tree | ExplorerPluginSelectionKinds.View;

        Assert.Multiple(() =>
        {
            Assert.That(kinds.Includes(ExplorerPluginSelectionKind.Tree), Is.True);
            Assert.That(kinds.Includes(ExplorerPluginSelectionKind.View), Is.True);
            Assert.That(kinds.Includes(ExplorerPluginSelectionKind.TagIndex), Is.False);
        });
    }

    [Test]
    public void Includes_fails_closed_for_an_unrecognised_kind_even_against_all()
    {
        Assert.That(
            ExplorerPluginSelectionKinds.All.Includes((ExplorerPluginSelectionKind)99),
            Is.False,
            "applicability must fail closed rather than admit an unknown kind");
    }

    [Test]
    public void Includes_is_false_for_every_kind_when_none_is_declared()
    {
        Assert.Multiple(() =>
        {
            Assert.That(ExplorerPluginSelectionKinds.None.Includes(ExplorerPluginSelectionKind.Tree), Is.False);
            Assert.That(ExplorerPluginSelectionKinds.None.Includes(ExplorerPluginSelectionKind.View), Is.False);
            Assert.That(ExplorerPluginSelectionKinds.None.Includes(ExplorerPluginSelectionKind.TagIndex), Is.False);
        });
    }

    [Test]
    public void A_descriptor_applies_to_every_kind_by_default()
    {
        var descriptor = Descriptor();

        Assert.Multiple(() =>
        {
            Assert.That(descriptor.SelectionKinds, Is.EqualTo(ExplorerPluginSelectionKinds.All));
            Assert.That(descriptor.AppliesTo(ExplorerPluginSelectionKind.Tree), Is.True);
            Assert.That(descriptor.AppliesTo(ExplorerPluginSelectionKind.View), Is.True);
            Assert.That(descriptor.AppliesTo(ExplorerPluginSelectionKind.TagIndex), Is.True);
        });
    }

    [Test]
    public void A_descriptor_applies_only_to_the_kinds_it_declares()
    {
        var descriptor = Descriptor() with { SelectionKinds = ExplorerPluginSelectionKinds.TagIndex };

        Assert.Multiple(() =>
        {
            Assert.That(descriptor.AppliesTo(ExplorerPluginSelectionKind.TagIndex), Is.True);
            Assert.That(descriptor.AppliesTo(ExplorerPluginSelectionKind.Tree), Is.False);
            Assert.That(descriptor.AppliesTo(ExplorerPluginSelectionKind.View), Is.False);
        });
    }

    private static ExplorerPluginDescriptor Descriptor() => new()
    {
        PluginId = "sample",
        Label = "Sample",
        Surface = ExplorerPluginSurface.Selection,
    };
}
