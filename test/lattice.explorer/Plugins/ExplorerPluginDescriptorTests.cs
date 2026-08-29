using Orleans.Lattice.Explorer.Plugins;

namespace Orleans.Lattice.Explorer.Tests.Plugins;

[TestFixture]
public sealed class ExplorerPluginDescriptorTests
{
    [Test]
    public void Descriptor_carries_identity_and_placement()
    {
        var descriptor = new ExplorerPluginDescriptor
        {
            PluginId = "orleans.lattice.backups",
            Label = "Backups",
            Surface = ExplorerPluginSurface.Area,
            Order = 20,
        };

        Assert.Multiple(() =>
        {
            Assert.That(descriptor.PluginId, Is.EqualTo("orleans.lattice.backups"));
            Assert.That(descriptor.Label, Is.EqualTo("Backups"));
            Assert.That(descriptor.Surface, Is.EqualTo(ExplorerPluginSurface.Area));
            Assert.That(descriptor.Order, Is.EqualTo(20));
        });
    }

    [Test]
    public void Order_defaults_to_zero()
    {
        var descriptor = new ExplorerPluginDescriptor
        {
            PluginId = "a",
            Label = "A",
            Surface = ExplorerPluginSurface.Selection,
        };

        Assert.That(descriptor.Order, Is.Zero);
    }

    [Test]
    public void PluginId_null_throws()
    {
        Assert.That(
            () => new ExplorerPluginDescriptor { PluginId = null!, Label = "A", Surface = ExplorerPluginSurface.Area },
            Throws.ArgumentNullException);
    }

    [TestCase("")]
    [TestCase(" ")]
    [TestCase("\t")]
    public void PluginId_blank_throws(string pluginId)
    {
        Assert.That(
            () => new ExplorerPluginDescriptor
            {
                PluginId = pluginId,
                Label = "A",
                Surface = ExplorerPluginSurface.Area,
            },
            Throws.ArgumentException);
    }

    [Test]
    public void Label_null_throws()
    {
        Assert.That(
            () => new ExplorerPluginDescriptor { PluginId = "a", Label = null!, Surface = ExplorerPluginSurface.Area },
            Throws.ArgumentNullException);
    }

    [TestCase("")]
    [TestCase("   ")]
    public void Label_blank_throws(string label)
    {
        Assert.That(
            () => new ExplorerPluginDescriptor { PluginId = "a", Label = label, Surface = ExplorerPluginSurface.Area },
            Throws.ArgumentException);
    }

    [Test]
    public void Descriptors_with_the_same_values_are_equal()
    {
        var left = new ExplorerPluginDescriptor
        {
            PluginId = "a",
            Label = "A",
            Surface = ExplorerPluginSurface.Area,
            Order = 3,
        };

        var right = new ExplorerPluginDescriptor
        {
            PluginId = "a",
            Label = "A",
            Surface = ExplorerPluginSurface.Area,
            Order = 3,
        };

        Assert.Multiple(() =>
        {
            Assert.That(left, Is.EqualTo(right));
            Assert.That(left.GetHashCode(), Is.EqualTo(right.GetHashCode()));
        });
    }

    [Test]
    public void PluginId_comparison_is_ordinal_so_casing_is_significant()
    {
        var lower = new ExplorerPluginDescriptor
        {
            PluginId = "backups",
            Label = "A",
            Surface = ExplorerPluginSurface.Area,
        };

        var upper = lower with { PluginId = "Backups" };

        Assert.That(lower, Is.Not.EqualTo(upper));
    }

    [Test]
    public void Surface_declares_exactly_the_two_navigation_tiers()
    {
        Assert.That(
            Enum.GetValues<ExplorerPluginSurface>(),
            Is.EquivalentTo(new[] { ExplorerPluginSurface.Area, ExplorerPluginSurface.Selection }));
    }
}
