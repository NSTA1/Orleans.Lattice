using Orleans.Lattice.Explorer.Core;

namespace Orleans.Lattice.Explorer.Tests;

[TestFixture]
public class ExplorerInfoTests
{
    [Test]
    public void ApplicationName_is_the_product_name()
    {
        Assert.That(ExplorerInfo.ApplicationName, Is.EqualTo("Orleans.Lattice.Explorer"));
    }

    [Test]
    public void Description_is_present()
    {
        Assert.That(ExplorerInfo.Description, Is.Not.Empty);
    }
}
