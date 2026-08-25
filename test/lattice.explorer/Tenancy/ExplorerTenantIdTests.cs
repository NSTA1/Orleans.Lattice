using Orleans.Lattice.Explorer.Core.Tenancy;

namespace Orleans.Lattice.Explorer.Tests.Tenancy;

[TestFixture]
public class ExplorerTenantIdTests
{
    [Test]
    public void Ctor_value_setsValue()
    {
        var id = new ExplorerTenantId("acme");

        Assert.That(id.Value, Is.EqualTo("acme"));
    }

    [Test]
    public void Ctor_null_throws()
    {
        Assert.That(() => new ExplorerTenantId(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void Ctor_empty_throws()
    {
        Assert.That(() => new ExplorerTenantId(string.Empty), Throws.ArgumentException);
    }

    [Test]
    public void Default_isDefaultTenantId()
    {
        Assert.That(ExplorerTenantId.Default.Value, Is.EqualTo(ExplorerTenantTrees.DefaultTenantId));
    }

    [Test]
    public void ToString_returnsValue()
    {
        Assert.That(new ExplorerTenantId("globex").ToString(), Is.EqualTo("globex"));
    }

    [Test]
    public void Equality_sameValue_areEqual()
    {
        Assert.That(new ExplorerTenantId("acme"), Is.EqualTo(new ExplorerTenantId("acme")));
    }

    [Test]
    public void Equality_differentValue_areNotEqual()
    {
        Assert.That(new ExplorerTenantId("acme"), Is.Not.EqualTo(new ExplorerTenantId("globex")));
    }

    [Test]
    public void Equality_isOrdinalCaseSensitive()
    {
        Assert.That(new ExplorerTenantId("acme"), Is.Not.EqualTo(new ExplorerTenantId("ACME")));
    }
}
