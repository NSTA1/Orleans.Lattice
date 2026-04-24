namespace MultiSiteManufacturing.Tests;

[TestFixture]
public class ScaffoldTests
{
    [Test]
    public void Contracts_assembly_loads()
    {
        var assembly = typeof(MultiSiteManufacturing.Contracts.V1.FactEnvelope).Assembly;
        Assert.That(assembly, Is.Not.Null);
    }

    [Test]
    public void Host_assembly_loads()
    {
        var assembly = typeof(Program).Assembly;
        Assert.That(assembly, Is.Not.Null);
    }
}
