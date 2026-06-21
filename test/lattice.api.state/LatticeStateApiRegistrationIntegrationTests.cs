using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Api.State.Tests;

/// <summary>
/// Integration-level registration tests asserting a silo running
/// <c>AddLattice(...).AddLatticeStateApi()</c> starts and exposes the
/// state-API options.
/// </summary>
[TestFixture]
[Category("Integration")]
public class LatticeStateApiRegistrationIntegrationTests
{
    private readonly LatticeStateApiClusterFixture _fixture = new();

    [OneTimeSetUp]
    public Task OneTimeSetUp() => _fixture.InitializeAsync();

    [OneTimeTearDown]
    public Task OneTimeTearDown() => _fixture.DisposeAsync();

    [Test]
    public void Silo_starts_and_resolves_LatticeApiStateOptions()
    {
        var options = _fixture.SiloServices.GetRequiredService<IOptions<LatticeApiStateOptions>>();
        Assert.That(options.Value, Is.Not.Null);
    }
}
