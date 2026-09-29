using Orleans.Lattice.Explorer.Core.Configuration;

namespace Orleans.Lattice.Samples.Explorer.Tests;

[TestFixture]
public sealed class SampleExplorerEnvironmentTests
{
    [Test]
    public void It_seeds_the_endpoint_over_loopback_h2c_and_the_administrator_sign_in()
    {
        var environment = new SampleExplorerEnvironment(new Uri("http://localhost:5198/"));

        Assert.That(environment.GetVariable(EnvironmentExplorerBootstrap.EndpointVariable), Is.EqualTo("http://localhost:5198"));
        Assert.That(environment.GetVariable(EnvironmentExplorerBootstrap.InsecureDevVariable), Is.EqualTo("true"));
        Assert.That(environment.GetVariable(EnvironmentExplorerBootstrap.UsernameVariable), Is.EqualTo(SampleIdentities.Administrator));
        Assert.That(environment.GetVariable(EnvironmentExplorerBootstrap.PasswordVariable), Is.EqualTo(SampleIdentities.AdministratorPassword));
    }

    [Test]
    public void Anything_else_is_unset_whatever_the_process_environment_holds() =>
        Assert.That(new SampleExplorerEnvironment(new Uri("http://localhost:1/")).GetVariable("PATH"), Is.Null);

    [Test]
    public void It_signs_in_as_the_identity_it_is_given()
    {
        var environment = new SampleExplorerEnvironment(new Uri("http://localhost:1/"), SampleIdentities.GlobexAdmin);

        Assert.That(environment.GetVariable(EnvironmentExplorerBootstrap.UsernameVariable), Is.EqualTo(SampleIdentities.GlobexAdmin));
        Assert.That(environment.GetVariable(EnvironmentExplorerBootstrap.PasswordVariable), Is.Not.Empty);
    }

    [Test]
    public void Without_an_identity_it_seeds_no_sign_in()
    {
        var environment = new SampleExplorerEnvironment(new Uri("http://localhost:1/"), signInAs: null);

        Assert.That(environment.GetVariable(EnvironmentExplorerBootstrap.EndpointVariable), Is.EqualTo("http://localhost:1"));
        Assert.That(environment.GetVariable(EnvironmentExplorerBootstrap.UsernameVariable), Is.Null);
        Assert.That(environment.GetVariable(EnvironmentExplorerBootstrap.PasswordVariable), Is.Null);
    }

    [Test]
    public void A_null_endpoint_throws() =>
        Assert.That(() => new SampleExplorerEnvironment(null!), Throws.ArgumentNullException);
}
