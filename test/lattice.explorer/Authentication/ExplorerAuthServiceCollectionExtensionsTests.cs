using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using Orleans.Lattice.Explorer.Core.Authentication;
using Orleans.Lattice.Explorer.Core.Configuration;

namespace Orleans.Lattice.Explorer.Tests.Authentication;

[TestFixture]
public class ExplorerAuthServiceCollectionExtensionsTests
{
    [Test]
    public void AddExplorerAuth_registersAuthSessionAndInMemoryStore()
    {
        var services = new ServiceCollection();
        services.AddSingleton(Substitute.For<IExplorerSession>());

        services.AddExplorerAuth();

        using var provider = services.BuildServiceProvider();
        Assert.That(provider.GetService<IExplorerAuthSession>(), Is.InstanceOf<ExplorerAuthSession>());
        Assert.That(provider.GetService<ICredentialStore>(), Is.InstanceOf<InMemoryCredentialStore>());
    }

    [Test]
    public void AddExplorerAuth_keepsPreviouslyRegisteredCredentialStore()
    {
        var services = new ServiceCollection();
        services.AddSingleton(Substitute.For<IExplorerSession>());
        var platformStore = Substitute.For<ICredentialStore>();
        services.AddSingleton(platformStore);

        services.AddExplorerAuth();

        using var provider = services.BuildServiceProvider();
        Assert.That(provider.GetService<ICredentialStore>(), Is.SameAs(platformStore));
    }

    [Test]
    public void AddExplorerAuth_nullServices_throws()
    {
        Assert.That(() => ((IServiceCollection)null!).AddExplorerAuth(), Throws.ArgumentNullException);
    }
}
