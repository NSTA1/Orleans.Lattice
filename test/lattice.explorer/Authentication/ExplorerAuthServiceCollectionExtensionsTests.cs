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
    public void AddExplorerAuth_registersDefaultReauthOptions_withNoChallengePath()
    {
        var services = new ServiceCollection();
        services.AddSingleton(Substitute.For<IExplorerSession>());

        services.AddExplorerAuth();

        using var provider = services.BuildServiceProvider();
        var reauth = provider.GetService<ExplorerReauthOptions>();
        Assert.Multiple(() =>
        {
            Assert.That(reauth, Is.Not.Null);
            Assert.That(reauth!.ChallengePath, Is.Null, "the core default carries no challenge path (UI degrades to a reload)");
            Assert.That(reauth.AppendReturnUrl, Is.True);
            Assert.That(reauth.ReturnUrlParameter, Is.EqualTo(ExplorerReauthOptions.DefaultReturnUrlParameter));
        });
    }

    [Test]
    public void AddExplorerAuth_keepsPreviouslyRegisteredReauthOptions()
    {
        var services = new ServiceCollection();
        services.AddSingleton(Substitute.For<IExplorerSession>());
        var configured = new ExplorerReauthOptions { ChallengePath = "/custom/reauth" };
        services.AddSingleton(configured);

        services.AddExplorerAuth();

        using var provider = services.BuildServiceProvider();
        Assert.That(provider.GetRequiredService<ExplorerReauthOptions>(), Is.SameAs(configured));
    }

    [Test]
    public void AddExplorerAuth_nullServices_throws()
    {
        Assert.That(() => ((IServiceCollection)null!).AddExplorerAuth(), Throws.ArgumentNullException);
    }
}
