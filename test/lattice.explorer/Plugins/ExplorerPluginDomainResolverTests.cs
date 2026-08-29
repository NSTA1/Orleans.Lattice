using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Explorer.Plugins;

namespace Orleans.Lattice.Explorer.Tests.Plugins;

[TestFixture]
public sealed class ExplorerPluginDomainResolverTests
{
    [Test]
    public void GetDeclaredContract_returns_the_type_the_plugin_declared()
    {
        var host = PluginTestHost.Create(
            new FakeExplorerPlugin("a", domainContract: typeof(ISampleDomain)));

        Assert.That(host.Domains.GetDeclaredContract("a"), Is.EqualTo(typeof(ISampleDomain)));
    }

    [Test]
    public void GetDeclaredContract_returns_null_when_none_was_declared()
    {
        var host = PluginTestHost.Create(new FakeExplorerPlugin("a"));

        Assert.That(host.Domains.GetDeclaredContract("a"), Is.Null);
    }

    [Test]
    public void GetDeclaredContract_returns_null_for_an_unregistered_plugin()
    {
        var host = PluginTestHost.Create();

        Assert.That(host.Domains.GetDeclaredContract("missing"), Is.Null);
    }

    [Test]
    public void Resolve_hands_over_the_declared_contract()
    {
        var domain = new SampleDomain();
        var host = PluginTestHost.Create(
            [new FakeExplorerPlugin("a", domainContract: typeof(ISampleDomain))],
            services => services.AddSingleton<ISampleDomain>(domain));

        Assert.That(host.Domains.Resolve<ISampleDomain>("a"), Is.SameAs(domain));
    }

    [Test]
    public void Resolve_of_an_undeclared_contract_is_refused_as_over_reach()
    {
        var host = PluginTestHost.Create(
            [new FakeExplorerPlugin("a", domainContract: typeof(ISampleDomain))],
            services =>
            {
                services.AddSingleton<ISampleDomain>(new SampleDomain());
                services.AddSingleton<IOtherDomain>(new OtherDomain());
            });

        Assert.That(
            () => host.Domains.Resolve<IOtherDomain>("a"),
            Throws.TypeOf<ExplorerPluginDomainException>().With.Message.Contains("may not resolve"));
    }

    [Test]
    public void Resolve_for_a_plugin_that_declared_nothing_throws()
    {
        var host = PluginTestHost.Create(
            [new FakeExplorerPlugin("a")],
            services => services.AddSingleton<ISampleDomain>(new SampleDomain()));

        Assert.That(
            () => host.Domains.Resolve<ISampleDomain>("a"),
            Throws.TypeOf<ExplorerPluginDomainException>().With.Message.Contains("declares no domain contract"));
    }

    [Test]
    public void Resolve_for_an_unregistered_plugin_throws()
    {
        var host = PluginTestHost.Create();

        Assert.That(
            () => host.Domains.Resolve<ISampleDomain>("missing"),
            Throws.TypeOf<ExplorerPluginDomainException>().With.Message.Contains("No plugin is registered"));
    }

    [Test]
    public void Resolve_throws_when_the_declared_contract_is_not_in_the_container()
    {
        var host = PluginTestHost.Create(new FakeExplorerPlugin("a", domainContract: typeof(ISampleDomain)));

        Assert.That(
            () => host.Domains.Resolve<ISampleDomain>("a"),
            Throws.TypeOf<ExplorerPluginDomainException>().With.Message.Contains("no such service is registered"));
    }

    [Test]
    public void Resolve_rejects_a_null_plugin_id()
    {
        var host = PluginTestHost.Create();

        Assert.Multiple(() =>
        {
            Assert.That(() => host.Domains.Resolve<ISampleDomain>(null!), Throws.ArgumentNullException);
            Assert.That(() => host.Domains.GetDeclaredContract(null!), Throws.ArgumentNullException);
            Assert.That(
                () => host.Domains.TryResolve<ISampleDomain>(null!, out _),
                Throws.ArgumentNullException);
        });
    }

    [Test]
    public void TryResolve_hands_over_the_declared_contract()
    {
        var domain = new SampleDomain();
        var host = PluginTestHost.Create(
            [new FakeExplorerPlugin("a", domainContract: typeof(ISampleDomain))],
            services => services.AddSingleton<ISampleDomain>(domain));

        var resolved = host.Domains.TryResolve<ISampleDomain>("a", out var actual);

        Assert.Multiple(() =>
        {
            Assert.That(resolved, Is.True);
            Assert.That(actual, Is.SameAs(domain));
        });
    }

    [Test]
    public void TryResolve_returns_false_for_over_reach_rather_than_throwing()
    {
        var host = PluginTestHost.Create(
            [new FakeExplorerPlugin("a", domainContract: typeof(ISampleDomain))],
            services => services.AddSingleton<IOtherDomain>(new OtherDomain()));

        var resolved = host.Domains.TryResolve<IOtherDomain>("a", out var actual);

        Assert.Multiple(() =>
        {
            Assert.That(resolved, Is.False);
            Assert.That(actual, Is.Null);
        });
    }

    [Test]
    public void TryResolve_returns_false_when_nothing_was_declared()
    {
        var host = PluginTestHost.Create(
            [new FakeExplorerPlugin("a")],
            services => services.AddSingleton<ISampleDomain>(new SampleDomain()));

        Assert.That(host.Domains.TryResolve<ISampleDomain>("a", out var actual), Is.False);
        Assert.That(actual, Is.Null);
    }

    [Test]
    public void TryResolve_returns_false_when_the_declared_contract_is_not_in_the_container()
    {
        var host = PluginTestHost.Create(new FakeExplorerPlugin("a", domainContract: typeof(ISampleDomain)));

        Assert.That(host.Domains.TryResolve<ISampleDomain>("a", out var actual), Is.False);
        Assert.That(actual, Is.Null);
    }

    [Test]
    public void TryResolve_returns_false_for_an_unregistered_plugin()
    {
        var host = PluginTestHost.Create();

        Assert.That(host.Domains.TryResolve<ISampleDomain>("missing", out var actual), Is.False);
        Assert.That(actual, Is.Null);
    }

    [Test]
    public void Constructor_rejects_null_dependencies()
    {
        var host = PluginTestHost.Create();
        var services = new ServiceCollection().BuildServiceProvider();

        Assert.Multiple(() =>
        {
            Assert.That(() => new ExplorerPluginDomainResolver(null!, services), Throws.ArgumentNullException);
            Assert.That(() => new ExplorerPluginDomainResolver(host.Catalog, null!), Throws.ArgumentNullException);
        });
    }

    [Test]
    public void Generic_plugin_interface_declares_its_contract_without_boilerplate()
    {
        IExplorerPlugin plugin = new TypedPlugin();

        Assert.That(plugin.DomainContract, Is.EqualTo(typeof(ISampleDomain)));
    }

    [Test]
    public void Exception_carries_the_supplied_message_and_inner_exception()
    {
        var inner = new InvalidOperationException("inner");

        Assert.Multiple(() =>
        {
            Assert.That(new ExplorerPluginDomainException().Message, Is.Not.Empty);
            Assert.That(new ExplorerPluginDomainException("boom").Message, Is.EqualTo("boom"));
            Assert.That(new ExplorerPluginDomainException("boom", inner).InnerException, Is.SameAs(inner));
        });
    }

    internal interface ISampleDomain;

    internal interface IOtherDomain;

    private sealed class SampleDomain : ISampleDomain;

    private sealed class OtherDomain : IOtherDomain;

    /// <summary>
    /// A plugin that declares its domain contract through
    /// <see cref="IExplorerPlugin{TDomain}"/> rather than by implementing
    /// <see cref="IExplorerPlugin.DomainContract"/> by hand.
    /// </summary>
    private sealed class TypedPlugin : IExplorerPlugin<ISampleDomain>
    {
        public ExplorerPluginDescriptor Descriptor { get; } = new()
        {
            PluginId = "typed",
            Label = "Typed",
            Surface = ExplorerPluginSurface.Area,
        };

        public Type ViewType => typeof(TypedPlugin);

        public IExplorerPluginAccessGate AccessGate => ExplorerPluginAccessGates.Allowed;
    }
}
