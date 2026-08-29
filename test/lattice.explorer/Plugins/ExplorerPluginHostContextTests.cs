using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Explorer.Plugins;

namespace Orleans.Lattice.Explorer.Tests.Plugins;

[TestFixture]
public sealed class ExplorerPluginHostContextTests
{
    [Test]
    public void Context_is_bound_to_the_plugin_it_was_created_for()
    {
        var host = PluginTestHost.Create(new FakeExplorerPlugin("a"));

        Assert.That(host.Contexts.Create("a").PluginId, Is.EqualTo("a"));
    }

    [Test]
    public void Ambient_facts_read_through_to_the_host_state()
    {
        var host = PluginTestHost.Create(new FakeExplorerPlugin("a"));
        var context = host.Contexts.Create("a");

        host.State.Selection = new ExplorerPluginSelection
        {
            Id = "tree-1",
            Label = "Tree one",
            Kind = ExplorerPluginSelectionKind.Tree,
        };
        host.State.Connection = new ExplorerPluginConnectionStatus(ExplorerPluginConnectionState.Connected);
        host.State.Tenant = new ExplorerPluginTenantScope(
            IsActive: true,
            ActiveTenantId: "acme",
            ExplorerPluginTenantVisibility.AllTenants);

        Assert.Multiple(() =>
        {
            Assert.That(context.Selection!.Id, Is.EqualTo("tree-1"));
            Assert.That(context.Connection.State, Is.EqualTo(ExplorerPluginConnectionState.Connected));
            Assert.That(context.Tenant.ActiveTenantId, Is.EqualTo("acme"));
            Assert.That(context.Tenant.Visibility, Is.EqualTo(ExplorerPluginTenantVisibility.AllTenants));
        });
    }

    [Test]
    public void Selection_is_null_when_nothing_is_selected()
    {
        var host = PluginTestHost.Create(new FakeExplorerPlugin("a"));

        Assert.That(host.Contexts.Create("a").Selection, Is.Null);
    }

    [Test]
    public void Changed_forwards_the_host_states_transitions()
    {
        var host = PluginTestHost.Create(new FakeExplorerPlugin("a"));
        var context = host.Contexts.Create("a");

        var observed = new List<ExplorerPluginHostChange>();
        void Handler(ExplorerPluginHostChange change) => observed.Add(change);
        context.Changed += Handler;

        host.State.Raise(ExplorerPluginHostChange.Selection);
        host.State.Raise(ExplorerPluginHostChange.Tenant);

        Assert.That(
            observed,
            Is.EqualTo(new[] { ExplorerPluginHostChange.Selection, ExplorerPluginHostChange.Tenant }).AsCollection);
    }

    [Test]
    public void Unsubscribing_from_changed_detaches_the_handler()
    {
        var host = PluginTestHost.Create(new FakeExplorerPlugin("a"));
        var context = host.Contexts.Create("a");

        var raised = 0;
        void Handler(ExplorerPluginHostChange _) => raised++;
        context.Changed += Handler;
        context.Changed -= Handler;

        host.State.Raise(ExplorerPluginHostChange.Connection);

        Assert.Multiple(() =>
        {
            Assert.That(raised, Is.Zero);
            Assert.That(host.State.SubscriberCount, Is.Zero);
        });
    }

    [Test]
    public void Preferences_are_namespaced_to_the_owning_plugin()
    {
        var host = PluginTestHost.Create(
            new FakeExplorerPlugin("alpha"),
            new FakeExplorerPlugin("beta"));

        host.Contexts.Create("alpha").Preferences.GetOrDefault("page-size", 25);
        host.Contexts.Create("beta").Preferences.GetOrDefault("page-size", 25);

        Assert.That(host.Preferences.ObservedKeys, Is.EqualTo(new[] { "alpha/page-size", "beta/page-size" }).AsCollection);
    }

    [Test]
    public async Task Two_plugins_may_use_the_same_preference_key_without_colliding()
    {
        var host = PluginTestHost.Create(
            new FakeExplorerPlugin("alpha"),
            new FakeExplorerPlugin("beta"));

        var alpha = host.Contexts.Create("alpha").Preferences;
        var beta = host.Contexts.Create("beta").Preferences;

        await alpha.SetAsync("page-size", 10);
        await beta.SetAsync("page-size", 50);

        Assert.Multiple(() =>
        {
            Assert.That(alpha.GetOrDefault("page-size", 0), Is.EqualTo(10));
            Assert.That(beta.GetOrDefault("page-size", 0), Is.EqualTo(50));
        });
    }

    [Test]
    public async Task Scoped_preferences_forward_every_operation()
    {
        var host = PluginTestHost.Create(new FakeExplorerPlugin("alpha"));
        var preferences = host.Contexts.Create("alpha").Preferences;

        Assert.That(preferences.IsLoaded, Is.False);
        await preferences.EnsureLoadedAsync();

        await preferences.SetAsync("k", "v");
        var found = preferences.TryGet<string>("k", out var value);
        await preferences.RemoveAsync("k");
        var afterRemove = preferences.TryGet<string>("k", out _);

        Assert.Multiple(() =>
        {
            Assert.That(preferences.IsLoaded, Is.True);
            Assert.That(host.Preferences.EnsureLoadedCalls, Is.EqualTo(1));
            Assert.That(found, Is.True);
            Assert.That(value, Is.EqualTo("v"));
            Assert.That(afterRemove, Is.False);
            Assert.That(host.Preferences.ObservedKeys, Is.All.StartWith("alpha/"));
        });
    }

    [Test]
    public void TryGet_of_an_absent_preference_yields_the_default()
    {
        var host = PluginTestHost.Create(new FakeExplorerPlugin("alpha"));
        var preferences = host.Contexts.Create("alpha").Preferences;

        Assert.Multiple(() =>
        {
            Assert.That(preferences.TryGet<int>("missing", out var value), Is.False);
            Assert.That(value, Is.Zero);
            Assert.That(preferences.GetOrDefault("missing", 7), Is.EqualTo(7));
        });
    }

    [Test]
    public void Scoped_preferences_reject_a_null_key()
    {
        var host = PluginTestHost.Create(new FakeExplorerPlugin("alpha"));
        var preferences = host.Contexts.Create("alpha").Preferences;

        Assert.Multiple(() =>
        {
            Assert.That(() => preferences.TryGet<int>(null!, out _), Throws.ArgumentNullException);
            Assert.That(() => preferences.GetOrDefault(null!, 0), Throws.ArgumentNullException);
            Assert.That(async () => await preferences.SetAsync(null!, 0), Throws.ArgumentNullException);
            Assert.That(async () => await preferences.RemoveAsync(null!), Throws.ArgumentNullException);
        });
    }

    [Test]
    public void GetDomain_resolves_only_the_bound_plugins_declared_contract()
    {
        var domain = new SampleDomain();
        var host = PluginTestHost.Create(
            [
                new FakeExplorerPlugin("declaring", domainContract: typeof(ISampleDomain)),
                new FakeExplorerPlugin("bare"),
            ],
            services => services.AddSingleton<ISampleDomain>(domain));

        Assert.Multiple(() =>
        {
            Assert.That(host.Contexts.Create("declaring").GetDomain<ISampleDomain>(), Is.SameAs(domain));
            Assert.That(
                () => host.Contexts.Create("bare").GetDomain<ISampleDomain>(),
                Throws.TypeOf<ExplorerPluginDomainException>());
        });
    }

    [Test]
    public void TryGetDomain_reports_absence_without_throwing()
    {
        var host = PluginTestHost.Create(new FakeExplorerPlugin("bare"));

        Assert.Multiple(() =>
        {
            Assert.That(host.Contexts.Create("bare").TryGetDomain<ISampleDomain>(out var domain), Is.False);
            Assert.That(domain, Is.Null);
        });
    }

    [Test]
    public void Constructor_validates_its_arguments()
    {
        var host = PluginTestHost.Create();

        Assert.Multiple(() =>
        {
            Assert.That(
                () => new ExplorerPluginHostContext(null!, host.State, host.Preferences, host.Domains),
                Throws.ArgumentNullException);
            Assert.That(
                () => new ExplorerPluginHostContext(" ", host.State, host.Preferences, host.Domains),
                Throws.ArgumentException);
            Assert.That(
                () => new ExplorerPluginHostContext("a", null!, host.Preferences, host.Domains),
                Throws.ArgumentNullException);
            Assert.That(
                () => new ExplorerPluginHostContext("a", host.State, null!, host.Domains),
                Throws.ArgumentNullException);
            Assert.That(
                () => new ExplorerPluginHostContext("a", host.State, host.Preferences, null!),
                Throws.ArgumentNullException);
        });
    }

    [Test]
    public void Factory_returns_a_stable_instance_per_plugin_id()
    {
        var host = PluginTestHost.Create(
            new FakeExplorerPlugin("a"),
            new FakeExplorerPlugin("b"));

        Assert.Multiple(() =>
        {
            Assert.That(host.Contexts.Create("a"), Is.SameAs(host.Contexts.Create("a")));
            Assert.That(host.Contexts.Create("a"), Is.Not.SameAs(host.Contexts.Create("b")));
        });
    }

    [Test]
    public void Factory_rejects_a_null_plugin_id()
    {
        var host = PluginTestHost.Create();

        Assert.That(() => host.Contexts.Create(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void Factory_constructor_rejects_null_dependencies()
    {
        var host = PluginTestHost.Create();

        Assert.Multiple(() =>
        {
            Assert.That(
                () => new ExplorerPluginHostContextFactory(null!, host.Preferences, host.Domains),
                Throws.ArgumentNullException);
            Assert.That(
                () => new ExplorerPluginHostContextFactory(host.State, null!, host.Domains),
                Throws.ArgumentNullException);
            Assert.That(
                () => new ExplorerPluginHostContextFactory(host.State, host.Preferences, null!),
                Throws.ArgumentNullException);
        });
    }

    internal interface ISampleDomain;

    private sealed class SampleDomain : ISampleDomain;
}
