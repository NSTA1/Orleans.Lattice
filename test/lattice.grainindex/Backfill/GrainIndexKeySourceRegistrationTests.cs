using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using Orleans.Lattice.GrainIndex.Backfill;

namespace Orleans.Lattice.GrainIndex.Tests.Backfill;

/// <summary>
/// Covers the three <c>AddGrainIndexKeySource</c> overloads and the backfill
/// services <c>AddGrainIndex</c> wires up.
/// </summary>
[TestFixture]
public sealed class GrainIndexKeySourceRegistrationTests
{
    /// <summary>A key source the container can construct for the generic overload.</summary>
    internal sealed class ConstructedKeySource : IGrainKeySource
    {
        /// <inheritdoc />
        public IAsyncEnumerable<string> EnumerateKeysAsync(
            string? resumeAfterExclusive,
            CancellationToken cancellationToken) =>
            new ListGrainKeySource(["a"]).EnumerateKeysAsync(resumeAfterExclusive, cancellationToken);
    }

    private static StubSiloBuilder Builder() => new();

    [Test]
    public void The_generic_overload_registers_a_source_the_container_constructs()
    {
        var builder = Builder();
        builder.AddGrainIndexKeySource<ConstructedKeySource>("users");
        using var provider = builder.Services.BuildServiceProvider();

        Assert.That(provider.GetKeyedService<IGrainKeySource>("users"), Is.InstanceOf<ConstructedKeySource>());
    }

    [Test]
    public void The_instance_overload_registers_the_instance_given()
    {
        var source = new ListGrainKeySource(["a"]);
        var builder = Builder();
        builder.AddGrainIndexKeySource("users", source);
        using var provider = builder.Services.BuildServiceProvider();

        Assert.That(provider.GetKeyedService<IGrainKeySource>("users"), Is.SameAs(source));
    }

    [Test]
    public void The_factory_overload_builds_the_source_from_the_container()
    {
        var source = new ListGrainKeySource(["a"]);
        var builder = Builder();
        builder.AddGrainIndexKeySource("users", _ => source);
        using var provider = builder.Services.BuildServiceProvider();

        Assert.That(provider.GetKeyedService<IGrainKeySource>("users"), Is.SameAs(source));
    }

    [Test]
    public void Sources_registered_for_different_indexes_stay_distinct()
    {
        var users = new ListGrainKeySource(["a"]);
        var orders = new ListGrainKeySource(["b"]);
        var builder = Builder();
        builder.AddGrainIndexKeySource("users", users);
        builder.AddGrainIndexKeySource("orders", orders);
        using var provider = builder.Services.BuildServiceProvider();

        Assert.Multiple(() =>
        {
            Assert.That(provider.GetKeyedService<IGrainKeySource>("users"), Is.SameAs(users));
            Assert.That(provider.GetKeyedService<IGrainKeySource>("orders"), Is.SameAs(orders));
        });
    }

    [Test]
    public void Every_overload_rejects_a_null_or_blank_argument()
    {
        var builder = Builder();
        var source = new ListGrainKeySource(["a"]);

        Assert.Multiple(() =>
        {
            Assert.That(
                () => GrainIndexServiceCollectionExtensions.AddGrainIndexKeySource<ConstructedKeySource>(null!, "users"),
                Throws.ArgumentNullException);
            Assert.That(
                () => builder.AddGrainIndexKeySource<ConstructedKeySource>(" "),
                Throws.ArgumentException);
            Assert.That(
                () => GrainIndexServiceCollectionExtensions.AddGrainIndexKeySource(null!, "users", source),
                Throws.ArgumentNullException);
            Assert.That(
                () => builder.AddGrainIndexKeySource("users", (IGrainKeySource)null!),
                Throws.ArgumentNullException);
            Assert.That(
                () => builder.AddGrainIndexKeySource(" ", source),
                Throws.ArgumentException);
            Assert.That(
                () => GrainIndexServiceCollectionExtensions.AddGrainIndexKeySource(
                    null!, "users", _ => source),
                Throws.ArgumentNullException);
            Assert.That(
                () => builder.AddGrainIndexKeySource("users", (Func<IServiceProvider, IGrainKeySource>)null!),
                Throws.ArgumentNullException);
            Assert.That(
                () => builder.AddGrainIndexKeySource(" ", _ => source),
                Throws.ArgumentException);
        });
    }

    [Test]
    public void Declaring_an_index_registers_the_backfill_services_once()
    {
        var builder = Builder();
        builder.AddGrainIndex<ITestStringKeyedGrain, TestGrainState>(cfg => cfg.WithName("users").Include(s => s.Age));
        builder.AddGrainIndex<ITestStringKeyedGrain, TestGrainState>(cfg => cfg.WithName("orders").Include(s => s.Age));

        Assert.Multiple(() =>
        {
            Assert.That(Count<IGrainIndexBackfillStore>(builder), Is.EqualTo(1));
            Assert.That(Count<IGrainIndexBackfillActivator>(builder), Is.EqualTo(1));
            Assert.That(Count<IGrainKeySourceResolver>(builder), Is.EqualTo(1));
            Assert.That(Count<TimeProvider>(builder), Is.EqualTo(1));
            Assert.That(
                builder.Services.Count(d =>
                    d.ServiceType == typeof(Microsoft.Extensions.Hosting.IHostedService)
                    && d.ImplementationType == typeof(GrainIndexBackfillHostedService)),
                Is.EqualTo(1),
                "A silo declaring several indexes must not run the start-up pass once per index.");
        });
    }

    [Test]
    public void The_backfill_start_up_service_is_registered_after_the_registry_reconciler()
    {
        var builder = Builder();
        builder.AddGrainIndex<ITestStringKeyedGrain, TestGrainState>(cfg => cfg.WithName("users").Include(s => s.Age));

        var hosted = builder.Services
            .Where(d => d.ServiceType == typeof(Microsoft.Extensions.Hosting.IHostedService))
            .Select(d => d.ImplementationType)
            .ToList();

        Assert.That(
            hosted.IndexOf(typeof(GrainIndexBackfillHostedService)),
            Is.GreaterThan(hosted.IndexOf(typeof(Orleans.Lattice.GrainIndex.Registry.GrainIndexRegistryHostedService))),
            "The crawl reads the needs-backfill flag that reconciliation raises, so it has to run after it.");
    }

    [Test]
    public void A_host_may_replace_the_default_activator()
    {
        var builder = Builder();
        var custom = Substitute.For<IGrainIndexBackfillActivator>();
        builder.Services.AddSingleton(custom);
        builder.AddGrainIndex<ITestStringKeyedGrain, TestGrainState>(cfg => cfg.WithName("users").Include(s => s.Age));

        Assert.That(Count<IGrainIndexBackfillActivator>(builder), Is.EqualTo(1),
            "The registration is a try-add, so a host that supplied its own keeps it.");
    }

    private static int Count<TService>(StubSiloBuilder builder) =>
        builder.Services.Count(d => d.ServiceType == typeof(TService));
}
