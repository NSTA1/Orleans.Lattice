using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using Orleans.Lattice.Views;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.State.Tests;

[TestFixture]
public sealed class LatticeStateViewSourceAuthorizationTests
{
    [TestCase("preserving", true)]
    [TestCase("rekeyed", false)]
    [TestCase("history", false)]
    [TestCase("aggregate", false)]
    [TestCase("custom", false)]
    public async Task Partial_source_grants_require_proven_key_preservation(string kind, bool expected)
    {
        using var services = CreateServices(kind, partial: true);
        var access = await ResolveAsync(services);

        Assert.That(access.Allowed, Is.EqualTo(expected));
        if (expected)
        {
            Assert.That(access.KeyFilter, Is.Not.Null);
            Assert.That(access.KeyFilter!("x/allowed"), Is.True);
            Assert.That(access.KeyFilter("y/denied"), Is.False);
        }
    }

    [TestCase("preserving")]
    [TestCase("rekeyed")]
    [TestCase("history")]
    [TestCase("aggregate")]
    [TestCase("custom")]
    public async Task Whole_source_grants_preserve_access_to_all_projection_kinds(string kind)
    {
        using var services = CreateServices(kind, partial: false);
        var access = await ResolveAsync(services);

        Assert.Multiple(() =>
        {
            Assert.That(access.Allowed, Is.True);
            Assert.That(access.KeyFilter, Is.Null);
        });
    }

    private static ValueTask<(bool Allowed, Func<string, bool>? KeyFilter)> ResolveAsync(ServiceProvider services) =>
        LatticeStateViewSource.ResolveReadAccessAsync(
            services,
            Substitute.For<IGrainFactory>(),
            new LatticeStateVisibilityFilter(services, new LatticeApiStateOptions()),
            "view-test",
            new LatticeSubject("reader"),
            CancellationToken.None);

    private static ServiceProvider CreateServices(string kind, bool partial)
    {
        var catalog = Substitute.For<IViewCatalog>();
        var services = new ServiceCollection()
            .AddSingleton(catalog)
            .AddSingleton<ILatticeAccessGate>(new SourceGate(partial));
        services.AddSerializer();
        var provider = services.BuildServiceProvider();
        ILatticeViewProjection? projection = kind switch
        {
            "preserving" => new PredicateLatticeViewProjection(),
            "rekeyed" => new PredicateLatticeViewProjection(keySelector: k => "x/" + k, keySelectorVersion: "v1"),
            "history" => new HistoryLatticeViewProjection(
                new HistoryRowCodec(provider.GetRequiredService<Serializer<HistoryRow>>())),
            "aggregate" => null,
            _ => Substitute.For<ILatticeViewProjection>(),
        };
        catalog.TryGet("test").Returns(new ViewRegistration(
            "test", "source", projection,
            kind == "aggregate" ? Substitute.For<ILatticeAggregationProjection>() : null));
        return provider;
    }

    private sealed class SourceGate(bool partial) : ILatticeAccessGate
    {
        public ValueTask<LatticeAccessDecision> AuthorizeAsync(
            in LatticeAccessRequest request, CancellationToken cancellationToken = default) =>
            new(partial
                ? LatticeAccessDecision.Filtered(k => k.StartsWith("x/", StringComparison.Ordinal))
                : LatticeAccessDecision.Allow());
    }
}
