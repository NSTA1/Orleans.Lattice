using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace Orleans.Lattice.GrainIndex.Tests;

/// <summary>
/// Covers <see cref="GrainIndexStartupValidator"/>: the hosted service that
/// forces every declared index's lazily-built options to resolve while the host
/// is starting, so a misconfiguration fails startup instead of surfacing on the
/// first write.
/// </summary>
[TestFixture]
public sealed class GrainIndexStartupValidatorTests
{
    private static ServiceProvider Provider(Action<StubSiloBuilder> configure)
    {
        var builder = new StubSiloBuilder();
        configure(builder);
        return builder.BuildServiceProvider();
    }

    private static GrainIndexStartupValidator ValidatorFrom(ServiceProvider provider) =>
        new(provider.GetRequiredService<IOptions<GrainIndexDeclarationOptions>>(),
            provider.GetRequiredService<IOptionsMonitor<GrainIndexOptions>>());

    [Test]
    public async Task Start_completes_for_a_valid_declaration_set()
    {
        using var provider = Provider(static builder => builder
            .AddGrainIndex<ITestStringKeyedGrain, TestGrainState>(
                static cfg => cfg.WithName("users").Include(x => x.Age))
            .AddGrainIndex<ITestGuidKeyedGrain, TestGrainState>(
                static cfg => cfg.WithName("orders").Include(x => x.Country)));

        await ValidatorFrom(provider).StartAsync(CancellationToken.None);

        Assert.Pass("Starting a host with valid declarations must not fail.");
    }

    [Test]
    public void Start_surfaces_a_per_index_options_failure_that_nothing_else_would_have_triggered()
    {
        using var provider = Provider(static builder => builder
            .AddGrainIndex<ITestStringKeyedGrain, TestGrainState>(
                static cfg => cfg.WithName("users").Include(x => x.Age))
            .ConfigureGrainIndex("users", static options => options.BackfillBatchSize = 0));

        Assert.That(
            async () => await ValidatorFrom(provider).StartAsync(CancellationToken.None),
            Throws.TypeOf<OptionsValidationException>().With.Message.Contains("users"),
            "Named options are built lazily, so without this pass an invalid index would only "
            + "fail the first time something asked for it by name.");
    }

    [Test]
    public void Start_surfaces_a_declaration_set_failure()
    {
        using var provider = Provider(static builder => builder
            .AddGrainIndex<ITestStringKeyedGrain, TestGrainState>(
                static cfg => cfg.WithName("users").Include(x => x.Age))
            .AddGrainIndex<ITestGuidKeyedGrain, TestGrainState>(
                static cfg => cfg.WithName("users").Include(x => x.Country)));

        Assert.That(
            async () => await ValidatorFrom(provider).StartAsync(CancellationToken.None),
            Throws.TypeOf<OptionsValidationException>().With.Message.Contains("declared more than once"));
    }

    [Test]
    public async Task Start_completes_when_no_index_is_declared()
    {
        using var provider = Provider(static _ => { });
        provider.Dispose();

        var services = new ServiceCollection();
        services.AddOptions();
        await using var bare = services.BuildServiceProvider();

        await new GrainIndexStartupValidator(
            bare.GetRequiredService<IOptions<GrainIndexDeclarationOptions>>(),
            bare.GetRequiredService<IOptionsMonitor<GrainIndexOptions>>())
            .StartAsync(CancellationToken.None);

        Assert.Pass("A silo that declares no grain index must still start.");
    }

    [Test]
    public async Task Stop_is_a_no_op()
    {
        using var provider = Provider(static builder =>
            builder.AddGrainIndex<ITestStringKeyedGrain, TestGrainState>(
                static cfg => cfg.WithName("users").Include(x => x.Age)));

        await ValidatorFrom(provider).StopAsync(CancellationToken.None);

        Assert.Pass("The validator holds no resources, so stopping it does nothing.");
    }
}
