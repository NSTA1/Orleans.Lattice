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
        var optionsMonitor = new RecordingOptionsMonitor(
            provider.GetRequiredService<IOptionsMonitor<GrainIndexOptions>>());

        await new GrainIndexStartupValidator(
            provider.GetRequiredService<IOptions<GrainIndexDeclarationOptions>>(),
            optionsMonitor)
            .StartAsync(CancellationToken.None);

        Assert.That(optionsMonitor.RequestedNames, Is.EqualTo(new[] { "users", "orders" }),
            "Starting a host with valid declarations must resolve every named index option.");
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

        var optionsMonitor = new RecordingOptionsMonitor(
            bare.GetRequiredService<IOptionsMonitor<GrainIndexOptions>>());
        var task = new GrainIndexStartupValidator(
            bare.GetRequiredService<IOptions<GrainIndexDeclarationOptions>>(),
            optionsMonitor)
            .StartAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(task.IsCompletedSuccessfully, Is.True,
                "With no declared indexes, startup should complete without asynchronous work.");
            Assert.That(optionsMonitor.RequestedNames, Is.Empty,
                "A silo that declares no grain index must not resolve any named index options.");
        });
        await task;
    }

    [Test]
    public async Task Stop_is_a_no_op()
    {
        using var provider = Provider(static builder =>
            builder.AddGrainIndex<ITestStringKeyedGrain, TestGrainState>(
                static cfg => cfg.WithName("users").Include(x => x.Age)));

        var task = ValidatorFrom(provider).StopAsync(CancellationToken.None);

        Assert.That(task.IsCompletedSuccessfully, Is.True,
            "The validator holds no resources, so stopping it should complete synchronously.");
        await task;
    }

    private sealed class RecordingOptionsMonitor(IOptionsMonitor<GrainIndexOptions> inner)
        : IOptionsMonitor<GrainIndexOptions>
    {
        private readonly List<string> _requestedNames = [];

        public GrainIndexOptions CurrentValue => inner.CurrentValue;

        public IReadOnlyList<string> RequestedNames => _requestedNames;

        public GrainIndexOptions Get(string? name)
        {
            _requestedNames.Add(name ?? Options.DefaultName);
            return inner.Get(name);
        }

        public IDisposable? OnChange(Action<GrainIndexOptions, string?> listener) => inner.OnChange(listener);
    }
}
