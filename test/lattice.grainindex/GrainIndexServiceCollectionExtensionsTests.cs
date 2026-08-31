using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.GrainIndex.Enrollment;
using Orleans.Runtime;
using Orleans.Serialization;

namespace Orleans.Lattice.GrainIndex.Tests;

/// <summary>
/// Covers <see cref="GrainIndexServiceCollectionExtensions"/>: what
/// <c>AddGrainIndex</c> registers, how the declaration seeds the named options,
/// how <c>ConfigureGrainIndex</c> overrides them, and that an invalid
/// declaration fails when the options are resolved.
/// </summary>
[TestFixture]
public sealed class GrainIndexServiceCollectionExtensionsTests
{
    private static ServiceProvider Provider(Action<StubSiloBuilder> configure)
    {
        var builder = new StubSiloBuilder();
        configure(builder);
        return builder.BuildServiceProvider();
    }

    private static GrainIndexOptions OptionsFor(ServiceProvider provider, string indexName) =>
        provider.GetRequiredService<IOptionsMonitor<GrainIndexOptions>>().Get(indexName);

    [Test]
    public void Add_grain_index_rejects_a_null_builder() =>
        Assert.That(
            () => GrainIndexServiceCollectionExtensions.AddGrainIndex<ITestStringKeyedGrain, TestGrainState>(
                null!, static cfg => cfg.Include(x => x.Age)),
            Throws.ArgumentNullException);

    [Test]
    public void Add_grain_index_rejects_a_null_configure_delegate() =>
        Assert.That(
            () => new StubSiloBuilder().AddGrainIndex<ITestStringKeyedGrain, TestGrainState>(null!),
            Throws.ArgumentNullException);

    [Test]
    public void Add_grain_index_returns_the_same_builder_so_silo_setup_chains()
    {
        var builder = new StubSiloBuilder();

        var returned = builder.AddGrainIndex<ITestStringKeyedGrain, TestGrainState>(
            static cfg => cfg.WithName("users").Include(x => x.Age));

        Assert.That(returned, Is.SameAs(builder));
    }

    [Test]
    public void A_declared_index_is_resolvable_from_dependency_injection()
    {
        using var provider = Provider(static builder =>
            builder.AddGrainIndex<ITestStringKeyedGrain, TestGrainState>(
                static cfg => cfg.WithName("users").Include(x => x.Age)));

        var declarations = provider.GetRequiredService<IOptions<GrainIndexDeclarationOptions>>().Value;

        Assert.Multiple(() =>
        {
            Assert.That(declarations.Definitions, Has.Count.EqualTo(1));
            Assert.That(declarations.Definitions[0].Name, Is.EqualTo("users"));
            Assert.That(declarations.Definitions[0].GrainInterfaceType, Is.EqualTo(typeof(ITestStringKeyedGrain)));
            Assert.That(declarations.Definitions[0].StateType, Is.EqualTo(typeof(TestGrainState)));
            Assert.That(
                declarations.Definitions[0].PropertyDescriptors.Select(d => d.Name),
                Is.EqualTo(new[] { "Age" }));
        });
    }

    [Test]
    public void Several_indexes_can_be_declared_on_one_silo()
    {
        using var provider = Provider(static builder => builder
            .AddGrainIndex<ITestStringKeyedGrain, TestGrainState>(
                static cfg => cfg.WithName("users").Include(x => x.Age))
            .AddGrainIndex<ITestGuidKeyedGrain, TestGrainState>(
                static cfg => cfg.WithName("orders").Include(x => x.Country)));

        var declarations = provider.GetRequiredService<IOptions<GrainIndexDeclarationOptions>>().Value;

        Assert.That(
            declarations.Definitions.Select(d => d.Name),
            Is.EqualTo(new[] { "users", "orders" }));
    }

    [Test]
    public void The_declaration_seeds_the_default_reserved_tree_name()
    {
        using var provider = Provider(static builder =>
            builder.AddGrainIndex<ITestStringKeyedGrain, TestGrainState>(
                static cfg => cfg.WithName("users").Include(x => x.Age)));

        Assert.That(OptionsFor(provider, "users").TreeName, Is.EqualTo("__grainindex/users"));
    }

    [Test]
    public void The_declaration_seeds_allow_replication_as_false_by_default()
    {
        using var provider = Provider(static builder =>
            builder.AddGrainIndex<ITestStringKeyedGrain, TestGrainState>(
                static cfg => cfg.WithName("users").Include(x => x.Age)));

        Assert.That(OptionsFor(provider, "users").AllowReplication, Is.False);
    }

    [Test]
    public void The_declaration_seeds_an_explicit_replication_opt_in()
    {
        using var provider = Provider(static builder =>
            builder.AddGrainIndex<ITestStringKeyedGrain, TestGrainState>(
                static cfg => cfg.WithName("users").AllowReplication().Include(x => x.Age)));

        Assert.That(OptionsFor(provider, "users").AllowReplication, Is.True);
    }

    [Test]
    public void The_declaration_seeds_an_explicit_tree_name_and_backfill_knobs()
    {
        using var provider = Provider(static builder =>
            builder.AddGrainIndex<ITestStringKeyedGrain, TestGrainState>(static cfg => cfg
                .WithName("users")
                .WithTreeName("__grainindex/custom")
                .WithBackfillBatchSize(16)
                .WithBackfillInterval(TimeSpan.FromMinutes(2))
                .Include(x => x.Age)));

        var options = OptionsFor(provider, "users");

        Assert.Multiple(() =>
        {
            Assert.That(options.TreeName, Is.EqualTo("__grainindex/custom"));
            Assert.That(options.BackfillBatchSize, Is.EqualTo(16));
            Assert.That(options.BackfillInterval, Is.EqualTo(TimeSpan.FromMinutes(2)));
        });
    }

    [Test]
    public void Backfill_knobs_the_declaration_left_alone_keep_the_option_defaults()
    {
        using var provider = Provider(static builder =>
            builder.AddGrainIndex<ITestStringKeyedGrain, TestGrainState>(
                static cfg => cfg.WithName("users").Include(x => x.Age)));

        var options = OptionsFor(provider, "users");

        Assert.Multiple(() =>
        {
            Assert.That(options.BackfillBatchSize, Is.EqualTo(GrainIndexOptions.DefaultBackfillBatchSize));
            Assert.That(options.BackfillInterval, Is.EqualTo(GrainIndexOptions.DefaultBackfillInterval));
        });
    }

    [Test]
    public void Configure_grain_index_overrides_one_index_by_name()
    {
        using var provider = Provider(static builder => builder
            .AddGrainIndex<ITestStringKeyedGrain, TestGrainState>(
                static cfg => cfg.WithName("users").Include(x => x.Age))
            .AddGrainIndex<ITestGuidKeyedGrain, TestGrainState>(
                static cfg => cfg.WithName("orders").Include(x => x.Country))
            .ConfigureGrainIndex("users", static options => options.BackfillBatchSize = 8));

        Assert.Multiple(() =>
        {
            Assert.That(OptionsFor(provider, "users").BackfillBatchSize, Is.EqualTo(8));
            Assert.That(
                OptionsFor(provider, "orders").BackfillBatchSize,
                Is.EqualTo(GrainIndexOptions.DefaultBackfillBatchSize));
        });
    }

    [Test]
    public void Configure_grain_index_by_name_overrides_a_value_the_declaration_seeded()
    {
        using var provider = Provider(static builder => builder
            .AddGrainIndex<ITestStringKeyedGrain, TestGrainState>(
                static cfg => cfg.WithName("users").Include(x => x.Age))
            .ConfigureGrainIndex("users", static options =>
            {
                options.TreeName = "__grainindex/users-v2";
                options.AllowReplication = true;
            }));

        var options = OptionsFor(provider, "users");

        Assert.Multiple(() =>
        {
            Assert.That(options.TreeName, Is.EqualTo("__grainindex/users-v2"));
            Assert.That(options.AllowReplication, Is.True);
        });
    }

    [Test]
    public void Configure_grain_index_without_a_name_applies_to_every_index()
    {
        using var provider = Provider(static builder => builder
            .AddGrainIndex<ITestStringKeyedGrain, TestGrainState>(
                static cfg => cfg.WithName("users").Include(x => x.Age))
            .AddGrainIndex<ITestGuidKeyedGrain, TestGrainState>(
                static cfg => cfg.WithName("orders").Include(x => x.Country))
            .ConfigureGrainIndex(static options => options.BackfillInterval = TimeSpan.FromSeconds(30)));

        Assert.Multiple(() =>
        {
            Assert.That(OptionsFor(provider, "users").BackfillInterval, Is.EqualTo(TimeSpan.FromSeconds(30)));
            Assert.That(OptionsFor(provider, "orders").BackfillInterval, Is.EqualTo(TimeSpan.FromSeconds(30)));
        });
    }

    [Test]
    public void Configure_grain_index_returns_the_same_builder_in_both_forms()
    {
        var builder = new StubSiloBuilder();

        Assert.Multiple(() =>
        {
            Assert.That(builder.ConfigureGrainIndex(static _ => { }), Is.SameAs(builder));
            Assert.That(builder.ConfigureGrainIndex("users", static _ => { }), Is.SameAs(builder));
        });
    }

    [Test]
    public void Configure_grain_index_rejects_a_null_builder()
    {
        Assert.Multiple(() =>
        {
            Assert.That(
                () => GrainIndexServiceCollectionExtensions.ConfigureGrainIndex(null!, static _ => { }),
                Throws.ArgumentNullException);
            Assert.That(
                () => GrainIndexServiceCollectionExtensions.ConfigureGrainIndex(null!, "users", static _ => { }),
                Throws.ArgumentNullException);
        });
    }

    [Test]
    public void Configure_grain_index_rejects_a_null_configure_delegate()
    {
        var builder = new StubSiloBuilder();

        Assert.Multiple(() =>
        {
            Assert.That(() => builder.ConfigureGrainIndex(null!), Throws.ArgumentNullException);
            Assert.That(() => builder.ConfigureGrainIndex("users", null!), Throws.ArgumentNullException);
        });
    }

    [Test]
    public void Configure_grain_index_rejects_a_null_index_name() =>
        Assert.That(
            () => new StubSiloBuilder().ConfigureGrainIndex(null!, static _ => { }),
            Throws.ArgumentNullException);

    [TestCase("")]
    [TestCase("   ")]
    public void Configure_grain_index_rejects_an_empty_or_whitespace_index_name(string indexName) =>
        Assert.That(
            () => new StubSiloBuilder().ConfigureGrainIndex(indexName, static _ => { }),
            Throws.ArgumentException);

    [Test]
    public void Add_grain_index_registers_the_validators_and_the_startup_check_once()
    {
        var builder = new StubSiloBuilder();
        builder
            .AddGrainIndex<ITestStringKeyedGrain, TestGrainState>(
                static cfg => cfg.WithName("users").Include(x => x.Age))
            .AddGrainIndex<ITestGuidKeyedGrain, TestGrainState>(
                static cfg => cfg.WithName("orders").Include(x => x.Country));

        // Resolving the hosted-service enumerable constructs the whole
        // registered graph, including the registry reconciler, which needs the
        // silo services a real host always has.
        builder.Services.AddLogging();
        builder.Services.AddSerializer();
        builder.Services.AddSingleton(Substitute.For<IGrainFactory>());

        using var provider = builder.BuildServiceProvider();

        Assert.Multiple(() =>
        {
            Assert.That(
                provider.GetServices<IValidateOptions<GrainIndexOptions>>()
                    .OfType<GrainIndexOptionsValidator>().Count(),
                Is.EqualTo(1));
            Assert.That(
                provider.GetServices<IValidateOptions<GrainIndexDeclarationOptions>>()
                    .OfType<GrainIndexDeclarationOptionsValidator>().Count(),
                Is.EqualTo(1));
            Assert.That(
                provider.GetServices<IHostedService>().OfType<GrainIndexStartupValidator>().Count(),
                Is.EqualTo(1));
        });
    }

    [Test]
    public void An_index_with_no_included_property_fails_when_the_declaration_set_resolves()
    {
        using var provider = Provider(static builder =>
            builder.AddGrainIndex<ITestStringKeyedGrain, TestGrainState>(
                static cfg => cfg.WithName("users")));

        Assert.That(
            () => provider.GetRequiredService<IOptions<GrainIndexDeclarationOptions>>().Value,
            Throws.TypeOf<OptionsValidationException>()
                .With.Message.Contains("users").And.Message.Contains("projects no properties"));
    }

    [Test]
    public void A_duplicate_index_name_fails_when_the_declaration_set_resolves()
    {
        using var provider = Provider(static builder => builder
            .AddGrainIndex<ITestStringKeyedGrain, TestGrainState>(
                static cfg => cfg.WithName("users").Include(x => x.Age))
            .AddGrainIndex<ITestGuidKeyedGrain, TestGrainState>(
                static cfg => cfg.WithName("users").Include(x => x.Country)));

        Assert.That(
            () => provider.GetRequiredService<IOptions<GrainIndexDeclarationOptions>>().Value,
            Throws.TypeOf<OptionsValidationException>()
                .With.Message.Contains("users").And.Message.Contains("declared more than once"));
    }

    [Test]
    public void A_tree_name_outside_the_reserved_namespace_fails_when_the_index_options_resolve()
    {
        using var provider = Provider(static builder => builder
            .AddGrainIndex<ITestStringKeyedGrain, TestGrainState>(
                static cfg => cfg.WithName("users").Include(x => x.Age))
            .ConfigureGrainIndex("users", static options => options.TreeName = "app-users"));

        Assert.That(
            () => OptionsFor(provider, "users"),
            Throws.TypeOf<OptionsValidationException>()
                .With.Message.Contains("users").And.Message.Contains(GrainIndexTreeNames.ReservedPrefix));
    }

    [Test]
    public void Declaring_an_index_over_a_grain_whose_key_cannot_be_encoded_fails_at_declaration_time() =>
        Assert.That(
            () => new StubSiloBuilder().AddGrainIndex<ITestCompoundKeyedGrain, TestGrainState>(
                static cfg => cfg.WithName("compound").Include(x => x.Age)),
            Throws.TypeOf<GrainIndexKeyEncodingException>(),
            "A grain that cannot be indexed is a declaration error, not a grain to skip silently.");

    [Test]
    public void The_declaration_registers_the_enrolment_path_the_indexed_attribute_binds_to()
    {
        var builder = new StubSiloBuilder();
        builder.AddGrainIndex<ITestStringKeyedGrain, TestGrainState>(
            static cfg => cfg.WithName("users").Include(x => x.Age));

        Assert.Multiple(() =>
        {
            Assert.That(
                builder.Services.Any(d =>
                    d.ServiceType == typeof(IAttributeToFactoryMapper<IndexedAttribute>)),
                Is.True,
                "Without the mapper an [Indexed] parameter is just an unresolvable constructor "
                + "argument, so every tracked grain would fail to activate.");
            Assert.That(
                builder.Services.Any(d => d.ServiceType == typeof(IGrainIndexEnrollmentStore)),
                Is.True);
            Assert.That(
                builder.Services.Any(d => d.ServiceType == typeof(GrainIndexEnrollmentSet<>)),
                Is.True);
            Assert.That(
                builder.Services.Any(d => d.ServiceType == typeof(GrainIndexOutboxDrainer)),
                Is.True);
            Assert.That(
                builder.Services.Any(d =>
                    d.ServiceType == typeof(IHostedService)
                    && d.ImplementationType == typeof(GrainIndexOutboxHostedService)),
                Is.True);
        });
    }

    [Test]
    public void The_declaration_registers_the_operator_surface()
    {
        var builder = new StubSiloBuilder();
        builder.AddGrainIndex<ITestStringKeyedGrain, TestGrainState>(
            static cfg => cfg.WithName("users").Include(x => x.Age));

        Assert.That(
            builder.Services.Any(d => d.ServiceType == typeof(IGrainIndexAdmin)),
            Is.True,
            "An operator has to be able to resolve IGrainIndexAdmin without wiring it by hand.");
    }

    [Test]
    public void Declaring_several_indexes_registers_the_operator_surface_once()
    {
        var builder = new StubSiloBuilder();
        builder
            .AddGrainIndex<ITestStringKeyedGrain, TestGrainState>(
                static cfg => cfg.WithName("users").Include(x => x.Age))
            .AddGrainIndex<ITestGuidKeyedGrain, TestGrainState>(
                static cfg => cfg.WithName("others").Include(x => x.Age));

        Assert.That(
            builder.Services.Count(d => d.ServiceType == typeof(IGrainIndexAdmin)),
            Is.EqualTo(1));
    }

    [Test]
    public void Declaring_several_indexes_registers_the_enrolment_path_once()
    {
        var builder = new StubSiloBuilder();
        builder
            .AddGrainIndex<ITestStringKeyedGrain, TestGrainState>(
                static cfg => cfg.WithName("users").Include(x => x.Age))
            .AddGrainIndex<ITestGuidKeyedGrain, TestGrainState>(
                static cfg => cfg.WithName("others").Include(x => x.Age));

        Assert.Multiple(() =>
        {
            Assert.That(
                builder.Services.Count(d => d.ServiceType == typeof(GrainIndexOutboxDrainer)),
                Is.EqualTo(1),
                "One drain serves the whole silo: a second would race the first over the same "
                + "outbox entries.");
            Assert.That(
                builder.Services.Count(d =>
                    d.ServiceType == typeof(IHostedService)
                    && d.ImplementationType == typeof(GrainIndexOutboxHostedService)),
                Is.EqualTo(1));
            Assert.That(
                builder.Services.Count(d =>
                    d.ServiceType == typeof(IAttributeToFactoryMapper<IndexedAttribute>)),
                Is.EqualTo(1));
        });
    }

    [Test]
    public void The_outbox_settings_take_their_documented_defaults()
    {
        using var provider = Provider(static builder => builder
            .AddGrainIndex<ITestStringKeyedGrain, TestGrainState>(
                static cfg => cfg.WithName("users").Include(x => x.Age)));

        var options = provider.GetRequiredService<IOptions<GrainIndexOutboxOptions>>().Value;

        Assert.Multiple(() =>
        {
            Assert.That(options.Enabled, Is.True);
            Assert.That(options.RetryInterval, Is.EqualTo(GrainIndexOutboxOptions.DefaultRetryInterval));
        });
    }

    [Test]
    public void Configure_grain_index_outbox_overrides_the_silo_wide_settings()
    {
        using var provider = Provider(static builder => builder
            .AddGrainIndex<ITestStringKeyedGrain, TestGrainState>(
                static cfg => cfg.WithName("users").Include(x => x.Age))
            .ConfigureGrainIndexOutbox(static options =>
            {
                options.Enabled = false;
                options.MaxBatchSize = 4;
            }));

        var options = provider.GetRequiredService<IOptions<GrainIndexOutboxOptions>>().Value;

        Assert.Multiple(() =>
        {
            Assert.That(options.Enabled, Is.False);
            Assert.That(options.MaxBatchSize, Is.EqualTo(4));
        });
    }

    [Test]
    public void Configure_grain_index_outbox_rejects_a_null_argument()
    {
        Assert.Multiple(() =>
        {
            Assert.That(
                () => GrainIndexServiceCollectionExtensions.ConfigureGrainIndexOutbox(
                    null!, static _ => { }),
                Throws.ArgumentNullException);
            Assert.That(
                () => new StubSiloBuilder().ConfigureGrainIndexOutbox(null!),
                Throws.ArgumentNullException);
        });
    }

    [Test]
    public void Configure_grain_index_outbox_returns_the_same_builder_so_silo_setup_chains()
    {
        var builder = new StubSiloBuilder();

        Assert.That(builder.ConfigureGrainIndexOutbox(static _ => { }), Is.SameAs(builder));
    }
}
