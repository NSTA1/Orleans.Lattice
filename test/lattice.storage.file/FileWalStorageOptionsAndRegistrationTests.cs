using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Orleans.Serialization;

namespace Orleans.Lattice.Storage.File.Tests;

/// <summary>
/// Unit tests for the file WAL package's ancillary public surface: the
/// options validator, the injective path-segment encoder, provider
/// argument guards, and the <see cref="LatticeFileServiceCollectionExtensions"/>
/// DI registration.
/// </summary>
[TestFixture]
public sealed class FileWalStorageOptionsAndRegistrationTests
{
    private ServiceProvider _services = null!;
    private Serializer<WalRecord> _serializer = null!;

    [OneTimeSetUp]
    public void OneTimeSetUp()
    {
        _services = new ServiceCollection().AddSerializer().BuildServiceProvider();
        _serializer = _services.GetRequiredService<Serializer<WalRecord>>();
    }

    [OneTimeTearDown]
    public void OneTimeTearDown() => _services.Dispose();

    // --- options validator ------------------------------------------------

    [Test]
    public void Validator_accepts_a_well_formed_options_instance()
    {
        var validator = new FileWalStorageOptionsValidator();
        var result = validator.Validate(null, new FileWalStorageOptions { RootDirectory = "C:/wal" });
        Assert.That(result.Succeeded, Is.True);
    }

    [Test]
    public void Validator_rejects_an_empty_root_directory()
    {
        var validator = new FileWalStorageOptionsValidator();
        var result = validator.Validate(null, new FileWalStorageOptions { RootDirectory = "   " });
        Assert.That(result.Failed, Is.True);
    }

    [Test]
    public void Validator_rejects_a_non_positive_compaction_threshold()
    {
        var validator = new FileWalStorageOptionsValidator();
        var result = validator.Validate(null, new FileWalStorageOptions
        {
            RootDirectory = "C:/wal",
            CompactionThreshold = 0d,
        });
        Assert.That(result.Failed, Is.True);
    }

    [Test]
    public void Validator_rejects_a_negative_minimum_dead_bytes()
    {
        var validator = new FileWalStorageOptionsValidator();
        var result = validator.Validate(null, new FileWalStorageOptions
        {
            RootDirectory = "C:/wal",
            CompactionMinimumDeadBytes = -1,
        });
        Assert.That(result.Failed, Is.True);
    }

    // --- path-segment encoder --------------------------------------------

    [Test]
    public void EncodePathSegment_passes_unreserved_characters_through_unchanged()
    {
        Assert.That(FileWalStorageProvider.EncodePathSegment("tree-01_A.b"), Is.EqualTo("tree-01_A.b"));
    }

    [Test]
    public void EncodePathSegment_percent_encodes_path_separators_and_reserved_bytes()
    {
        var encoded = FileWalStorageProvider.EncodePathSegment("a/b\\c:d");
        Assert.That(encoded, Does.Not.Contain("/"));
        Assert.That(encoded, Does.Not.Contain("\\"));
        Assert.That(encoded, Does.Not.Contain(":"));
    }

    [Test]
    public void EncodePathSegment_is_injective_for_colliding_inputs()
    {
        // "a/b" and "a%2Fb" must not collide: the literal percent in the
        // second input is itself escaped.
        var first = FileWalStorageProvider.EncodePathSegment("a/b");
        var second = FileWalStorageProvider.EncodePathSegment("a%2Fb");
        Assert.That(first, Is.Not.EqualTo(second));
    }

    [Test]
    public void EncodePathSegment_maps_an_empty_tree_id_to_a_stable_placeholder()
    {
        Assert.That(FileWalStorageProvider.EncodePathSegment(string.Empty), Is.EqualTo("_"));
    }

    // --- provider argument guards ----------------------------------------

    [Test]
    public void Constructor_throws_when_options_are_null()
    {
        Assert.That(
            () => new FileWalStorageProvider(null!, _serializer),
            Throws.ArgumentNullException);
    }

    [Test]
    public void Constructor_throws_when_serializer_is_null()
    {
        var options = Options.Create(new FileWalStorageOptions { RootDirectory = "C:/wal" });
        Assert.That(
            () => new FileWalStorageProvider(options, null!),
            Throws.ArgumentNullException);
    }

    [Test]
    public void Constructor_throws_when_root_directory_is_blank()
    {
        var options = Options.Create(new FileWalStorageOptions { RootDirectory = "  " });
        Assert.That(
            () => new FileWalStorageProvider(options, _serializer),
            Throws.ArgumentException);
    }

    [Test]
    public void ReadAsync_rejects_a_non_positive_max_entries()
    {
        var options = Options.Create(new FileWalStorageOptions
        {
            RootDirectory = Path.Combine(Path.GetTempPath(), "lattice-file-wal-tests", Guid.NewGuid().ToString("N")),
        });
        using var sut = new FileWalStorageProvider(options, _serializer);

        Assert.That(
            async () =>
            {
                await foreach (var _ in sut.ReadAsync("t", 0, -1L, 0, CancellationToken.None))
                {
                }
            },
            Throws.InstanceOf<ArgumentOutOfRangeException>());
    }

    // --- DI registration --------------------------------------------------

    [Test]
    public void AddFileWalStorage_throws_when_the_builder_is_null()
    {
        Assert.That(
            () => ((ISiloBuilder)null!).AddFileWalStorage(_ => { }),
            Throws.ArgumentNullException);
    }

    [Test]
    public void AddFileWalStorage_throws_when_the_configure_callback_is_null()
    {
        var builder = new StubSiloBuilder(new ServiceCollection());
        Assert.That(
            () => builder.AddFileWalStorage(null!),
            Throws.ArgumentNullException);
    }

    [Test]
    public void AddFileWalStorage_binds_options_via_IOptions()
    {
        var services = new ServiceCollection();
        var builder = new StubSiloBuilder(services);

        builder.AddFileWalStorage(o => o.RootDirectory = "C:/wal/custom");

        var sp = services.BuildServiceProvider();
        var bound = sp.GetRequiredService<IOptions<FileWalStorageOptions>>().Value;
        Assert.That(bound.RootDirectory, Is.EqualTo("C:/wal/custom"));
    }

    [Test]
    public void AddFileWalStorage_registers_the_provider_factory_under_IWalStorageProvider()
    {
        var services = new ServiceCollection();
        var builder = new StubSiloBuilder(services);

        builder.AddFileWalStorage(o => o.RootDirectory = "C:/wal");

        var descriptor = services.Single(d => d.ServiceType == typeof(IWalStorageProvider));
        Assert.Multiple(() =>
        {
            Assert.That(descriptor.Lifetime, Is.EqualTo(ServiceLifetime.Singleton));
            Assert.That(descriptor.ImplementationFactory, Is.Not.Null);
        });
    }

    [Test]
    public void AddFileWalStorage_replaces_a_prior_in_memory_baseline()
    {
        var services = new ServiceCollection();
        var builder = new StubSiloBuilder(services);

        builder.AddWalStorage(); // baseline (mimics what AddLattice does)
        builder.AddFileWalStorage(o => o.RootDirectory = "C:/wal");

        var providers = services.Where(d => d.ServiceType == typeof(IWalStorageProvider)).ToList();
        Assert.Multiple(() =>
        {
            Assert.That(providers, Has.Count.EqualTo(1),
                "AddFileWalStorage must Replace the baseline, not stack a second descriptor.");
            Assert.That(providers[0].ImplementationFactory, Is.Not.Null);
            Assert.That(providers[0].ImplementationType, Is.Null);
        });
    }

    [Test]
    public void AddFileWalStorage_registers_the_options_validator()
    {
        var services = new ServiceCollection();
        var builder = new StubSiloBuilder(services);

        builder.AddFileWalStorage(o => o.RootDirectory = "C:/wal");

        var validators = services
            .Where(d => d.ServiceType == typeof(IValidateOptions<FileWalStorageOptions>))
            .ToList();
        Assert.That(validators, Has.Count.EqualTo(1));
    }

    /// <summary>
    /// Minimal <see cref="ISiloBuilder"/> stub that exposes a
    /// <see cref="IServiceCollection"/> for assertions. The Orleans
    /// extension methods called here only touch <c>Services</c>, so the
    /// rest of the interface is left unimplemented.
    /// </summary>
    private sealed class StubSiloBuilder(IServiceCollection services) : ISiloBuilder
    {
        public IServiceCollection Services { get; } = services;
        public IConfiguration Configuration { get; } = new ConfigurationBuilder().Build();
    }
}
