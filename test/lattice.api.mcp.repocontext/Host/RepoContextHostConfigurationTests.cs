using Microsoft.Extensions.Configuration;
using Orleans.Lattice.Api.Mcp.RepoContext.Host;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Host;

/// <summary>
/// Unit tests for <see cref="RepoContextHostConfiguration"/>: profile resolution,
/// per-store overrides, path defaults, and the fail-fast validation that refuses
/// to start when a selected provider is missing a required credential.
/// </summary>
[TestFixture]
public sealed class RepoContextHostConfigurationTests
{
    private static RepoContextHostConfiguration From(params (string Key, string Value)[] pairs)
    {
        var dict = pairs.ToDictionary(p => p.Key, p => (string?)p.Value);
        var configuration = new ConfigurationBuilder().AddInMemoryCollection(dict).Build();
        return RepoContextHostConfiguration.FromConfiguration(configuration);
    }

    [Test]
    public void FromConfiguration_defaults_to_the_local_profile()
    {
        var config = From();

        Assert.Multiple(() =>
        {
            Assert.That(config.Profile, Is.EqualTo(DurabilityProfile.Local));
            Assert.That(config.Wal, Is.EqualTo(WalProvider.File));
            Assert.That(config.GrainStorage, Is.EqualTo(RelationalStore.Sqlite));
            Assert.That(config.Reminders, Is.EqualTo(RelationalStore.Sqlite));
            Assert.That(config.Clustering, Is.EqualTo(ClusteringProvider.Localhost));
            Assert.That(config.UsesSqlite, Is.True);
            Assert.That(config.UsesFileWal, Is.True);
            Assert.That(config.UsesPostgres, Is.False);
        });
    }

    [Test]
    public void FromConfiguration_places_wal_and_sqlite_under_the_data_root_by_default()
    {
        var config = From((RepoContextHostConfiguration.DataRootKey, "/srv/data"));

        Assert.Multiple(() =>
        {
            Assert.That(config.DataRoot, Is.EqualTo("/srv/data"));
            Assert.That(config.WalDirectory, Is.EqualTo("/srv/data/wal"));
            Assert.That(config.SqlitePath, Is.EqualTo("/srv/data/repocontext.db"));
        });
    }

    [Test]
    public void FromConfiguration_honours_explicit_wal_and_sqlite_paths()
    {
        var config = From(
            (RepoContextHostConfiguration.WalDirKey, "/mnt/wal"),
            (RepoContextHostConfiguration.SqlitePathKey, "/mnt/db/repo.db"));

        Assert.Multiple(() =>
        {
            Assert.That(config.WalDirectory, Is.EqualTo("/mnt/wal"));
            Assert.That(config.SqlitePath, Is.EqualTo("/mnt/db/repo.db"));
        });
    }

    [Test]
    public void FromConfiguration_postgres_profile_defaults_all_relational_stores_to_postgres()
    {
        var config = From(
            (RepoContextHostConfiguration.DurabilityKey, "postgres"),
            (RepoContextHostConfiguration.PostgresConnectionKey, "Host=db;Database=repo"));

        Assert.Multiple(() =>
        {
            Assert.That(config.Profile, Is.EqualTo(DurabilityProfile.Postgres));
            Assert.That(config.GrainStorage, Is.EqualTo(RelationalStore.Postgres));
            Assert.That(config.Reminders, Is.EqualTo(RelationalStore.Postgres));
            Assert.That(config.Wal, Is.EqualTo(WalProvider.File));
            Assert.That(config.UsesPostgres, Is.True);
        });
    }

    [Test]
    public void FromConfiguration_azure_profile_selects_azure_across_every_store()
    {
        var config = From(
            (RepoContextHostConfiguration.DurabilityKey, "azure"),
            (RepoContextHostConfiguration.AzureConnectionKey, "UseDevelopmentStorage=true"));

        Assert.Multiple(() =>
        {
            Assert.That(config.Profile, Is.EqualTo(DurabilityProfile.Azure));
            Assert.That(config.Wal, Is.EqualTo(WalProvider.Azure));
            Assert.That(config.GrainStorage, Is.EqualTo(RelationalStore.Azure));
            Assert.That(config.Reminders, Is.EqualTo(RelationalStore.Azure));
            Assert.That(config.Clustering, Is.EqualTo(ClusteringProvider.Azure));
        });
    }

    [Test]
    public void FromConfiguration_allows_a_mixed_profile_via_per_store_overrides()
    {
        var config = From(
            (RepoContextHostConfiguration.DurabilityKey, "local"),
            (RepoContextHostConfiguration.RemindersKey, "postgres"),
            (RepoContextHostConfiguration.PostgresConnectionKey, "Host=db"));

        Assert.Multiple(() =>
        {
            Assert.That(config.GrainStorage, Is.EqualTo(RelationalStore.Sqlite));
            Assert.That(config.Reminders, Is.EqualTo(RelationalStore.Postgres));
            Assert.That(config.UsesSqlite, Is.True);
            Assert.That(config.UsesPostgres, Is.True);
        });
    }

    [Test]
    public void FromConfiguration_overrides_the_wal_provider_to_azure_independently()
    {
        var config = From(
            (RepoContextHostConfiguration.DurabilityKey, "local"),
            (RepoContextHostConfiguration.WalProviderKey, "azure"),
            (RepoContextHostConfiguration.AzureConnectionKey, "UseDevelopmentStorage=true"));

        Assert.Multiple(() =>
        {
            Assert.That(config.Wal, Is.EqualTo(WalProvider.Azure));
            Assert.That(config.GrainStorage, Is.EqualTo(RelationalStore.Sqlite));
            Assert.That(config.UsesFileWal, Is.False);
        });
    }

    [Test]
    public void FromConfiguration_throws_when_postgres_is_selected_without_a_connection_string()
    {
        Assert.That(
            () => From((RepoContextHostConfiguration.DurabilityKey, "postgres")),
            Throws.InvalidOperationException.With.Message.Contains(
                RepoContextHostConfiguration.PostgresConnectionKey));
    }

    [Test]
    public void FromConfiguration_throws_when_azure_is_selected_without_a_connection_string()
    {
        Assert.That(
            () => From((RepoContextHostConfiguration.DurabilityKey, "azure")),
            Throws.InvalidOperationException.With.Message.Contains(
                RepoContextHostConfiguration.AzureConnectionKey));
    }

    [Test]
    public void FromConfiguration_throws_when_a_wal_azure_override_lacks_a_connection_string()
    {
        Assert.That(
            () => From(
                (RepoContextHostConfiguration.DurabilityKey, "local"),
                (RepoContextHostConfiguration.WalProviderKey, "azure")),
            Throws.InvalidOperationException.With.Message.Contains(
                RepoContextHostConfiguration.AzureConnectionKey));
    }

    [Test]
    public void FromConfiguration_throws_on_an_unknown_profile()
    {
        Assert.That(
            () => From((RepoContextHostConfiguration.DurabilityKey, "sqlserver")),
            Throws.InvalidOperationException);
    }

    [Test]
    public void FromConfiguration_throws_on_a_non_numeric_port()
    {
        Assert.That(
            () => From((RepoContextHostConfiguration.McpPortKey, "not-a-port")),
            Throws.InvalidOperationException);
    }

    [Test]
    public void FromConfiguration_reads_embedding_and_port_overrides()
    {
        var config = From(
            (RepoContextHostConfiguration.EmbeddingEndpointKey, "http://onyx:9000"),
            (RepoContextHostConfiguration.EmbeddingModelKey, "custom-model"),
            (RepoContextHostConfiguration.EmbeddingDimensionKey, "1024"),
            (RepoContextHostConfiguration.McpPortKey, "9999"));

        Assert.Multiple(() =>
        {
            Assert.That(config.EmbeddingEndpoint, Is.EqualTo(new Uri("http://onyx:9000")));
            Assert.That(config.EmbeddingModel, Is.EqualTo("custom-model"));
            Assert.That(config.EmbeddingDimension, Is.EqualTo(1024));
            Assert.That(config.McpPort, Is.EqualTo(9999));
        });
    }

    [Test]
    public void FromConfiguration_defaults_embedding_endpoint_to_the_onyx_companion()
    {
        var config = From();

        Assert.That(
            config.EmbeddingEndpoint,
            Is.EqualTo(new Uri(OnyxEmbeddingOptions.DefaultBaseAddress)));
    }

    [Test]
    public void FromConfiguration_throws_on_a_null_configuration()
        => Assert.That(() => RepoContextHostConfiguration.FromConfiguration(null!), Throws.ArgumentNullException);
}
