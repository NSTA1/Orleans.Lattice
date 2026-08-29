using Microsoft.Extensions.Configuration;
using Orleans.Lattice.Api.Mcp.RepoContext.Host;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Host;

/// <summary>
/// Unit coverage for the environment-variable parsing tail of
/// <see cref="RepoContextHostConfiguration"/> - the per-provider token maps and
/// the range checks that the profile-level tests do not reach.
///
/// This is the container's only configuration surface, and every knob here
/// selects a durability provider. A token silently mapping to the wrong provider
/// (or to a permissive default instead of failing) would let the host come up
/// writing to somewhere the operator did not intend, so each accepted spelling
/// and each rejection is pinned explicitly.
/// </summary>
[TestFixture]
public sealed class RepoContextHostConfigurationParsingTests
{
    private static RepoContextHostConfiguration From(params (string Key, string Value)[] pairs)
    {
        var dict = pairs.ToDictionary(p => p.Key, p => (string?)p.Value);
        var configuration = new ConfigurationBuilder().AddInMemoryCollection(dict).Build();
        return RepoContextHostConfiguration.FromConfiguration(configuration);
    }

    private static InvalidOperationException? Rejects(params (string Key, string Value)[] pairs)
        => Assert.Throws<InvalidOperationException>(() => From(pairs));

    [TestCase("file", WalProvider.File)]
    [TestCase("FILE", WalProvider.File)]
    [TestCase("azure", WalProvider.Azure)]
    [TestCase("azuretable", WalProvider.Azure)]
    [TestCase("AzureTable", WalProvider.Azure)]
    public void Wal_provider_tokens_map_to_the_expected_provider(string token, WalProvider expected)
    {
        var config = From(
            (RepoContextHostConfiguration.WalProviderKey, token),
            (RepoContextHostConfiguration.AzureConnectionKey, "UseDevelopmentStorage=true"));

        Assert.That(config.Wal, Is.EqualTo(expected));
    }

    [Test]
    public void An_unknown_wal_provider_token_is_rejected()
    {
        var ex = Rejects((RepoContextHostConfiguration.WalProviderKey, "cassandra"));

        Assert.That(ex!.Message, Does.Contain(RepoContextHostConfiguration.WalProviderKey)
            .And.Contain("cassandra"));
    }

    [TestCase("sqlite", RelationalStore.Sqlite)]
    [TestCase("SQLite", RelationalStore.Sqlite)]
    [TestCase("postgres", RelationalStore.Postgres)]
    [TestCase("postgresql", RelationalStore.Postgres)]
    [TestCase("azure", RelationalStore.Azure)]
    [TestCase("azuretable", RelationalStore.Azure)]
    public void Grain_storage_tokens_map_to_the_expected_store(string token, RelationalStore expected)
    {
        var config = From(
            (RepoContextHostConfiguration.GrainStorageKey, token),
            (RepoContextHostConfiguration.PostgresConnectionKey, "Host=localhost"),
            (RepoContextHostConfiguration.AzureConnectionKey, "UseDevelopmentStorage=true"));

        Assert.That(config.GrainStorage, Is.EqualTo(expected));
    }

    [TestCase("sqlite", RelationalStore.Sqlite)]
    [TestCase("postgres", RelationalStore.Postgres)]
    [TestCase("azure", RelationalStore.Azure)]
    public void Reminder_store_tokens_map_to_the_expected_store(string token, RelationalStore expected)
    {
        var config = From(
            (RepoContextHostConfiguration.RemindersKey, token),
            (RepoContextHostConfiguration.PostgresConnectionKey, "Host=localhost"),
            (RepoContextHostConfiguration.AzureConnectionKey, "UseDevelopmentStorage=true"));

        Assert.That(config.Reminders, Is.EqualTo(expected));
    }

    [Test]
    public void An_unknown_relational_store_token_is_rejected_and_names_its_own_key()
    {
        var ex = Rejects((RepoContextHostConfiguration.RemindersKey, "mongo"));

        Assert.That(ex!.Message, Does.Contain(RepoContextHostConfiguration.RemindersKey)
            .And.Contain("mongo"),
            "The message must name the offending key so an operator can find it among several store knobs.");
    }

    [TestCase("localhost", ClusteringProvider.Localhost)]
    [TestCase("local", ClusteringProvider.Localhost)]
    [TestCase("LOCAL", ClusteringProvider.Localhost)]
    [TestCase("azure", ClusteringProvider.Azure)]
    public void Clustering_tokens_map_to_the_expected_provider(string token, ClusteringProvider expected)
    {
        var config = From(
            (RepoContextHostConfiguration.ClusteringKey, token),
            (RepoContextHostConfiguration.AzureConnectionKey, "UseDevelopmentStorage=true"));

        Assert.That(config.Clustering, Is.EqualTo(expected));
    }

    [Test]
    public void An_unknown_clustering_token_is_rejected()
    {
        var ex = Rejects((RepoContextHostConfiguration.ClusteringKey, "consul"));

        Assert.That(ex!.Message, Does.Contain(RepoContextHostConfiguration.ClusteringKey)
            .And.Contain("consul"));
    }

    [Test]
    public void A_non_absolute_embedding_endpoint_is_rejected()
    {
        var ex = Rejects((RepoContextHostConfiguration.EmbeddingEndpointKey, "not a uri"));

        Assert.That(ex!.Message, Does.Contain(RepoContextHostConfiguration.EmbeddingEndpointKey)
            .And.Contain("absolute"));
    }

    [Test]
    public void An_absolute_embedding_endpoint_is_accepted()
    {
        var config = From((RepoContextHostConfiguration.EmbeddingEndpointKey, "http://embed:9000/"));

        Assert.That(config.EmbeddingEndpoint, Is.EqualTo(new Uri("http://embed:9000/")));
    }

    [TestCase("0")]
    [TestCase("-1")]
    [TestCase("65536")]
    [TestCase("99999")]
    public void An_out_of_range_mcp_port_is_rejected(string port)
    {
        var ex = Rejects((RepoContextHostConfiguration.McpPortKey, port));

        Assert.That(ex!.Message, Does.Contain(RepoContextHostConfiguration.McpPortKey)
            .And.Contain("1-65535"));
    }

    [TestCase("1")]
    [TestCase("65535")]
    public void A_boundary_mcp_port_is_accepted(string port)
    {
        var config = From((RepoContextHostConfiguration.McpPortKey, port));

        Assert.That(config.McpPort, Is.EqualTo(int.Parse(port)));
    }

    [TestCase("0")]
    [TestCase("-8")]
    public void A_non_positive_embedding_dimension_is_rejected(string dimension)
    {
        var ex = Rejects((RepoContextHostConfiguration.EmbeddingDimensionKey, dimension));

        Assert.That(ex!.Message, Does.Contain(RepoContextHostConfiguration.EmbeddingDimensionKey)
            .And.Contain("positive"));
    }

    [Test]
    public void Validation_reports_every_failure_at_once()
    {
        // An operator fixing a misconfigured container should not have to restart
        // it once per mistake, so the guard accumulates rather than short-circuits.
        var ex = Rejects(
            (RepoContextHostConfiguration.McpPortKey, "0"),
            (RepoContextHostConfiguration.EmbeddingDimensionKey, "0"));

        Assert.Multiple(() =>
        {
            Assert.That(ex!.Message, Does.Contain(RepoContextHostConfiguration.McpPortKey));
            Assert.That(ex.Message, Does.Contain(RepoContextHostConfiguration.EmbeddingDimensionKey));
        });
    }

    [Test]
    public void The_azure_wal_table_name_defaults_and_is_overridable()
    {
        var defaulted = From();
        var overridden = From((RepoContextHostConfiguration.AzureWalTableKey, "CustomWalTable"));

        Assert.Multiple(() =>
        {
            Assert.That(defaulted.AzureWalTableName, Is.Not.Null.And.Not.Empty);
            Assert.That(overridden.AzureWalTableName, Is.EqualTo("CustomWalTable"));
        });
    }

    [Test]
    public void An_unknown_durability_profile_is_rejected()
    {
        var ex = Rejects((RepoContextHostConfiguration.DurabilityKey, "hybrid"));

        Assert.That(ex!.Message, Does.Contain(RepoContextHostConfiguration.DurabilityKey)
            .And.Contain("hybrid"));
    }

    [TestCase(DurabilityProfile.Postgres, RelationalStore.Postgres)]
    [TestCase(DurabilityProfile.Azure, RelationalStore.Azure)]
    public void A_profile_selects_its_matching_relational_default(
        DurabilityProfile profile,
        RelationalStore expected)
    {
        var config = From(
            (RepoContextHostConfiguration.DurabilityKey, profile.ToString().ToLowerInvariant()),
            (RepoContextHostConfiguration.PostgresConnectionKey, "Host=localhost"),
            (RepoContextHostConfiguration.AzureConnectionKey, "UseDevelopmentStorage=true"));

        Assert.Multiple(() =>
        {
            Assert.That(config.GrainStorage, Is.EqualTo(expected));
            Assert.That(config.Reminders, Is.EqualTo(expected));
        });
    }
}
