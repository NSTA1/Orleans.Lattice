using Microsoft.Extensions.Configuration;
using Orleans.Lattice.Replication;

namespace Orleans.Lattice.Replication.Tests.Security;

[TestFixture]
public class ConfigurationBindingSecretSourceTests
{
    private static IConfigurationSection BuildSection(Dictionary<string, string?> data)
    {
        var root = new ConfigurationBuilder()
            .AddInMemoryCollection(data!)
            .Build();
        return root.GetSection("LatticeReplication:Secrets");
    }

    [Test]
    public void Constructor_throws_on_null_section()
    {
        Assert.That(() => new ConfigurationBindingSecretSource(null!), Throws.ArgumentNullException);
    }

    [Test]
    public async Task GetOutboundSecretAsync_returns_cluster_wide_when_no_per_peer_override()
    {
        var section = BuildSection(new()
        {
            ["LatticeReplication:Secrets:Secret"] = "cluster-secret",
        });

        var src = new ConfigurationBindingSecretSource(section);
        Assert.That(await src.GetOutboundSecretAsync("peer", CancellationToken.None), Is.EqualTo("cluster-secret"));
    }

    [Test]
    public async Task GetOutboundSecretAsync_prefers_per_peer_secret()
    {
        var section = BuildSection(new()
        {
            ["LatticeReplication:Secrets:Secret"] = "cluster-secret",
            ["LatticeReplication:Secrets:PeerSecrets:peer-a"] = "peer-a-secret",
        });

        var src = new ConfigurationBindingSecretSource(section);
        Assert.That(await src.GetOutboundSecretAsync("peer-a", CancellationToken.None), Is.EqualTo("peer-a-secret"));
        Assert.That(await src.GetOutboundSecretAsync("peer-b", CancellationToken.None), Is.EqualTo("cluster-secret"));
    }

    [Test]
    public void GetOutboundSecretAsync_throws_on_null_peer_id()
    {
        var src = new ConfigurationBindingSecretSource(BuildSection(new()));
        Assert.That(
            async () => await src.GetOutboundSecretAsync(null!, CancellationToken.None),
            Throws.ArgumentNullException);
    }

    [Test]
    public async Task GetAcceptedSecretsAsync_returns_empty_for_empty_section()
    {
        var src = new ConfigurationBindingSecretSource(BuildSection(new()));
        var snap = await src.GetAcceptedSecretsAsync(CancellationToken.None);
        Assert.That(snap.Secrets, Is.Empty);
    }

    [Test]
    public async Task GetAcceptedSecretsAsync_combines_secret_and_accepted_list()
    {
        var section = BuildSection(new()
        {
            ["LatticeReplication:Secrets:Secret"] = "alpha",
            ["LatticeReplication:Secrets:AcceptedSecrets:0"] = "alpha",
            ["LatticeReplication:Secrets:AcceptedSecrets:1"] = "beta",
        });

        var src = new ConfigurationBindingSecretSource(section);
        var snap = await src.GetAcceptedSecretsAsync(CancellationToken.None);
        Assert.That(snap.Secrets, Is.EqualTo(new[] { "alpha", "beta" }));
    }

    [Test]
    public async Task GetAcceptedSecretsAsync_returns_stable_version_for_same_section_state()
    {
        var section = BuildSection(new()
        {
            ["LatticeReplication:Secrets:Secret"] = "alpha",
            ["LatticeReplication:Secrets:AcceptedSecrets:0"] = "beta",
        });
        var src = new ConfigurationBindingSecretSource(section);

        var s1 = await src.GetAcceptedSecretsAsync(CancellationToken.None);
        var s2 = await src.GetAcceptedSecretsAsync(CancellationToken.None);
        Assert.That(s2.Version, Is.EqualTo(s1.Version));
    }

    [Test]
    public async Task GetAcceptedSecretsAsync_version_changes_when_secrets_change()
    {
        var src1 = new ConfigurationBindingSecretSource(BuildSection(new()
        {
            ["LatticeReplication:Secrets:Secret"] = "alpha",
        }));
        var src2 = new ConfigurationBindingSecretSource(BuildSection(new()
        {
            ["LatticeReplication:Secrets:Secret"] = "alpha2",
        }));

        var v1 = (await src1.GetAcceptedSecretsAsync(CancellationToken.None)).Version;
        var v2 = (await src2.GetAcceptedSecretsAsync(CancellationToken.None)).Version;
        Assert.That(v2, Is.Not.EqualTo(v1));
    }

    [Test]
    public async Task GetOutboundSecretAsync_returns_null_when_secret_is_whitespace_only()
    {
        var section = BuildSection(new()
        {
            ["LatticeReplication:Secrets:Secret"] = "   ",
        });
        var src = new ConfigurationBindingSecretSource(section);
        Assert.That(await src.GetOutboundSecretAsync("peer", CancellationToken.None), Is.Null);
    }

    [Test]
    public async Task GetAcceptedSecretsAsync_returns_empty_when_secret_is_whitespace_only()
    {
        var section = BuildSection(new()
        {
            ["LatticeReplication:Secrets:Secret"] = "   ",
        });
        var src = new ConfigurationBindingSecretSource(section);
        var snap = await src.GetAcceptedSecretsAsync(CancellationToken.None);
        Assert.That(snap.Secrets, Is.Empty);
    }
}
