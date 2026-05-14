using Orleans.Lattice.Replication;

namespace Orleans.Lattice.Replication.Tests.Security;

[TestFixture]
public class EnvironmentVariableSecretSourceTests
{
    private string? _savedSecret;
    private string? _savedAccepted;
    private string? _savedPerPeer;

    [SetUp]
    public void SetUp()
    {
        _savedSecret = Environment.GetEnvironmentVariable(LatticeReplicationEnvironmentVariables.Secret);
        _savedAccepted = Environment.GetEnvironmentVariable(LatticeReplicationEnvironmentVariables.AcceptedSecrets);
        _savedPerPeer = Environment.GetEnvironmentVariable(LatticeReplicationEnvironmentVariables.PeerSecretPrefix + "US_WEST_2");
        Environment.SetEnvironmentVariable(LatticeReplicationEnvironmentVariables.Secret, null);
        Environment.SetEnvironmentVariable(LatticeReplicationEnvironmentVariables.AcceptedSecrets, null);
        Environment.SetEnvironmentVariable(LatticeReplicationEnvironmentVariables.PeerSecretPrefix + "US_WEST_2", null);
    }

    [TearDown]
    public void TearDown()
    {
        Environment.SetEnvironmentVariable(LatticeReplicationEnvironmentVariables.Secret, _savedSecret);
        Environment.SetEnvironmentVariable(LatticeReplicationEnvironmentVariables.AcceptedSecrets, _savedAccepted);
        Environment.SetEnvironmentVariable(LatticeReplicationEnvironmentVariables.PeerSecretPrefix + "US_WEST_2", _savedPerPeer);
    }

    [Test]
    public async Task GetOutboundSecretAsync_returns_null_when_no_env_var_set()
    {
        var src = new EnvironmentVariableSecretSource();
        var v = await src.GetOutboundSecretAsync("peer", CancellationToken.None);
        Assert.That(v, Is.Null);
    }

    [Test]
    public async Task GetOutboundSecretAsync_returns_cluster_wide_secret_when_set()
    {
        Environment.SetEnvironmentVariable(LatticeReplicationEnvironmentVariables.Secret, "cluster-secret");
        var src = new EnvironmentVariableSecretSource();
        var v = await src.GetOutboundSecretAsync("peer", CancellationToken.None);
        Assert.That(v, Is.EqualTo("cluster-secret"));
    }

    [Test]
    public async Task GetOutboundSecretAsync_prefers_per_peer_override_over_cluster_wide()
    {
        Environment.SetEnvironmentVariable(LatticeReplicationEnvironmentVariables.Secret, "cluster-secret");
        Environment.SetEnvironmentVariable(LatticeReplicationEnvironmentVariables.PeerSecretPrefix + "US_WEST_2", "peer-secret");
        var src = new EnvironmentVariableSecretSource();
        var v = await src.GetOutboundSecretAsync("us-west-2", CancellationToken.None);
        Assert.That(v, Is.EqualTo("peer-secret"));
    }

    [Test]
    public void GetOutboundSecretAsync_throws_when_peer_id_null()
    {
        var src = new EnvironmentVariableSecretSource();
        Assert.That(
            async () => await src.GetOutboundSecretAsync(null!, CancellationToken.None),
            Throws.ArgumentNullException);
    }

    [Test]
    public async Task GetAcceptedSecretsAsync_returns_empty_when_nothing_set()
    {
        var src = new EnvironmentVariableSecretSource();
        var snap = await src.GetAcceptedSecretsAsync(CancellationToken.None);
        Assert.That(snap.Secrets, Is.Empty);
    }

    [Test]
    public async Task GetAcceptedSecretsAsync_includes_primary_when_only_secret_set()
    {
        Environment.SetEnvironmentVariable(LatticeReplicationEnvironmentVariables.Secret, "alpha");
        var src = new EnvironmentVariableSecretSource();
        var snap = await src.GetAcceptedSecretsAsync(CancellationToken.None);
        Assert.That(snap.Secrets, Has.Count.EqualTo(1));
        Assert.That(snap.Secrets[0], Is.EqualTo("alpha"));
    }

    [Test]
    public async Task GetAcceptedSecretsAsync_merges_accepted_list_with_primary_and_deduplicates()
    {
        Environment.SetEnvironmentVariable(LatticeReplicationEnvironmentVariables.Secret, "alpha");
        Environment.SetEnvironmentVariable(LatticeReplicationEnvironmentVariables.AcceptedSecrets, "alpha, beta ; gamma");
        var src = new EnvironmentVariableSecretSource();
        var snap = await src.GetAcceptedSecretsAsync(CancellationToken.None);
        Assert.That(snap.Secrets, Is.EqualTo(new[] { "alpha", "beta", "gamma" }));
    }

    [Test]
    public async Task GetAcceptedSecretsAsync_returns_stable_version_for_same_env_state()
    {
        Environment.SetEnvironmentVariable(LatticeReplicationEnvironmentVariables.Secret, "alpha");
        Environment.SetEnvironmentVariable(LatticeReplicationEnvironmentVariables.AcceptedSecrets, "beta");
        var src = new EnvironmentVariableSecretSource();
        var s1 = await src.GetAcceptedSecretsAsync(CancellationToken.None);
        var s2 = await src.GetAcceptedSecretsAsync(CancellationToken.None);
        Assert.That(s2.Version, Is.EqualTo(s1.Version));
    }

    [Test]
    public async Task GetAcceptedSecretsAsync_version_changes_when_secrets_change()
    {
        Environment.SetEnvironmentVariable(LatticeReplicationEnvironmentVariables.Secret, "alpha");
        var src = new EnvironmentVariableSecretSource();
        var v1 = (await src.GetAcceptedSecretsAsync(CancellationToken.None)).Version;
        Environment.SetEnvironmentVariable(LatticeReplicationEnvironmentVariables.Secret, "alpha2");
        var v2 = (await src.GetAcceptedSecretsAsync(CancellationToken.None)).Version;
        Assert.That(v2, Is.Not.EqualTo(v1));
    }

    [Test]
    public void NormaliseClusterId_throws_when_cluster_id_exceeds_length_limit()
    {
        var oversize = new string('a', 257);
        Assert.That(
            () => EnvironmentVariableSecretSource.NormaliseClusterId(oversize),
            Throws.InstanceOf<ArgumentOutOfRangeException>());
    }

    [Test]
    public void NormaliseClusterId_throws_when_cluster_id_null()
    {
        Assert.That(
            () => EnvironmentVariableSecretSource.NormaliseClusterId(null!),
            Throws.ArgumentNullException);
    }

    [Test]
    public void NormaliseClusterId_uppercases_and_substitutes_non_alphanumeric()
    {
        var n = EnvironmentVariableSecretSource.NormaliseClusterId("us-west-2.region");
        Assert.That(n, Is.EqualTo("US_WEST_2_REGION"));
    }

    [Test]
    public async Task GetOutboundSecretAsync_returns_null_when_cluster_wide_secret_is_whitespace()
    {
        Environment.SetEnvironmentVariable(LatticeReplicationEnvironmentVariables.Secret, "   ");
        var src = new EnvironmentVariableSecretSource();
        var v = await src.GetOutboundSecretAsync("peer", CancellationToken.None);
        Assert.That(v, Is.Null);
    }

    [Test]
    public async Task GetOutboundSecretAsync_falls_back_to_cluster_wide_when_per_peer_is_whitespace()
    {
        Environment.SetEnvironmentVariable(LatticeReplicationEnvironmentVariables.Secret, "cluster-secret");
        Environment.SetEnvironmentVariable(LatticeReplicationEnvironmentVariables.PeerSecretPrefix + "US_WEST_2", "   ");
        var src = new EnvironmentVariableSecretSource();
        var v = await src.GetOutboundSecretAsync("us-west-2", CancellationToken.None);
        Assert.That(v, Is.EqualTo("cluster-secret"));
    }

    [Test]
    public async Task GetOutboundSecretAsync_resolves_per_peer_override_for_cluster_id_with_non_alphanumeric_chars()
    {
        // End-to-end check: a cluster id like "peer.x" must be looked up
        // via the upper-snake-case env-var name LATTICE_REPLICATION_PEER_SECRET__PEER_X.
        var saved = Environment.GetEnvironmentVariable(LatticeReplicationEnvironmentVariables.PeerSecretPrefix + "PEER_X");
        try
        {
            Environment.SetEnvironmentVariable(LatticeReplicationEnvironmentVariables.PeerSecretPrefix + "PEER_X", "peer-x-secret");
            var src = new EnvironmentVariableSecretSource();
            var v = await src.GetOutboundSecretAsync("peer.x", CancellationToken.None);
            Assert.That(v, Is.EqualTo("peer-x-secret"));
        }
        finally
        {
            Environment.SetEnvironmentVariable(LatticeReplicationEnvironmentVariables.PeerSecretPrefix + "PEER_X", saved);
        }
    }

    [Test]
    public async Task GetAcceptedSecretsAsync_skips_whitespace_only_entries_in_accepted_list()
    {
        Environment.SetEnvironmentVariable(LatticeReplicationEnvironmentVariables.Secret, "alpha");
        Environment.SetEnvironmentVariable(LatticeReplicationEnvironmentVariables.AcceptedSecrets, " ,beta, , gamma ,  ");
        var src = new EnvironmentVariableSecretSource();
        var snap = await src.GetAcceptedSecretsAsync(CancellationToken.None);
        Assert.That(snap.Secrets, Is.EqualTo(new[] { "alpha", "beta", "gamma" }));
    }
}
