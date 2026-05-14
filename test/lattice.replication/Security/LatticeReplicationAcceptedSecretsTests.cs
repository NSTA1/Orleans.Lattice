using Orleans.Lattice.Replication;

namespace Orleans.Lattice.Replication.Tests.Security;

[TestFixture]
public class LatticeReplicationAcceptedSecretsTests
{
    [Test]
    public void Constructor_throws_when_secrets_null()
    {
        Assert.That(
            () => new LatticeReplicationAcceptedSecrets(null!, "v1"),
            Throws.ArgumentNullException);
    }

    [Test]
    public void Constructor_throws_when_version_null()
    {
        Assert.That(
            () => new LatticeReplicationAcceptedSecrets(Array.Empty<string>(), null!),
            Throws.ArgumentNullException);
    }

    [Test]
    public void Constructor_round_trips_supplied_values()
    {
        var secrets = new[] { "alpha", "beta" };
        var snap = new LatticeReplicationAcceptedSecrets(secrets, "v1");
        Assert.That(snap.Secrets, Is.EqualTo(secrets));
        Assert.That(snap.Version, Is.EqualTo("v1"));
    }

    [Test]
    public void Empty_has_no_secrets_and_stable_version_token()
    {
        Assert.That(LatticeReplicationAcceptedSecrets.Empty.Secrets, Is.Empty);
        Assert.That(LatticeReplicationAcceptedSecrets.Empty.Version, Is.Not.Null.And.Not.Empty);
    }

    [Test]
    public void Empty_returns_same_singleton_instance()
    {
        Assert.That(LatticeReplicationAcceptedSecrets.Empty, Is.SameAs(LatticeReplicationAcceptedSecrets.Empty));
    }
}
