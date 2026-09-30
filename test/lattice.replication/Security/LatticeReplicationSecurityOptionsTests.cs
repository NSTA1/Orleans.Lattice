using Orleans.Lattice.Replication;

namespace Orleans.Lattice.Replication.Tests.Security;

[TestFixture]
public class LatticeReplicationSecurityOptionsTests
{
    [Test]
    public void Defaults_are_secure_by_default()
    {
        var o = new LatticeReplicationSecurityOptions();
        Assert.That(o.RequireAuthentication, Is.True, "RequireAuthentication must default to true so unauthenticated peers are rejected.");
        Assert.That(o.ScanConfigurationForSecrets, Is.True, "ScanConfigurationForSecrets must default to true so appsettings-leaked secrets fail closed.");
    }

    [Test]
    public void Default_refresh_interval_is_30_seconds()
    {
        var o = new LatticeReplicationSecurityOptions();
        Assert.That(o.SecretRefreshInterval, Is.EqualTo(TimeSpan.FromSeconds(30)));
    }

    [Test]
    public void BindCredentialToOriginCluster_defaults_to_on()
    {
        // Secure by default, like its two siblings. Leaving it off made the
        // claimed origin a self-assertion: the accepted-secret set carries no
        // peer attribution, so a match proves only that the caller holds some
        // accepted secret, and every downstream origin check then compared two
        // caller-chosen values. Under a single cluster-wide secret every origin
        // resolves the same value so the check passes and nothing changes; only
        // an asymmetric per-peer scheme must opt out.
        var o = new LatticeReplicationSecurityOptions();
        Assert.That(o.BindCredentialToOriginCluster, Is.True);
    }

    [Test]
    public void Properties_round_trip()
    {
        var o = new LatticeReplicationSecurityOptions
        {
            RequireAuthentication = false,
            SecretRefreshInterval = TimeSpan.FromMinutes(5),
            ScanConfigurationForSecrets = false,
            BindCredentialToOriginCluster = false,
        };
        Assert.That(o.RequireAuthentication, Is.False);
        Assert.That(o.SecretRefreshInterval, Is.EqualTo(TimeSpan.FromMinutes(5)));
        Assert.That(o.ScanConfigurationForSecrets, Is.False);
        Assert.That(o.BindCredentialToOriginCluster, Is.False);
    }
}
