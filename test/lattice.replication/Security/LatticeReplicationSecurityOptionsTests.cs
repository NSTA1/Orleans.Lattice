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
    public void Properties_round_trip()
    {
        var o = new LatticeReplicationSecurityOptions
        {
            RequireAuthentication = false,
            SecretRefreshInterval = TimeSpan.FromMinutes(5),
            ScanConfigurationForSecrets = false,
        };
        Assert.That(o.RequireAuthentication, Is.False);
        Assert.That(o.SecretRefreshInterval, Is.EqualTo(TimeSpan.FromMinutes(5)));
        Assert.That(o.ScanConfigurationForSecrets, Is.False);
    }
}
