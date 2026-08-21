namespace Orleans.Lattice.Membership.Entra.Graph.Tests;

/// <summary>
/// Unit tests for <see cref="LatticeEntraGraphOptions"/>: the defaults and the
/// <see cref="LatticeEntraGraphOptions.ResolveAuthority"/> builder, which composes
/// the MSAL authority from the login host and tenant id and returns an empty string
/// when either input is missing.
/// </summary>
public class LatticeEntraGraphOptionsTests
{
    [Test]
    public void Defaults_are_the_documented_values()
    {
        var options = new LatticeEntraGraphOptions();

        Assert.Multiple(() =>
        {
            Assert.That(options.AuthorityHost, Is.EqualTo(LatticeEntraGraphOptions.DefaultAuthorityHost));
            Assert.That(options.Scopes, Is.EqualTo(new[] { LatticeEntraGraphOptions.DefaultScope }));
            Assert.That(options.SecurityEnabledOnly, Is.False);
            Assert.That(options.TokenRefreshSkew, Is.EqualTo(TimeSpan.FromMinutes(5)));
            Assert.That(options.DirectorySubjectIdSource, Is.EqualTo(EntraDirectorySubjectIdSource.ObjectId));
            Assert.That(options.Credential, Is.Null);
        });
    }

    [Test]
    public void ResolveAuthority_with_host_and_tenant_composes_authority()
    {
        var options = new LatticeEntraGraphOptions { TenantId = "tenant-1" };

        Assert.That(
            options.ResolveAuthority(),
            Is.EqualTo("https://login.microsoftonline.com/tenant-1"));
    }

    [Test]
    public void ResolveAuthority_trims_trailing_slash_from_host()
    {
        var options = new LatticeEntraGraphOptions
        {
            AuthorityHost = "https://login.microsoftonline.com/",
            TenantId = "tenant-1",
        };

        Assert.That(
            options.ResolveAuthority(),
            Is.EqualTo("https://login.microsoftonline.com/tenant-1"));
    }

    [Test]
    public void ResolveAuthority_missing_tenant_returns_empty()
    {
        var options = new LatticeEntraGraphOptions { TenantId = string.Empty };

        Assert.That(options.ResolveAuthority(), Is.Empty);
    }

    [Test]
    public void ResolveAuthority_missing_host_returns_empty()
    {
        var options = new LatticeEntraGraphOptions
        {
            AuthorityHost = "   ",
            TenantId = "tenant-1",
        };

        Assert.That(options.ResolveAuthority(), Is.Empty);
    }
}
