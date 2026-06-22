using Orleans.Lattice.Explorer.Core.Configuration;

namespace Orleans.Lattice.Explorer.Tests.Configuration;

[TestFixture]
public class EndpointValidationTests
{
    [TestCase("https://host:443")]
    [TestCase("http://localhost:5199")]
    [TestCase("https://10.0.0.1")]
    public void TryValidate_AcceptsAbsoluteHttpUrls(string endpoint)
    {
        var ok = EndpointValidation.TryValidate(endpoint, out var error);

        Assert.That(ok, Is.True);
        Assert.That(error, Is.Null);
    }

    [TestCase("")]
    [TestCase("   ")]
    [TestCase(null)]
    [TestCase("host:443")]
    [TestCase("ftp://host")]
    [TestCase("/relative/path")]
    [TestCase("not a url")]
    public void TryValidate_RejectsInvalidEndpoints(string? endpoint)
    {
        var ok = EndpointValidation.TryValidate(endpoint, out var error);

        Assert.That(ok, Is.False);
        Assert.That(error, Is.Not.Null.And.Not.Empty);
    }
}

[TestFixture]
public class ExplorerConfigurationTests
{
    [Test]
    public void ToConnectionSettings_MapsEndpointAndTransport()
    {
        var config = new ExplorerConfiguration
        {
            Endpoint = "http://localhost:5199",
            AllowUnencryptedHttp2 = true,
        };

        var settings = config.ToConnectionSettings();

        Assert.That(settings.Address, Is.EqualTo("http://localhost:5199"));
        Assert.That(settings.AllowUnencryptedHttp2, Is.True);
        Assert.That(settings.Authentication, Is.Null);
    }

    [Test]
    public void ToConnectionSettings_CarriesHeadersIntoAuthSeam()
    {
        var config = new ExplorerConfiguration
        {
            Endpoint = "https://host",
            Headers = new Dictionary<string, string> { ["authorization"] = "Bearer t" },
        };

        var settings = config.ToConnectionSettings();

        Assert.That(settings.Authentication, Is.Not.Null);
        Assert.That(settings.Authentication!.HasHeaders, Is.True);
    }

    [Test]
    public void Defaults_UseCurrentSchemaVersion()
    {
        Assert.That(new ExplorerConfiguration().SchemaVersion, Is.EqualTo(ExplorerConfiguration.CurrentSchemaVersion));
    }
}
