using Orleans.Lattice.Explorer.Web;

namespace Orleans.Lattice.Explorer.Tests.Web;

/// <summary>
/// Unit tests for <see cref="LatticeExplorerWebOptions"/> base-path normalization
/// and the derived route prefix / base href used when the explorer is mounted
/// under a subpath.
/// </summary>
[TestFixture]
public class LatticeExplorerWebOptionsTests
{
    [Test]
    public void BasePath_defaults_to_root()
    {
        var options = new LatticeExplorerWebOptions();
        Assert.That(options.BasePath, Is.EqualTo("/"));
    }

    [Test]
    public void UseEnvironmentBootstrap_defaults_to_true()
    {
        var options = new LatticeExplorerWebOptions();
        Assert.That(options.UseEnvironmentBootstrap, Is.True);
    }

    [TestCase("/explorer", "/explorer")]
    [TestCase("explorer", "/explorer")]
    [TestCase("/explorer/", "/explorer")]
    [TestCase("  /explorer  ", "/explorer")]
    [TestCase("/nested/explorer/", "/nested/explorer")]
    public void BasePath_is_normalized_to_leading_slash_no_trailing(string input, string expected)
    {
        var options = new LatticeExplorerWebOptions { BasePath = input };
        Assert.That(options.BasePath, Is.EqualTo(expected));
    }

    [Test]
    public void DataProtection_options_default_to_null()
    {
        var options = new LatticeExplorerWebOptions();
        Assert.Multiple(() =>
        {
            Assert.That(options.DataProtectionKeyRingBlobUri, Is.Null);
            Assert.That(options.DataProtectionKeyRingCredential, Is.Null);
            Assert.That(options.DataProtectionApplicationName, Is.Null);
            Assert.That(options.ConfigureDataProtection, Is.Null);
        });
    }
}
