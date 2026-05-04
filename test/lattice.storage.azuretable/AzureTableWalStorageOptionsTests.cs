using Azure.Data.Tables;

namespace Orleans.Lattice.Storage.AzureTable.Tests;

/// <summary>
/// Unit tests for <see cref="AzureTableWalStorageOptions"/> covering
/// every <see cref="AzureTableWalStorageOptions.Validate"/> failure
/// mode and the happy paths for each of the three supported
/// authentication shapes.
/// </summary>
[TestFixture]
public class AzureTableWalStorageOptionsTests
{
    [Test]
    public void Validate_throws_when_TableName_is_empty()
    {
        var options = new AzureTableWalStorageOptions
        {
            TableName = "",
            ConnectionString = "UseDevelopmentStorage=true",
        };

        Assert.That(options.Validate, Throws.InvalidOperationException);
    }

    [Test]
    public void Validate_throws_when_no_authentication_mode_is_configured()
    {
        var options = new AzureTableWalStorageOptions();

        Assert.That(options.Validate, Throws.InvalidOperationException);
    }

    [Test]
    public void Validate_throws_when_ConnectionString_and_ServiceUri_are_both_supplied()
    {
        var options = new AzureTableWalStorageOptions
        {
            ConnectionString = "UseDevelopmentStorage=true",
            ServiceUri = new Uri("https://example.table.core.windows.net"),
        };

        Assert.That(options.Validate, Throws.InvalidOperationException);
    }

    [Test]
    public void Validate_throws_when_ConnectionString_and_TokenCredential_are_both_supplied()
    {
        var options = new AzureTableWalStorageOptions
        {
            ConnectionString = "UseDevelopmentStorage=true",
            TokenCredential = new FakeTokenCredential(),
        };

        Assert.That(options.Validate, Throws.InvalidOperationException);
    }

    [Test]
    public void Validate_throws_when_ServiceUri_has_no_credential()
    {
        var options = new AzureTableWalStorageOptions
        {
            ServiceUri = new Uri("https://example.table.core.windows.net"),
        };

        Assert.That(options.Validate, Throws.InvalidOperationException);
    }

    [Test]
    public void Validate_throws_when_both_TokenCredential_and_SharedKeyCredential_are_supplied()
    {
        var options = new AzureTableWalStorageOptions
        {
            ServiceUri = new Uri("https://example.table.core.windows.net"),
            TokenCredential = new FakeTokenCredential(),
            SharedKeyCredential = new TableSharedKeyCredential("acct", "ZmFrZQ=="),
        };

        Assert.That(options.Validate, Throws.InvalidOperationException);
    }

    [Test]
    public void Validate_succeeds_for_ConnectionString_mode()
    {
        var options = new AzureTableWalStorageOptions
        {
            ConnectionString = "UseDevelopmentStorage=true",
        };

        Assert.That(options.Validate, Throws.Nothing);
    }

    [Test]
    public void Validate_succeeds_for_ServiceUri_plus_TokenCredential()
    {
        var options = new AzureTableWalStorageOptions
        {
            ServiceUri = new Uri("https://example.table.core.windows.net"),
            TokenCredential = new FakeTokenCredential(),
        };

        Assert.That(options.Validate, Throws.Nothing);
    }

    [Test]
    public void Validate_succeeds_for_ServiceUri_plus_SharedKeyCredential()
    {
        var options = new AzureTableWalStorageOptions
        {
            ServiceUri = new Uri("https://example.table.core.windows.net"),
            SharedKeyCredential = new TableSharedKeyCredential("acct", "ZmFrZQ=="),
        };

        Assert.That(options.Validate, Throws.Nothing);
    }

    [Test]
    public void DefaultTableName_is_OrleansLatticeWal()
    {
        var options = new AzureTableWalStorageOptions();

        Assert.That(options.TableName, Is.EqualTo(AzureTableWalStorageOptions.DefaultTableName));
        Assert.That(options.TableName, Is.EqualTo("OrleansLatticeWal"));
    }

    /// <summary>
    /// Minimal Azure.Core <see cref="Azure.Core.TokenCredential"/>
    /// stand-in for tests that need a non-null credential reference
    /// without any actual auth flow.
    /// </summary>
    private sealed class FakeTokenCredential : Azure.Core.TokenCredential
    {
        public override Azure.Core.AccessToken GetToken(
            Azure.Core.TokenRequestContext requestContext,
            CancellationToken cancellationToken) =>
            new("fake", DateTimeOffset.UtcNow.AddHours(1));

        public override ValueTask<Azure.Core.AccessToken> GetTokenAsync(
            Azure.Core.TokenRequestContext requestContext,
            CancellationToken cancellationToken) =>
            ValueTask.FromResult(GetToken(requestContext, cancellationToken));
    }
}
