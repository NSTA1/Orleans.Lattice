using Azure.Core;
using Azure.Storage;

namespace Orleans.Lattice.Backup.AzureBlob.Tests;

/// <summary>
/// Unit tests for <see cref="LatticeBackupAzureBlobOptions"/> validation and
/// container-client construction. These run without an emulator - building a
/// client from a connection string or endpoint URI issues no network call.
/// </summary>
[TestFixture]
public class LatticeBackupAzureBlobOptionsTests
{
    private const string DevConnectionString = "UseDevelopmentStorage=true";

    private static readonly Uri BlobEndpoint = new("https://example.blob.core.windows.net");

    [Test]
    public void DefaultContainerName_is_the_documented_default()
    {
        var options = new LatticeBackupAzureBlobOptions();
        Assert.That(options.ContainerName, Is.EqualTo(LatticeBackupAzureBlobOptions.DefaultContainerName));
    }

    [Test]
    public void Validate_throws_when_no_authentication_mode_is_configured()
    {
        var options = new LatticeBackupAzureBlobOptions();
        Assert.That(options.Validate, Throws.InvalidOperationException);
    }

    [Test]
    public void Validate_throws_when_container_name_is_empty()
    {
        var options = new LatticeBackupAzureBlobOptions
        {
            ConnectionString = DevConnectionString,
            ContainerName = "  ",
        };

        Assert.That(options.Validate, Throws.InvalidOperationException);
    }

    [Test]
    public void Validate_throws_when_more_than_one_mode_is_configured()
    {
        var options = new LatticeBackupAzureBlobOptions
        {
            ConnectionString = DevConnectionString,
            ServiceUri = BlobEndpoint,
            TokenCredential = new StubTokenCredential(),
        };

        Assert.That(options.Validate, Throws.InvalidOperationException);
    }

    [Test]
    public void Validate_throws_when_service_uri_has_no_credential()
    {
        var options = new LatticeBackupAzureBlobOptions { ServiceUri = BlobEndpoint };
        Assert.That(options.Validate, Throws.InvalidOperationException);
    }

    [Test]
    public void Validate_throws_when_service_uri_has_both_credentials()
    {
        var options = new LatticeBackupAzureBlobOptions
        {
            ServiceUri = BlobEndpoint,
            TokenCredential = new StubTokenCredential(),
            SharedKeyCredential = new StorageSharedKeyCredential("acct", Convert.ToBase64String(new byte[] { 1, 2, 3 })),
        };

        Assert.That(options.Validate, Throws.InvalidOperationException);
    }

    [Test]
    public void Validate_throws_when_credential_has_no_service_uri()
    {
        var options = new LatticeBackupAzureBlobOptions { TokenCredential = new StubTokenCredential() };
        Assert.That(options.Validate, Throws.InvalidOperationException);
    }

    [Test]
    public void Validate_accepts_connection_string_only()
    {
        var options = new LatticeBackupAzureBlobOptions { ConnectionString = DevConnectionString };
        Assert.That(options.Validate, Throws.Nothing);
    }

    [Test]
    public void Validate_accepts_service_uri_with_token_credential()
    {
        var options = new LatticeBackupAzureBlobOptions
        {
            ServiceUri = BlobEndpoint,
            TokenCredential = new StubTokenCredential(),
        };

        Assert.That(options.Validate, Throws.Nothing);
    }

    [Test]
    public void Validate_accepts_service_uri_with_shared_key_credential()
    {
        var options = new LatticeBackupAzureBlobOptions
        {
            ServiceUri = BlobEndpoint,
            SharedKeyCredential = new StorageSharedKeyCredential("acct", Convert.ToBase64String(new byte[] { 1, 2, 3 })),
        };

        Assert.That(options.Validate, Throws.Nothing);
    }

    [Test]
    public void BuildContainerClient_uses_the_configured_container_name()
    {
        var options = new LatticeBackupAzureBlobOptions
        {
            ConnectionString = DevConnectionString,
            ContainerName = "custom-backup-container",
        };

        var client = options.BuildContainerClient();

        Assert.That(client.Name, Is.EqualTo("custom-backup-container"));
    }

    [Test]
    public void BuildContainerClient_builds_from_service_uri_and_token_credential()
    {
        var options = new LatticeBackupAzureBlobOptions
        {
            ServiceUri = BlobEndpoint,
            TokenCredential = new StubTokenCredential(),
        };

        var client = options.BuildContainerClient();

        Assert.That(client.Name, Is.EqualTo(LatticeBackupAzureBlobOptions.DefaultContainerName));
    }

    private sealed class StubTokenCredential : TokenCredential
    {
        public override AccessToken GetToken(TokenRequestContext requestContext, CancellationToken cancellationToken) =>
            new("stub", DateTimeOffset.MaxValue);

        public override ValueTask<AccessToken> GetTokenAsync(TokenRequestContext requestContext, CancellationToken cancellationToken) =>
            new(new AccessToken("stub", DateTimeOffset.MaxValue));
    }
}
