using Azure.Storage;
using Azure.Storage.Blobs;

namespace Orleans.Lattice.Backup.AzureBlob.Tests;

/// <summary>
/// Additional coverage for <see cref="LatticeBackupAzureBlobOptions"/> paths not
/// exercised by <see cref="LatticeBackupAzureBlobOptionsTests"/>: the shared-key
/// service-client build, the credential-without-service-uri validation branch,
/// the client-options callback, and the pre-built service-client mode. All run
/// without an emulator - constructing a client issues no network call.
/// </summary>
[TestFixture]
public sealed class LatticeBackupAzureBlobOptionsCoverageTests
{
    private const string DevConnectionString = "UseDevelopmentStorage=true";

    private static readonly Uri BlobEndpoint = new("https://example.blob.core.windows.net");

    private static StorageSharedKeyCredential SharedKey() =>
        new("acct", Convert.ToBase64String(new byte[] { 1, 2, 3 }));

    [Test]
    public void Validate_throws_when_a_credential_accompanies_a_non_uri_mode()
    {
        // ConnectionString satisfies the single-mode requirement, but a credential
        // without ServiceUri is still invalid and must be rejected.
        var options = new LatticeBackupAzureBlobOptions
        {
            ConnectionString = DevConnectionString,
            TokenCredential = new StubTokenCredential(),
        };

        Assert.That(options.Validate, Throws.InvalidOperationException);
    }

    [Test]
    public void BuildContainerClient_builds_from_service_uri_and_shared_key_credential()
    {
        var options = new LatticeBackupAzureBlobOptions
        {
            ServiceUri = BlobEndpoint,
            SharedKeyCredential = SharedKey(),
            ContainerName = "shared-key-container",
        };

        var client = options.BuildContainerClient();

        Assert.That(client.Name, Is.EqualTo("shared-key-container"));
    }

    [Test]
    public void BuildContainerClient_invokes_the_client_options_callback()
    {
        var invoked = false;
        var options = new LatticeBackupAzureBlobOptions
        {
            ConnectionString = DevConnectionString,
            ConfigureClientOptions = _ => invoked = true,
        };

        options.BuildContainerClient();

        Assert.That(invoked, Is.True);
    }

    [Test]
    public void BuildContainerClient_uses_a_prebuilt_service_client_verbatim()
    {
        var invoked = false;
        var serviceClient = new BlobServiceClient(DevConnectionString);
        var options = new LatticeBackupAzureBlobOptions
        {
            ServiceClient = serviceClient,
            ContainerName = "prebuilt-container",
            ConfigureClientOptions = _ => invoked = true,
        };

        var client = options.BuildContainerClient();

        Assert.Multiple(() =>
        {
            Assert.That(client.Name, Is.EqualTo("prebuilt-container"));
            Assert.That(client.AccountName, Is.EqualTo(serviceClient.AccountName));
            Assert.That(invoked, Is.False, "ConfigureClientOptions must be ignored when a ServiceClient is supplied.");
        });
    }

    private sealed class StubTokenCredential : Azure.Core.TokenCredential
    {
        public override Azure.Core.AccessToken GetToken(Azure.Core.TokenRequestContext requestContext, CancellationToken cancellationToken) =>
            new("stub", DateTimeOffset.MaxValue);

        public override ValueTask<Azure.Core.AccessToken> GetTokenAsync(Azure.Core.TokenRequestContext requestContext, CancellationToken cancellationToken) =>
            new(new Azure.Core.AccessToken("stub", DateTimeOffset.MaxValue));
    }
}
