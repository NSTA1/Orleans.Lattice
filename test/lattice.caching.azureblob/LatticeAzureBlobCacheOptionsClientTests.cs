using Azure.Storage;
using Azure.Storage.Blobs;

namespace Orleans.Lattice.Caching.AzureBlob.Tests;

/// <summary>
/// Unit tests for the credential-selection branches of
/// <see cref="LatticeAzureBlobCacheOptions.BuildContainerClient"/> and the
/// "credential without <see cref="LatticeAzureBlobCacheOptions.ServiceUri"/>"
/// validation branch, complementing <see cref="LatticeAzureBlobCacheOptionsTests"/>.
/// None touch the network - constructing a client from an endpoint plus a
/// credential does no I/O.
/// </summary>
[TestFixture]
public sealed class LatticeAzureBlobCacheOptionsClientTests
{
    private const string DevConnectionString = "UseDevelopmentStorage=true";
    private static readonly Uri Endpoint = new("https://account.blob.core.windows.net");

    private static StorageSharedKeyCredential SharedKey() =>
        new("account", Convert.ToBase64String(new byte[] { 1, 2, 3, 4 }));

    [Test]
    public void Validate_throws_when_credential_set_with_connection_string_but_no_service_uri()
    {
        var options = new LatticeAzureBlobCacheOptions
        {
            ConnectionString = DevConnectionString,
            TokenCredential = new FakeTokenCredential(),
        };

        var ex = Assert.Throws<InvalidOperationException>(() => options.Validate());
        Assert.That(ex!.Message, Does.Contain(nameof(LatticeAzureBlobCacheOptions.ServiceUri)));
    }

    [Test]
    public void BuildContainerClient_builds_from_service_uri_and_token_credential()
    {
        var options = new LatticeAzureBlobCacheOptions
        {
            ServiceUri = Endpoint,
            TokenCredential = new FakeTokenCredential(),
            ContainerName = "token-container",
        };

        var container = options.BuildContainerClient();

        Assert.Multiple(() =>
        {
            Assert.That(container.Name, Is.EqualTo("token-container"));
            Assert.That(container.AccountName, Is.EqualTo("account"));
        });
    }

    [Test]
    public void BuildContainerClient_builds_from_service_uri_and_shared_key_credential()
    {
        var options = new LatticeAzureBlobCacheOptions
        {
            ServiceUri = Endpoint,
            SharedKeyCredential = SharedKey(),
            ContainerName = "sharedkey-container",
        };

        var container = options.BuildContainerClient();

        Assert.Multiple(() =>
        {
            Assert.That(container.Name, Is.EqualTo("sharedkey-container"));
            Assert.That(container.AccountName, Is.EqualTo("account"));
        });
    }

    [Test]
    public void BuildContainerClient_invokes_ConfigureClientOptions_for_token_credential_mode()
    {
        var invoked = false;
        var options = new LatticeAzureBlobCacheOptions
        {
            ServiceUri = Endpoint,
            SharedKeyCredential = SharedKey(),
            ConfigureClientOptions = _ => invoked = true,
        };

        options.BuildContainerClient();

        Assert.That(invoked, Is.True);
    }
}
