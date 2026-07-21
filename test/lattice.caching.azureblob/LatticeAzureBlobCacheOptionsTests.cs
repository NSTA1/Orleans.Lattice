using Azure.Storage;
using Azure.Storage.Blobs;

namespace Orleans.Lattice.Caching.AzureBlob.Tests;

/// <summary>
/// Unit tests for <see cref="LatticeAzureBlobCacheOptions"/> authentication-mode
/// validation and container-client construction. None touch the network -
/// building a client from a connection string or endpoint does no I/O.
/// </summary>
[TestFixture]
public sealed class LatticeAzureBlobCacheOptionsTests
{
    private const string DevConnectionString = "UseDevelopmentStorage=true";
    private static readonly Uri Endpoint = new("https://account.blob.core.windows.net");

    [Test]
    public void Validate_throws_when_no_authentication_mode_is_configured()
    {
        var options = new LatticeAzureBlobCacheOptions();

        var ex = Assert.Throws<InvalidOperationException>(() => options.Validate());
        Assert.That(ex!.Message, Does.Contain("one authentication mode"));
    }

    [Test]
    public void Validate_throws_when_connection_string_and_service_uri_are_both_set()
    {
        var options = new LatticeAzureBlobCacheOptions
        {
            ConnectionString = DevConnectionString,
            ServiceUri = Endpoint,
            TokenCredential = new FakeTokenCredential(),
        };

        var ex = Assert.Throws<InvalidOperationException>(() => options.Validate());
        Assert.That(ex!.Message, Does.Contain("mutually exclusive"));
    }

    [Test]
    public void Validate_throws_when_service_uri_has_no_credential()
    {
        var options = new LatticeAzureBlobCacheOptions { ServiceUri = Endpoint };

        var ex = Assert.Throws<InvalidOperationException>(() => options.Validate());
        Assert.That(ex!.Message, Does.Contain("exactly one"));
    }

    [Test]
    public void Validate_throws_when_service_uri_has_both_credentials()
    {
        var options = new LatticeAzureBlobCacheOptions
        {
            ServiceUri = Endpoint,
            TokenCredential = new FakeTokenCredential(),
            SharedKeyCredential = new StorageSharedKeyCredential("account", Convert.ToBase64String(new byte[] { 1, 2, 3 })),
        };

        var ex = Assert.Throws<InvalidOperationException>(() => options.Validate());
        Assert.That(ex!.Message, Does.Contain("exactly one"));
    }

    [Test]
    public void Validate_throws_when_credential_is_set_without_service_uri()
    {
        var options = new LatticeAzureBlobCacheOptions { TokenCredential = new FakeTokenCredential() };

        var ex = Assert.Throws<InvalidOperationException>(() => options.Validate());
        Assert.That(ex!.Message, Does.Contain(nameof(LatticeAzureBlobCacheOptions.ServiceUri)));
    }

    [Test]
    public void Validate_throws_when_container_name_is_blank()
    {
        var options = new LatticeAzureBlobCacheOptions
        {
            ConnectionString = DevConnectionString,
            ContainerName = "   ",
        };

        var ex = Assert.Throws<InvalidOperationException>(() => options.Validate());
        Assert.That(ex!.Message, Does.Contain(nameof(LatticeAzureBlobCacheOptions.ContainerName)));
    }

    [Test]
    public void Validate_passes_for_connection_string_mode()
    {
        var options = new LatticeAzureBlobCacheOptions { ConnectionString = DevConnectionString };
        Assert.DoesNotThrow(() => options.Validate());
    }

    [Test]
    public void Validate_passes_for_service_uri_with_token_credential()
    {
        var options = new LatticeAzureBlobCacheOptions
        {
            ServiceUri = Endpoint,
            TokenCredential = new FakeTokenCredential(),
        };
        Assert.DoesNotThrow(() => options.Validate());
    }

    [Test]
    public void Validate_passes_for_prebuilt_service_client()
    {
        var options = new LatticeAzureBlobCacheOptions
        {
            ServiceClient = new BlobServiceClient(DevConnectionString),
        };
        Assert.DoesNotThrow(() => options.Validate());
    }

    [Test]
    public void DefaultContainerName_is_the_documented_value()
    {
        Assert.That(new LatticeAzureBlobCacheOptions().ContainerName, Is.EqualTo("orleans-lattice-cache"));
    }

    [Test]
    public void BuildContainerClient_uses_the_configured_container_name()
    {
        var options = new LatticeAzureBlobCacheOptions
        {
            ConnectionString = DevConnectionString,
            ContainerName = "custom-container",
        };

        var container = options.BuildContainerClient();

        Assert.That(container.Name, Is.EqualTo("custom-container"));
    }

    [Test]
    public void BuildContainerClient_prefers_the_prebuilt_service_client()
    {
        var serviceClient = new BlobServiceClient(DevConnectionString);
        var options = new LatticeAzureBlobCacheOptions
        {
            ServiceClient = serviceClient,
            ContainerName = "shared",
        };

        var container = options.BuildContainerClient();

        Assert.That(container.AccountName, Is.EqualTo(serviceClient.AccountName));
        Assert.That(container.Name, Is.EqualTo("shared"));
    }

    [Test]
    public void BuildContainerClient_invokes_ConfigureClientOptions()
    {
        var invoked = false;
        var options = new LatticeAzureBlobCacheOptions
        {
            ConnectionString = DevConnectionString,
            ConfigureClientOptions = _ => invoked = true,
        };

        options.BuildContainerClient();

        Assert.That(invoked, Is.True);
    }

    [Test]
    public void BuildContainerClient_ignores_ConfigureClientOptions_when_service_client_supplied()
    {
        var invoked = false;
        var options = new LatticeAzureBlobCacheOptions
        {
            ServiceClient = new BlobServiceClient(DevConnectionString),
            ConfigureClientOptions = _ => invoked = true,
        };

        options.BuildContainerClient();

        Assert.That(invoked, Is.False);
    }
}
