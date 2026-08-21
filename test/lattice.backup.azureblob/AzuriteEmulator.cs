using Azure.Storage.Blobs;

namespace Orleans.Lattice.Backup.AzureBlob.Tests;

/// <summary>
/// Shared connection details for the Azure Storage emulator (Azurite) that the
/// <c>AzureStorageEmulator</c>-category fixtures in this assembly drive. The
/// blob-service API version is pinned to the newest the CI Azurite build (3.36.0)
/// accepts (2025-11-05): the SDK's default API version outruns the emulator, so
/// an unpinned client is rejected and every emulator test self-skips. Kept in one
/// place so the fixture moves with the emulator on upgrade rather than carrying
/// its own copy of the pin.
/// </summary>
internal static class AzuriteEmulator
{
    public const string ConnectionString = "UseDevelopmentStorage=true";

    public const BlobClientOptions.ServiceVersion ApiVersion =
        BlobClientOptions.ServiceVersion.V2025_11_05;

    public static BlobServiceClient CreateServiceClient() =>
        new(ConnectionString, new BlobClientOptions(ApiVersion));
}
