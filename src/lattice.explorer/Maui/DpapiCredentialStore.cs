using System.Runtime.Versioning;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Orleans.Lattice.Explorer.Core.Authentication;

namespace Orleans.Lattice.Explorer;

/// <summary>
/// A Windows <see cref="ICredentialStore"/> that rests the explorer's sign-in
/// credential on the machine encrypted with DPAPI
/// (<see cref="ProtectedData"/>, <see cref="DataProtectionScope.CurrentUser"/>).
/// The encrypted blob lives in a per-user file alongside the JSON config; only
/// the signed-in Windows user can decrypt it, and the plaintext credential never
/// touches the config store.
/// </summary>
[SupportedOSPlatform("windows")]
public sealed class DpapiCredentialStore : ICredentialStore
{
    private static readonly byte[] Entropy = Encoding.UTF8.GetBytes("Orleans.Lattice.Explorer.Credential.v1");

    private readonly string _filePath;
    private readonly object _gate = new();

    /// <summary>Creates a store that persists the encrypted credential at <paramref name="filePath"/>.</summary>
    /// <param name="filePath">The full path to the per-user encrypted credential file.</param>
    public DpapiCredentialStore(string filePath)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(filePath);
        _filePath = filePath;
    }

    /// <inheritdoc />
    public Task<StoredCredential?> GetAsync(CancellationToken cancellationToken = default)
    {
        lock (_gate)
        {
            if (!File.Exists(_filePath))
            {
                return Task.FromResult<StoredCredential?>(null);
            }

            try
            {
                var protectedBytes = File.ReadAllBytes(_filePath);
                var plain = ProtectedData.Unprotect(protectedBytes, Entropy, DataProtectionScope.CurrentUser);
                var credential = JsonSerializer.Deserialize<StoredCredential>(plain);
                return Task.FromResult(credential);
            }
            catch (Exception ex) when (ex is CryptographicException or IOException or JsonException or UnauthorizedAccessException)
            {
                // A corrupt or undecryptable blob is treated as "signed out".
                return Task.FromResult<StoredCredential?>(null);
            }
        }
    }

    /// <inheritdoc />
    public Task SetAsync(StoredCredential credential, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(credential);

        lock (_gate)
        {
            var directory = Path.GetDirectoryName(_filePath);
            if (!string.IsNullOrEmpty(directory))
            {
                Directory.CreateDirectory(directory);
            }

            var plain = JsonSerializer.SerializeToUtf8Bytes(credential);
            var protectedBytes = ProtectedData.Protect(plain, Entropy, DataProtectionScope.CurrentUser);

            var tempPath = _filePath + ".tmp";
            File.WriteAllBytes(tempPath, protectedBytes);
            File.Move(tempPath, _filePath, overwrite: true);
        }

        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public Task ClearAsync(CancellationToken cancellationToken = default)
    {
        lock (_gate)
        {
            if (File.Exists(_filePath))
            {
                File.Delete(_filePath);
            }
        }

        return Task.CompletedTask;
    }
}
