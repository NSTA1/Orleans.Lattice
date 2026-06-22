using System.Globalization;
using System.Security.Cryptography;
using System.Text;

namespace Orleans.Lattice.Api.State.Grpc;

/// <summary>
/// Encodes, parses, and verifies salted PBKDF2-HMAC-SHA256 password hashes in the
/// portable, self-describing <c>pbkdf2-sha256$&lt;iterations&gt;$&lt;base64-salt&gt;$&lt;base64-derived-key&gt;</c>
/// format that is the contract shared by the credential-generation helper scripts
/// (PowerShell / bash) and the server-side <see cref="EnvVarCredentialAuthorizer"/>.
/// </summary>
/// <remarks>
/// <para>
/// PBKDF2-HMAC-SHA256 is chosen for cross-platform parity: PowerShell
/// (<c>Rfc2898DeriveBytes</c>), bash (<c>openssl</c>), and the server
/// (<see cref="Rfc2898DeriveBytes"/>) can all produce and verify it with no
/// third-party dependency. The iteration count is recorded in every encoded hash
/// so existing credentials keep verifying after the default is raised.
/// </para>
/// <para>
/// Verification re-derives the key with the salt and iteration count embedded in
/// the stored hash and compares it to the presented key in constant time via
/// <see cref="CryptographicOperations.FixedTimeEquals(ReadOnlySpan{byte}, ReadOnlySpan{byte})"/>,
/// so it leaks neither the stored key nor a length-dependent timing signal.
/// </para>
/// </remarks>
public static class LatticePasswordHash
{
    /// <summary>The algorithm prefix every encoded hash carries.</summary>
    public const string AlgorithmPrefix = "pbkdf2-sha256";

    /// <summary>The default iteration count used when a caller does not specify one.</summary>
    public const int DefaultIterations = 210_000;

    /// <summary>The salt length, in bytes, produced by <see cref="Hash(string, int)"/>.</summary>
    public const int SaltSizeBytes = 16;

    /// <summary>The derived-key length, in bytes.</summary>
    public const int DerivedKeySizeBytes = 32;

    /// <summary>
    /// Derives a fresh salted hash of <paramref name="password"/> using a new
    /// cryptographically random salt and the supplied <paramref name="iterations"/>,
    /// returning the encoded <c>pbkdf2-sha256$...</c> string.
    /// </summary>
    /// <param name="password">The plaintext password to hash. Must not be <see langword="null"/>.</param>
    /// <param name="iterations">The PBKDF2 iteration count; defaults to <see cref="DefaultIterations"/>.</param>
    /// <returns>The encoded hash string.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="password"/> is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="iterations"/> is not positive.</exception>
    public static string Hash(string password, int iterations = DefaultIterations)
    {
        ArgumentNullException.ThrowIfNull(password);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(iterations);

        var salt = RandomNumberGenerator.GetBytes(SaltSizeBytes);
        return Encode(password, salt, iterations);
    }

    /// <summary>
    /// Encodes a hash of <paramref name="password"/> with the explicit
    /// <paramref name="salt"/> and <paramref name="iterations"/>. Intended for
    /// reproducing a known vector; production callers use <see cref="Hash(string, int)"/>
    /// so the salt is random per credential.
    /// </summary>
    /// <param name="password">The plaintext password to hash.</param>
    /// <param name="salt">The salt to use; must be non-empty.</param>
    /// <param name="iterations">The PBKDF2 iteration count.</param>
    /// <returns>The encoded hash string.</returns>
    public static string Encode(string password, ReadOnlySpan<byte> salt, int iterations)
    {
        ArgumentNullException.ThrowIfNull(password);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(iterations);
        if (salt.IsEmpty)
        {
            throw new ArgumentException("Salt must be non-empty.", nameof(salt));
        }

        var derived = Rfc2898DeriveBytes.Pbkdf2(
            Encoding.UTF8.GetBytes(password),
            salt,
            iterations,
            HashAlgorithmName.SHA256,
            DerivedKeySizeBytes);

        return string.Concat(
            AlgorithmPrefix,
            "$",
            iterations.ToString(CultureInfo.InvariantCulture),
            "$",
            Convert.ToBase64String(salt),
            "$",
            Convert.ToBase64String(derived));
    }

    /// <summary>
    /// Verifies <paramref name="password"/> against a stored
    /// <paramref name="encodedHash"/> in the <c>pbkdf2-sha256$...</c> format,
    /// re-deriving with the salt and iteration count embedded in the hash and
    /// comparing in constant time.
    /// </summary>
    /// <param name="password">The presented plaintext password.</param>
    /// <param name="encodedHash">The stored encoded hash.</param>
    /// <returns>
    /// <see langword="true"/> when the password matches the hash; otherwise
    /// <see langword="false"/> (including when the hash is malformed).
    /// </returns>
    public static bool Verify(string? password, string? encodedHash)
    {
        if (password is null || !TryParse(encodedHash, out var parsed))
        {
            return false;
        }

        var derived = Rfc2898DeriveBytes.Pbkdf2(
            Encoding.UTF8.GetBytes(password),
            parsed.Salt,
            parsed.Iterations,
            HashAlgorithmName.SHA256,
            parsed.DerivedKey.Length);

        return CryptographicOperations.FixedTimeEquals(derived, parsed.DerivedKey);
    }

    /// <summary>
    /// Parses an encoded <c>pbkdf2-sha256$&lt;iterations&gt;$&lt;base64-salt&gt;$&lt;base64-derived-key&gt;</c>
    /// string into its components.
    /// </summary>
    /// <param name="encodedHash">The encoded hash string.</param>
    /// <param name="result">The parsed components, when successful.</param>
    /// <returns><see langword="true"/> when the hash is well-formed; otherwise <see langword="false"/>.</returns>
    public static bool TryParse(string? encodedHash, out LatticePasswordHashComponents result)
    {
        result = default;
        if (string.IsNullOrEmpty(encodedHash))
        {
            return false;
        }

        var parts = encodedHash.Split('$');
        if (parts.Length != 4 || !string.Equals(parts[0], AlgorithmPrefix, StringComparison.Ordinal))
        {
            return false;
        }

        if (!int.TryParse(parts[1], NumberStyles.None, CultureInfo.InvariantCulture, out var iterations) || iterations <= 0)
        {
            return false;
        }

        byte[] salt;
        byte[] derivedKey;
        try
        {
            salt = Convert.FromBase64String(parts[2]);
            derivedKey = Convert.FromBase64String(parts[3]);
        }
        catch (FormatException)
        {
            return false;
        }

        if (salt.Length == 0 || derivedKey.Length == 0)
        {
            return false;
        }

        result = new LatticePasswordHashComponents(iterations, salt, derivedKey);
        return true;
    }
}

/// <summary>
/// The parsed components of an encoded <c>pbkdf2-sha256$...</c> hash: the
/// iteration count, the salt, and the derived key.
/// </summary>
/// <param name="Iterations">The PBKDF2 iteration count.</param>
/// <param name="Salt">The salt bytes.</param>
/// <param name="DerivedKey">The derived-key bytes.</param>
public readonly record struct LatticePasswordHashComponents(int Iterations, byte[] Salt, byte[] DerivedKey);
