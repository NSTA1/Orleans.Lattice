using System.Globalization;
using System.Text;
using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Operations;

/// <summary>
/// Validates operation ids and composes the storage-safe grain keys of the
/// coordinated-operation grains. An operation grain is keyed by its tenant and id
/// together, so two tenants that choose the same id address two different grains
/// and a read in one tenant can never reach another tenant's operation.
/// </summary>
/// <remarks>
/// Both grains persist state, so their keys follow the storage-safe compound key
/// rules: the tenant is percent-encoded and the operation id is restricted to a
/// character set that excludes the field separator, so a key is never ambiguous.
/// </remarks>
internal static class LatticeOperationKey
{
    /// <summary>The longest operation id accepted.</summary>
    internal const int MaxOperationIdLength = 128;

    private const char FieldSeparator = '|';
    private const char EscapeChar = '%';
    private const string IndexPrefix = "idx";

    /// <summary>
    /// Validates a caller-supplied operation id: 1 to <see cref="MaxOperationIdLength"/>
    /// characters drawn from ASCII letters, digits, <c>-</c>, <c>_</c> and <c>.</c>.
    /// </summary>
    /// <param name="operationId">The id.</param>
    /// <param name="paramName">The parameter name reported on failure.</param>
    /// <exception cref="ArgumentException">The id is null, empty, too long, or carries a disallowed character.</exception>
    internal static void ThrowIfInvalid(string? operationId, string paramName)
    {
        if (!IsValid(operationId))
        {
            throw new ArgumentException(
                $"An operation id must be 1 to {MaxOperationIdLength} characters of ASCII letters, digits, '-', '_' and '.'.",
                paramName);
        }
    }

    /// <summary>Returns whether <paramref name="operationId"/> is a well-formed operation id.</summary>
    /// <param name="operationId">The id.</param>
    /// <returns><see langword="true"/> when valid.</returns>
    internal static bool IsValid(string? operationId)
    {
        if (string.IsNullOrEmpty(operationId) || operationId.Length > MaxOperationIdLength)
        {
            return false;
        }

        foreach (var ch in operationId)
        {
            if (!(char.IsAsciiLetterOrDigit(ch) || ch is '-' or '_' or '.'))
            {
                return false;
            }
        }

        return true;
    }

    /// <summary>Generates a fresh operation id.</summary>
    /// <returns>A 32-character lowercase hexadecimal id.</returns>
    internal static string NewId() => Guid.NewGuid().ToString("N", CultureInfo.InvariantCulture);

    /// <summary>Composes an operation grain key.</summary>
    /// <param name="tenantId">The tenant id.</param>
    /// <param name="operationId">A validated operation id.</param>
    /// <returns>The storage-safe key.</returns>
    [GrainKeyBuilder]
    internal static string For(string tenantId, string operationId)
    {
        var builder = new StringBuilder(tenantId.Length + operationId.Length + 1);
        AppendEncoded(builder, tenantId);
        builder.Append(FieldSeparator);
        builder.Append(operationId);
        return builder.ToString();
    }

    /// <summary>Composes a tenant's operation index grain key.</summary>
    /// <param name="tenantId">The tenant id.</param>
    /// <returns>The storage-safe key.</returns>
    [GrainKeyBuilder]
    internal static string ForIndex(string tenantId)
    {
        var builder = new StringBuilder(tenantId.Length + IndexPrefix.Length + 1);
        builder.Append(IndexPrefix);
        builder.Append(FieldSeparator);
        AppendEncoded(builder, tenantId);
        return builder.ToString();
    }

    /// <summary>Splits an operation grain key into its tenant and operation id.</summary>
    /// <param name="key">A key from <see cref="For"/>.</param>
    /// <returns>The tenant id and the operation id.</returns>
    /// <exception cref="FormatException">The key is not an operation grain key.</exception>
    internal static (string TenantId, string OperationId) Parse(string key)
    {
        var separator = key.LastIndexOf(FieldSeparator);
        if (separator < 0 || separator == key.Length - 1)
        {
            throw new FormatException($"'{key}' is not an operation grain key.");
        }

        return (Decode(key.AsSpan(0, separator)), key[(separator + 1)..]);
    }

    /// <summary>Recovers the tenant id from an index grain key.</summary>
    /// <param name="key">A key from <see cref="ForIndex"/>.</param>
    /// <returns>The tenant id.</returns>
    /// <exception cref="FormatException">The key is not an index grain key.</exception>
    internal static string ParseIndex(string key)
    {
        var prefixLength = IndexPrefix.Length + 1;
        if (key.Length < prefixLength
            || !key.StartsWith(IndexPrefix, StringComparison.Ordinal)
            || key[IndexPrefix.Length] != FieldSeparator)
        {
            throw new FormatException($"'{key}' is not an operation index grain key.");
        }

        return Decode(key.AsSpan(prefixLength));
    }

    private static void AppendEncoded(StringBuilder builder, string value)
    {
        foreach (var ch in value)
        {
            if (IsSafe(ch))
            {
                builder.Append(ch);
            }
            else
            {
                builder.Append(EscapeChar);
                builder.Append(((int)ch).ToString("X4", CultureInfo.InvariantCulture));
            }
        }
    }

    private static string Decode(ReadOnlySpan<char> value)
    {
        var builder = new StringBuilder(value.Length);
        for (var i = 0; i < value.Length; i++)
        {
            if (value[i] == EscapeChar && i + 4 < value.Length)
            {
                builder.Append((char)int.Parse(value.Slice(i + 1, 4), NumberStyles.HexNumber, CultureInfo.InvariantCulture));
                i += 4;
            }
            else
            {
                builder.Append(value[i]);
            }
        }

        return builder.ToString();
    }

    private static bool IsSafe(char ch) =>
        ch != FieldSeparator
        && ch != EscapeChar
        && ch is not ('/' or '\\' or '#' or '?')
        && ch is not (>= '\u0000' and <= '\u001f')
        && ch is not (>= '\u007f' and <= '\u009f');
}
