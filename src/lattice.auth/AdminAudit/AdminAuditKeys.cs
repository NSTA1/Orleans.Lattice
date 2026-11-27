using System.Globalization;

namespace Orleans.Lattice.Auth;

/// <summary>
/// Key layout of the <see cref="AuthConstants.AdminAuditTree"/> tree.
/// <list type="bullet">
/// <item><description>A pending intent: <c>pending/{operationId}</c>.</description></item>
/// <item><description>A final record: <c>{scope}/{ticks:D19}\u001f{operationId}</c>, where
/// <c>scope</c> is <c>cluster</c> or <c>t/{tenant}</c> and <c>ticks</c> are the intent's UTC
/// ticks. A partition is one prefix scan and sorts oldest first. The key is derived from
/// the intent alone, so every finalizer of one call computes the same key and the
/// create-only write admits exactly one.</description></item>
/// </list>
/// </summary>
internal static class AdminAuditKeys
{
    /// <summary>The prefix of every pending intent key.</summary>
    internal const string PendingPrefix = "pending/";

    /// <summary>The exclusive upper bound of the pending range (<c>'/' + 1</c>).</summary>
    internal const string PendingEnd = "pending0";

    /// <summary>The separator between the ticks prefix and the operation id in a final key.</summary>
    internal const char Separator = '\u001f';

    /// <summary>The pending intent key for <paramref name="operationId"/>.</summary>
    /// <param name="operationId">The call's operation id.</param>
    internal static string Pending(string operationId) => string.Concat(PendingPrefix, operationId);

    /// <summary>The final record key for <paramref name="record"/>.</summary>
    /// <param name="record">The intent or final record.</param>
    internal static string Final(LatticeAdminAuditRecord record) =>
        string.Create(
            CultureInfo.InvariantCulture,
            $"{record.TargetScope}/{record.TimestampUtc.UtcTicks:D19}{Separator}{record.OperationId}");

    /// <summary>The inclusive start of the final records of <paramref name="scope"/>.</summary>
    /// <param name="scope">The partition (<c>cluster</c> or <c>t/{tenant}</c>).</param>
    internal static string ScopeStart(string scope) => string.Concat(scope, "/");

    /// <summary>The exclusive end of the final records of <paramref name="scope"/> (<c>'/' + 1</c>).</summary>
    /// <param name="scope">The partition.</param>
    internal static string ScopeEnd(string scope) => string.Concat(scope, "0");

    /// <summary>
    /// Reads the intent ticks from a final key, or returns <see langword="false"/> when
    /// the key is not a final key.
    /// </summary>
    /// <param name="key">The key.</param>
    /// <param name="ticks">The intent's UTC ticks.</param>
    internal static bool TryReadTicks(string key, out long ticks)
    {
        ticks = 0;
        var separator = key.LastIndexOf(Separator);
        if (separator < 19 || key.StartsWith(PendingPrefix, StringComparison.Ordinal))
        {
            return false;
        }

        return long.TryParse(key.AsSpan(separator - 19, 19), NumberStyles.None, CultureInfo.InvariantCulture, out ticks);
    }
}
