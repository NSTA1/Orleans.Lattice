using System.Security.Cryptography;
using System.Text;

namespace Orleans.Lattice.Backup;

/// <summary>
/// The single definition of a backup set's content address and of the rule that
/// decides whether a set records durable membership at all. Set membership is not
/// stored as a set: the only durable trace is the
/// <see cref="BackupManifest.SetId"/> stamp pushed onto each member's own
/// manifest, so an id is minted only for a set whose membership is actually
/// stamped. Kept in one place so the capture path that mints an id and the
/// restore path that reverse-resolves a legacy single-member id can never drift
/// apart.
/// </summary>
internal static class BackupSetIdentity
{
    /// <summary>
    /// The smallest member count whose set membership is durably recorded. A
    /// single-member set is deliberately left unstamped - it is indistinguishable
    /// from a plain backup and lists as one - so it is given no set id either.
    /// </summary>
    public const int MinimumRecordedMembers = 2;

    /// <summary>
    /// Whether a set of this many members records durable membership, and so
    /// carries a set id.
    /// </summary>
    /// <param name="memberCount">The number of member backups in the set.</param>
    /// <returns><c>true</c> when the set's membership is stamped and identified.</returns>
    public static bool RecordsMembership(int memberCount) =>
        memberCount >= MinimumRecordedMembers;

    /// <summary>
    /// Computes the content-addressed set id over the ordered member backup ids:
    /// the lowercase-hex SHA-256 of the newline-terminated ids in member order, so
    /// a set of identical members in identical order always registers the same id.
    /// </summary>
    /// <param name="memberBackupIds">The ordered member backup ids. Must not be <c>null</c>.</param>
    /// <returns>The lowercase-hex SHA-256 content address of the member list.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="memberBackupIds"/> is <c>null</c>.</exception>
    public static string Compute(IReadOnlyList<string> memberBackupIds)
    {
        ArgumentNullException.ThrowIfNull(memberBackupIds);

        using var hasher = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        foreach (var id in memberBackupIds)
        {
            hasher.AppendData(Encoding.UTF8.GetBytes(id));
            hasher.AppendData("\n"u8);
        }

        return Convert.ToHexStringLower(hasher.GetHashAndReset());
    }
}
