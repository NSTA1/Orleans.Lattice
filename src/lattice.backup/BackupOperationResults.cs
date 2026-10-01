using System.Diagnostics.CodeAnalysis;
using System.Globalization;

namespace Orleans.Lattice.Backup;

/// <summary>
/// Converts between backup engine results and the string result map a tracked
/// operation records (keys in <see cref="BackupOperationResultKeys"/>).
/// </summary>
public static class BackupOperationResults
{
    private const char ListSeparator = ',';

    /// <summary>
    /// Reconstructs the <see cref="LatticeRestoreResult"/> of a succeeded restore
    /// or cold restore from its result map, for example to revert a shadow-cutover
    /// restore.
    /// </summary>
    /// <param name="result">The operation's result map. Must not be <c>null</c>.</param>
    /// <param name="restore">The reconstructed restore result when this returns <see langword="true"/>.</param>
    /// <returns><see langword="true"/> when the map describes a restore.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="result"/> is <c>null</c>.</exception>
    public static bool TryReadRestoreResult(
        IReadOnlyDictionary<string, string> result,
        [NotNullWhen(true)] out LatticeRestoreResult? restore)
    {
        ArgumentNullException.ThrowIfNull(result);
        restore = null;
        if (!result.TryGetValue(BackupOperationResultKeys.BackupId, out var backupId)
            || !result.TryGetValue(BackupOperationResultKeys.TargetTreeId, out var targetTreeId)
            || !result.TryGetValue(BackupOperationResultKeys.RestoreOperationId, out var operationId)
            || !result.TryGetValue(BackupOperationResultKeys.Mode, out var modeText)
            || !Enum.TryParse<LatticeRestoreMode>(modeText, ignoreCase: false, out var mode)
            || string.IsNullOrEmpty(backupId)
            || string.IsNullOrEmpty(targetTreeId)
            || string.IsNullOrEmpty(operationId))
        {
            return false;
        }

        restore = new LatticeRestoreResult(
            backupId,
            targetTreeId,
            mode,
            operationId,
            ReadList(result, BackupOperationResultKeys.ManifestChain),
            ReadLong(result, BackupOperationResultKeys.EntriesApplied),
            result.GetValueOrDefault(BackupOperationResultKeys.ShadowPhysicalTreeId),
            result.GetValueOrDefault(BackupOperationResultKeys.PreviousPhysicalTreeId),
            ReadLong(result, BackupOperationResultKeys.DeadLetteredCrossTenant),
            ReadLong(result, BackupOperationResultKeys.DeadLetteredOverQuota));
        return true;
    }

    /// <summary>Reads the member backup ids of a succeeded set capture.</summary>
    /// <param name="result">The operation's result map. Must not be <c>null</c>.</param>
    /// <returns>The member ids in scope order, or empty when the map carries none.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="result"/> is <c>null</c>.</exception>
    public static IReadOnlyList<string> ReadMemberBackupIds(IReadOnlyDictionary<string, string> result)
    {
        ArgumentNullException.ThrowIfNull(result);
        return ReadList(result, BackupOperationResultKeys.MemberBackupIds);
    }

    /// <summary>Builds the result map of a capture.</summary>
    internal static IReadOnlyDictionary<string, string> ToResultMap(LatticeBackupCaptureResult capture) =>
        new Dictionary<string, string>(1, StringComparer.Ordinal)
        {
            [BackupOperationResultKeys.BackupId] = capture.BackupId,
        };

    /// <summary>Builds the result map of a set capture.</summary>
    internal static IReadOnlyDictionary<string, string> ToResultMap(LatticeBackupSetCaptureResult set)
    {
        var members = new string[set.Members.Count];
        for (var i = 0; i < members.Length; i++)
        {
            members[i] = set.Members[i].BackupId;
        }

        var map = new Dictionary<string, string>(2, StringComparer.Ordinal)
        {
            [BackupOperationResultKeys.MemberBackupIds] = string.Join(ListSeparator, members),
        };

        if (set.SetManifest.SetId is { } setId)
        {
            map[BackupOperationResultKeys.SetId] = setId;
        }

        return map;
    }

    /// <summary>Builds the result map of a restore.</summary>
    internal static IReadOnlyDictionary<string, string> ToResultMap(LatticeRestoreResult restore)
    {
        var map = new Dictionary<string, string>(10, StringComparer.Ordinal)
        {
            [BackupOperationResultKeys.BackupId] = restore.BackupId,
            [BackupOperationResultKeys.TargetTreeId] = restore.TargetTreeId,
            [BackupOperationResultKeys.Mode] = restore.Mode.ToString(),
            [BackupOperationResultKeys.RestoreOperationId] = restore.OperationId,
            [BackupOperationResultKeys.ManifestChain] = string.Join(ListSeparator, restore.ManifestChain),
            [BackupOperationResultKeys.EntriesApplied] = restore.EntriesApplied.ToString(CultureInfo.InvariantCulture),
            [BackupOperationResultKeys.DeadLetteredCrossTenant] = restore.DeadLetteredCrossTenant.ToString(CultureInfo.InvariantCulture),
            [BackupOperationResultKeys.DeadLetteredOverQuota] = restore.DeadLetteredOverQuota.ToString(CultureInfo.InvariantCulture),
        };

        if (restore.ShadowPhysicalTreeId is { } shadow)
        {
            map[BackupOperationResultKeys.ShadowPhysicalTreeId] = shadow;
        }

        if (restore.PreviousPhysicalTreeId is { } previous)
        {
            map[BackupOperationResultKeys.PreviousPhysicalTreeId] = previous;
        }

        return map;
    }

    private static IReadOnlyList<string> ReadList(IReadOnlyDictionary<string, string> result, string key) =>
        result.TryGetValue(key, out var joined) && joined.Length > 0
            ? joined.Split(ListSeparator)
            : [];

    private static long ReadLong(IReadOnlyDictionary<string, string> result, string key) =>
        result.TryGetValue(key, out var text)
        && long.TryParse(text, NumberStyles.None, CultureInfo.InvariantCulture, out var value)
            ? value
            : 0;
}
