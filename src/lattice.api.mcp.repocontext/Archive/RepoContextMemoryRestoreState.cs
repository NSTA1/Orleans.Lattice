using System.Globalization;
using System.Text;

namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// What a restore attempt did to the memory tree, as a state a later attempt can read
/// back rather than infer.
/// </summary>
internal enum RepoContextMemoryRestoreOutcome
{
    /// <summary>
    /// The snapshot was imported and the resulting tree was verified to hold at least
    /// as many records as the snapshot carried.
    /// </summary>
    Restored,

    /// <summary>
    /// An import began, wrote records, and did not finish. The tree holds a partial
    /// snapshot: strictly more than nothing and strictly less than the archive.
    /// <para>
    /// This is the state the whole marker exists for. Without it a partial tree is
    /// indistinguishable from a legitimately populated one, because the only available
    /// signal - "does the store hold any record" - answers the same way for both.
    /// </para>
    /// </summary>
    Partial,

    /// <summary>
    /// The tree already holds a complete restore, or holds records that no restore in
    /// this store's history put there, so there is nothing for a restore to heal.
    /// </summary>
    NothingToRestore,

    /// <summary>
    /// No restore was attempted: the mode is <see cref="RepoContextMemoryArchiveRestoreMode.Off"/>
    /// or no snapshot file exists yet.
    /// </summary>
    NotAttempted,

    /// <summary>
    /// Every candidate snapshot failed to import and none of them wrote anything, so
    /// the tree is exactly as it was.
    /// </summary>
    Failed,
}

/// <summary>
/// The durable record of what the last restore attempt did to this store's memory
/// tree, written into the tree itself so it survives a restart and travels with the
/// state it describes.
/// <para>
/// <b>Why this exists.</b> Restore mode
/// <see cref="RepoContextMemoryArchiveRestoreMode.Auto"/> decides whether to act by
/// asking "does the store hold any memory record". That question is a proxy for "has
/// this tree already been restored", and the two agree on a healthy tree and on a
/// fresh one - and disagree on exactly the tree that needs help. An import that fails
/// partway leaves records behind, so the next attempt sees a non-empty store, declines,
/// and reports a populated tree. The recovery action is disarmed by the failure it is
/// recovering from. See issue #2641.
/// </para>
/// <para>
/// <b>Why it is written before the import rather than after it.</b> This is a
/// write-ahead intent record. <see cref="RepoContextMemoryRestoreOutcome.Partial"/> is
/// written before the first record lands and is only ever cleared by a verification
/// step that runs after the import returns. A marker written after the fact could not
/// describe an import that never returned, which is precisely the case that needs
/// describing. A crash therefore leaves the marker reading
/// <see cref="RepoContextMemoryRestoreOutcome.Partial"/>, which is the truthful and
/// safe reading.
/// </para>
/// <para>
/// <b>Why the completion stamp is a separate step.</b> A marker written by the same
/// code path that writes the records inherits that path's failure modes, so a partial
/// restore could stamp itself complete. The stamp is applied only after the tree has
/// been counted and found to hold at least what the snapshot carried.
/// </para>
/// <para>
/// <b>Why the key sits outside the repository prefix.</b> Both the export and the
/// emptiness probe enumerate from <see cref="RepoContextKeys.AllReposPrefix"/>, so a
/// marker stored under it would be archived and would count as a memory record - a
/// restore would then import a foreign store's completion stamp and a store holding
/// nothing but a marker would read as non-empty. The marker describes this store, so
/// it stays local to it.
/// </para>
/// </summary>
/// <param name="Outcome">What the recorded attempt did.</param>
/// <param name="Records">The record count the attempt reported, or 0 when it reported none.</param>
/// <param name="StampedUtcTicks">When the state was written, in UTC ticks.</param>
/// <param name="Source">The snapshot file the attempt was reading, or <see langword="null"/>.</param>
internal readonly record struct RepoContextMemoryRestoreState(
    RepoContextMemoryRestoreOutcome Outcome,
    long Records,
    long StampedUtcTicks,
    string? Source)
{
    /// <summary>
    /// The store-local key the marker is written to. It deliberately does not begin
    /// with <see cref="RepoContextKeys.RepoSegment"/>, so neither the archive export
    /// nor the emptiness probe can see it.
    /// </summary>
    internal const string Key = "archive/memory-restore-state";

    /// <summary>The encoding version prefix, so an older marker is recognised rather than misread.</summary>
    private const string Version = "v1";

    private const char Separator = '|';

    /// <summary>
    /// Encodes the state as UTF-8 text. Text rather than a serialized type on purpose:
    /// the marker is store-local operational state, and adding a wire-format type with
    /// a permanent alias for it would make a local bookkeeping record part of the
    /// serialization contract.
    /// </summary>
    /// <returns>The encoded marker.</returns>
    internal byte[] Encode()
    {
        var source = (Source ?? string.Empty).Replace(Separator, '/');
        return Encoding.UTF8.GetBytes(string.Join(
            Separator,
            Version,
            Outcome.ToString(),
            Records.ToString(CultureInfo.InvariantCulture),
            StampedUtcTicks.ToString(CultureInfo.InvariantCulture),
            source));
    }

    /// <summary>
    /// Decodes a marker, or returns <see langword="null"/> when there is none or it
    /// cannot be read.
    /// <para>
    /// An unreadable marker decodes to <see langword="null"/> - "no marker" - and not
    /// to <see cref="RepoContextMemoryRestoreOutcome.Partial"/>. The two are not
    /// symmetric: reading a damaged marker as a partial would authorise a restore over
    /// a store that may be legitimately populated, and importing an archive over live
    /// memory can resurrect records that were deliberately forgotten. Declining to act
    /// on an unreadable marker leaves a recoverable situation; acting on one does not.
    /// </para>
    /// </summary>
    /// <param name="stored">The stored marker bytes, or <see langword="null"/> when the key is absent.</param>
    /// <returns>The decoded state, or <see langword="null"/> when absent or unreadable.</returns>
    internal static RepoContextMemoryRestoreState? Decode(byte[]? stored)
    {
        if (stored is null || stored.Length == 0)
        {
            return null;
        }

        string text;
        try
        {
            text = Encoding.UTF8.GetString(stored);
        }
        catch (ArgumentException)
        {
            return null;
        }

        var parts = text.Split(Separator);
        if (parts.Length < 4 || parts[0] != Version)
        {
            return null;
        }

        if (!Enum.TryParse<RepoContextMemoryRestoreOutcome>(parts[1], out var outcome)
            || !long.TryParse(parts[2], CultureInfo.InvariantCulture, out var records)
            || !long.TryParse(parts[3], CultureInfo.InvariantCulture, out var ticks))
        {
            return null;
        }

        var source = parts.Length > 4 && parts[4].Length > 0 ? parts[4] : null;
        return new RepoContextMemoryRestoreState(outcome, records, ticks, source);
    }
}
