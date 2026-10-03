using System.Diagnostics;
using System.Text;
using Microsoft.Data.Sqlite;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Host;

/// <summary>
/// Finds, and optionally deletes, leaf snapshot storage that no live row reaches
/// (issue #4391): <c>leaf-snapshot</c> manifests and <c>leaf-snapshot-segment</c>
/// rows left behind by leaf removals before issue #4383, and the <c>leaf</c> rows
/// a cleared leaf's deactivation wrote back without a tree id before issue #4419.
/// </summary>
/// <remarks>
/// <para>
/// <b>Why a storage sweep.</b> These rows belong to leaves that no longer exist,
/// so no tree walk, owed-clear record or retry in the library knows their keys.
/// Only a pass over the grain-storage rows themselves can find them.
/// </para>
/// <para>
/// <b>What counts as stranded.</b> Rows are grouped by the leaf identity that owns
/// them: a leaf row and a manifest are keyed by the leaf's Guid, and a segment by
/// that Guid followed by <c>/</c>. A leaf identity is live when a live row refers
/// to it. Every row other than a leaf or snapshot row - shard roots, internal
/// nodes, materialiser pins, cursors, operation records, of any grain type - is a
/// root, and the identities it mentions are live; a live leaf's row then makes the
/// identities it mentions (its siblings) live too. Everything else is stranded. A
/// row "mentions" an identity when its payload or key carries the Guid as 32 or 36
/// hex characters, or as its 16 raw bytes in either byte order, so a reference in
/// any encoding keeps a leaf. The rule errs towards keeping: a stray match keeps
/// rows that could have gone, never the reverse. One mention is not a reference: a
/// JSON shard root's <c>LeafAccessModel</c> records every leaf the shard has seen
/// visited, removed ones included, and nothing routes through it; measured on a
/// live host it was what named most leaves that had no row. A binary shard root is
/// scanned whole. A snapshot whose leaf row is
/// missing but which a shard still routes to is kept, because it may hold the only
/// copy of that leaf's data.
/// </para>
/// <para>
/// <b>When it runs.</b> Before the silo starts, from
/// <see cref="RepoContextHostBuilder.PrepareDataPaths"/>, while nothing else has the
/// database open, so no leaf can be created or cleared while the rows are being
/// classified. It is opt-in and idempotent: a second run finds nothing.
/// </para>
/// </remarks>
public static class SqliteSnapshotOrphanSweep
{
    /// <summary>The grain-state name of a leaf's own row.</summary>
    public const string LeafState = "leaf";

    /// <summary>The grain-state name of a leaf's snapshot manifest.</summary>
    public const string ManifestState = "leaf-snapshot";

    /// <summary>The grain-state name of a leaf's snapshot segment.</summary>
    public const string SegmentState = "leaf-snapshot-segment";

    /// <summary>The grain-state name of a shard root.</summary>
    public const string ShardRootState = "shardroot";

    /// <summary>The shard-root property holding its leaf access statistics.</summary>
    private const string AccessStatisticsProperty = "LeafAccessModel";

    /// <summary>The rows deleted per transaction when none is given.</summary>
    public const int DefaultBatchSize = 500;

    /// <summary>
    /// Classifies the leaf and snapshot rows of the database at
    /// <paramref name="databasePath"/> and, in <see cref="SqliteSnapshotSweepMode.Delete"/>
    /// mode, deletes the stranded ones and returns the freed pages to the filesystem.
    /// </summary>
    /// <param name="databasePath">The SQLite database file.</param>
    /// <param name="mode">Whether to run, and whether to delete.</param>
    /// <param name="batchSize">The most rows deleted in one transaction.</param>
    /// <returns>What the sweep found and removed.</returns>
    /// <exception cref="ArgumentException"><paramref name="databasePath"/> is null or whitespace.</exception>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="batchSize"/> is not positive.</exception>
    public static SqliteSnapshotSweepOutcome Run(
        string databasePath,
        SqliteSnapshotSweepMode mode,
        int batchSize = DefaultBatchSize)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(databasePath);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(batchSize);

        if (mode == SqliteSnapshotSweepMode.Off)
        {
            return SqliteSnapshotSweepOutcome.NotRun;
        }

        var elapsed = Stopwatch.StartNew();

        using var connection = new SqliteConnection(SqliteSchemaInitializer.BuildConnectionString(databasePath));
        connection.Open();

        // Fold the write-ahead log into the file first, so the before and after sizes
        // compare the same thing.
        Checkpoint(connection);
        var bytesBefore = FileSize(databasePath);

        var owners = LoadOwnedRows(connection);
        var live = FindLive(connection, owners);
        var stranded = owners.Where(o => !live.Contains(o.Key)).Select(o => o.Value).ToList();

        var rowsDeleted = 0;
        if (mode == SqliteSnapshotSweepMode.Delete && stranded.Count > 0)
        {
            rowsDeleted = Delete(connection, stranded.SelectMany(o => o.RowIds).ToList(), batchSize);
            ReturnFreedPages(connection);
        }

        connection.Close();
        SqliteConnection.ClearPool(connection);

        return new SqliteSnapshotSweepOutcome(
            mode,
            owners.Count,
            stranded.Count,
            stranded.Sum(o => o.LeafRows),
            stranded.Sum(o => o.ManifestRows),
            stranded.Sum(o => o.SegmentRows),
            stranded.Sum(o => o.PayloadBytes),
            rowsDeleted,
            bytesBefore,
            FileSize(databasePath),
            elapsed.Elapsed);
    }

    /// <summary>
    /// Groups every leaf, manifest and segment row by the leaf identity that owns it.
    /// A row whose key does not have the expected shape is left out, so it is never deleted.
    /// </summary>
    private static Dictionary<OwnerKey, Owner> LoadOwnedRows(SqliteConnection connection)
    {
        var owners = new Dictionary<OwnerKey, Owner>();
        using var command = connection.CreateCommand();
        command.CommandText =
            "SELECT rowid, ServiceId, GrainTypeString, GrainIdN0, GrainIdN1, GrainIdExtensionString, "
            + "IFNULL(LENGTH(PayloadBinary), 0) FROM OrleansStorage WHERE GrainTypeString IN ($leaf, $manifest, $segment);";
        command.Parameters.AddWithValue("$leaf", LeafState);
        command.Parameters.AddWithValue("$manifest", ManifestState);
        command.Parameters.AddWithValue("$segment", SegmentState);

        using var reader = command.ExecuteReader();
        while (reader.Read())
        {
            var type = reader.GetString(2);
            var extension = reader.IsDBNull(5) ? null : reader.GetString(5);
            Guid leaf;
            if (type == SegmentState)
            {
                if (!TryParseSegmentOwner(extension, out leaf))
                {
                    continue;
                }
            }
            else
            {
                if (!string.IsNullOrEmpty(extension))
                {
                    continue;
                }

                leaf = GuidFromKeyColumns(reader.GetInt64(3), reader.GetInt64(4));
            }

            var key = new OwnerKey(reader.GetString(1), leaf);
            if (!owners.TryGetValue(key, out var owner))
            {
                owners[key] = owner = new Owner();
            }

            owner.RowIds.Add(reader.GetInt64(0));
            owner.PayloadBytes += reader.GetInt64(6);
            switch (type)
            {
                case LeafState:
                    owner.LeafRows++;
                    break;
                case ManifestState:
                    owner.ManifestRows++;
                    break;
                default:
                    owner.SegmentRows++;
                    break;
            }
        }

        return owners;
    }

    /// <summary>
    /// The leaf identities a live row reaches: those any non-leaf, non-snapshot row
    /// mentions, then, transitively, those a reached leaf's own row mentions.
    /// </summary>
    private static HashSet<OwnerKey> FindLive(SqliteConnection connection, Dictionary<OwnerKey, Owner> owners)
    {
        var candidatesByService = owners.Keys
            .GroupBy(k => k.ServiceId, StringComparer.Ordinal)
            .ToDictionary(g => g.Key, g => g.Select(k => k.Leaf).ToHashSet(), StringComparer.Ordinal);

        var live = new HashSet<OwnerKey>();
        var pending = new Queue<OwnerKey>();
        var leafMentions = new Dictionary<OwnerKey, List<Guid>>();

        using (var command = connection.CreateCommand())
        {
            command.CommandText =
                "SELECT ServiceId, GrainTypeString, GrainIdN0, GrainIdN1, GrainIdExtensionString, PayloadBinary "
                + "FROM OrleansStorage WHERE GrainTypeString NOT IN ($manifest, $segment);";
            command.Parameters.AddWithValue("$manifest", ManifestState);
            command.Parameters.AddWithValue("$segment", SegmentState);

            using var reader = command.ExecuteReader();
            var found = new HashSet<Guid>();
            while (reader.Read())
            {
                var service = reader.GetString(0);
                if (!candidatesByService.TryGetValue(service, out var candidates))
                {
                    continue;
                }

                var extension = reader.IsDBNull(4) ? null : reader.GetString(4);
                found.Clear();
                if (extension is not null)
                {
                    FindMentions(Encoding.UTF8.GetBytes(extension), candidates, found);
                }

                if (!reader.IsDBNull(5))
                {
                    var payload = reader.GetFieldValue<byte[]>(5);
                    if (reader.GetString(1) == ShardRootState && TryFindAccessStatistics(payload, out var start, out var end))
                    {
                        // The shard root's leaf access statistics record every leaf it has
                        // seen visited, removed ones included, and nothing routes through
                        // them, so they do not keep a leaf. The rest of the row still does.
                        FindMentions(payload.AsSpan(0, start), candidates, found);
                        FindMentions(payload.AsSpan(end), candidates, found);
                    }
                    else
                    {
                        FindMentions(payload, candidates, found);
                    }
                }

                var isOwnedLeafRow = reader.GetString(1) == LeafState && string.IsNullOrEmpty(extension);
                if (isOwnedLeafRow)
                {
                    // A leaf row's mentions keep their targets live only if the leaf
                    // itself is live, so a stranded chain does not keep itself alive.
                    var self = GuidFromKeyColumns(reader.GetInt64(2), reader.GetInt64(3));
                    found.Remove(self);
                    if (found.Count > 0)
                    {
                        leafMentions[new OwnerKey(service, self)] = [.. found];
                    }

                    continue;
                }

                foreach (var leaf in found)
                {
                    var key = new OwnerKey(service, leaf);
                    if (live.Add(key))
                    {
                        pending.Enqueue(key);
                    }
                }
            }
        }

        while (pending.TryDequeue(out var key))
        {
            if (!leafMentions.TryGetValue(key, out var mentions))
            {
                continue;
            }

            foreach (var leaf in mentions)
            {
                var next = new OwnerKey(key.ServiceId, leaf);
                if (live.Add(next))
                {
                    pending.Enqueue(next);
                }
            }
        }

        return live;
    }

    /// <summary>
    /// Adds to <paramref name="found"/> every candidate Guid <paramref name="data"/>
    /// carries as 32 or 36 hex characters (either case) or as 16 raw bytes in either
    /// byte order.
    /// </summary>
    internal static void FindMentions(ReadOnlySpan<byte> data, HashSet<Guid> candidates, HashSet<Guid> found)
    {
        for (var i = 0; i + 16 <= data.Length; i++)
        {
            var window = data.Slice(i, 16);
            var mixed = new Guid(window);
            if (candidates.Contains(mixed))
            {
                found.Add(mixed);
            }

            var bigEndian = new Guid(window, bigEndian: true);
            if (candidates.Contains(bigEndian))
            {
                found.Add(bigEndian);
            }
        }

        Span<char> text = stackalloc char[36];
        for (var i = 0; i + 32 <= data.Length; i++)
        {
            if (!IsHex(data[i]))
            {
                continue;
            }

            if (TryReadGuid(data.Slice(i), 32, text, out var compact) && candidates.Contains(compact))
            {
                found.Add(compact);
            }

            if (i + 36 <= data.Length
                && TryReadGuid(data.Slice(i), 36, text, out var hyphenated)
                && candidates.Contains(hyphenated))
            {
                found.Add(hyphenated);
            }
        }
    }

    /// <summary>
    /// Finds the byte range of the top-level <c>LeafAccessModel</c> property's value in a
    /// JSON-encoded shard root. Returns <see langword="false"/> - so the whole payload is
    /// scanned - for a binary payload, malformed JSON, or JSON without the property.
    /// </summary>
    internal static bool TryFindAccessStatistics(byte[] payload, out int start, out int end)
    {
        start = end = 0;
        if (payload.Length == 0 || payload[0] != (byte)'{')
        {
            return false;
        }

        try
        {
            var reader = new System.Text.Json.Utf8JsonReader(payload);
            while (reader.Read())
            {
                if (reader.TokenType == System.Text.Json.JsonTokenType.PropertyName
                    && reader.CurrentDepth == 1
                    && reader.ValueTextEquals(AccessStatisticsProperty))
                {
                    if (!reader.Read())
                    {
                        return false;
                    }

                    start = (int)reader.TokenStartIndex;
                    reader.Skip();
                    end = (int)reader.BytesConsumed;
                    return true;
                }
            }
        }
        catch (System.Text.Json.JsonException)
        {
        }

        return false;
    }

    private static bool TryReadGuid(ReadOnlySpan<byte> data, int length, Span<char> buffer, out Guid value)
    {
        value = default;
        if (data.Length < length)
        {
            return false;
        }

        var chars = buffer[..length];
        for (var j = 0; j < length; j++)
        {
            var b = data[j];
            if (b > 0x7F)
            {
                return false;
            }

            chars[j] = (char)b;
        }

        return Guid.TryParseExact(chars, length == 32 ? "N" : "D", out value);
    }

    private static bool IsHex(byte b) =>
        (b >= (byte)'0' && b <= (byte)'9') || (b >= (byte)'a' && b <= (byte)'f') || (b >= (byte)'A' && b <= (byte)'F');

    /// <summary>
    /// Parses a segment key, <c>{guid}/{index}</c> or <c>{guid}/g{generation}/{index}</c>,
    /// to the leaf Guid that owns it.
    /// </summary>
    internal static bool TryParseSegmentOwner(string? key, out Guid leaf)
    {
        leaf = default;
        if (key is null)
        {
            return false;
        }

        var parts = key.Split('/');
        var shapeOk = parts.Length switch
        {
            2 => IsDigits(parts[1]),
            3 => parts[1].Length > 1 && parts[1][0] == 'g' && IsDigits(parts[1][1..]) && IsDigits(parts[2]),
            _ => false,
        };

        return shapeOk && parts[0].Length == 32 && Guid.TryParseExact(parts[0], "N", out leaf);
    }

    private static bool IsDigits(string s) => s.Length > 0 && s.All(char.IsAsciiDigit);

    /// <summary>
    /// The leaf Guid behind a Guid-keyed row's <c>GrainIdN0</c> / <c>GrainIdN1</c>
    /// columns, which hold the Guid's bytes as two little-endian 64-bit halves.
    /// </summary>
    internal static Guid GuidFromKeyColumns(long n0, long n1)
    {
        Span<byte> bytes = stackalloc byte[16];
        BitConverter.TryWriteBytes(bytes, n0);
        BitConverter.TryWriteBytes(bytes[8..], n1);
        return new Guid(bytes);
    }

    private static int Delete(SqliteConnection connection, List<long> rowIds, int batchSize)
    {
        var deleted = 0;
        for (var start = 0; start < rowIds.Count; start += batchSize)
        {
            var batch = rowIds.Skip(start).Take(batchSize);
            using var transaction = connection.BeginTransaction();
            using var command = connection.CreateCommand();
            command.Transaction = transaction;

            // Restricted to the three owned grain types as well as the row ids, so a
            // row id can never reach anything else.
            command.CommandText =
                $"DELETE FROM OrleansStorage WHERE rowid IN ({string.Join(',', batch)}) "
                + "AND GrainTypeString IN ($leaf, $manifest, $segment);";
            command.Parameters.AddWithValue("$leaf", LeafState);
            command.Parameters.AddWithValue("$manifest", ManifestState);
            command.Parameters.AddWithValue("$segment", SegmentState);
            deleted += command.ExecuteNonQuery();
            transaction.Commit();
        }

        return deleted;
    }

    /// <summary>
    /// Returns the deleted rows' pages to the filesystem now rather than leaving them
    /// to the paced background reclaimer, and folds the write-ahead log back into the
    /// file. In <c>auto_vacuum=FULL</c> the commits already did so; in <c>NONE</c> the
    /// pages stay on SQLite's freelist for reuse until a <c>VACUUM</c>.
    /// </summary>
    private static void ReturnFreedPages(SqliteConnection connection)
    {
        long autoVacuum;
        using (var mode = connection.CreateCommand())
        {
            mode.CommandText = "PRAGMA auto_vacuum;";
            autoVacuum = Convert.ToInt64(mode.ExecuteScalar(), System.Globalization.CultureInfo.InvariantCulture);
        }

        if (autoVacuum == 2)
        {
            using var vacuum = connection.CreateCommand();
            vacuum.CommandText = "PRAGMA incremental_vacuum;";
            using var reader = vacuum.ExecuteReader();
            while (reader.Read())
            {
            }
        }

        Checkpoint(connection);
    }

    private static void Checkpoint(SqliteConnection connection)
    {
        using var checkpoint = connection.CreateCommand();
        checkpoint.CommandText = "PRAGMA wal_checkpoint(TRUNCATE);";
        using var reader = checkpoint.ExecuteReader();
        while (reader.Read())
        {
        }
    }

    private static long FileSize(string path) => File.Exists(path) ? new FileInfo(path).Length : 0;

    private readonly record struct OwnerKey(string ServiceId, Guid Leaf);

    private sealed class Owner
    {
        public List<long> RowIds { get; } = [];

        public int LeafRows { get; set; }

        public int ManifestRows { get; set; }

        public int SegmentRows { get; set; }

        public long PayloadBytes { get; set; }
    }
}
