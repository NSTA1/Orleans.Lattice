namespace Orleans.Lattice.Storage.File;

/// <summary>
/// The file-system operations <see cref="FileWalShard"/> performs to open,
/// rewrite, and replace its segment file.
/// <para>
/// This exists so a test can observe <i>when</i> the shard flushes to disk
/// relative to acknowledging an operation (issue #3462). Every durability test
/// that only reopens a shard in the same process passes identically with
/// <see cref="FileWalStorageOptions.FlushToDisk"/> off, because unsynced bytes
/// are still in the OS page cache, so round-trip survival cannot pin the
/// fsync-before-ack guarantee. A recording <see cref="FileStream"/> returned
/// from this seam can. Production always uses
/// <see cref="PhysicalFileWalFileSystem.Instance"/>, which behaves exactly as
/// the direct <see cref="FileStream"/> and <see cref="System.IO.File.Move(string, string, bool)"/>
/// calls it replaced.
/// </para>
/// </summary>
internal interface IFileWalFileSystem
{
    /// <summary>
    /// Opens (creating if absent) the shard's live segment file for exclusive
    /// read/write access.
    /// </summary>
    /// <param name="path">Full path of the segment file.</param>
    /// <returns>An open stream the shard owns and disposes.</returns>
    FileStream OpenLog(string path);

    /// <summary>
    /// Creates (truncating if present) the temporary file a compaction rewrites
    /// the retained entries into, for exclusive write access.
    /// </summary>
    /// <param name="path">Full path of the temporary file.</param>
    /// <returns>An open stream the shard owns and disposes.</returns>
    FileStream CreateCompactionTarget(string path);

    /// <summary>
    /// Replaces the live segment file with the fully-written compaction target.
    /// </summary>
    /// <param name="compactedPath">Path of the compaction target to move.</param>
    /// <param name="logPath">Path of the live segment file it replaces.</param>
    void ReplaceLog(string compactedPath, string logPath);
}
