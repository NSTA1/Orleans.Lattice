namespace Orleans.Lattice.Storage.File;

/// <summary>
/// The production <see cref="IFileWalFileSystem"/>: opens real
/// <see cref="FileStream"/> instances and replaces the segment file with an
/// overwriting move. Stateless, so a single instance serves every shard.
/// </summary>
internal sealed class PhysicalFileWalFileSystem : IFileWalFileSystem
{
    /// <summary>The shared instance.</summary>
    public static readonly PhysicalFileWalFileSystem Instance = new();

    private PhysicalFileWalFileSystem()
    {
    }

    /// <inheritdoc />
    public FileStream OpenLog(string path) =>
        new(path, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None);

    /// <inheritdoc />
    public FileStream CreateCompactionTarget(string path) =>
        new(path, FileMode.Create, FileAccess.Write, FileShare.None);

    /// <inheritdoc />
    public void ReplaceLog(string compactedPath, string logPath) =>
        System.IO.File.Move(compactedPath, logPath, overwrite: true);
}
