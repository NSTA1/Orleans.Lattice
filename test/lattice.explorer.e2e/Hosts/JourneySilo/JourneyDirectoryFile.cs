using Orleans.Lattice.Explorer.EndToEnd.Hosting;

namespace Orleans.Lattice.Explorer.EndToEnd.Hosts.Silo;

/// <summary>
/// The harness's identity directory as this silo last read it from
/// <see cref="JourneyHostEnvironment.DirectoryFile"/>, re-read whenever the file's write
/// time moves, so a user added to a running estate can sign in at once.
/// </summary>
internal sealed class JourneyDirectoryFile
{
    private readonly string _path;
    private readonly Lock _gate = new();
    private DateTime _readAt;
    private JourneyDirectoryDocument _document;

    /// <summary>Opens the directory at <paramref name="path"/>.</summary>
    /// <param name="path">The directory file.</param>
    public JourneyDirectoryFile(string path)
    {
        _path = path;
        _readAt = File.GetLastWriteTimeUtc(path);
        _document = JourneyDirectoryDocument.Read(path);
    }

    /// <summary>Reads the directory named by the environment, or fails the host start.</summary>
    public static JourneyDirectoryFile FromEnvironment()
    {
        var path = Environment.GetEnvironmentVariable(JourneyHostEnvironment.DirectoryFile);
        if (string.IsNullOrWhiteSpace(path) || !File.Exists(path))
        {
            throw new InvalidOperationException(
                $"The journey silo needs its identity directory: set {JourneyHostEnvironment.DirectoryFile} to an existing file.");
        }

        return new JourneyDirectoryFile(path);
    }

    /// <summary>The directory as it now stands.</summary>
    public JourneyDirectoryDocument Current
    {
        get
        {
            var written = File.GetLastWriteTimeUtc(_path);
            if (written == _readAt)
            {
                return _document;
            }

            lock (_gate)
            {
                if (written != _readAt)
                {
                    try
                    {
                        _document = JourneyDirectoryDocument.Read(_path);
                        _readAt = written;
                    }
                    catch (IOException)
                    {
                        // A write in progress: keep the last complete directory and read again next time.
                    }
                }

                return _document;
            }
        }
    }
}
