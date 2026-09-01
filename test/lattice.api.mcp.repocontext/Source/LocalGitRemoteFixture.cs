using LibGit2Sharp;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Source;

/// <summary>
/// An offline git remote for the source-strategy tests: a real repository created
/// in a temporary directory and fetched from over a local path, so every test runs
/// without network access. The fixture owns both the origin repository and the
/// staging root the source stages work trees under, and deletes both on dispose.
/// </summary>
internal sealed class LocalGitRemoteFixture : IDisposable
{
    private readonly string _root;

    private LocalGitRemoteFixture(string root, string originPath, string stagingRoot)
    {
        _root = root;
        OriginPath = originPath;
        StagingRoot = stagingRoot;
    }

    /// <summary>The local path the source fetches from, used as the remote url.</summary>
    public string OriginPath { get; }

    /// <summary>The staging root a source under test creates work trees under.</summary>
    public string StagingRoot { get; }

    /// <summary>The fully qualified ref name of the origin's default branch.</summary>
    public string BranchRef { get; private set; } = RepoContextGitReference.DefaultReference;

    /// <summary>
    /// Creates an origin repository with one initial commit containing
    /// <paramref name="files"/>.
    /// </summary>
    /// <param name="files">The initial file set, keyed by repository-relative path.</param>
    /// <returns>The initialised fixture.</returns>
    public static LocalGitRemoteFixture Create(IReadOnlyDictionary<string, string> files)
    {
        ArgumentNullException.ThrowIfNull(files);

        var root = Path.Combine(
            Path.GetTempPath(), "lattice-git-src-" + Guid.NewGuid().ToString("N")[..12]);
        var originPath = Path.Combine(root, "origin");
        var stagingRoot = Path.Combine(root, "staging");
        Directory.CreateDirectory(originPath);
        Directory.CreateDirectory(stagingRoot);

        Repository.Init(originPath);
        var fixture = new LocalGitRemoteFixture(root, originPath, stagingRoot);
        fixture.Commit("initial", files, deletions: null);

        using var repo = new Repository(originPath);

        // Never assume the default branch name: LibGit2Sharp's init default differs
        // between environments, so the fixture reports whatever it actually created.
        fixture.BranchRef = repo.Head.CanonicalName;
        return fixture;
    }

    /// <summary>
    /// Applies a change set to the origin and commits it.
    /// </summary>
    /// <param name="message">The commit message.</param>
    /// <param name="writes">Files to create or overwrite, keyed by repository-relative path.</param>
    /// <param name="deletions">Repository-relative paths to delete.</param>
    /// <returns>The new commit's SHA.</returns>
    public string Commit(
        string message,
        IReadOnlyDictionary<string, string>? writes,
        IReadOnlyList<string>? deletions)
    {
        if (writes is not null)
        {
            foreach (var (path, content) in writes)
            {
                var full = Path.Combine(OriginPath, path.Replace('/', Path.DirectorySeparatorChar));
                Directory.CreateDirectory(Path.GetDirectoryName(full)!);
                File.WriteAllText(full, content);
            }
        }

        if (deletions is not null)
        {
            foreach (var path in deletions)
            {
                var full = Path.Combine(OriginPath, path.Replace('/', Path.DirectorySeparatorChar));
                if (File.Exists(full))
                {
                    File.Delete(full);
                }
            }
        }

        using var repo = new Repository(OriginPath);
        Commands.Stage(repo, "*");
        var signature = new Signature("Lattice Test", "test@example.invalid", DateTimeOffset.UtcNow);
        return repo.Commit(message, signature, signature).Sha;
    }

    /// <summary>The SHA the origin's default branch currently points at.</summary>
    /// <returns>The head commit SHA.</returns>
    public string HeadSha()
    {
        using var repo = new Repository(OriginPath);
        return repo.Head.Tip!.Sha;
    }

    /// <summary>
    /// A git source pointing at this fixture's origin, with a short fetch timeout so
    /// a wedged transport cannot hang a test run.
    /// </summary>
    /// <param name="repoId">The repository identity the source is declared under.</param>
    /// <param name="remoteUrl">An override remote url, for the unreachable-remote tests.</param>
    /// <returns>The configured options.</returns>
    public RepoContextGitSourceOptions Source(string repoId, string? remoteUrl = null) => new()
    {
        RepoId = repoId,
        RemoteUrl = remoteUrl ?? OriginPath,
        Reference = BranchRef,
        Depth = 0,
        FetchTimeout = TimeSpan.FromSeconds(60),
    };

    /// <summary>A registry declaring <paramref name="sources"/> against this fixture's staging root.</summary>
    /// <param name="sources">The git sources to declare.</param>
    /// <returns>The registry.</returns>
    public RepoContextGitSourceRegistry Registry(params RepoContextGitSourceOptions[] sources) =>
        new(sources, StagingRoot);

    /// <inheritdoc />
    public void Dispose() => ForceDelete(_root);

    /// <summary>
    /// Deletes a directory tree containing a git repository. Git marks pack and object
    /// files read-only, which a plain recursive delete refuses on Windows.
    /// </summary>
    /// <param name="path">The directory to delete.</param>
    internal static void ForceDelete(string path)
    {
        if (!Directory.Exists(path))
        {
            return;
        }

        foreach (var file in Directory.EnumerateFiles(path, "*", SearchOption.AllDirectories))
        {
            try
            {
                File.SetAttributes(file, FileAttributes.Normal);
            }
            catch (IOException)
            {
                // Best effort: the delete below reports anything still locked.
            }
            catch (UnauthorizedAccessException)
            {
            }
        }

        try
        {
            Directory.Delete(path, recursive: true);
        }
        catch (IOException)
        {
            // A temp tree left behind never fails a test; the OS reclaims it.
        }
        catch (UnauthorizedAccessException)
        {
        }
    }
}
