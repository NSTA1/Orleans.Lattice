using System.Buffers;
using LibGit2Sharp;
using GitBlob = LibGit2Sharp.Blob;
using GitCommit = LibGit2Sharp.Commit;
using GitRepository = LibGit2Sharp.Repository;
using GitTree = LibGit2Sharp.Tree;

namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// The shipped git transport, backed by LibGit2Sharp: a managed implementation that
/// needs no external <c>git</c> binary on the host, supports shallow and incremental
/// fetch, and exposes the resolved commit SHA that anchors an index generation.
/// <para>
/// The staging work tree is a pure cache. Nothing about "what is currently indexed"
/// is stored there - that lives in the index itself - so a deleted or corrupted
/// staging directory costs one full re-fetch and never costs correctness: the
/// changeset is always recomputed by diffing the commit tree against the stored
/// per-file digests.
/// </para>
/// </summary>
internal sealed class LibGit2SharpRepoContextGitFetcher : IRepoContextGitFetcher
{
    /// <summary>The remote name the staging clone tracks.</summary>
    private const string RemoteName = "origin";

    // The leading window scanned for a NUL byte to classify a blob as binary. It
    // matches RepoTreeWalker's window (and Git's own FIRST_FEW_BYTES) so a file is
    // classified identically whichever source indexed it.
    private const int BinarySniffByteCount = 8000;

    /// <inheritdoc />
    public RepoContextGitFetchResult Fetch(RepoContextGitFetchRequest request, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(request);
        cancellationToken.ThrowIfCancellationRequested();

        var source = request.Source;
        var credential = request.Credential;

        try
        {
            Directory.CreateDirectory(request.WorkTreePath);
            if (!GitRepository.IsValid(request.WorkTreePath))
            {
                GitRepository.Init(request.WorkTreePath);
            }

            using var repo = new GitRepository(request.WorkTreePath);
            EnsureRemote(repo, source.RemoteUrl);

            var previousSha = repo.Head.Tip?.Sha;
            cancellationToken.ThrowIfCancellationRequested();

            var fetchOptions = new FetchOptions
            {
                Prune = true,
                CredentialsProvider = (_, _, _) => credential.IsAnonymous
                    ? new DefaultCredentials()
                    : new UsernamePasswordCredentials
                    {
                        Username = credential.Username,
                        Password = credential.Secret,
                    },
            };

            if (source.Depth > 0)
            {
                fetchOptions.Depth = source.Depth;
            }

            if (RepoContextGitReference.IsTag(source.Reference))
            {
                fetchOptions.TagFetchMode = TagFetchMode.All;
            }

            Commands.Fetch(
                repo,
                RemoteName,
                [RepoContextGitReference.RefSpec(source.Reference)],
                fetchOptions,
                logMessage: null);

            cancellationToken.ThrowIfCancellationRequested();

            var target = ResolveCommit(repo, source.Reference)
                ?? throw new RepoContextGitSourceException(
                    "The configured ref '" + source.Reference + "' did not resolve to a commit after fetch.");

            // The ref has not moved since the last successfully indexed generation:
            // leave the work tree untouched so a no-op refresh costs nothing beyond
            // the fetch itself.
            if (string.Equals(target.Sha, request.LastIndexedCommitSha, StringComparison.OrdinalIgnoreCase))
            {
                return new RepoContextGitFetchResult
                {
                    CommitSha = target.Sha,
                    PreviousCommitSha = previousSha,
                    CheckedOut = false,
                };
            }

            var (diffAvailable, added, modified, deleted) = SummariseChanges(repo, previousSha, target);

            Commands.Checkout(repo, target, new CheckoutOptions
            {
                CheckoutModifiers = CheckoutModifiers.Force,
            });

            return new RepoContextGitFetchResult
            {
                CommitSha = target.Sha,
                PreviousCommitSha = previousSha,
                CheckedOut = true,
                DiffAvailable = diffAvailable,
                Added = added,
                Modified = modified,
                Deleted = deleted,
            };
        }
        catch (RepoContextGitSourceException)
        {
            throw;
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex)
        {
            // The transport quotes the remote URL (and therefore any userinfo it
            // carries) in several of its messages, so every escaping message is
            // redacted against the credential and against URL userinfo.
            throw new RepoContextGitSourceException(
                RepoContextSecretRedactor.Redact(ex.Message, credential), ex);
        }
    }

    /// <inheritdoc />
    public IReadOnlyList<RepoFileEntry> ScanCommit(
        string workTreePath,
        string commitSha,
        IReadOnlyList<string>? includeGlobs,
        IReadOnlyList<string>? excludeGlobs,
        bool excludeBinary,
        CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(workTreePath);
        ArgumentException.ThrowIfNullOrWhiteSpace(commitSha);

        try
        {
            using var repo = new GitRepository(workTreePath);
            var commit = repo.Lookup<GitCommit>(commitSha)
                ?? throw new RepoContextGitSourceException(
                    "Commit '" + commitSha + "' is not present in the staging object database.");

            var includes = CompileGlobs(includeGlobs);
            var excludes = CompileGlobs(excludeGlobs);

            var entries = new List<RepoFileEntry>();
            CollectTree(commit.Tree, prefix: string.Empty, includes, excludes, excludeBinary, entries, cancellationToken);
            return entries;
        }
        catch (RepoContextGitSourceException)
        {
            throw;
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex)
        {
            throw new RepoContextGitSourceException(
                RepoContextSecretRedactor.Redact(ex.Message, credential: null), ex);
        }
    }

    private static void EnsureRemote(GitRepository repo, string remoteUrl)
    {
        var existing = repo.Network.Remotes[RemoteName];
        if (existing is null)
        {
            repo.Network.Remotes.Add(RemoteName, remoteUrl);
            return;
        }

        if (!string.Equals(existing.Url, remoteUrl, StringComparison.Ordinal))
        {
            // Re-pointing an existing staging tree at a new configured remote keeps
            // configuration the declared truth: the operator edits the URL and the
            // next refresh follows it, rather than silently serving the old remote.
            repo.Network.Remotes.Update(RemoteName, r => r.Url = remoteUrl);
        }
    }

    private static GitCommit? ResolveCommit(GitRepository repo, string reference)
    {
        var localRef = RepoContextGitReference.LocalTrackingRef(reference);
        var resolved = repo.Lookup<GitObject>(localRef);
        return resolved switch
        {
            GitCommit commit => commit,
            TagAnnotation tag => tag.Target as GitCommit,
            _ => null,
        };
    }

    private static (bool Available, int Added, int Modified, int Deleted) SummariseChanges(
        GitRepository repo, string? previousSha, GitCommit target)
    {
        if (string.IsNullOrEmpty(previousSha))
        {
            return (false, 0, 0, 0);
        }

        // A shallow fetch legitimately prunes the previous commit from the object
        // database, so an unresolvable previous commit is expected rather than an
        // error: the summary is diagnostic and the authoritative changeset comes
        // from the digest diff downstream.
        var previous = repo.Lookup<GitCommit>(previousSha);
        if (previous is null)
        {
            return (false, 0, 0, 0);
        }

        var changes = repo.Diff.Compare<TreeChanges>(previous.Tree, target.Tree);
        return (true, changes.Added.Count(), changes.Modified.Count(), changes.Deleted.Count());
    }

    private static GlobMatcher[] CompileGlobs(IReadOnlyList<string>? globs)
    {
        if (globs is not { Count: > 0 })
        {
            return [];
        }

        var compiled = new List<GlobMatcher>(globs.Count);
        foreach (var glob in globs)
        {
            if (!string.IsNullOrWhiteSpace(glob))
            {
                compiled.Add(GlobMatcher.Compile(glob));
            }
        }

        return [.. compiled];
    }

    private static void CollectTree(
        GitTree tree,
        string prefix,
        GlobMatcher[] includes,
        GlobMatcher[] excludes,
        bool excludeBinary,
        List<RepoFileEntry> sink,
        CancellationToken cancellationToken)
    {
        foreach (var entry in tree)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var path = prefix.Length == 0 ? entry.Name : prefix + "/" + entry.Name;
            switch (entry.TargetType)
            {
                case TreeEntryTargetType.Tree:
                    CollectTree((GitTree)entry.Target, path, includes, excludes, excludeBinary, sink, cancellationToken);
                    break;

                case TreeEntryTargetType.Blob:
                    if (Included(path, includes, excludes)
                        && ReadEntry(path, (GitBlob)entry.Target, excludeBinary) is { } fileEntry)
                    {
                        sink.Add(fileEntry);
                    }

                    break;

                default:
                    // A submodule (GitLink) has no content in this repository's object
                    // database, so it is not indexable content and is skipped, exactly
                    // as the mounted walk skips a nested .git directory.
                    break;
            }
        }
    }

    private static bool Included(string path, GlobMatcher[] includes, GlobMatcher[] excludes)
    {
        foreach (var exclude in excludes)
        {
            if (exclude.IsMatch(path))
            {
                return false;
            }
        }

        if (includes.Length == 0)
        {
            return true;
        }

        foreach (var include in includes)
        {
            if (include.IsMatch(path))
            {
                return true;
            }
        }

        return false;
    }

    private static RepoFileEntry? ReadEntry(string path, GitBlob blob, bool excludeBinary)
    {
        var size = blob.Size;
        if (size > int.MaxValue)
        {
            return null;
        }

        var length = (int)size;

        // The blob is rented rather than allocated: a cold scan of a large repository
        // reads every file exactly once, and a per-file array would push most of a
        // repository's byte volume through the large object heap.
        var buffer = ArrayPool<byte>.Shared.Rent(Math.Max(length, 1));
        try
        {
            using (var stream = blob.GetContentStream())
            {
                stream.ReadExactly(buffer, 0, length);
            }

            var content = buffer.AsSpan(0, length);
            if (excludeBinary && LooksBinary(content))
            {
                return null;
            }

            return new RepoFileEntry(
                path,
                FileDigest.Compute(content),
                length,
                LanguageClassifier.Classify(path));
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    private static bool LooksBinary(ReadOnlySpan<byte> content)
    {
        var window = content.Length <= BinarySniffByteCount ? content : content[..BinarySniffByteCount];
        return window.IndexOf((byte)0) >= 0;
    }
}
