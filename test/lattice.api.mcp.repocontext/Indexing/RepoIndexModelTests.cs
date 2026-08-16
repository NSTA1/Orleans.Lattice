namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Indexing;

/// <summary>
/// Construction and projection tests for the asynchronous-indexing model types
/// (<see cref="RepoIndexStatus"/>, <see cref="RepoIndexPhase"/>,
/// <see cref="RepoIndexJobRequest"/>, <see cref="RepoIndexProgress"/>,
/// <see cref="RepoIndexProgressUpdate"/>, and the internal
/// <see cref="RepoIndexJobState"/>). They pin the wire-visible defaults, the
/// verbatim member carriage the SDK projects, and the state-to-snapshot
/// projection the <c>repocontext_index_status</c> tool depends on.
/// </summary>
[TestFixture]
public sealed class RepoIndexModelTests
{
    [Test]
    public void Status_ordinals_are_stable()
    {
        Assert.Multiple(() =>
        {
            Assert.That((int)RepoIndexStatus.None, Is.EqualTo(0));
            Assert.That((int)RepoIndexStatus.Running, Is.EqualTo(1));
            Assert.That((int)RepoIndexStatus.Completed, Is.EqualTo(2));
            Assert.That((int)RepoIndexStatus.Failed, Is.EqualTo(3));
        });
    }

    [Test]
    public void Phase_ordinals_are_stable()
    {
        Assert.Multiple(() =>
        {
            Assert.That((int)RepoIndexPhase.Pending, Is.EqualTo(0));
            Assert.That((int)RepoIndexPhase.Walking, Is.EqualTo(1));
            Assert.That((int)RepoIndexPhase.Reconciling, Is.EqualTo(2));
            Assert.That((int)RepoIndexPhase.Applying, Is.EqualTo(3));
            Assert.That((int)RepoIndexPhase.Vectorising, Is.EqualTo(4));
            Assert.That((int)RepoIndexPhase.Done, Is.EqualTo(5));
        });
    }

    [Test]
    public void JobRequest_carries_its_members()
    {
        var request = new RepoIndexJobRequest
        {
            RepoRoot = "/work/acme",
            RepoId = "acme",
            IncludeGlobs = new[] { "**/*.cs" },
            ExcludeGlobs = new[] { "**/bin/**" },
        };

        Assert.Multiple(() =>
        {
            Assert.That(request.RepoRoot, Is.EqualTo("/work/acme"));
            Assert.That(request.RepoId, Is.EqualTo("acme"));
            Assert.That(request.IncludeGlobs, Is.EqualTo(new[] { "**/*.cs" }));
            Assert.That(request.ExcludeGlobs, Is.EqualTo(new[] { "**/bin/**" }));
        });
    }

    [Test]
    public void JobRequest_globs_default_to_null()
    {
        var request = new RepoIndexJobRequest { RepoRoot = "/work/acme", RepoId = "acme" };

        Assert.Multiple(() =>
        {
            Assert.That(request.IncludeGlobs, Is.Null);
            Assert.That(request.ExcludeGlobs, Is.Null);
        });
    }

    [Test]
    public void ProgressUpdate_is_empty_by_default()
    {
        var update = default(RepoIndexProgressUpdate);

        Assert.Multiple(() =>
        {
            Assert.That(update.Phase, Is.Null);
            Assert.That(update.FilesScanned, Is.Null);
            Assert.That(update.FilesAdded, Is.Null);
            Assert.That(update.FilesUpdated, Is.Null);
            Assert.That(update.FilesRemoved, Is.Null);
            Assert.That(update.FilesUnchanged, Is.Null);
            Assert.That(update.ChunksTotal, Is.Null);
            Assert.That(update.ChunksCommitted, Is.Null);
            Assert.That(update.FilesEmbedded, Is.Null);
            Assert.That(update.FilesContentProjected, Is.Null);
        });
    }

    [Test]
    public void ProgressUpdate_carries_the_fields_it_sets()
    {
        var update = new RepoIndexProgressUpdate
        {
            Phase = RepoIndexPhase.Applying,
            ChunksTotal = 4,
            ChunksCommitted = 2,
            FilesContentProjected = 12,
        };

        Assert.Multiple(() =>
        {
            Assert.That(update.Phase, Is.EqualTo(RepoIndexPhase.Applying));
            Assert.That(update.ChunksTotal, Is.EqualTo(4));
            Assert.That(update.ChunksCommitted, Is.EqualTo(2));
            Assert.That(update.FilesContentProjected, Is.EqualTo(12));
            Assert.That(update.FilesAdded, Is.Null);
        });
    }

    [Test]
    public void JobState_defaults_to_a_never_started_snapshot()
    {
        var progress = new RepoIndexJobState().ToProgress("acme");

        Assert.Multiple(() =>
        {
            Assert.That(progress.RepoId, Is.EqualTo("acme"));
            Assert.That(progress.Status, Is.EqualTo(RepoIndexStatus.None));
            Assert.That(progress.Phase, Is.EqualTo(RepoIndexPhase.Pending));
            Assert.That(progress.Attempt, Is.EqualTo(0));
            Assert.That(progress.StartedAt, Is.Null);
            Assert.That(progress.CompletedAt, Is.Null);
            Assert.That(progress.ElapsedMilliseconds, Is.Null);
            Assert.That(progress.Error, Is.Null);
        });
    }

    [Test]
    public void JobState_projects_every_field_into_the_snapshot()
    {
        var started = DateTimeOffset.UnixEpoch;
        var updated = started.AddSeconds(1);
        var completed = started.AddSeconds(2);
        var state = new RepoIndexJobState
        {
            Request = new RepoIndexJobRequest { RepoRoot = "/work/acme", RepoId = "acme" },
            Status = RepoIndexStatus.Completed,
            Phase = RepoIndexPhase.Done,
            FilesScanned = 10,
            FilesAdded = 4,
            FilesUpdated = 3,
            FilesRemoved = 1,
            FilesUnchanged = 2,
            ChunksTotal = 5,
            ChunksCommitted = 5,
            FilesEmbedded = 7,
            FilesContentProjected = 9,
            Attempt = 2,
            StartedAt = started,
            UpdatedAt = updated,
            CompletedAt = completed,
            ElapsedMilliseconds = 2000,
            Error = null,
        };

        var progress = state.ToProgress("acme");

        Assert.Multiple(() =>
        {
            Assert.That(progress.RepoId, Is.EqualTo("acme"));
            Assert.That(progress.Status, Is.EqualTo(RepoIndexStatus.Completed));
            Assert.That(progress.Phase, Is.EqualTo(RepoIndexPhase.Done));
            Assert.That(progress.FilesScanned, Is.EqualTo(10));
            Assert.That(progress.FilesAdded, Is.EqualTo(4));
            Assert.That(progress.FilesUpdated, Is.EqualTo(3));
            Assert.That(progress.FilesRemoved, Is.EqualTo(1));
            Assert.That(progress.FilesUnchanged, Is.EqualTo(2));
            Assert.That(progress.ChunksTotal, Is.EqualTo(5));
            Assert.That(progress.ChunksCommitted, Is.EqualTo(5));
            Assert.That(progress.FilesEmbedded, Is.EqualTo(7));
            Assert.That(progress.FilesContentProjected, Is.EqualTo(9));
            Assert.That(progress.Attempt, Is.EqualTo(2));
            Assert.That(progress.StartedAt, Is.EqualTo(started));
            Assert.That(progress.UpdatedAt, Is.EqualTo(updated));
            Assert.That(progress.CompletedAt, Is.EqualTo(completed));
            Assert.That(progress.ElapsedMilliseconds, Is.EqualTo(2000));
            Assert.That(progress.Error, Is.Null);
        });
    }
}
