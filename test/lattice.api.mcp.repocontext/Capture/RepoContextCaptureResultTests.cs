namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Capture;

/// <summary>
/// Construction tests for the capture and maintenance tool result DTOs
/// (<see cref="RepoContextEntryView"/>, <see cref="RepoContextScanResult"/>,
/// <see cref="RepoContextTopicSummary"/>, <see cref="RepoContextTopicsResult"/>,
/// <see cref="RepoContextRememberResult"/>, <see cref="RepoContextUpdateResult"/>,
/// and <see cref="RepoContextForgetResult"/>): each carries its required members
/// verbatim so the SDK projects a stable, agent-readable payload.
/// </summary>
[TestFixture]
public sealed class RepoContextCaptureResultTests
{
    [Test]
    public void EntryView_carries_its_members()
    {
        var view = new RepoContextEntryView
        {
            Key = "repo/acme/mem/notes/1",
            Exists = true,
            Kind = "Memory",
            RepoId = "acme",
            Topic = "notes",
            Id = "1",
            Fields = new Dictionary<string, string> { ["title"] = "t" },
            Tags = new[] { "x" },
            Links = new Dictionary<string, IReadOnlyList<string>>(),
            Expires = false,
            ExpiresAtUtc = null,
            RemainingSeconds = null,
            HasExpired = false,
        };

        Assert.Multiple(() =>
        {
            Assert.That(view.Key, Is.EqualTo("repo/acme/mem/notes/1"));
            Assert.That(view.Fields["title"], Is.EqualTo("t"));
            Assert.That(view.Tags, Is.EqualTo(new[] { "x" }));
        });
    }

    [Test]
    public void ScanResult_carries_entries_and_token()
    {
        var result = new RepoContextScanResult
        {
            Entries = Array.Empty<RepoContextEntryView>(),
            ContinuationToken = "next",
            HasMore = true,
        };

        Assert.Multiple(() =>
        {
            Assert.That(result.Entries, Is.Empty);
            Assert.That(result.ContinuationToken, Is.EqualTo("next"));
            Assert.That(result.HasMore, Is.True);
        });
    }

    [Test]
    public void TopicsResult_carries_summaries()
    {
        var result = new RepoContextTopicsResult
        {
            RepoId = "acme",
            Topics = new[] { new RepoContextTopicSummary { Topic = "notes", EntryCount = 3 } },
        };

        Assert.Multiple(() =>
        {
            Assert.That(result.RepoId, Is.EqualTo("acme"));
            Assert.That(result.Topics[0].Topic, Is.EqualTo("notes"));
            Assert.That(result.Topics[0].EntryCount, Is.EqualTo(3));
        });
    }

    [Test]
    public void RememberResult_carries_its_members()
    {
        var result = new RepoContextRememberResult
        {
            Key = "repo/acme/mem/notes/1",
            RepoId = "acme",
            Topic = "notes",
            Id = "1",
            Created = true,
            Expires = true,
            ExpiresAtUtc = "2027-01-01T00:00:00.0000000Z",
        };

        Assert.Multiple(() =>
        {
            Assert.That(result.Created, Is.True);
            Assert.That(result.ExpiresAtUtc, Is.EqualTo("2027-01-01T00:00:00.0000000Z"));
        });
    }

    [Test]
    public void UpdateResult_carries_its_counts()
    {
        var result = new RepoContextUpdateResult
        {
            Key = "repo/acme/file/a.cs",
            Kind = "File",
            FieldsUpdated = 2,
            TagsAdded = 1,
            TagsRemoved = 0,
        };

        Assert.Multiple(() =>
        {
            Assert.That(result.FieldsUpdated, Is.EqualTo(2));
            Assert.That(result.TagsAdded, Is.EqualTo(1));
            Assert.That(result.TagsRemoved, Is.EqualTo(0));
        });
    }

    [Test]
    public void ForgetResult_carries_its_members()
    {
        var result = new RepoContextForgetResult
        {
            Key = "repo/acme/mem/notes/1",
            Mode = "lapse",
            Existed = true,
            ExpiresAtUtc = "2027-01-01T00:00:00.0000000Z",
        };

        Assert.Multiple(() =>
        {
            Assert.That(result.Mode, Is.EqualTo("lapse"));
            Assert.That(result.Existed, Is.True);
            Assert.That(result.ExpiresAtUtc, Is.EqualTo("2027-01-01T00:00:00.0000000Z"));
        });
    }
}
