using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Capture;

/// <summary>
/// Unit tests for <see cref="RepoContextEntryProjection"/>: it flattens each
/// record family into a stable <see cref="RepoContextEntryView"/> - scalars,
/// tags, and memory link relations - carries the parsed identity, projects the
/// supplied remaining life, and returns an <see cref="RepoContextEntryView.Exists"/>-false
/// view for an absent value.
/// </summary>
[TestFixture]
public sealed class RepoContextEntryProjectionTests
{
    private static readonly Serializer Serializer = new ServiceCollection()
        .AddSerializer()
        .BuildServiceProvider()
        .GetRequiredService<Serializer>();

    private static HybridLogicalClock Clock(long ticks) => new() { WallClockTicks = ticks, Counter = 0 };

    private static RepoContextKey Parse(string key)
    {
        Assert.That(RepoContextKeys.TryParse(key, out var parsed), Is.True, key);
        return parsed;
    }

    [Test]
    public void Project_flattens_a_memory_record_with_scalars_tags_and_links()
    {
        var key = Parse(RepoContextKeys.Memory("acme", "decisions", "42"));
        var record = new MemoryRecord
        {
            RepoId = "acme",
            Topic = "decisions",
            Id = "42",
            Kind = MemoryKind.Decision,
            Title = RepoContextValues.Lww("Use CRDTs", Clock(1)),
            Body = RepoContextValues.Lww("Because they converge.", Clock(1)),
        };
        record.Tags.Add(Encoding.UTF8.GetBytes("architecture"), "a", 0);
        var targets = new OrSet();
        targets.Add(Encoding.UTF8.GetBytes("repo/acme/file/a.cs"), "a", 0);
        record.Links.Set("relatesTo", "a", targets);

        var view = RepoContextEntryProjection.Project(
            key, Serializer.SerializeToArray(record), Serializer, RepoContextRemainingLife.NeverExpires);

        Assert.Multiple(() =>
        {
            Assert.That(view.Exists, Is.True);
            Assert.That(view.Kind, Is.EqualTo("Memory"));
            Assert.That(view.RepoId, Is.EqualTo("acme"));
            Assert.That(view.Topic, Is.EqualTo("decisions"));
            Assert.That(view.Id, Is.EqualTo("42"));
            Assert.That(view.Fields["kind"], Is.EqualTo("Decision"));
            Assert.That(view.Fields["title"], Is.EqualTo("Use CRDTs"));
            Assert.That(view.Fields["body"], Is.EqualTo("Because they converge."));
            Assert.That(view.Tags, Is.EqualTo(new[] { "architecture" }));
            Assert.That(view.Links["relatesTo"], Is.EqualTo(new[] { "repo/acme/file/a.cs" }));
            Assert.That(view.Expires, Is.False);
            Assert.That(view.RemainingSeconds, Is.Null);
        });
    }

    [Test]
    public void Project_flattens_a_file_record_scalars()
    {
        var key = Parse(RepoContextKeys.File("acme", "src/a.cs"));
        var record = new FileNode
        {
            RepoId = "acme",
            Path = "src/a.cs",
            Digest = RepoContextValues.Lww("deadbeef", Clock(1)),
            Language = RepoContextValues.Lww("csharp", Clock(1)),
            SizeBytes = RepoContextValues.Lww(1024, Clock(1)),
        };

        var view = RepoContextEntryProjection.Project(
            key, Serializer.SerializeToArray(record), Serializer, RepoContextRemainingLife.NeverExpires);

        Assert.Multiple(() =>
        {
            Assert.That(view.Kind, Is.EqualTo("File"));
            Assert.That(view.Path, Is.EqualTo("src/a.cs"));
            Assert.That(view.Fields["digest"], Is.EqualTo("deadbeef"));
            Assert.That(view.Fields["language"], Is.EqualTo("csharp"));
            Assert.That(view.Fields["sizeBytes"], Is.EqualTo("1024"));
        });
    }

    [Test]
    public void Project_of_an_absent_value_reports_not_exists_with_empty_collections()
    {
        var key = Parse(RepoContextKeys.Memory("acme", "notes", "gone"));

        var view = RepoContextEntryProjection.Project(
            key, value: null, Serializer, RepoContextRemainingLife.NeverExpires);

        Assert.Multiple(() =>
        {
            Assert.That(view.Exists, Is.False);
            Assert.That(view.Fields, Is.Empty);
            Assert.That(view.Tags, Is.Empty);
            Assert.That(view.Links, Is.Empty);
            Assert.That(view.Key, Is.EqualTo(RepoContextKeys.Memory("acme", "notes", "gone")));
        });
    }

    [Test]
    public void Project_carries_a_finite_remaining_life()
    {
        var key = Parse(RepoContextKeys.Memory("acme", "notes", "1"));
        var record = new MemoryRecord { RepoId = "acme", Topic = "notes", Id = "1" };
        var now = new DateTime(2024, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        var life = RepoContextRemainingLife.FromExpiry(now.AddMinutes(5).Ticks, now);

        var view = RepoContextEntryProjection.Project(
            key, Serializer.SerializeToArray(record), Serializer, life);

        Assert.Multiple(() =>
        {
            Assert.That(view.Expires, Is.True);
            Assert.That(view.HasExpired, Is.False);
            Assert.That(view.RemainingSeconds, Is.EqualTo(300).Within(1));
            Assert.That(view.ExpiresAtUtc, Is.EqualTo(now.AddMinutes(5).ToString("O")));
        });
    }
}
