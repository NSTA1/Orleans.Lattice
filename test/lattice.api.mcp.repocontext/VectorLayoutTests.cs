namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests;

/// <summary>
/// Tests for the vector layout contract added to <see cref="RepoContextKeys"/>,
/// <see cref="RepoContextRecordKind"/>, and <see cref="RepoContextTrees"/>: the
/// vector key builders and their prefixes, <see cref="RepoContextKeys.TryParse"/>
/// round-trips, and routing of each vector kind onto its dedicated tree.
/// </summary>
[TestFixture]
public sealed class VectorLayoutTests
{
    [Test]
    public void Vector_builds_the_metadata_key()
        => Assert.That(RepoContextKeys.Vector("acme", "v1"), Is.EqualTo("repo/acme/vec/v1"));

    [Test]
    public void VectorPayload_builds_the_content_addressed_key()
        => Assert.That(RepoContextKeys.VectorPayload("acme", "sha256:abc"),
            Is.EqualTo("repo/acme/vpay/sha256:abc"));

    [Test]
    public void VectorMembership_builds_the_collection_key()
        => Assert.That(RepoContextKeys.VectorMembership("acme", "code"),
            Is.EqualTo("repo/acme/vmem/code"));

    [Test]
    public void Vector_metadata_key_round_trips_through_TryParse()
    {
        var key = RepoContextKeys.Vector("acme", "v1");
        Assert.That(RepoContextKeys.TryParse(key, out var parsed), Is.True);
        Assert.Multiple(() =>
        {
            Assert.That(parsed.Kind, Is.EqualTo(RepoContextRecordKind.VectorMetadata));
            Assert.That(parsed.RepoId, Is.EqualTo("acme"));
            Assert.That(parsed.VectorId, Is.EqualTo("v1"));
        });
    }

    [Test]
    public void Vector_payload_key_round_trips_through_TryParse()
    {
        var key = RepoContextKeys.VectorPayload("acme", "sha256:abc");
        Assert.That(RepoContextKeys.TryParse(key, out var parsed), Is.True);
        Assert.Multiple(() =>
        {
            Assert.That(parsed.Kind, Is.EqualTo(RepoContextRecordKind.VectorPayload));
            Assert.That(parsed.ContentAddress, Is.EqualTo("sha256:abc"));
        });
    }

    [Test]
    public void Vector_membership_key_round_trips_through_TryParse()
    {
        var key = RepoContextKeys.VectorMembership("acme", "code");
        Assert.That(RepoContextKeys.TryParse(key, out var parsed), Is.True);
        Assert.Multiple(() =>
        {
            Assert.That(parsed.Kind, Is.EqualTo(RepoContextRecordKind.VectorMembership));
            Assert.That(parsed.Collection, Is.EqualTo("code"));
        });
    }

    [Test]
    public void Reserved_characters_in_a_vector_id_round_trip()
    {
        var key = RepoContextKeys.Vector("acme", "a/b");
        Assert.That(key, Is.EqualTo("repo/acme/vec/a%2Fb"));
        Assert.That(RepoContextKeys.TryParse(key, out var parsed), Is.True);
        Assert.That(parsed.VectorId, Is.EqualTo("a/b"));
    }

    [Test]
    public void Every_vector_family_prefix_is_a_prefix_of_its_key()
    {
        Assert.Multiple(() =>
        {
            Assert.That(RepoContextKeys.Vector("acme", "v1"),
                Does.StartWith(RepoContextKeys.VectorsPrefix("acme")));
            Assert.That(RepoContextKeys.VectorPayload("acme", "a"),
                Does.StartWith(RepoContextKeys.VectorPayloadsPrefix("acme")));
            Assert.That(RepoContextKeys.VectorMembership("acme", "c"),
                Does.StartWith(RepoContextKeys.VectorMembershipsPrefix("acme")));
        });
    }

    [Test]
    public void Vector_builders_reject_null_arguments()
    {
        Assert.Multiple(() =>
        {
            Assert.That(() => RepoContextKeys.Vector("acme", null!), Throws.ArgumentNullException);
            Assert.That(() => RepoContextKeys.VectorPayload("acme", null!), Throws.ArgumentNullException);
            Assert.That(() => RepoContextKeys.VectorMembership("acme", null!), Throws.ArgumentNullException);
        });
    }

    [Test]
    public void Vector_kinds_route_to_their_dedicated_trees()
    {
        Assert.Multiple(() =>
        {
            Assert.That(RepoContextTrees.ForKind(RepoContextRecordKind.VectorMetadata),
                Is.EqualTo(RepoContextTrees.VectorMetadata));
            Assert.That(RepoContextTrees.ForKind(RepoContextRecordKind.VectorPayload),
                Is.EqualTo(RepoContextTrees.VectorPayload));
            Assert.That(RepoContextTrees.ForKind(RepoContextRecordKind.VectorMembership),
                Is.EqualTo(RepoContextTrees.VectorMembership));
        });
    }
}
