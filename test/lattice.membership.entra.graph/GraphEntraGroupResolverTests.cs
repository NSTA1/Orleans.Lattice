using Orleans.Lattice.Membership.Entra;

namespace Orleans.Lattice.Membership.Entra.Graph.Tests;

/// <summary>
/// Unit tests for <see cref="GraphEntraGroupResolver"/>: it forwards the caller's
/// subject id to the Graph member-groups seam and returns the resolved ids.
/// </summary>
public class GraphEntraGroupResolverTests
{
    [Test]
    public void Constructor_null_client_throws()
    {
        Assert.That(() => new GraphEntraGroupResolver(null!), Throws.ArgumentNullException);
    }

    [Test]
    public async Task ResolveGroupsAsync_forwards_subject_and_returns_groups()
    {
        var client = new FakeGraphMemberGroupsClient("g-1", "g-2", "g-3");
        var resolver = new GraphEntraGroupResolver(client);
        var context = new EntraGroupResolutionContext("subject-oid", "tenant-1");

        var groups = await resolver.ResolveGroupsAsync(context);

        Assert.That(client.LastSubjectId, Is.EqualTo("subject-oid"));
        Assert.That(groups, Is.EquivalentTo(new[] { "g-1", "g-2", "g-3" }));
    }

    [Test]
    public void ResolveGroupsAsync_null_context_throws()
    {
        var resolver = new GraphEntraGroupResolver(new FakeGraphMemberGroupsClient());
        Assert.That(
            async () => await resolver.ResolveGroupsAsync(null!),
            Throws.ArgumentNullException);
    }
}
