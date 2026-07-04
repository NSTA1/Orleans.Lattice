namespace Orleans.Lattice.Membership.Entra.Graph.Tests;

/// <summary>
/// A fake <see cref="IEntraGraphMemberGroupsClient"/> that returns a fixed set of
/// group ids and records the subject id it was asked about. No live Graph call.
/// </summary>
internal sealed class FakeGraphMemberGroupsClient(params string[] groups) : IEntraGraphMemberGroupsClient
{
    private readonly IReadOnlyCollection<string> _groups = groups;

    /// <summary>The subject id of the most recent call, or <c>null</c> when never called.</summary>
    public string? LastSubjectId { get; private set; }

    /// <inheritdoc />
    public Task<IReadOnlyCollection<string>> GetTransitiveGroupIdsAsync(string subjectId, CancellationToken cancellationToken)
    {
        LastSubjectId = subjectId;
        return Task.FromResult(_groups);
    }
}
