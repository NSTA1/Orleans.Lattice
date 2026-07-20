using NSubstitute;
using Orleans.Lattice.Api.Auth;
using Orleans.Lattice.Auth;
using Orleans.Lattice.Explorer.Access;
using Orleans.Lattice.Explorer.UI.Access;
using Orleans.Lattice.Membership;

namespace Orleans.Lattice.Explorer.Tests.Access;

/// <summary>
/// Unit coverage for <see cref="AccessRuleFormat.FriendlyReason"/>, which swaps the
/// raw subject id embedded in a deny <see cref="AuthExplanation.Reason"/> for its
/// friendly directory display name while leaving the reason byte-for-byte unchanged
/// whenever the id is unresolved, blank, or the reason is <see langword="null"/>.
/// Every case is deterministic: the <see cref="PrincipalLabelResolver"/> is seeded
/// through its public warm-up API over a stubbed membership service, with no
/// wall-clock, ordering, or GC dependence.
/// </summary>
[TestFixture]
public sealed class AccessRuleFormatFriendlyReasonTests
{
    private const string SubjectOid = "9c3ad1a1-a39a-4980-b401-6c44d29481c9";

    private static async Task<PrincipalLabelResolver> ResolverWithAsync(string id, string displayName)
    {
        var membership = Substitute.For<IMembershipAdminService>();
        membership
            .ResolveDirectoryPrincipalAsync(id, Arg.Any<CancellationToken>())
            .Returns(new DirectoryPrincipalDescriptor { Id = id, DisplayName = displayName, Kind = DirectoryPrincipalKind.User });
        var resolver = new PrincipalLabelResolver(membership);
        await resolver.ResolveLabelAsync(id);
        return resolver;
    }

    private static PrincipalLabelResolver EmptyResolver() =>
        new(Substitute.For<IMembershipAdminService>());

    private static AuthExplanation Explanation(string subjectId, string? reason) => new()
    {
        SubjectId = subjectId,
        Scope = LatticeScope.Tree("factory-floor"),
        Operation = LatticeOperation.Read,
        Allowed = false,
        Reason = reason,
    };

    // ----- Guards -----

    [Test]
    public void FriendlyReason_null_explanation_throws()
    {
        Assert.That(() => AccessRuleFormat.FriendlyReason(null!, EmptyResolver()), Throws.ArgumentNullException);
    }

    [Test]
    public void FriendlyReason_null_labels_throws()
    {
        Assert.That(() => AccessRuleFormat.FriendlyReason(Explanation(SubjectOid, "denied"), null!), Throws.ArgumentNullException);
    }

    // ----- The swap -----

    [Test]
    public async Task FriendlyReason_swaps_resolved_subject_id_for_display_name()
    {
        var resolver = await ResolverWithAsync(SubjectOid, "Alice Ng");
        var reason = $"No matching rule for subject '{SubjectOid}' on tree 'factory-floor'; applied default effect Deny.";

        var result = AccessRuleFormat.FriendlyReason(Explanation(SubjectOid, reason), resolver);

        Assert.That(result, Is.EqualTo("No matching rule for subject 'Alice Ng' on tree 'factory-floor'; applied default effect Deny."));
    }

    [Test]
    public async Task FriendlyReason_swaps_subject_id_in_the_denied_by_rule_reason()
    {
        var resolver = await ResolverWithAsync(SubjectOid, "Alice Ng");
        var reason = $"Denied by rule 'no-writes' (tree scope) for subject '{SubjectOid}' on tree 'factory-floor'.";

        var result = AccessRuleFormat.FriendlyReason(Explanation(SubjectOid, reason), resolver);

        Assert.That(result, Is.EqualTo("Denied by rule 'no-writes' (tree scope) for subject 'Alice Ng' on tree 'factory-floor'."));
    }

    // ----- Degrade-to-id-only fallbacks -----

    [Test]
    public void FriendlyReason_unresolved_subject_id_leaves_reason_unchanged()
    {
        var reason = $"No matching rule for subject '{SubjectOid}' on tree 'factory-floor'; applied default effect Deny.";

        var result = AccessRuleFormat.FriendlyReason(Explanation(SubjectOid, reason), EmptyResolver());

        Assert.That(result, Is.EqualTo(reason));
    }

    [Test]
    public void FriendlyReason_null_reason_returns_null()
    {
        var result = AccessRuleFormat.FriendlyReason(Explanation(SubjectOid, null), EmptyResolver());

        Assert.That(result, Is.Null);
    }

    [Test]
    public void FriendlyReason_empty_subject_id_leaves_reason_unchanged()
    {
        var reason = "Denied by rule 'no-writes' (tree scope) for subject '' on tree 'factory-floor'.";

        var result = AccessRuleFormat.FriendlyReason(Explanation(string.Empty, reason), EmptyResolver());

        Assert.That(result, Is.EqualTo(reason));
    }
}
