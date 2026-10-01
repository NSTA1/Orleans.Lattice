using System.Collections.Immutable;
using Orleans.Lattice.Api.Apps;
using Orleans.Lattice.Explorer.UI.Areas.Apps.Catalogue;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Apps.Catalogue;

/// <summary>
/// Whether the caller holds an app role is read from the role bindings and the caller's
/// groups alone (issues #3902 and #4150), and unknown membership is never guessed.
/// </summary>
[TestFixture]
public sealed class AppRoleHoldingAssessmentTests
{
    private static readonly AppsCallerGroups InOperators = new("alice", ["operators"]);
    private static readonly AppsCallerGroups InNothing = new("explorer-admin", []);

    [Test]
    public void A_member_of_a_bound_group_holds_its_role()
    {
        var assessment = AppRoleHoldingAssessment.Assess([("editor", "operators"), ("viewer", "viewers")], InOperators);

        Assert.Multiple(() =>
        {
            Assert.That(assessment.HoldsAny, Is.True);
            Assert.That(assessment.HeldRoles, Is.EqualTo(new[] { "editor" }));
            Assert.That(assessment.MemberGroups, Is.EqualTo(new[] { "operators" }));
            Assert.That(assessment.MissingGroups, Is.EqualTo(new[] { "viewers" }));
            Assert.That(assessment.BindingsText, Is.EqualTo("editor to operators, viewer to viewers"));
        });
    }

    [Test]
    public void A_caller_in_no_bound_group_holds_nothing_whatever_else_they_may_do()
    {
        var assessment = AppRoleHoldingAssessment.Assess([("editor", "operators"), ("viewer", "operators")], InNothing);

        Assert.Multiple(() =>
        {
            Assert.That(assessment.HoldsAny, Is.False);
            Assert.That(assessment.HeldRoles, Is.Empty);
            Assert.That(assessment.MissingGroups, Is.EqualTo(new[] { "operators" }), "each group is named once");
        });
    }

    [Test]
    public void Unknown_membership_is_never_guessed()
    {
        var assessment = AppRoleHoldingAssessment.Assess([("editor", "operators")], AppsCallerGroups.Unknown);

        Assert.Multiple(() =>
        {
            Assert.That(assessment.HoldsAny, Is.Null);
            Assert.That(assessment.MembershipKnown, Is.False);
            Assert.That(assessment.Lines.Single().Membership, Is.EqualTo(AppGroupMembership.Unknown));
            Assert.That(assessment.MissingGroups, Is.Empty);
        });
    }

    [Test]
    public void An_unbound_role_is_held_by_nobody()
    {
        var assessment = AppRoleHoldingAssessment.Assess([("editor", null), ("viewer", " ")], InOperators);

        Assert.Multiple(() =>
        {
            Assert.That(assessment.Lines.Select(line => line.Membership), Is.All.EqualTo(AppGroupMembership.Unbound));
            Assert.That(assessment.HoldsAny, Is.False);
            Assert.That(assessment.BoundGroups, Is.Empty);
            Assert.That(assessment.BindingsText, Is.EqualTo("editor to no group, viewer to no group"));
        });
    }

    [Test]
    public void An_installed_app_is_assessed_from_every_recorded_binding_and_its_unbound_roles()
    {
        var app = new AppDescriptor
        {
            Slug = "task-board",
            Version = "1.0.0",
            Provenance = new AppProvenanceDescriptor { Source = "in-image", Publisher = "Contoso" },
            Roles = [new AppRoleDescriptor { Name = "editor" }, new AppRoleDescriptor { Name = "viewer" }, new AppRoleDescriptor { Name = "auditor" }],
            RoleBindings =
            [
                new AppRoleBindingDescriptor { RoleName = "editor", GroupId = "operators" },
                new AppRoleBindingDescriptor { RoleName = "editor", GroupId = "leads" },
                new AppRoleBindingDescriptor { RoleName = "viewer", GroupId = "viewers" },
            ],
        };

        var assessment = AppRoleHoldingAssessment.Assess(app, InOperators);

        Assert.That(assessment.Lines, Is.EqualTo(ImmutableArray.Create(
            new AppRoleHoldingLine("editor", "operators", AppGroupMembership.Member),
            new AppRoleHoldingLine("editor", "leads", AppGroupMembership.NotMember),
            new AppRoleHoldingLine("viewer", "viewers", AppGroupMembership.NotMember),
            new AppRoleHoldingLine("auditor", null, AppGroupMembership.Unbound))));
    }

    [Test]
    public void Membership_of_a_group_compares_ids_exactly()
    {
        Assert.Multiple(() =>
        {
            Assert.That(InOperators.Of("operators"), Is.EqualTo(AppGroupMembership.Member));
            Assert.That(InOperators.Of("Operators"), Is.EqualTo(AppGroupMembership.NotMember));
            Assert.That(AppsCallerGroups.Unknown.Of("operators"), Is.EqualTo(AppGroupMembership.Unknown));
            Assert.That(AppsCallerGroups.Unknown.IsKnown, Is.False);
        });
    }

    [Test]
    public void Null_arguments_are_refused()
    {
        Assert.Multiple(() =>
        {
            Assert.That(() => AppRoleHoldingAssessment.Assess((IEnumerable<(string, string?)>)null!, InOperators), Throws.ArgumentNullException);
            Assert.That(() => AppRoleHoldingAssessment.Assess([("editor", "operators")], null!), Throws.ArgumentNullException);
            Assert.That(() => AppRoleHoldingAssessment.Assess((AppDescriptor)null!, InOperators), Throws.ArgumentNullException);
            Assert.That(() => InOperators.Of(null!), Throws.ArgumentNullException);
        });
    }
}
