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

    [Test]
    public void Assess_no_roles_reports_no_bindings_and_known_empty_membership()
    {
        var assessment = AppRoleHoldingAssessment.Assess(Array.Empty<(string Role, string? Group)>(), InOperators);

        Assert.Multiple(() =>
        {
            Assert.That(assessment.BindingsText, Is.EqualTo("no role"));
            Assert.That(assessment.HoldsAny, Is.False);
            Assert.That(assessment.Lines, Is.Empty);
            Assert.That(assessment.HeldRoles, Is.Empty);
            Assert.That(assessment.BoundGroups, Is.Empty);
            Assert.That(assessment.MemberGroups, Is.Empty);
            Assert.That(assessment.MissingGroups, Is.Empty);
        });
    }

    [Test]
    public void Assess_duplicate_bindings_deduplicate_roles_and_groups_in_first_seen_order()
    {
        var caller = new AppsCallerGroups("alice", ["operators", "leads"]);
        var assessment = AppRoleHoldingAssessment.Assess(
            [("editor", "operators"), ("editor", "leads"), ("viewer", "operators"), ("editor", "operators"),
                ("auditor", "missing"), ("viewer", "missing"), ("Editor", "leads")], caller);

        Assert.Multiple(() =>
        {
            Assert.That(assessment.HeldRoles, Is.EqualTo(new[] { "editor", "viewer", "Editor" }));
            Assert.That(assessment.MemberGroups, Is.EqualTo(new[] { "operators", "leads" }));
            Assert.That(assessment.MissingGroups, Is.EqualTo(new[] { "missing" }));
            Assert.That(assessment.BoundGroups, Is.EqualTo(new[] { "operators", "leads", "missing" }));
            Assert.That(assessment.Lines, Has.Length.EqualTo(7), "presentation retains every binding even when summary lists deduplicate");
        });
    }

    [Test]
    public void Assess_default_role_array_reports_no_roles_even_with_recorded_bindings()
    {
        var app = AppsTestData.TaskBoard() with
        {
            Roles = default,
            RoleBindings = [new AppRoleBindingDescriptor { RoleName = "viewer", GroupId = "operators" }],
        };

        var assessment = AppRoleHoldingAssessment.Assess(app, InOperators);

        Assert.Multiple(() =>
        {
            Assert.That(assessment.BindingsText, Is.EqualTo("no role"));
            Assert.That(assessment.Lines, Is.Empty);
            Assert.That(assessment.HoldsAny, Is.False);
        });
    }

    [Test]
    public void Assess_default_binding_array_reports_each_declared_role_as_unbound()
    {
        var app = AppsTestData.TaskBoard() with
        {
            Roles = [new AppRoleDescriptor { Name = "editor" }, new AppRoleDescriptor { Name = "viewer" }],
            RoleBindings = default,
        };

        var assessment = AppRoleHoldingAssessment.Assess(app, InOperators);

        Assert.Multiple(() =>
        {
            Assert.That(assessment.Lines, Is.EqualTo(new[]
            {
                new AppRoleHoldingLine("editor", null, AppGroupMembership.Unbound),
                new AppRoleHoldingLine("viewer", null, AppGroupMembership.Unbound),
            }));
            Assert.That(assessment.HoldsAny, Is.False);
            Assert.That(assessment.BoundGroups, Is.Empty);
            Assert.That(assessment.BindingsText, Is.EqualTo("editor to no group, viewer to no group"));
        });
    }
}
