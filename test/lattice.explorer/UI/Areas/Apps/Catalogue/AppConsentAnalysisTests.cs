using Orleans.Lattice.Api.Apps;
using Orleans.Lattice.Auth;
using Orleans.Lattice.Explorer.UI.Areas.Apps.Catalogue;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Apps.Catalogue;

/// <summary>The consent arithmetic behind the review, drift detection and the upgrade diff.</summary>
[TestFixture]
public sealed class AppConsentAnalysisTests
{
    [Test]
    public void The_needed_ceiling_is_the_union_of_every_role()
    {
        Assert.That(AppConsentAnalysis.RequiredOperations(AppsTestData.TaskBoard()),
            Is.EqualTo(LatticeOperation.Read | LatticeOperation.RangeRead | LatticeOperation.Write | LatticeOperation.Delete));
    }

    [Test]
    public void Only_cross_app_scopes_and_adopted_trees_need_an_exception()
    {
        var own = AppConsentAnalysis.RequiredScopes(AppsTestData.TaskBoard());
        var outside = AppConsentAnalysis.RequiredScopes(AppsTestData.TaskBoard(crossApp: true, adopted: true));

        Assert.Multiple(() =>
        {
            Assert.That(own, Is.Empty);
            Assert.That(outside, Is.EqualTo(new[]
            {
                new AppExceptionScope { App = "crm", Tree = "contacts", Kind = LatticeScopeKind.Prefix, KeyOrPrefix = "eu-" },
                new AppExceptionScope { AdoptedTreeId = "legacy-archive" },
            }));
            Assert.That(AppConsentAnalysis.IsOutsideNamespace(new AppRoleScope { Tree = "t", App = "task-board" }, "task-board"), Is.False);
        });
    }

    [Test]
    public void A_grant_without_a_tree_covers_every_tree_but_not_the_reverse()
    {
        var any = new AppUiBridgeGrantDescriptor { Operation = "data.read" };
        var tasks = new AppUiBridgeGrantDescriptor { Operation = "data.read", Tree = "tasks" };

        Assert.Multiple(() =>
        {
            Assert.That(AppConsentAnalysis.Covers([any], tasks), Is.True);
            Assert.That(AppConsentAnalysis.Covers([tasks], any), Is.False);
            Assert.That(AppConsentAnalysis.Covers([tasks], tasks with { Operation = "data.write" }), Is.False);
        });
    }

    [Test]
    public void A_whole_tree_approval_covers_any_extent_of_that_tree()
    {
        var whole = new AppExceptionScope { App = "crm", Tree = "contacts" };
        var prefix = whole with { Kind = LatticeScopeKind.Prefix, KeyOrPrefix = "eu-" };

        Assert.Multiple(() =>
        {
            Assert.That(AppConsentAnalysis.Covers([whole], prefix), Is.True);
            Assert.That(AppConsentAnalysis.Covers([prefix], whole), Is.False);
            Assert.That(AppConsentAnalysis.Covers([prefix], prefix), Is.True);
        });
    }

    [Test]
    public void A_prefix_approval_covers_any_key_or_narrower_prefix_under_it_as_the_cluster_does()
    {
        // #4326: the cluster's AppRoleCompiler.IsCovered accepts these; the review must not report them as gaps.
        var prefix = new AppExceptionScope { App = "crm", Tree = "contacts", Kind = LatticeScopeKind.Prefix, KeyOrPrefix = "eu-" };
        var key = prefix with { Kind = LatticeScopeKind.Key, KeyOrPrefix = "eu-123" };
        var narrower = prefix with { KeyOrPrefix = "eu-west-" };

        Assert.Multiple(() =>
        {
            Assert.That(AppConsentAnalysis.Covers([prefix], key), Is.True);
            Assert.That(AppConsentAnalysis.Covers([prefix], narrower), Is.True);
            Assert.That(AppConsentAnalysis.Covers([narrower], prefix), Is.False, "a narrower prefix does not cover a wider one");
            Assert.That(AppConsentAnalysis.Covers([prefix], key with { KeyOrPrefix = "us-1" }), Is.False);
            Assert.That(AppConsentAnalysis.Covers([prefix], prefix with { Kind = LatticeScopeKind.Tree, KeyOrPrefix = null }), Is.False);
            Assert.That(AppConsentAnalysis.Covers([prefix], key with { Tree = "leads" }), Is.False, "coverage never crosses trees");
            Assert.That(AppConsentAnalysis.Covers([prefix with { KeyOrPrefix = "EU-" }], key), Is.False, "prefixes compare ordinally");
        });
    }

    [Test]
    public void A_key_approval_covers_only_that_key()
    {
        var key = new AppExceptionScope { App = "crm", Tree = "contacts", Kind = LatticeScopeKind.Key, KeyOrPrefix = "eu-123" };

        Assert.Multiple(() =>
        {
            Assert.That(AppConsentAnalysis.Covers([key], key), Is.True);
            Assert.That(AppConsentAnalysis.Covers([key], key with { KeyOrPrefix = "eu-1234" }), Is.False);
            Assert.That(AppConsentAnalysis.Covers([key], key with { Kind = LatticeScopeKind.Prefix }), Is.False, "a key does not cover the prefix spelled the same");
        });
    }

    [Test]
    public void A_wider_recorded_prefix_neither_blocks_activation_nor_reads_as_drift_or_reconsent()
    {
        var app = AppsTestData.TaskBoard(crossApp: true);
        var wider = new AppExceptionScope { App = "crm", Tree = "contacts", Kind = LatticeScopeKind.Prefix, KeyOrPrefix = "e" };
        var consent = new AppConsentReport
        {
            Slug = app.Slug,
            Version = app.Version,
            Ceiling = new AppCapabilityCeilingDescriptor
            {
                AllowedOperations = AppConsentAnalysis.RequiredOperations(app),
                ApprovedExceptionScopes = [wider],
            },
            BridgeGrants = app.Ui!.Bridge,
        };

        Assert.Multiple(() =>
        {
            Assert.That(AppConsentAnalysis.Preview(app, AppConsentDraft.FromReport(consent)), Is.Empty);
            Assert.That(AppConsentAnalysis.Drift(app, consent), Is.Empty);
            Assert.That(AppConsentAnalysis.Diff(app, AppsTestData.TaskBoard("1.0.1", crossApp: true), consent).ScopesAdded, Is.Empty);
        });
    }

    [Test]
    public void The_requested_consent_would_activate()
    {
        var app = AppsTestData.TaskBoard(crossApp: true, adopted: true);

        Assert.That(AppConsentAnalysis.Preview(app, AppConsentDraft.Requested(app)), Is.Empty);
    }

    [Test]
    public void The_preview_names_every_gap_the_cluster_would_refuse()
    {
        var app = AppsTestData.TaskBoard(crossApp: true) with
        {
            Trees = [new AppTreeDescriptor { Name = "tasks", OwnershipConflict = "owned by crm" }],
        };
        var draft = new AppConsentDraft(LatticeOperation.Read, [], []);

        var issues = AppConsentAnalysis.Preview(app, draft);

        Assert.Multiple(() =>
        {
            Assert.That(issues.Select(issue => issue.Kind), Is.EqualTo(new[]
            {
                AppActivationIssueKind.TreeOwnershipConflict,
                AppActivationIssueKind.CeilingExceeded,
                AppActivationIssueKind.CeilingExceeded,
                AppActivationIssueKind.ScopeNotApproved,
                AppActivationIssueKind.BridgeConsentRequired,
                AppActivationIssueKind.BridgeConsentRequired,
                AppActivationIssueKind.BridgeConsentRequired,
            }));
            Assert.That(issues[1].Text, Does.Contain("viewer asks to range read"));
            Assert.That(issues[3].Text, Does.Contain("a/crm/contacts").And.Contain("outside a/task-board/"));
            Assert.That(issues[4].Text, Does.Contain("read its own trees"));
        });
    }

    [Test]
    public void Drift_is_what_the_recorded_consent_no_longer_covers()
    {
        var app = AppsTestData.TaskBoard();
        var covered = new AppConsentReport
        {
            Slug = app.Slug,
            Version = app.Version,
            Ceiling = new AppCapabilityCeilingDescriptor { AllowedOperations = AppConsentAnalysis.RequiredOperations(app) },
            BridgeGrants = app.Ui!.Bridge,
        };
        var stale = covered with { BridgeGrants = [] };

        Assert.Multiple(() =>
        {
            Assert.That(AppConsentAnalysis.Drift(app, covered), Is.Empty);
            Assert.That(AppConsentAnalysis.Drift(app, stale).Select(issue => issue.Kind), Is.All.EqualTo(AppActivationIssueKind.BridgeConsentRequired));
            Assert.That(AppConsentDraft.FromReport(covered with { BridgeGrants = null }).BridgeGrants, Is.Empty);
        });
    }

    [Test]
    public void The_upgrade_diff_lists_trees_roles_ceiling_and_bridge_and_flags_reconsent()
    {
        var installed = AppsTestData.TaskBoard();
        var consent = new AppConsentReport
        {
            Slug = installed.Slug,
            Version = installed.Version,
            Ceiling = new AppCapabilityCeilingDescriptor { AllowedOperations = AppConsentAnalysis.RequiredOperations(installed) },
            BridgeGrants = installed.Ui!.Bridge,
        };
        var next = AppsTestData.TaskBoard(
            "2.0.0",
            crossApp: true,
            bridge: [new AppUiBridgeGrantDescriptor { Operation = "data.read" }, new AppUiBridgeGrantDescriptor { Operation = "data.delete" }],
            editorOperations: LatticeOperation.Read | LatticeOperation.Write | LatticeOperation.Delete | LatticeOperation.Admin) with
        {
            Trees = [new AppTreeDescriptor { Name = "tasks" }, new AppTreeDescriptor { Name = "boards" }],
        };

        var diff = AppConsentAnalysis.Diff(installed, next, consent);

        Assert.Multiple(() =>
        {
            Assert.That(diff.Slug, Is.EqualTo("task-board"));
            Assert.That((diff.FromVersion, diff.ToVersion), Is.EqualTo(("1.0.0", "2.0.0")));
            Assert.That(diff.TreesAdded, Is.EqualTo(new[] { "boards" }));
            Assert.That(diff.TreesRemoved, Is.Empty);
            Assert.That(diff.RolesChanged, Is.EqualTo(new[] { "editor", "viewer" }));
            Assert.That(diff.CeilingAdded, Is.EqualTo(LatticeOperation.Admin));
            Assert.That(diff.ScopesAdded, Has.Length.EqualTo(1));
            Assert.That(diff.BridgeAdded.Select(grant => grant.Operation), Is.EqualTo(new[] { "data.delete" }));
            Assert.That(diff.BridgeRemoved.Select(grant => grant.Operation), Is.EqualTo(new[] { "data.write", "context.user" }));
            Assert.That(diff.RequiresReconsent, Is.True);
            Assert.That(diff.IsEmpty, Is.False);
        });
    }

    [Test]
    public void An_upgrade_that_changes_nothing_that_matters_is_empty()
    {
        var installed = AppsTestData.TaskBoard();
        var diff = AppConsentAnalysis.Diff(installed, AppsTestData.TaskBoard("1.0.1"), consent: null);

        Assert.Multiple(() =>
        {
            Assert.That(diff.IsEmpty, Is.True);
            Assert.That(diff.RequiresReconsent, Is.False);
        });
    }

    [Test]
    public void A_draft_becomes_the_ceiling_it_sends()
    {
        var scope = new AppExceptionScope { AdoptedTreeId = "legacy" };
        var draft = new AppConsentDraft(LatticeOperation.Read, [scope], []);

        Assert.That(draft.ToCeiling(), Is.EqualTo(new AppCapabilityCeilingDescriptor { AllowedOperations = LatticeOperation.Read, ApprovedExceptionScopes = [scope] })
            .Using<AppCapabilityCeilingDescriptor>((a, b) => a.AllowedOperations == b.AllowedOperations && a.ApprovedExceptionScopes.SequenceEqual(b.ApprovedExceptionScopes)));
    }
}
