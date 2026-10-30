using Bunit;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;
using Orleans.Lattice.Explorer.UI.Areas.Schema;
using Orleans.Lattice.Schema;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Schema;

/// <summary>
/// Issue #5234 (and the #1257 requirements it absorbed): the Versions and Policy tabs
/// explain stamping, target advancement, read-time upgrading, migration and
/// enforcement as separate operations; make what differs between versions, and which
/// versions can still be read, inspectable from the catalog the cluster reports; and
/// stay accurate when versioning or its catalog is absent, partly installed, fully
/// installed, or refused to the caller.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class SchemaVersionExperienceTests : SchemaTestContext
{
    private static LatticeSchemaRuntimeRegistration OrdersCatalog() => new()
    {
        Descriptors = [new(7, 1, "Initial"), new(7, 2, "Quantity spelled out"), new(7, 3, "Orders")],
        Upcasters =
        [
            new()
            {
                SchemaId = 7,
                FromVersion = 1,
                ToVersion = 2,
                Transform = LatticeValueTransform.Passthrough(
                    LatticeValueTransform.RenameMember("qty", "quantity"),
                    LatticeValueTransform.DropMember("legacy")),
            },
            new() { SchemaId = 7, FromVersion = 2, ToVersion = 3, TransformId = "orders-v3" },
        ],
        SupportedTransformIds = [],
    };

    private void UseFullSupport(LatticeSchemaRuntimeRegistration? catalog = null)
    {
        UseEstate();
        Schema.DefaultCapabilities = tree => FakeSchemaControl.All(tree) with { SupportsRuntimeRegistration = true };
        Schema.Registry = catalog ?? OrdersCatalog();
    }

    private IRenderedComponent<SchemaTreePage> OpenVersions(string tree = "orders")
    {
        var cut = RenderAt<SchemaTreePage>($"schema/{tree}?tab=versions");
        cut.WaitUntil(() => Assert.That(cut.FindAll("[role=tabpanel] .lt-skeleton"), Is.Empty));
        return cut;
    }

    private static AngleSharp.Dom.IElement Button(IRenderedComponent<SchemaTreePage> cut, string text) =>
        cut.FindAll("button").Single(button => button.TextContent.Trim() == text);

    [Test]
    public void The_operations_on_a_version_are_explained_as_separate_things()
    {
        UseEstate();

        var cut = OpenVersions();

        cut.WaitUntil(() =>
        {
            var explanation = cut.Find("aside[aria-label='How versioning works']");
            Assert.That(explanation.QuerySelectorAll("dt").Select(term => term.TextContent),
                Is.EqualTo(new[] { "Stamping", "Advancing the target", "Upgrading on read", "Migrating", "Enforcing" }));
            Assert.That(explanation.TextContent, Does.Contain("materialised views over this tree are not refreshed"));
            Assert.That(explanation.TextContent, Does.Contain("rules cannot be scoped to a version"));
            Assert.That(explanation.TextContent, Does.Contain("other silos pick it up shortly after"));
        });
    }

    [Test]
    public void Full_support_shows_how_each_version_reads_and_what_each_hop_changes()
    {
        UseFullSupport();

        var cut = OpenVersions();

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find(".lt-schema-subtitle").TextContent, Is.EqualTo("Family 7, which this tree stamps"));
            var rows = cut.FindAll("table")
                .Single(table => table.TextContent.Contains("Quantity spelled out", StringComparison.Ordinal))
                .QuerySelectorAll("tbody tr")
                .Select(row => row.Children.Select(cell => cell.TextContent.Trim()).ToArray())
                .ToArray();
            Assert.That(rows, Is.EqualTo(new[]
            {
                new[] { "1", "Initial", "Upgraded as it is read, through versions 1, 2, 3" },
                new[] { "2", "Quantity spelled out", "Upgraded as it is read, through versions 2, 3" },
                new[] { "3", "Orders", "Reads as stored: this is the target version" },
            }));

            var hops = cut.Find("ol.lt-schema-hops");
            Assert.That(hops.GetAttribute("aria-label"), Is.EqualTo("What changes between versions of family 7"));
            Assert.That(hops.QuerySelectorAll(".lt-schema-hops__label").Select(label => label.TextContent),
                Is.EqualTo(new[] { "Version 1 to 2", "Version 2 to 3" }));
            Assert.That(hops.QuerySelectorAll("ul .lt-schema-hops__step").Select(step => step.TextContent),
                Is.EqualTo(new[] { "Renames qty to quantity.", "Removes legacy." }));
            Assert.That(hops.TextContent, Does.Contain("Runs the host transform orders-v3"));
            Assert.That(hops.QuerySelector(".lt-schema-error")!.TextContent, Does.Contain("does not install this transform"));
        });
    }

    [Test]
    public void An_empty_catalog_says_older_values_cannot_be_read_until_a_path_is_registered()
    {
        UseFullSupport(new LatticeSchemaRuntimeRegistration());

        var cut = OpenVersions();

        cut.WaitUntil(() => Assert.That(cut.Markup, Does.Contain(
            "No versions or migration paths are registered. A value stamped below version 3 cannot be read until a path from its version to the target is registered.")));
    }

    [Test]
    public void Partial_support_versioning_without_a_reported_catalog_says_the_host_defines_it_in_code()
    {
        UseEstate();

        var cut = OpenVersions();

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find("#lt-schema-registry-title").TextContent, Is.EqualTo("Versions and migration paths"));
            Assert.That(cut.Markup, Does.Contain("This host defines its versions and migration paths in code and does not report them"));
            Assert.That(cut.FindAll("textarea"), Is.Empty, "nothing to register into");
        });
        Assert.That(Schema.CountOf("GetRegistry"), Is.Zero);
    }

    [Test]
    public void Absent_versioning_offers_no_catalog_and_no_version_controls()
    {
        UseFullSupport();
        Schema.VersioningRegistered = false;

        var cut = OpenVersions();

        cut.WaitUntil(() => Assert.That(cut.Find("[role=tabpanel] .lt-empty__title").TextContent, Is.EqualTo("Versioning is not available")));
        Assert.Multiple(() =>
        {
            Assert.That(cut.FindAll("#lt-schema-registry-title"), Is.Empty);
            Assert.That(cut.FindAll("[role=tabpanel] button").Select(button => button.TextContent.Trim()),
                Has.None.EqualTo("Advance target version..."));
            Assert.That(Schema.CountOf("GetRegistry"), Is.Zero);
        });
    }

    [Test]
    public void Denied_reading_versioning_shows_no_catalog_and_never_asks_for_it()
    {
        UseFullSupport();
        Schema.Capabilities["orders"] = tree => FakeSchemaControl.All(tree) with
        {
            SupportsRuntimeRegistration = true,
            CanViewVersionConfig = false,
            CanManageVersion = false,
        };

        var cut = OpenVersions();

        cut.WaitUntil(() => Assert.That(cut.Find("[role=tabpanel] .lt-empty__title").TextContent, Is.EqualTo("You may not read this tree's versioning")));
        Assert.Multiple(() =>
        {
            Assert.That(cut.FindAll("#lt-schema-registry-title"), Is.Empty);
            Assert.That(Schema.CountOf("GetRegistry"), Is.Zero);
            Assert.That(Schema.CountOf("GetVersionConfig"), Is.Zero);
        });
    }

    [Test]
    public void A_catalog_read_the_cluster_refuses_is_explained_and_the_old_catalog_is_not_kept()
    {
        UseFullSupport();
        var cut = OpenVersions();
        cut.WaitUntil(() => Assert.That(cut.FindAll("ol.lt-schema-hops"), Has.Count.EqualTo(1)));

        Schema.Faults["GetRegistry"] = new LatticeAuthorizationDeniedException("denied");
        Button(cut, "Refresh definitions").Click();

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find(".lt-schema-editor .lt-schema-error").TextContent, Is.EqualTo("You are not permitted to read the schema definitions."));
            Assert.That(cut.FindAll("ol.lt-schema-hops"), Is.Empty);
            Assert.That(cut.Markup, Does.Not.Contain("Quantity spelled out"));
        });
    }

    [Test]
    public void Advancing_past_every_path_warns_which_registered_versions_would_be_stranded()
    {
        UseFullSupport();
        var cut = OpenVersions();
        cut.WaitUntil(() => Assert.That(cut.FindAll("ol.lt-schema-hops"), Has.Count.EqualTo(1)));
        Button(cut, "Advance target version...").Click();
        cut.Find("[role=tabpanel] input[inputmode=numeric]").Input("4");
        Button(cut, "Advance...").Click();

        cut.WaitUntil(() =>
        {
            var dialog = cut.Find("[role=alertdialog]");
            Assert.That(dialog.TextContent, Does.Contain("Materialised views over this tree are not refreshed"));
            Assert.That(dialog.QuerySelectorAll(".lt-schema-error").Select(warning => warning.TextContent), Is.EqualTo(new[]
            {
                "Version 4 is not registered for family 7. Register it, and a migration path to it, before advancing.",
                "Registered versions 1, 2 and 3 have no migration path to version 4. A value stamped with one of them could not be read once the target advances, and a migration would stop at it.",
            }));
        });
    }

    [Test]
    public void Advancing_along_a_registered_path_raises_no_warning()
    {
        var catalog = OrdersCatalog() with
        {
            Descriptors = [.. OrdersCatalog().Descriptors, new(7, 4, "Split")],
            Upcasters = [.. OrdersCatalog().Upcasters, new() { SchemaId = 7, FromVersion = 3, ToVersion = 4, Transform = LatticeValueTransform.Passthrough() }],
        };
        UseFullSupport(catalog);
        var cut = OpenVersions();
        cut.WaitUntil(() => Assert.That(cut.FindAll("ol.lt-schema-hops li.lt-schema-hops__hop"), Has.Count.EqualTo(3)));
        Button(cut, "Advance target version...").Click();
        Button(cut, "Advance...").Click();

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find("[role=alertdialog]").TextContent, Does.Contain("Advance to version 4"));
            Assert.That(cut.FindAll("[role=alertdialog] .lt-schema-error"), Is.Empty);
        });
    }

    [Test]
    public void Without_a_reported_catalog_an_advance_is_not_judged()
    {
        UseEstate();
        var cut = OpenVersions();
        cut.WaitUntil(() => Assert.That(cut.FindAll("button").Count(button => button.TextContent.Trim() == "Advance target version..."), Is.EqualTo(1)));
        Button(cut, "Advance target version...").Click();
        Button(cut, "Advance...").Click();

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find("[role=alertdialog]").TextContent, Does.Contain("Materialised views over this tree are not refreshed"));
            Assert.That(cut.FindAll("[role=alertdialog] .lt-schema-error"), Is.Empty);
        });
    }

    [Test]
    public void A_migration_review_says_it_stops_unchanged_at_a_value_it_cannot_upgrade()
    {
        UseEstate();
        var cut = OpenVersions();
        cut.WaitUntil(() => Assert.That(cut.FindAll("button").Count(button => button.TextContent.Trim() == "Migrate stored values..."), Is.EqualTo(1)));
        Button(cut, "Migrate stored values...").Click();

        cut.WaitUntil(() => Assert.That(cut.Find("[role=dialog]").TextContent, Does.Contain(
            "If a stored value cannot be upgraded to version 3 - no migration path leads there, or it is stamped above it - the migration stops at that value, names it and leaves every value as it was.")));
    }

    [Test]
    public void Advance_and_migrate_review_says_the_same()
    {
        UseEstate();
        var cut = OpenVersions();
        cut.WaitUntil(() => Assert.That(cut.FindAll("button").Count(button => button.TextContent.Trim() == "Advance target version..."), Is.EqualTo(1)));
        Button(cut, "Advance target version...").Click();
        Button(cut, "Advance and migrate...").Click();

        cut.WaitUntil(() => Assert.That(cut.Find("[role=alertdialog]").TextContent, Does.Contain("If a stored value cannot be upgraded to version 4")));
    }

    [Test]
    public void A_versioned_trees_policy_says_it_covers_every_version()
    {
        UseEstate();

        var cut = RenderAt<SchemaTreePage>("schema/orders");

        cut.WaitUntil(() => Assert.That(cut.Find("[role=tabpanel]").TextContent, Does.Contain(
            "This tree is versioned, and this one policy covers values of every version: rules cannot be scoped to a version.")));
    }

    [Test]
    public void An_unversioned_trees_policy_does_not()
    {
        UseEstate();
        Schema.Policies["scratch"] = SchemaTestData.Policy();

        var cut = RenderAt<SchemaTreePage>("schema/scratch");

        cut.WaitUntil(() => Assert.That(cut.FindAll("[role=tabpanel] tbody tr"), Has.Count.EqualTo(3)));
        Assert.That(cut.Find("[role=tabpanel]").TextContent, Does.Not.Contain("covers values of every version"));
    }

    [Test]
    public void Stranded_text_reads_for_one_version_and_for_several()
    {
        Assert.Multiple(() =>
        {
            Assert.That(SchemaVersionsPanel.StrandedText([2], 5), Is.EqualTo(
                "Registered version 2 has no migration path to version 5. A value stamped with it could not be read once the target advances, and a migration would stop at it."));
            Assert.That(SchemaVersionsPanel.StrandedText([1, 2], 5), Does.StartWith("Registered versions 1 and 2 have no migration path to version 5."));
        });
    }

    [Test]
    public void Reach_text_covers_every_reach()
    {
        Assert.That(Enum.GetValues<SchemaVersionReach>().Select(reach => SchemaRegistryPanel.ReachText(new SchemaVersionRow(1, null, reach, [1, 2]))),
            Is.EqualTo(new[]
            {
                "Not this tree's family",
                "Reads as stored: this is the target version",
                "Upgraded as it is read, through versions 1, 2",
                "Cannot be read: no migration path leads to the target",
                "Cannot be read: it is newer than the target",
            }));
    }
}
