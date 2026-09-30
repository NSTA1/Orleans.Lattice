using Bunit;
using Orleans.Lattice.Explorer.UI.Areas.Schema;
using Orleans.Lattice.Explorer.UI.Design.Tokens;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;
using Orleans.Lattice.Schema;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Schema;

/// <summary>
/// The Versions tab: get, set, clear, advance, migrate, and advance and migrate;
/// the irreversible verbs are confirmed by naming the tree; migrations are staged
/// operations that move to their status page; the #1257 limitation is stated;
/// versioning that is not registered, a reader, and a running operation.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class SchemaVersionsPanelTests : SchemaTestContext
{
    private IRenderedComponent<SchemaTreePage> Open(string tree = "orders", LtBreakpoint? band = null)
    {
        var cut = RenderAt<SchemaTreePage>($"schema/{tree}?tab=versions", band);
        cut.WaitUntil(() => Assert.That(cut.FindAll("[role=tabpanel] .lt-skeleton"), Is.Empty));
        return cut;
    }

    private static AngleSharp.Dom.IElement Button(IRenderedComponent<SchemaTreePage> cut, string text) =>
        cut.FindAll("button").Single(button => button.TextContent.Trim() == text);

    private static void ClickWhenShown(IRenderedComponent<SchemaTreePage> cut, string text)
    {
        cut.WaitUntil(() => Assert.That(cut.FindAll("button").Count(button => button.TextContent.Trim() == text), Is.EqualTo(1)));
        Button(cut, text).Click();
    }

    private static void ConfirmNaming(IRenderedComponent<SchemaTreePage> cut, string tree)
    {
        cut.WaitUntil(() => Assert.That(cut.FindAll("[role=alertdialog]"), Has.Count.EqualTo(1)));
        Assert.That(cut.Find("[role=alertdialog] button[type=submit]").HasAttribute("disabled"), Is.True, "the tree must be named first");
        cut.Find("[role=alertdialog] input").Input(tree);
        cut.Find("[role=alertdialog] form").Submit();
    }

    [Test]
    public void It_shows_the_family_target_and_strict_ingest_and_says_what_it_cannot_show()
    {
        UseEstate();

        var cut = Open();

        cut.WaitUntil(() =>
        {
            var terms = cut.FindAll("[role=tabpanel] dl.lt-dl dd").Select(value => value.TextContent.Trim()).ToArray();
            Assert.That(terms[..2], Is.EqualTo(new[] { "7", "3" }));
            Assert.That(terms[2], Does.StartWith("On: a replicated or restored value"));
            Assert.That(cut.Find(".lt-schema-note").TextContent, Does.Contain("cannot show what differs between two versions"));
            Assert.That(cut.Find(".lt-schema-note").TextContent, Does.Contain("Advancing the target cannot be undone"));
            Assert.That(cut.FindAll("[role=tabpanel] button").Select(button => button.TextContent.Trim()), Is.EqualTo(new[]
            {
                "Advance target version...", "Migrate stored values...", "Change config", "Turn off versioning",
            }));
        });
    }

    [Test]
    public void A_default_version_config_reads_as_unversioned_and_offers_to_turn_versioning_on()
    {
        // The cluster reads an absent config as family 0 at version 0 (#3985).
        UseTrees("scratch");
        Schema.UnversionedReadsAsDefault = true;

        var cut = Open("scratch");

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find("[role=tabpanel] .lt-empty__title").TextContent, Is.EqualTo("Unversioned"));
            Assert.That(cut.FindAll("button").Count(button => button.TextContent.Trim() == "Turn on versioning"), Is.EqualTo(1));
            Assert.That(cut.Find(".lt-schema-meta").TextContent, Does.Contain("Versioning: Unversioned"));
        });
    }

    [Test]
    public void Turning_versioning_on_sets_a_first_config()
    {
        UseTrees("scratch");
        var cut = Open("scratch");

        ClickWhenShown(cut, "Turn on versioning");
        cut.Find("[role=switch]").Click();
        Button(cut, "Save config").Click();

        cut.WaitUntil(() =>
        {
            Assert.That(Schema.Versions["scratch"], Is.EqualTo(new LatticeSchemaVersionConfig(1, 1, strictIngest: true)));
            Assert.That(ToastService.Toasts.Single().Message, Is.EqualTo("The version config of scratch is saved."));
            Assert.That(cut.FindAll("[role=tabpanel] dl.lt-dl dd")[1].TextContent, Is.EqualTo("1"));
        });
    }

    [TestCase("x", "1", "Enter the schema family as a whole number.")]
    [TestCase("1", "0", "Enter a target version of 1 or more.")]
    [TestCase("1", "-2", "Enter a target version of 1 or more.")]
    public void A_config_that_does_not_parse_is_explained(string family, string version, string expected)
    {
        UseEstate();
        var cut = Open();
        ClickWhenShown(cut, "Change config");

        var inputs = cut.FindAll("[role=tabpanel] input[inputmode=numeric]");
        Assert.That(inputs.Select(input => input.GetAttribute("value")), Is.EqualTo(new[] { "7", "3" }), "seeded from the applied config");
        inputs[0].Input(family);
        cut.FindAll("[role=tabpanel] input[inputmode=numeric]")[1].Input(version);
        Button(cut, "Save config").Click();

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find(".lt-schema-error").TextContent, Is.EqualTo(expected));
            Assert.That(Schema.CountOf("SetVersionConfig"), Is.Zero);
        });
    }

    [Test]
    public void Advancing_is_checked_confirmed_by_naming_the_tree_and_irreversible()
    {
        UseEstate();
        var cut = Open();
        ClickWhenShown(cut, "Advance target version...");

        Assert.That(cut.Find("[role=tabpanel] input[inputmode=numeric]").GetAttribute("value"), Is.EqualTo("4"), "one step up by default");
        cut.Find("[role=tabpanel] input[inputmode=numeric]").Input("3");
        Button(cut, "Advance...").Click();
        cut.WaitUntil(() => Assert.That(cut.Find(".lt-field__error").TextContent, Does.Contain("Enter a version above 3.")));

        cut.Find("[role=tabpanel] input[inputmode=numeric]").Input("5");
        Button(cut, "Advance...").Click();
        cut.WaitUntil(() => Assert.That(cut.Find("[role=alertdialog]").TextContent, Does.Contain("cannot be lowered again")));
        ConfirmNaming(cut, "orders");

        cut.WaitUntil(() =>
        {
            Assert.That(Schema.Versions["orders"].TargetVersion, Is.EqualTo(5u));
            Assert.That(ToastService.Toasts.Single().Message, Is.EqualTo("orders now stamps new writes with version 5."));
            Assert.That(cut.FindAll("[role=tabpanel] dl.lt-dl dd")[1].TextContent, Is.EqualTo("5"));
        });
    }

    [Test]
    public void A_refused_advance_is_explained_in_the_form()
    {
        UseEstate();
        Schema.Faults["AdvanceTargetVersion"] = new LatticeAuthorizationDeniedException("denied");
        var cut = Open();
        ClickWhenShown(cut, "Advance target version...");
        Button(cut, "Advance...").Click();
        ConfirmNaming(cut, "orders");

        cut.WaitUntil(() => Assert.That(cut.Find(".lt-field__error").TextContent, Does.Contain("You are not permitted to advance the target version.")));
    }

    [Test]
    public void Advance_and_migrate_is_a_staged_operation_that_opens_its_status_page()
    {
        UseEstate();
        Schema.OperationGate = new TaskCompletionSource<LatticeSchemaRemediationReport>();
        var cut = Open();
        ClickWhenShown(cut, "Advance target version...");
        Button(cut, "Advance and migrate...").Click();
        cut.WaitUntil(() => Assert.That(cut.Find("[role=alertdialog]").TextContent, Does.Contain("You can follow the migration on the Remediation tab")));
        ConfirmNaming(cut, "orders");

        cut.WaitUntil(() =>
        {
            Assert.That(Navigation.Uri, Does.EndWith("schema/orders?tab=remediation"));
            Assert.That(Operations.Find("orders")!.Kind, Is.EqualTo(SchemaOperationKind.AdvanceAndMigrate));
            Assert.That(Operations.Find("orders")!.Summary, Is.EqualTo("Advancing to version 4 and migrating every value"));
            Assert.That(Operations.Find("orders")!.IsActive, Is.True);
            Assert.That(Schema.Versions["orders"].TargetVersion, Is.EqualTo(4u));
        });
    }

    [Test]
    public void Migrating_is_reviewed_then_runs_in_the_background()
    {
        UseEstate();
        Schema.OperationGate = new TaskCompletionSource<LatticeSchemaRemediationReport>();
        var cut = Open();
        ClickWhenShown(cut, "Migrate stored values...");

        cut.WaitUntil(() => Assert.That(cut.Find("[role=dialog]").TextContent, Does.Contain("Every stored value is rewritten at the current target version, 3")));
        Button(cut, "Start migration").Click();

        cut.WaitUntil(() =>
        {
            Assert.That(Schema.CountOf("MigrateToTargetVersion"), Is.EqualTo(1));
            Assert.That(Operations.Find("orders")!.Summary, Is.EqualTo("Migrating every value to version 3"));
            Assert.That(Navigation.Uri, Does.EndWith("schema/orders?tab=remediation"));
        });
    }

    [Test]
    public void A_running_operation_is_named_with_a_link_to_its_status_and_blocks_another()
    {
        UseEstate();
        Operations.Start("orders", SchemaOperationKind.Migrate, "Migrating every value to version 3", _ => new TaskCompletionSource<LatticeSchemaRemediationReport>().Task);

        var cut = Open();

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find(".lt-schema-running").TextContent, Does.Contain("Migrating every value to version 3 is running."));
            Assert.That(cut.Find(".lt-schema-running a").GetAttribute("href"), Is.EqualTo("schema/orders?tab=remediation"));
            Assert.That(Button(cut, "Migrate stored values...").HasAttribute("disabled"), Is.True);
        });
    }

    [Test]
    public void Turning_versioning_off_needs_the_tree_named()
    {
        UseEstate();
        var cut = Open();
        ClickWhenShown(cut, "Turn off versioning");
        cut.WaitUntil(() => Assert.That(cut.Find("[role=alertdialog]").TextContent, Does.Contain("New writes will no longer be stamped")));
        ConfirmNaming(cut, "orders");

        cut.WaitUntil(() =>
        {
            Assert.That(Schema.Versions.ContainsKey("orders"), Is.False);
            Assert.That(ToastService.Toasts.Single().Message, Is.EqualTo("orders is no longer versioned."));
            Assert.That(cut.Find("[role=tabpanel] .lt-empty__title").TextContent, Is.EqualTo("Unversioned"));
        });
    }

    [Test]
    public void A_cluster_without_versioning_says_so()
    {
        UseEstate();
        Schema.VersioningRegistered = false;

        var cut = Open();

        cut.WaitUntil(() => Assert.That(cut.Find("[role=tabpanel] .lt-empty__title").TextContent, Is.EqualTo("Versioning is not available")));
    }

    [Test]
    public void A_read_that_fails_can_be_tried_again()
    {
        UseEstate();
        Schema.Faults["GetVersionConfig"] = new LatticeAuthorizationDeniedException("denied");
        var cut = Open();
        cut.WaitUntil(() => Assert.That(cut.Find("[role=tabpanel] .lt-empty__body").TextContent, Is.EqualTo("You are not permitted to read the version config.")));

        Schema.Faults.Remove("GetVersionConfig");
        cut.Find("[role=tabpanel] .lt-empty__actions button").Click();

        cut.WaitUntil(() => Assert.That(cut.FindAll("[role=tabpanel] dl.lt-dl dd"), Has.Count.EqualTo(3)));
    }

    [Test]
    public void A_reader_sees_the_config_but_no_change_controls()
    {
        UseEstate();
        Schema.Capabilities["orders"] = FakeSchemaControl.ReadOnly;

        var cut = Open();

        cut.WaitUntil(() =>
        {
            Assert.That(cut.FindAll("[role=tabpanel] dl.lt-dl dd"), Has.Count.EqualTo(3));
            Assert.That(cut.FindAll("[role=tabpanel] button"), Is.Empty);
        });
    }

    [Test]
    public void A_caller_who_may_not_read_versioning_is_told_so()
    {
        UseEstate();
        Schema.Capabilities["orders"] = tree => FakeSchemaControl.ReadOnly(tree) with { CanViewVersionConfig = false };

        var cut = Open();

        cut.WaitUntil(() => Assert.That(cut.Find("[role=tabpanel] .lt-empty__title").TextContent, Is.EqualTo("You may not read this tree's versioning")));
    }

    [Test]
    public void Below_the_small_breakpoint_the_migration_review_is_a_sheet()
    {
        UseEstate();
        var cut = Open(band: LtBreakpoint.Compact);

        ClickWhenShown(cut, "Migrate stored values...");

        cut.WaitUntil(() => Assert.That(cut.Find("[role=dialog]").ClassList, Does.Contain("lt-dialog--end")));
        Button(cut, "Cancel").Click();
        cut.WaitUntil(() => Assert.That(cut.FindAll("[role=dialog]"), Is.Empty));
    }

    [Test]
    public void A_number_parses_only_as_a_whole_non_negative_value()
    {
        Assert.Multiple(() =>
        {
            Assert.That(SchemaVersionsPanel.TryParseNumber(" 12 ", out var twelve) && twelve == 12, Is.True);
            Assert.That(SchemaVersionsPanel.TryParseNumber("-1", out _), Is.False);
            Assert.That(SchemaVersionsPanel.TryParseNumber("1.0", out _), Is.False);
            Assert.That(SchemaVersionsPanel.TryParseNumber("4294967296", out _), Is.False);
        });
    }
}
