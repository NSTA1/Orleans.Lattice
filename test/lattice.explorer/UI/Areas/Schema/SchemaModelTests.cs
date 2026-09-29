using Orleans.Lattice.Explorer.UI.Areas.Schema;
using Orleans.Lattice.Explorer.UI.Navigation.Address;
using Orleans.Lattice.Explorer.UI.Transport;
using Orleans.Lattice.Schema;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Schema;

/// <summary>
/// The area's pure models: addresses and tabs, the rule and transform editors'
/// drafts, the text the area shows, and how faults read.
/// </summary>
[TestFixture]
public sealed class SchemaModelTests
{
    [Test]
    public void Addresses_put_the_tree_in_the_path_and_the_tab_in_the_query()
    {
        Assert.Multiple(() =>
        {
            Assert.That(SchemaAddresses.Directory.Format(), Is.EqualTo("/schema"));
            Assert.That(SchemaAddresses.AllTrees.Format(), Is.EqualTo("/schema?show=all"));
            Assert.That(SchemaAddresses.Tree("a/crm/orders").Format(), Is.EqualTo("/schema/a/crm/orders"));
            Assert.That(SchemaAddresses.Tree("orders", SchemaTabs.Policy).Format(), Is.EqualTo("/schema/orders"));
            Assert.That(SchemaAddresses.Tree("orders", SchemaTabs.DeadLetters).Format(), Is.EqualTo("/schema/orders?tab=dead-letters"));
            Assert.That(SchemaAddresses.StartScan("orders").Format(), Is.EqualTo("/schema/orders?tab=compliance&scan=start"));
            Assert.That(SchemaAddresses.App("crm").Format(), Is.EqualTo("/apps/crm"));
            Assert.That(SchemaAddresses.TreeIdOf(ExplorerAddress.Parse("/t/acme/schema/a/crm/orders?tab=versions")), Is.EqualTo("a/crm/orders"));
            Assert.That(SchemaAddresses.TreeIdOf(ExplorerAddress.Parse("/data/orders")), Is.Null);
            Assert.That(SchemaAddresses.TreeIdOf(SchemaAddresses.Directory), Is.Null);
            Assert.That(() => SchemaAddresses.Tree(""), Throws.ArgumentException);
            Assert.That(() => SchemaAddresses.App(""), Throws.ArgumentException);
        });
    }

    [TestCase(null, SchemaTabs.Policy)]
    [TestCase("versions", SchemaTabs.Versions)]
    [TestCase("compliance", SchemaTabs.Compliance)]
    [TestCase("remediation", SchemaTabs.Remediation)]
    [TestCase("dead-letters", SchemaTabs.DeadLetters)]
    [TestCase("Versions", SchemaTabs.Policy)]
    [TestCase("nonsense", SchemaTabs.Policy)]
    public void A_tab_query_value_names_a_known_tab_or_the_policy(string? value, string expected) =>
        Assert.That(SchemaTabs.Parse(value), Is.EqualTo(expected));

    [Test]
    public void The_tabs_are_the_five_the_spec_names_in_order() =>
        Assert.That(SchemaTabs.All.Select(tab => tab.Id), Is.EqualTo(new[] { "policy", "versions", "compliance", "remediation", "dead-letters" }));

    [Test]
    public void The_rule_draft_builds_each_kind_it_offers()
    {
        var draft = new SchemaRuleDraft();
        Assert.That(draft.IsDirty, Is.False);

        Assert.That(draft.TryBuild(out var utf8, out _), Is.True);
        draft.Kind = SchemaRuleDraftKind.Json;
        draft.Description = " whole docs ";
        Assert.That(draft.TryBuild(out var json, out _), Is.True);
        draft.Kind = SchemaRuleDraftKind.MaxLength;
        draft.MaxLength = "1024";
        Assert.That(draft.TryBuild(out var size, out _), Is.True);
        draft.Kind = SchemaRuleDraftKind.Pattern;
        draft.Pattern = "^a+$";
        draft.MemberPath = "name";
        Assert.That(draft.TryBuild(out var pattern, out _), Is.True);

        Assert.Multiple(() =>
        {
            Assert.That(utf8.EncodingKind, Is.EqualTo(LatticeSchemaEncodingKind.Utf8));
            Assert.That(json.EncodingKind, Is.EqualTo(LatticeSchemaEncodingKind.Json));
            Assert.That(json.Description, Is.EqualTo("whole docs"));
            Assert.That(size.MaxByteLength, Is.EqualTo(1024));
            Assert.That(pattern.Kind, Is.EqualTo(LatticeSchemaRuleKind.Regex));
            Assert.That(pattern.RegexPattern, Is.EqualTo("^a+$"));
            Assert.That(pattern.MemberPath, Is.EqualTo("name"));
            Assert.That(draft.IsDirty, Is.True);
        });

        draft.Reset();
        Assert.That(draft.IsDirty, Is.False);
    }

    [TestCase(nameof(SchemaRuleDraftKind.MaxLength), "", "", "Enter the largest size, in bytes, as a whole number.")]
    [TestCase(nameof(SchemaRuleDraftKind.MaxLength), "-1", "", "Enter the largest size, in bytes, as a whole number.")]
    [TestCase(nameof(SchemaRuleDraftKind.MaxLength), "1.5", "", "Enter the largest size, in bytes, as a whole number.")]
    [TestCase(nameof(SchemaRuleDraftKind.Pattern), "", "  ", "Enter the pattern values must match.")]
    [TestCase(nameof(SchemaRuleDraftKind.Pattern), "", "([a-z", "That pattern is not a valid regular expression.")]
    public void The_rule_draft_explains_what_is_missing(string kind, string size, string pattern, string expected)
    {
        var draft = new SchemaRuleDraft { Kind = Enum.Parse<SchemaRuleDraftKind>(kind), MaxLength = size, Pattern = pattern };

        Assert.Multiple(() =>
        {
            Assert.That(draft.TryBuild(out _, out var error), Is.False);
            Assert.That(error, Is.EqualTo(expected));
        });
    }

    [Test]
    public void The_transform_draft_builds_a_pass_through_of_its_steps_in_order()
    {
        var draft = new SchemaTransformDraft { Kind = SchemaTransformStepKind.Set, Path = "region", ValueKind = SchemaConstantKind.Text, Value = " eu " };
        Assert.That(draft.TryAdd(out _), Is.True);
        draft.Kind = SchemaTransformStepKind.Set;
        draft.Path = "count";
        draft.ValueKind = SchemaConstantKind.Number;
        draft.Value = "42";
        Assert.That(draft.TryAdd(out _), Is.True);
        draft.Path = "ratio";
        draft.Value = "2.5";
        Assert.That(draft.TryAdd(out _), Is.True);
        draft.Path = "active";
        draft.ValueKind = SchemaConstantKind.Boolean;
        draft.Value = "true";
        Assert.That(draft.TryAdd(out _), Is.True);
        draft.Path = "legacy";
        draft.ValueKind = SchemaConstantKind.Null;
        Assert.That(draft.TryAdd(out _), Is.True);
        draft.Kind = SchemaTransformStepKind.Remove;
        draft.Path = "tmp";
        Assert.That(draft.TryAdd(out _), Is.True);
        draft.Kind = SchemaTransformStepKind.Rename;
        draft.Path = "nm";
        draft.ToPath = "name";
        Assert.That(draft.TryAdd(out _), Is.True);

        Assert.That(draft.TryBuild(out var transform, out _), Is.True);

        Assert.Multiple(() =>
        {
            Assert.That(draft.Steps.Select(step => step.Describe()), Is.EqualTo(new[]
            {
                "Set region to \" eu \"",
                "Set count to 42",
                "Set ratio to 2.5",
                "Set active to true",
                "Set legacy to null",
                "Remove tmp",
                "Rename nm to name",
            }));
            Assert.That(transform.Kind, Is.EqualTo(LatticeValueTransformKind.Passthrough));
            Assert.That(transform.Children, Has.Length.EqualTo(7));
            Assert.That(transform.Children![0], Is.EqualTo(LatticeValueTransform.SetMember("region", LatticeValueTransform.Const(LatticeConstant.Text(" eu ")))));
            Assert.That(transform.Children[1], Is.EqualTo(LatticeValueTransform.SetMember("count", LatticeValueTransform.Const(LatticeConstant.Integer(42)))));
            Assert.That(transform.Children[2], Is.EqualTo(LatticeValueTransform.SetMember("ratio", LatticeValueTransform.Const(LatticeConstant.Real(2.5)))));
            Assert.That(transform.Children[3], Is.EqualTo(LatticeValueTransform.SetMember("active", LatticeValueTransform.Const(LatticeConstant.Bool(true)))));
            Assert.That(transform.Children[4], Is.EqualTo(LatticeValueTransform.SetMember("legacy", LatticeValueTransform.Const(LatticeConstant.Null()))));
            Assert.That(transform.Children[5], Is.EqualTo(LatticeValueTransform.DropMember("tmp")));
            Assert.That(transform.Children[6], Is.EqualTo(LatticeValueTransform.RenameMember("nm", "name")));
            Assert.That(draft.Path, Is.Empty, "the builder clears after each step");
        });

        draft.RemoveAt(99);
        draft.RemoveAt(0);
        Assert.That(draft.Steps, Has.Count.EqualTo(6));
        draft.Clear();
        Assert.That(draft.Steps, Is.Empty);
    }

    [TestCase("Set", "", "", "Text", "", "Enter the member the step acts on.")]
    [TestCase("Rename", "a", " ", "Text", "", "Enter the member's new name.")]
    [TestCase("Rename", "a", "a", "Text", "", "A member cannot be renamed to its own name.")]
    [TestCase("Set", "a", "", "Number", "many", "Enter a number, such as 42 or 2.5.")]
    [TestCase("Set", "a", "", "Number", "NaN", "Enter a number, such as 42 or 2.5.")]
    [TestCase("Set", "a", "", "Boolean", "yes", "Enter true or false.")]
    public void The_transform_draft_explains_what_is_missing(
        string kind, string path, string to, string valueKind, string value, string expected)
    {
        var draft = new SchemaTransformDraft
        {
            Kind = Enum.Parse<SchemaTransformStepKind>(kind),
            Path = path,
            ToPath = to,
            ValueKind = Enum.Parse<SchemaConstantKind>(valueKind),
            Value = value,
        };

        Assert.Multiple(() =>
        {
            Assert.That(draft.TryAdd(out var error), Is.False);
            Assert.That(error, Is.EqualTo(expected));
            Assert.That(draft.Steps, Is.Empty);
        });
    }

    [Test]
    public void An_empty_transform_is_not_built()
    {
        var draft = new SchemaTransformDraft();

        Assert.Multiple(() =>
        {
            Assert.That(draft.TryBuild(out _, out var error), Is.False);
            Assert.That(error, Is.EqualTo("Add at least one step."));
        });
    }

    [Test]
    public void Rules_versions_and_policies_read_as_plain_text()
    {
        Assert.Multiple(() =>
        {
            Assert.That(SchemaFormat.RuleKind(LatticeSchemaRule.Utf8()), Is.EqualTo("UTF-8"));
            Assert.That(SchemaFormat.RuleKind(LatticeSchemaRule.Json()), Is.EqualTo("JSON"));
            Assert.That(SchemaFormat.RuleKind(LatticeSchemaRule.MaxLength(1)), Is.EqualTo("Size"));
            Assert.That(SchemaFormat.RuleKind(LatticeSchemaRule.Regex("x")), Is.EqualTo("Pattern"));
            Assert.That(SchemaFormat.RuleKind(new LatticeSchemaRule { Kind = LatticeSchemaRuleKind.Structured }), Is.EqualTo("Structured"));
            Assert.That(SchemaFormat.RuleDetail(LatticeSchemaRule.MaxLength(4096, "fits a page")), Is.EqualTo("The value is at most 4,096 bytes - fits a page"));
            Assert.That(SchemaFormat.RuleDetail(LatticeSchemaRule.Regex("^x$", "code")), Is.EqualTo("Member code matches ^x$"));
            Assert.That(SchemaFormat.RuleDetail(LatticeSchemaRule.Regex("^x$")), Is.EqualTo("The value matches ^x$"));
            Assert.That(SchemaFormat.RuleDetail(LatticeSchemaRule.Utf8()), Is.EqualTo("The value is well-formed UTF-8"));
            Assert.That(SchemaFormat.RuleDetail(LatticeSchemaRule.Json()), Is.EqualTo("The value is one JSON document"));
            Assert.That(SchemaFormat.RuleDetail(new LatticeSchemaRule { Kind = LatticeSchemaRuleKind.Structured }), Does.StartWith("A structured predicate"));
            Assert.That(SchemaFormat.Version(new LatticeSchemaVersionConfig(7, 3, strictIngest: true)), Is.EqualTo("family 7 at version 3, strict ingest"));
            Assert.That(SchemaFormat.Policy(new LatticeSchemaPolicy([LatticeSchemaRule.Utf8()])), Is.EqualTo("1 rule"));
            Assert.That(SchemaFormat.Policy(SchemaTestData.Policy()), Is.EqualTo("3 rules"));
            Assert.That(SchemaFormat.Count(1204, "tree"), Is.EqualTo("1,204 trees"));
            Assert.That(SchemaFormat.Count(1, "entry", "entries"), Is.EqualTo("1 entry"));
            Assert.That(SchemaFormat.Time(new DateTimeOffset(2026, 3, 4, 5, 6, 7, TimeSpan.FromHours(2))), Is.EqualTo("2026-03-04 03:06:07 UTC"));
            Assert.That(SchemaFormat.Size(2), Is.EqualTo("2 bytes"));
        });
    }

    [Test]
    public void Phases_and_sources_read_as_words()
    {
        Assert.Multiple(() =>
        {
            Assert.That(Enum.GetValues<LatticeSchemaRemediationPhase>().Select(SchemaFormat.Phase), Is.EqualTo(new[]
            {
                "Idle", "Checking every value", "Building the remediated copy", "Cutting over", "Completed", "Aborted",
            }));
            Assert.That(Enum.GetValues<LatticeSchemaDeadLetterSource>().Select(SchemaFormat.Source), Is.EqualTo(new[]
            {
                "Replication", "Restore", "Local write",
            }));
        });
    }

    [Test]
    public void A_preview_is_text_when_it_decodes_and_hexadecimal_when_it_does_not()
    {
        Assert.Multiple(() =>
        {
            Assert.That(SchemaFormat.Preview(null), Is.Empty);
            Assert.That(SchemaFormat.Preview([]), Is.Empty);
            Assert.That(SchemaFormat.Preview("<b>hi</b>"u8.ToArray()), Is.EqualTo("<b>hi</b>"));
            Assert.That(SchemaFormat.Preview([0xFF, 0x00]), Is.EqualTo("FF00"));
            Assert.That(SchemaFormat.Preview(new byte[SchemaFormat.PreviewCharacters + 10]), Has.Length.EqualTo(SchemaFormat.PreviewCharacters + 3));
        });
    }

    [Test]
    public void Faults_read_as_one_plain_sentence()
    {
        Assert.Multiple(() =>
        {
            Assert.That(SchemaFailure.Describe(new LatticeAuthorizationDeniedException("no"), "set the policy"), Is.EqualTo("You are not permitted to set the policy."));
            Assert.That(SchemaFailure.Describe(new NotSupportedException("x"), "set the policy"), Is.EqualTo(SchemaFailure.NotServed));
            Assert.That(SchemaFailure.Describe(new ShellTransportException("x", true, new Exception()), "set the policy"), Is.EqualTo(SchemaFailure.NotAnswering));
            Assert.That(SchemaFailure.Describe(new ShellTransportException("gone", false, new Exception()), "set the policy"), Is.EqualTo("The cluster could not set the policy. gone."));
            Assert.That(SchemaFailure.Describe(new ArgumentException("Rule 2 is invalid."), "set the policy"), Is.EqualTo("Could not set the policy. Rule 2 is invalid."));
            Assert.That(SchemaFailure.Describe(new InvalidOperationException(" "), "set the policy"), Is.EqualTo("Could not set the policy. No reason was given."));
            Assert.That(() => SchemaFailure.Describe(new Exception(), " "), Throws.ArgumentException);
        });
    }

    [Test]
    public void An_app_tree_id_is_the_structural_app_namespace()
    {
        Assert.Multiple(() =>
        {
            Assert.That(SchemaAppDeclaration.TreeIdFor("crm", "orders"), Is.EqualTo("a/crm/orders"));
            Assert.That(() => SchemaAppDeclaration.TreeIdFor("", "orders"), Throws.ArgumentException);
        });
    }

    [Test]
    public void A_compliance_result_is_compliant_only_under_a_policy_with_no_failures()
    {
        Assert.Multiple(() =>
        {
            Assert.That(new SchemaComplianceResult(SchemaTestData.Report("t", 5, 0), DateTimeOffset.UnixEpoch).IsCompliant, Is.True);
            Assert.That(new SchemaComplianceResult(SchemaTestData.Report("t", 5, 1), DateTimeOffset.UnixEpoch).IsCompliant, Is.False);
            Assert.That(new SchemaComplianceResult(LatticeSchemaComplianceReport.Ungoverned("t"), DateTimeOffset.UnixEpoch).IsCompliant, Is.False);
        });
    }

    [Test]
    public void The_ledger_keeps_the_last_scan_per_tree_and_announces_it()
    {
        var ledger = new SchemaComplianceLedger();
        var recorded = new List<string>();
        ledger.Recorded += recorded.Add;

        ledger.Record("orders", SchemaTestData.Report("orders", 1, 0), DateTimeOffset.UnixEpoch);

        Assert.Multiple(() =>
        {
            Assert.That(ledger.Find("orders")!.Report.ScannedCount, Is.EqualTo(1));
            Assert.That(recorded, Is.EqualTo(new[] { "orders" }));
        });

        ledger.Forget("orders");
        Assert.That(ledger.Find("orders"), Is.Null);
    }
}
