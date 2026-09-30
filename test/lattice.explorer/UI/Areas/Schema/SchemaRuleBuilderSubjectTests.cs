using Bunit;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;
using Orleans.Lattice.Explorer.UI.Areas.Schema;
using Orleans.Lattice.Schema;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Schema;

/// <summary>
/// Issue #3985 in the rule builder: a presence card fits itself to what the
/// sample holds at its subject, so its claim, its "Reads:" sentence and the check
/// against the sample agree on an object or a list; the member list and the path
/// field say how to choose a member; and a policy with no rules is not offered
/// for saving.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class SchemaRuleBuilderSubjectTests : SchemaRuleBuilderTestBase
{
    private const string StructuralSwitch = "It holds an object or a list";

    private void UseTasks()
    {
        UseTrees("tasks");
        Data.Use(
            "tasks",
            ("tasks/t-001", "{\"title\":\"Calibrate press 3\",\"column\":\"doing\",\"meta\":{\"owner\":\"ana\"}}"),
            ("tasks/t-002", "{\"title\":\"Order bearings\",\"column\":\"todo\",\"meta\":{\"owner\":\"bo\"}}"),
            ("tasks/t-003", "{\"title\":\"Ship crate\",\"column\":\"done\",\"meta\":{}}"));
    }

    private static string GalleryExample(IRenderedComponent<SchemaTreePage> cut, string title) =>
        cut.FindAll(".lt-schema-gallery__option")
            .Single(option => option.QuerySelector(".lt-schema-gallery__title")!.TextContent.Trim() == title)
            .QuerySelector(".lt-schema-gallery__example")!.TextContent.Trim();

    private static string Reads(IRenderedComponent<SchemaTreePage> cut) => Collapse(cut.Find(".lt-schema-composer__reads").TextContent);

    private static string AgainstTheSample(IRenderedComponent<SchemaTreePage> cut) => Collapse(cut.Find(".lt-schema-composer__example").TextContent);

    private static bool IsOn(IRenderedComponent<SchemaTreePage> cut, string label) =>
        cut.FindAll("[role=switch]").Single(control => control.QuerySelector(".lt-switch__label")?.TextContent.Trim() == label)
            .GetAttribute("aria-checked") == "true";

    private static void ChooseMember(IRenderedComponent<SchemaTreePage> cut, string name) =>
        cut.FindAll(".lt-schema-shape__member").Single(member => member.QuerySelector(".lt-schema-shape__name")!.TextContent.Trim() == name).Click();

    [Test]
    public void Required_on_the_whole_value_of_objects_claims_reads_and_checks_the_same()
    {
        UseTasks();
        var cut = OpenEditor("tasks");
        StartRule(cut);

        cut.WaitUntil(() =>
        {
            Assert.That(cut.FindAll(".lt-schema-shape__member[aria-pressed='true']").Single().TextContent, Does.Contain("The whole value"));
            Assert.That(GalleryExample(cut, "Required"), Is.EqualTo("Set in 3 of 3 sampled values."));
            Assert.That(Reads(cut), Is.EqualTo("Reads: The value must be present, as any value"));
            Assert.That(AgainstTheSample(cut), Is.EqualTo("Against the sample: 3 of 3 values pass."));
            Assert.That(IsOn(cut, StructuralSwitch), Is.True);
        });

        Commit(cut);
        cut.WaitUntil(() => Assert.That(cut.Find(".lt-schema-ruleset__rule").TextContent, Does.Contain("all pass")));
        Save(cut);
        Assert.That(Schema.Policies["tasks"].Rules.Single().Predicate, Is.EqualTo(LatticePredicateNode.TypeOf(null, LatticeValueKind.Present)));
    }

    [Test]
    public void The_presence_check_follows_its_subject_typed_or_chosen()
    {
        UseTasks();
        var cut = OpenEditor("tasks");
        StartRule(cut);

        Path(cut, "meta");
        cut.WaitUntil(() =>
        {
            Assert.That(IsOn(cut, StructuralSwitch), Is.True, "meta holds objects");
            Assert.That(AgainstTheSample(cut), Is.EqualTo("Against the sample: 3 of 3 values pass."));
        });

        Path(cut, "title");
        cut.WaitUntil(() =>
        {
            Assert.That(IsOn(cut, StructuralSwitch), Is.False, "title holds text, which the older form checks on any cluster");
            Assert.That(Reads(cut), Is.EqualTo("Reads: title must be present as text, a number or true or false"));
            Assert.That(AgainstTheSample(cut), Is.EqualTo("Against the sample: 3 of 3 values pass."));
        });

        ChooseMember(cut, "The whole value");
        cut.WaitUntil(() =>
        {
            Assert.That(Field(cut, "Member path").GetAttribute("value"), Is.Empty);
            Assert.That(IsOn(cut, StructuralSwitch), Is.True);
            Assert.That(AgainstTheSample(cut), Is.EqualTo("Against the sample: 3 of 3 values pass."));
        });
    }

    [Test]
    public void Choosing_the_whole_value_reseeds_the_card_from_it()
    {
        UseTasks();
        var cut = OpenEditor("tasks");
        StartRule(cut);
        ChooseMember(cut, "title");
        Kind(cut, SchemaCardKind.Type);
        cut.WaitUntil(() => Assert.That(Reads(cut), Is.EqualTo("Reads: title must be text")));

        ChooseMember(cut, "The whole value");

        cut.WaitUntil(() =>
        {
            Assert.That(Reads(cut), Is.EqualTo("Reads: The value must be an object"));
            Assert.That(GalleryExample(cut, "Type"), Is.EqualTo("Seen as an object (3)."));
            Assert.That(AgainstTheSample(cut), Is.EqualTo("Against the sample: 3 of 3 values pass."));
        });
    }

    [Test]
    public void The_older_presence_check_on_objects_says_what_it_counts_everywhere()
    {
        UseTasks();
        var cut = OpenEditor("tasks");
        StartRule(cut);
        cut.WaitUntil(() => Assert.That(IsOn(cut, StructuralSwitch), Is.True));

        Tick(cut, StructuralSwitch);

        cut.WaitUntil(() =>
        {
            Assert.That(GalleryExample(cut, "Required"), Is.EqualTo("Set as text, a number or true or false in 0 of 3 sampled values."));
            Assert.That(Reads(cut), Is.EqualTo("Reads: The value must be present as text, a number or true or false"));
            Assert.That(Collapse(AgainstTheSample(cut)), Does.StartWith("Against the sample: 0 of 3 values pass."));
        });
    }

    [Test]
    public void A_presence_card_opened_before_the_sample_arrives_is_fitted_when_it_does()
    {
        UseTasks();
        var gate = new TaskCompletionSource();
        Data.ScanGate = gate;
        var cut = RenderAt<SchemaTreePage>("schema/tasks");
        cut.WaitUntil(() => Assert.That(Buttons(cut).Count(button => Text(button) == "Set a policy"), Is.EqualTo(1)));
        Click(cut, "Set a policy");
        StartRule(cut);
        cut.WaitUntil(() => Assert.That(IsOn(cut, StructuralSwitch), Is.False, "nothing is known yet"));

        gate.SetResult();

        cut.WaitUntil(() =>
        {
            Assert.That(IsOn(cut, StructuralSwitch), Is.True);
            Assert.That(AgainstTheSample(cut), Is.EqualTo("Against the sample: 3 of 3 values pass."));
        });
    }

    [Test]
    public void The_member_path_has_no_placeholder_that_looks_like_a_value_and_the_members_say_they_can_be_chosen()
    {
        UseTasks();
        var cut = OpenEditor("tasks");
        StartRule(cut);

        cut.WaitUntil(() =>
        {
            Assert.That(Field(cut, "Member path").HasAttribute("placeholder"), Is.False);
            Assert.That(cut.Find(".lt-schema-shape__caption").TextContent, Does.EndWith("Choose one to check it."));
            Assert.That(cut.FindAll(".lt-schema-shape__member").All(member => member.TagName == "BUTTON" && member.HasAttribute("aria-pressed")), Is.True);
        });
    }

    [Test]
    public void Save_is_offered_only_once_there_is_a_rule()
    {
        UseTrees("scratch");
        var cut = OpenEditor("scratch");
        var save = () => Buttons(cut).Single(button => Text(button) == "Save policy");

        cut.WaitUntil(() =>
        {
            Assert.That(save().HasAttribute("disabled"), Is.True);
            Assert.That(save().GetAttribute("aria-describedby"), Is.EqualTo("lt-schema-save-note"));
            Assert.That(cut.Find("#lt-schema-save-note").TextContent, Is.EqualTo("Add a rule to save the policy."));
        });

        StartRule(cut);
        cut.WaitUntil(() =>
        {
            Assert.That(save().HasAttribute("disabled"), Is.False, "the rule being written is saved with the policy");
            Assert.That(cut.FindAll("#lt-schema-save-note"), Is.Empty);
        });

        cut.FindAll(".lt-schema-composer button").Single(button => Text(button) == "Cancel").Click();
        cut.WaitUntil(() => Assert.That(save().HasAttribute("disabled"), Is.True));
    }

    [Test]
    public void Removing_every_rule_of_a_policy_points_at_clearing_it_instead()
    {
        UseTrees("orders");
        Schema.Policies["orders"] = new LatticeSchemaPolicy([LatticeSchemaRule.Json()]);
        var cut = OpenEditor();

        ClickLabelled(cut, "Remove rule 1");

        cut.WaitUntil(() =>
        {
            Assert.That(Buttons(cut).Single(button => Text(button) == "Save policy").HasAttribute("disabled"), Is.True);
            Assert.That(cut.Find("#lt-schema-save-note").TextContent,
                Is.EqualTo("Add a rule to save the policy. To accept every value, clear the policy instead."));
        });
    }
}
