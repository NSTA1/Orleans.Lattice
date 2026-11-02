using Orleans.Lattice.Explorer.EndToEnd.Tests.SelfTests;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Story;

/// <summary>
/// Holds the story's shape to the epic (#5401): the outline is the decision record's serial
/// order and contract graph, every journey's <see cref="JourneyChapterAttribute"/> agrees with it,
/// and every contract names a chapter of it.
/// </summary>
[TestFixture]
[Category("EndToEnd")]
public sealed class StoryOutlineTests
{
    /// <summary>The serial story run the epic's decision record fixes, per storyline.</summary>
    private static readonly string[] ContosoOrder = ["A1", "A2", "A9", "A3", "A4", "A5", "A6", "A10", "A7", "A8", "A11", "A12", "U1", "U2", "U6", "U3", "U5", "U4", "C1", "C2", "C3", "C4"];

    private static readonly string[] FabrikamOrder = ["T1", "T2", "T7", "T3", "T4", "T5", "T6"];

    /// <summary>The outline holds every chapter of the epic, in its serial order, each with its own issue.</summary>
    [Test]
    public void The_outline_is_the_epics_serial_story()
    {
        Assert.Multiple(() =>
        {
            Assert.That(StoryOutline.Contoso.Select(chapter => chapter.Id), Is.EqualTo(ContosoOrder));
            Assert.That(StoryOutline.Fabrikam.Select(chapter => chapter.Id), Is.EqualTo(FabrikamOrder));
            Assert.That(StoryOutline.All.Select(chapter => chapter.Issue), Is.Unique.And.All.InRange(5371, 5399));
            Assert.That(StoryOutline.All.Select(chapter => chapter.Issue).Order(), Is.EqualTo(Enumerable.Range(5371, 29)));
            Assert.That(StoryOutline.All, Has.All.Matches<OutlineChapter>(chapter => chapter.Storyline == StoryOutline.StorylineOf(chapter.Id)));
        });
    }

    /// <summary>The serial order already respects every chapter's consumes, so ordering a storyline by dependency keeps it.</summary>
    [TestCase(StoryNames.Storylines.Contoso)]
    [TestCase(StoryNames.Storylines.Fabrikam)]
    public void The_serial_order_respects_every_consume(string storyline)
    {
        var plan = StoryOutline.For(storyline).Select(chapter => new StoryPlanEntry(chapter.Id, chapter.After, null)).ToList();

        Assert.That(StoryPlan.Order(plan).Select(entry => entry.Id), Is.EqualTo(plan.Select(entry => entry.Id)));
    }

    /// <summary>Every consume stays inside its storyline: the two estates share nothing.</summary>
    [Test]
    public void Every_consume_is_in_the_same_storyline() =>
        Assert.That(
            StoryOutline.All.SelectMany(chapter => chapter.After.Select(consume => (chapter, consume)))
                .Where(pair => StoryOutline.Find(pair.consume)?.Storyline != pair.chapter.Storyline)
                .Select(pair => $"{pair.chapter.Id} consumes {pair.consume}"),
            Is.Empty);

    /// <summary>
    /// Every declared journey agrees with the outline: its chapter exists, its <c>After</c> is the
    /// outline's consumes, it derives from <see cref="JourneyChapter"/>, and it lives in
    /// <c>Journeys/&lt;id&gt;/</c> so the CI lane discovers its isolated leg from the folder. Only a
    /// self-test chapter lies outside the outline.
    /// </summary>
    [Test]
    public void Every_declared_chapter_agrees_with_the_outline()
    {
        var problems = new List<string>();
        foreach (var declared in DeclaredChapter.Discover(typeof(StoryOutlineTests).Assembly))
        {
            var outline = StoryOutline.Find(declared.Id);
            if (outline is null)
            {
                if (declared.Type.Namespace != typeof(StubStory).Namespace || declared.Storyline != StubStory.Storyline)
                {
                    problems.Add($"{declared.Type.FullName}: chapter {declared.Id} is not in the story outline");
                }

                continue;
            }

            if (!declared.After.Order(StringComparer.Ordinal).SequenceEqual(outline.After.Order(StringComparer.Ordinal)))
            {
                problems.Add($"{declared.Type.FullName}: After = [{string.Join(", ", declared.After)}], but {declared.Id}'s contract consumes [{string.Join(", ", outline.After)}]");
            }

            if (declared.Storyline != outline.Storyline)
            {
                problems.Add($"{declared.Type.FullName}: storyline '{declared.Storyline}', but {declared.Id} is {outline.Storyline}'s");
            }

            if (!typeof(JourneyChapter).IsAssignableFrom(declared.Type))
            {
                problems.Add($"{declared.Type.FullName}: a journey derives from JourneyChapter");
            }

            var expected = $"Orleans.Lattice.Explorer.EndToEnd.Tests.Journeys.{declared.Id}";
            if (declared.Type.Namespace != expected)
            {
                problems.Add($"{declared.Type.FullName}: a journey lives in Journeys/{declared.Id}/ (namespace {expected})");
            }
        }

        Assert.That(problems, Is.Empty, string.Join(Environment.NewLine, problems));
    }

    /// <summary>Every contract names a chapter of the outline.</summary>
    [Test]
    public void Every_contract_names_an_outline_chapter() =>
        Assert.That(ChapterContracts.Discovered.Chapters.Where(chapter => StoryOutline.Find(chapter) is null), Is.Empty);

    /// <summary>Each storyline has an estate, and the two are distinct product shapes (decision D2).</summary>
    [Test]
    public void Each_storyline_has_its_estate()
    {
        var contoso = StoryEstates.OptionsFor(StoryNames.Storylines.Contoso);
        var fabrikam = StoryEstates.OptionsFor(StoryNames.Storylines.Fabrikam);
        Assert.Multiple(() =>
        {
            Assert.That(contoso.Name, Is.EqualTo(StoryNames.Storylines.Contoso));
            Assert.That(contoso.Tenancy, Is.False);
            Assert.That(fabrikam.Name, Is.EqualTo(StoryNames.Storylines.Fabrikam));
            Assert.That(fabrikam.Tenancy, Is.True);
            Assert.That(() => StoryEstates.OptionsFor("selftest"), Throws.ArgumentException);
        });
    }
}
