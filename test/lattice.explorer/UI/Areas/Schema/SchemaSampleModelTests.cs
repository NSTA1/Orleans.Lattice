using System.Text;
using Orleans.Lattice.Explorer.UI.Areas.Schema;
using Orleans.Lattice.Explorer.UI.Design.Components;
using Orleans.Lattice.Schema;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Schema;

/// <summary>
/// What the builder learns from a sample: the inferred shape and its bounds,
/// the sample reader, the draft check, the gallery's live examples and seeds,
/// and the suggestions drawn from the shape.
/// </summary>
[TestFixture]
public sealed class SchemaSampleModelTests
{
    private static byte[] Json(string json) => Encoding.UTF8.GetBytes(json);

    private static SchemaShape Shape(params string[] values) => SchemaShape.Infer(values.Select(Json), []);

    [Test]
    public void The_shape_records_members_types_ranges_and_list_items()
    {
        var shape = Shape(
            "{\"total\":1.5,\"name\":\"ab\",\"lines\":[{\"qty\":1},{\"qty\":4}],\"flag\":true,\"note\":null}",
            "{\"total\":7,\"name\":\"abcd\",\"lines\":[]}",
            "not json");

        var total = shape.Find("total")!;
        var name = shape.Find("name")!;
        var lines = shape.Find("lines")!;
        var qty = SchemaShape.Find(lines.Items!, "qty")!;

        Assert.Multiple(() =>
        {
            Assert.That(shape.Documents, Is.EqualTo(3));
            Assert.That(shape.NotJson, Is.EqualTo(1));
            Assert.That(total.Numbers, Is.EqualTo((1.5, 7.0)));
            Assert.That(total.AllWhole, Is.False);
            Assert.That(total.Dominant, Is.EqualTo(SchemaValueType.Number));
            Assert.That(name.TextLengths, Is.EqualTo((2, 4)));
            Assert.That(lines.ItemCounts, Is.EqualTo((0, 2)));
            Assert.That(qty.Numbers, Is.EqualTo((1.0, 4.0)));
            Assert.That(qty.AllWhole, Is.True);
            Assert.That(qty.FullLabel, Is.EqualTo("lines[].qty"));
            Assert.That(qty.ItemScopes(), Is.EqualTo(new[] { lines.Items }));
            Assert.That(shape.Find("note")!.Nulls, Is.EqualTo(1));
            Assert.That(shape.Find("flag")!.Distinct, Is.EqualTo(new[] { "true" }));
            Assert.That(shape.Find("missing"), Is.Null);
            Assert.That(shape.Find("TOTAL"), Is.SameAs(total), "member names resolve as the cluster resolves them");
            Assert.That(shape.IsEmpty, Is.False);
        });
    }

    [Test]
    public void The_policy_names_members_no_sample_showed()
    {
        var predicate = LatticePredicateNode.Every("lines", LatticePredicateNode.TypeOf("sku", LatticeValueKind.String));
        var shape = SchemaShape.Infer([], [LatticeSchemaRule.Regex("^x$", "code.inner"), LatticeSchemaRule.Structured(predicate)]);

        var inner = shape.Find("code.inner")!;
        Assert.Multiple(() =>
        {
            Assert.That(inner.NamedByPolicy, Is.True);
            Assert.That(inner.Unseen, Is.True);
            Assert.That(SchemaShape.Find(shape.Find("lines")!.Items!, "sku")!.NamedByPolicy, Is.True);
            Assert.That(SchemaShape.Infer([], []).IsEmpty, Is.True);
        });
    }

    [Test]
    public void The_shape_is_bounded_and_leaves_out_names_a_path_cannot_address()
    {
        var wide = "{" + string.Join(",", Enumerable.Range(0, SchemaShape.MaximumChildren + 10).Select(index => $"\"m{index}\":1")) + ",\"a.b\":1}";
        var deep = new StringBuilder("1");
        for (var level = 0; level < SchemaShape.MaximumDepth + 4; level++)
        {
            deep.Insert(0, "{\"d\":").Append('}');
        }

        var shape = Shape(wide, deep.ToString());

        Assert.Multiple(() =>
        {
            Assert.That(shape.Truncated, Is.True);
            Assert.That(shape.Root.Children.Count, Is.LessThanOrEqualTo(SchemaShape.MaximumChildren));
            Assert.That(shape.Root.Children.Keys, Has.None.Contains('.'));
        });
    }

    [Test]
    public void Distinct_values_are_capped_and_the_overflow_noted()
    {
        var shape = Shape([.. Enumerable.Range(0, SchemaShape.DistinctLimit + 3).Select(index => $"{{\"s\":\"v{index}\"}}")]);

        Assert.That(shape.Find("s")!.Distinct, Has.Count.EqualTo(SchemaShape.DistinctLimit));
        Assert.That(shape.Find("s")!.ManyValues, Is.True);
    }

    [Test]
    public void A_card_on_a_list_item_member_is_wrapped_in_every_item_cards()
    {
        var shape = Shape("{\"grid\":[[{\"v\":1}]]}");
        var v = SchemaShape.Find(shape.Find("grid")!.Items!.Items!, "v")!;

        var card = SchemaShape.Wrap(v, SchemaRuleCard.Of(SchemaCardKind.Required));

        Assert.Multiple(() =>
        {
            Assert.That(card.Kind, Is.EqualTo(SchemaCardKind.EveryItem));
            Assert.That(card.Path, Is.EqualTo("grid"));
            Assert.That(card.Item!.Kind, Is.EqualTo(SchemaCardKind.EveryItem));
            Assert.That(card.Item.Path, Is.Empty);
            Assert.That(card.Item.Item!.Path, Is.EqualTo("v"));
            Assert.That(SchemaShape.Wrap(shape.Find("grid")!, SchemaRuleCard.Of(SchemaCardKind.Required)).Path, Is.EqualTo("grid"));
            Assert.That(() => SchemaShape.Wrap(null!, card), Throws.ArgumentNullException);
        });
    }

    [Test]
    public void The_draft_is_checked_rule_by_rule_with_the_first_failures_listed()
    {
        var sample = new SchemaSample("t", null,
        [
            .. Enumerable.Range(0, 8).Select(index => new SchemaSampleValue($"k{index}", Json(index % 2 == 0 ? "{\"a\":1}" : "{}"))),
            new SchemaSampleValue("bad", Json("nope")),
        ], 0, false);
        LatticeSchemaRule[] rules =
        [
            LatticeSchemaRule.Json("json"),
            LatticeSchemaRule.Structured(LatticePredicateNode.Compare(LatticeComparisonOperator.NotEqual, LatticePredicateNode.Member("a"), LatticePredicateNode.Const(LatticeConstant.Null())), "has a"),
        ];

        var result = SchemaPreviewResult.Evaluate(rules, sample);

        Assert.Multiple(() =>
        {
            Assert.That(result.Checked, Is.EqualTo(9));
            Assert.That(result.Passed, Is.EqualTo(4));
            Assert.That(result.Failed, Is.EqualTo(5));
            Assert.That(result.FailuresByRule, Is.EqualTo(new[] { 1, 5 }));
            Assert.That(result.Failures, Has.Count.EqualTo(SchemaPreviewResult.FailureLimit));
            Assert.That(result.Failures.Single(failure => failure.Key == "bad").Reason, Is.EqualTo("json"), "the first rule a value fails names it");
            Assert.That(result.IsValid, Is.True);
        });
    }

    [Test]
    public void A_draft_the_cluster_would_refuse_is_not_checked()
    {
        var result = SchemaPreviewResult.Evaluate([LatticeSchemaRule.Regex("(a)\\1")], SchemaSample.Empty("t", null));

        Assert.That(result.IsValid, Is.False);
        Assert.That(result.Error, Does.StartWith("The cluster would refuse this rule set"));
        Assert.That(() => SchemaPreviewResult.Evaluate(null!, SchemaSample.Empty("t", null)), Throws.ArgumentNullException);
    }

    [Test]
    public async Task The_sample_reader_reads_one_page_releases_it_and_reads_long_values_in_full()
    {
        var data = new FakeSchemaDataReader();
        data.Use("t", ("a", "{\"x\":1}"), ("b", "{\"long\":\"value\"}"), ("c", "{}"));
        data.Truncated.Add("b");
        var reader = new SchemaSampleReader(data, () => "acme");

        var sample = await reader.ReadAsync("t", CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(sample.Values.Select(value => value.Key), Is.EqualTo(new[] { "a", "b", "c" }));
            Assert.That(Encoding.UTF8.GetString(sample.Values[1].Value), Is.EqualTo("{\"long\":\"value\"}"));
            Assert.That(sample.Tenant, Is.EqualTo("acme"), "the sample remembers the tenant it was read under");
            Assert.That(sample.More, Is.False);
            Assert.That(sample.Unread, Is.Zero);
            Assert.That(data.Scans, Is.EqualTo(1));
        });
    }

    [Test]
    public async Task A_value_that_cannot_be_read_in_full_is_counted_not_guessed()
    {
        var data = new FakeSchemaDataReader();
        data.Use("t", ("a", "{\"x\":1}"));
        data.Truncated.Add("a");
        data.Trees["t"].Remove("a");
        data.Trees["t"]["a"] = Json("{\"x\":1}");
        var reader = new SchemaSampleReader(new VanishingReader(data), () => null);

        var sample = await reader.ReadAsync("t", CancellationToken.None);

        Assert.That(sample.Values, Is.Empty);
        Assert.That(sample.Unread, Is.EqualTo(1));
    }

    [Test]
    public void A_head_without_a_data_reader_has_no_sample()
    {
        var reader = new SchemaSampleReader(null, () => null);

        Assert.That(reader.IsAvailable, Is.False);
        Assert.That(() => reader.ReadAsync("t", CancellationToken.None), Throws.InvalidOperationException.With.Message.EqualTo(SchemaSampleReader.Unavailable));
    }

    [Test]
    public void A_versioned_value_is_judged_by_its_body()
    {
        var body = Json("{\"a\":1}");
        var enveloped = LatticeSchemaEnvelope.Encode(7, 2, body);

        Assert.That(SchemaSampleReader.Body(enveloped), Is.EqualTo(body));
        Assert.That(SchemaSampleReader.Body(body), Is.SameAs(body));
    }

    [Test]
    public void The_gallery_examples_come_from_the_sample_when_there_is_one()
    {
        var shape = Shape("{\"n\":2,\"s\":\"ab-1\",\"e\":\"a@example.com\",\"l\":[1,2]}", "{\"n\":9,\"s\":\"ab-2\",\"e\":\"b@example.com\",\"l\":[3]}");

        Assert.Multiple(() =>
        {
            Assert.That(SchemaCardExamples.Example(SchemaCardKind.NumberRange, shape.Find("n")), Is.EqualTo("Seen 2 to 9, all whole."));
            Assert.That(SchemaCardExamples.Example(SchemaCardKind.TextLength, shape.Find("s")), Is.EqualTo("Seen 4 to 4 characters."));
            Assert.That(SchemaCardExamples.Example(SchemaCardKind.TextMatch, shape.Find("s")), Is.EqualTo("Every value seen starts with \"ab-\"."));
            Assert.That(SchemaCardExamples.Example(SchemaCardKind.Format, shape.Find("e")), Is.EqualTo("Every value seen is an email address."));
            Assert.That(SchemaCardExamples.Example(SchemaCardKind.ListLength, shape.Find("l")), Is.EqualTo("Seen 1 to 2 items."));
            Assert.That(SchemaCardExamples.Example(SchemaCardKind.EveryItem, shape.Find("l")), Is.EqualTo("Items seen are mostly a number."));
            Assert.That(SchemaCardExamples.Example(SchemaCardKind.Required, shape.Find("n")), Is.EqualTo("Set in 2 of 2 sampled values."));
            Assert.That(SchemaCardExamples.Example(SchemaCardKind.Type, shape.Find("n")), Is.EqualTo("Seen as a number (2)."));
            Assert.That(SchemaCardExamples.Example(SchemaCardKind.OneOf, shape.Find("s")), Is.EqualTo("Seen: ab-1, ab-2."));
            Assert.That(SchemaCardExamples.Example(SchemaCardKind.Pattern, shape.Find("s")), Does.StartWith("Test it against \"ab-1\""));
        });
    }

    [Test]
    public void Every_gallery_card_has_a_title_a_meaning_and_an_example_without_a_sample()
    {
        foreach (var kind in SchemaCardExamples.Gallery)
        {
            Assert.That(SchemaCardExamples.Title(kind), Is.Not.Empty, kind.ToString());
            Assert.That(SchemaCardExamples.Meaning(kind), Is.Not.Empty, kind.ToString());
            Assert.That(SchemaCardExamples.Example(kind, null), Is.Not.Empty, kind.ToString());
        }

        Assert.That(SchemaCardExamples.Title(SchemaCardKind.AnyOf), Is.EqualTo("Any of"));
        Assert.That(SchemaCardExamples.Title(SchemaCardKind.Custom), Is.EqualTo("Custom rule"));
    }

    [Test]
    public void A_seeded_card_starts_from_the_data()
    {
        var shape = Shape("{\"n\":2,\"s\":\"open\",\"o\":{},\"e\":\"a@example.com\",\"l\":[1]}", "{\"n\":9,\"s\":\"done\",\"o\":{},\"e\":\"b@example.com\",\"l\":[2,3]}");

        Assert.Multiple(() =>
        {
            var range = SchemaCardExamples.Seed(SchemaCardKind.NumberRange, shape.Find("n"));
            Assert.That((range.Minimum, range.Maximum, range.IntegerOnly), Is.EqualTo(("2", "9", true)));
            Assert.That(SchemaCardExamples.Seed(SchemaCardKind.OneOf, shape.Find("s")).Values, Is.EqualTo(new[] { "done", "open" }));
            Assert.That(SchemaCardExamples.Seed(SchemaCardKind.Required, shape.Find("o")).Structural, Is.True);
            Assert.That(SchemaCardExamples.Seed(SchemaCardKind.Type, shape.Find("o")).ValueType, Is.EqualTo(SchemaValueType.Object));
            Assert.That(SchemaCardExamples.Seed(SchemaCardKind.Format, shape.Find("e")).Format, Is.EqualTo(SchemaTextFormat.Email));
            Assert.That(SchemaCardExamples.Seed(SchemaCardKind.ListLength, shape.Find("l")).Maximum, Is.EqualTo("2"));
            Assert.That(SchemaCardExamples.Seed(SchemaCardKind.EveryItem, shape.Find("l")).Item!.Kind, Is.EqualTo(SchemaCardKind.NumberRange));
            Assert.That(SchemaCardExamples.Seed(SchemaCardKind.TextLength, shape.Find("s")).Minimum, Is.EqualTo("4"));
        });
    }

    [Test]
    public void A_card_is_offered_only_where_it_can_be_written()
    {
        Assert.Multiple(() =>
        {
            Assert.That(SchemaCardExamples.IsAvailable(SchemaCardKind.MaxSize, wholeValue: true, predicateOnly: false, out _), Is.True);
            Assert.That(SchemaCardExamples.IsAvailable(SchemaCardKind.MaxSize, wholeValue: false, predicateOnly: false, out var member), Is.False);
            Assert.That(member, Is.EqualTo("Only for the whole value."));
            Assert.That(SchemaCardExamples.IsAvailable(SchemaCardKind.Format, wholeValue: false, predicateOnly: true, out var nested), Is.False);
            Assert.That(nested, Does.StartWith("Checks a member on its own"));
            Assert.That(SchemaCardExamples.IsAvailable(SchemaCardKind.NumberRange, wholeValue: false, predicateOnly: true, out _), Is.True);
        });
    }

    [Test]
    public void The_common_prefix_needs_two_texts()
    {
        Assert.That(SchemaCardExamples.CommonPrefix(["abc"]), Is.Empty);
        Assert.That(SchemaCardExamples.CommonPrefix(["abc", "abd", "ab"]), Is.EqualTo("ab"));
    }

    [Test]
    public async Task Suggestions_come_from_the_shape()
    {
        var shape = Shape("{\"order\":{\"total\":1,\"id\":\"x\"},\"s\":\"open\",\"l\":[{\"q\":1}]}", "{\"s\":\"done\"}");

        var members = await SchemaShapeSuggestionSource.Members(() => shape.Root).SuggestAsync(string.Empty, 20, CancellationToken.None);
        var values = await SchemaShapeSuggestionSource.Values(() => shape.Find("s")).SuggestAsync("op", 20, CancellationToken.None);
        var none = await SchemaShapeSuggestionSource.Members(() => null).SuggestAsync("x", 20, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(members.Items.Select(item => item.Value), Is.EqualTo(new[] { "l", "order", "order.id", "order.total", "s" }), "list items are another scope");
            Assert.That(values.Items.Select(item => item.Value), Is.EqualTo(new[] { "open" }));
            Assert.That(none, Is.EqualTo(LtSuggestionSet.Empty));
        });
    }

    /// <summary>A reader whose point reads find nothing, as when a value is deleted between the scan and the read.</summary>
    private sealed class VanishingReader(FakeSchemaDataReader inner) : Orleans.Lattice.Explorer.Core.Data.IDataReader
    {
        public Task<Orleans.Lattice.Explorer.Core.Data.DataPage> ScanAsync(string treeId, int pageSize, string? continuationToken = null, Orleans.Lattice.Explorer.Core.Data.TagFilter? tagFilter = null, string? keyPrefix = null, Orleans.Lattice.Api.State.EntryScanMode mode = Orleans.Lattice.Api.State.EntryScanMode.Live, CancellationToken cancellationToken = default) =>
            inner.ScanAsync(treeId, pageSize, continuationToken, tagFilter, keyPrefix, mode, cancellationToken);

        public Task<Orleans.Lattice.Explorer.Core.Data.DataEntry?> GetEntryAsync(string treeId, string key, CancellationToken cancellationToken = default) =>
            Task.FromResult<Orleans.Lattice.Explorer.Core.Data.DataEntry?>(null);

        public Task CancelScanAsync(string treeId, string? continuationToken, CancellationToken cancellationToken = default) => Task.CompletedTask;

        public Task<IReadOnlyList<Orleans.Lattice.Explorer.Core.Data.TagIndexRef>> ListTagIndexesForTreeAsync(string treeId, CancellationToken cancellationToken = default) => throw new NotSupportedException();

        public Task<IReadOnlyList<string>> ListTagValuesForIndexAsync(string treeId, string indexName, CancellationToken cancellationToken = default) => throw new NotSupportedException();

        public Task<IReadOnlyList<string>> ListCoveredTreesForIndexAsync(string indexName, CancellationToken cancellationToken = default) => throw new NotSupportedException();

        public Task<IReadOnlyList<string>> ListTagsForIndexAsync(string indexName, CancellationToken cancellationToken = default) => throw new NotSupportedException();

        public Task<Orleans.Lattice.Explorer.Core.Data.TagMemberPage> ScanTagMembersAsync(string indexName, string tag, int pageSize, string? continuationToken = null, CancellationToken cancellationToken = default) => throw new NotSupportedException();
    }
}
