using System.Text.Json;
using System.Text.Json.Serialization;
using Orleans.Lattice.Schema;
using Names = Orleans.Lattice.Explorer.EndToEnd.Tests.Story.StoryNames.Contoso;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Contracts;

/// <summary>
/// The Contoso schema, pinned (#5364): every policy, version family and registration the story
/// sets, in exactly the shapes the Explorer's rule builder compiles its cards to, so a policy a
/// journey builds card by card and one a seeder writes through the Schema API compare equal.
/// The registration JSON is what a journey pastes into the Versions tab's Register box; the
/// seeder deserializes the same text with the Explorer's settings.
/// </summary>
public static class ContosoSchema
{
    /// <summary>The orders' version family.</summary>
    public const uint OrdersFamily = 1;

    /// <summary>The lines' version family.</summary>
    public const uint LinesFamily = 2;

    /// <summary>
    /// The orders' registration (A4): version 1, and version 2, which adds <c>currency</c>
    /// defaulting to <c>EUR</c>, with the hop between them.
    /// </summary>
    public const string OrdersRegistrationJson = """
        {
          "descriptors": [
            { "schemaId": 1, "version": 1, "name": "Order" },
            { "schemaId": 1, "version": 2, "name": "Order with currency" }
          ],
          "upcasters": [
            { "schemaId": 1, "fromVersion": 1, "toVersion": 2,
              "transform": { "kind": "Passthrough", "children": [
                { "kind": "SetMember", "memberPath": "currency", "children": [
                  { "kind": "Constant", "constant": { "kind": "String", "stringValue": "EUR" } }
                ] }
              ] } }
          ]
        }
        """;

    /// <summary>The lines' registration (U6): version 1, and version 2, which adds <c>shift</c> defaulting to <c>day</c>.</summary>
    public const string LinesRegistrationJson = """
        {
          "descriptors": [
            { "schemaId": 2, "version": 1, "name": "Line" },
            { "schemaId": 2, "version": 2, "name": "Line with shift" }
          ],
          "upcasters": [
            { "schemaId": 2, "fromVersion": 1, "toVersion": 2,
              "transform": { "kind": "Passthrough", "children": [
                { "kind": "SetMember", "memberPath": "shift", "children": [
                  { "kind": "Constant", "constant": { "kind": "String", "stringValue": "day" } }
                ] }
              ] } }
          ]
        }
        """;

    private static readonly JsonSerializerOptions RegistrationJsonOptions = new(JsonSerializerDefaults.Web)
    {
        Converters = { new JsonStringEnumConverter() },
        UnmappedMemberHandling = JsonUnmappedMemberHandling.Disallow,
    };

    /// <summary>A4's <c>total</c> card: a Number range of at least 0.</summary>
    public static LatticeSchemaRule TotalAtLeastZero { get; } = LatticeSchemaRule.Structured(
        LatticePredicateNode.Compare(LatticeComparisonOperator.GreaterThanOrEqual, LatticePredicateNode.Member("total"), Integer(0)));

    /// <summary>A4's <c>status</c> card: One of a set, <c>open</c>, <c>shipped</c> or <c>cancelled</c>.</summary>
    public static LatticeSchemaRule StatusInSet { get; } = LatticeSchemaRule.Structured(OneOf("status", Names.Orders.Statuses));

    /// <summary>A4's strict policy on <c>factory-orders</c>.</summary>
    public static LatticeSchemaPolicy OrdersBaseline => new([TotalAtLeastZero, StatusInSet], strictIngest: true);

    /// <summary>U2's <c>status</c> rule after <b>Edit as regex</b>: a pattern on the member.</summary>
    public static LatticeSchemaRule StatusPattern { get; } = LatticeSchemaRule.Regex("^(open|shipped|cancelled)$", "status");

    /// <summary>U2's optional <c>currency</c> card: One of a set, <c>EUR</c>, <c>USD</c> or <c>GBP</c>, passing when missing.</summary>
    public static LatticeSchemaRule CurrencyOptional { get; } = LatticeSchemaRule.Structured(
        LatticePredicateNode.Bool(LatticeBooleanOperator.Or, IsMissing("currency"), OneOf("currency", ["EUR", "USD", "GBP"])));

    /// <summary>U2's revised strict policy on <c>factory-orders</c> (step 5).</summary>
    public static LatticeSchemaPolicy OrdersRevised => new([TotalAtLeastZero, StatusPattern, CurrencyOptional], strictIngest: true);

    /// <summary>
    /// The one rule on <c>factory-lines</c> the policy-as-code pipeline authors through the Schema
    /// API before U6 (off-UI): two members compared with each other, which no builder card says,
    /// so the builder keeps it as a Custom rule.
    /// </summary>
    public static LatticeSchemaRule LinesPipelineRule { get; } = LatticeSchemaRule.Structured(
        LatticePredicateNode.Compare(LatticeComparisonOperator.NotEqual, LatticePredicateNode.Member("owner"), LatticePredicateNode.Member("line")),
        "Pipeline: a line's owner is never the line itself");

    /// <summary>The pipeline's policy on <c>factory-lines</c> before U6: its one rule, not strict.</summary>
    public static LatticeSchemaPolicy LinesPipeline => new([LinesPipelineRule]);

    /// <summary>U6's non-strict policy on <c>factory-lines</c>: one rule per builder card, and the pipeline's rule kept as it is.</summary>
    public static LatticeSchemaPolicy LinesWorkshop => new(
    [
        LatticeSchemaRule.Structured(LatticePredicateNode.Compare(LatticeComparisonOperator.NotEqual, LatticePredicateNode.Member("line"), Null)),
        LatticeSchemaRule.Structured(LatticePredicateNode.Bool(
            LatticeBooleanOperator.Or,
            LatticePredicateNode.Compare(LatticeComparisonOperator.GreaterThanOrEqual, LatticePredicateNode.Member("rate"), Integer(0)),
            LatticePredicateNode.Compare(LatticeComparisonOperator.LessThan, LatticePredicateNode.Member("rate"), Integer(0)))),
        LatticeSchemaRule.Structured(OneOf("line", [.. Enumerable.Range(1, Names.Lines.Count).Select(index => "L" + index)])),
        LatticeSchemaRule.Structured(LatticePredicateNode.Bool(
            LatticeBooleanOperator.And,
            LatticePredicateNode.TypeOf("rate", LatticeValueKind.Integer),
            LatticePredicateNode.Compare(LatticeComparisonOperator.GreaterThanOrEqual, LatticePredicateNode.Member("rate"), Integer(0)),
            LatticePredicateNode.Compare(LatticeComparisonOperator.LessThanOrEqual, LatticePredicateNode.Member("rate"), Integer(1000)))),
        LatticeSchemaRule.Structured(LatticePredicateNode.Bool(
            LatticeBooleanOperator.And,
            LatticePredicateNode.TypeOf("line", LatticeValueKind.String),
            LatticePredicateNode.Compare(LatticeComparisonOperator.GreaterThanOrEqual, LatticePredicateNode.LengthOf("line"), Integer(2)),
            LatticePredicateNode.Compare(LatticeComparisonOperator.LessThanOrEqual, LatticePredicateNode.LengthOf("line"), Integer(3)))),
        LatticeSchemaRule.Regex(@"^[^@\s]+@[^@\s]+\.[^@\s]+$", "owner"),
        LatticeSchemaRule.Regex(@"^\d{4}-\d{2}-\d{2}$", "since"),
        LatticeSchemaRule.Structured(LatticePredicateNode.StringCall(
            LatticeStringMethod.StartsWith, LatticePredicateNode.Member("line"), LatticePredicateNode.Const(LatticeConstant.Text("L")))),
        LatticeSchemaRule.Structured(LatticePredicateNode.Bool(
            LatticeBooleanOperator.And,
            LatticePredicateNode.TypeOf("tags", LatticeValueKind.Array),
            LatticePredicateNode.Compare(LatticeComparisonOperator.GreaterThanOrEqual, LatticePredicateNode.LengthOf("tags"), Integer(1)),
            LatticePredicateNode.Compare(LatticeComparisonOperator.LessThanOrEqual, LatticePredicateNode.LengthOf("tags"), Integer(5)))),
        LatticeSchemaRule.Structured(LatticePredicateNode.Every("tags", LatticePredicateNode.StringCall(
            LatticeStringMethod.StartsWith, LatticePredicateNode.Self(), LatticePredicateNode.Const(LatticeConstant.Text(string.Empty))))),
        LatticeSchemaRule.Json(),
        LatticeSchemaRule.MaxLength(1024),
        LatticeSchemaRule.Regex(@"^L\d+$", "line"),
        LinesPipelineRule,
    ]);

    /// <summary>
    /// U2's remediation step (step 3): a negative <c>total</c> becomes 0, every other value is
    /// kept as it is.
    /// </summary>
    public static LatticeValueTransform NegativeTotalToZero { get; } = LatticeValueTransform.Passthrough(
        LatticeValueTransform.SetMember(
            "total",
            LatticeValueTransform.Conditional(
                LatticePredicateNode.Compare(LatticeComparisonOperator.LessThan, LatticePredicateNode.Member("total"), Integer(0)),
                LatticeValueTransform.Const(LatticeConstant.Integer(0)),
                LatticeValueTransform.Member("total"))));

    /// <summary>A registration JSON parsed exactly as the Explorer's Register box parses it.</summary>
    /// <param name="json">The JSON.</param>
    public static LatticeSchemaRuntimeRegistration ParseRegistration(string json) =>
        JsonSerializer.Deserialize<LatticeSchemaRuntimeRegistration>(json, RegistrationJsonOptions)
            ?? throw new JsonException("The registration JSON is null.");

    /// <summary>Whether <paramref name="policy"/> holds a rule equal to <paramref name="rule"/>.</summary>
    /// <param name="policy">The policy.</param>
    /// <param name="rule">The rule.</param>
    public static bool Holds(LatticeSchemaPolicy? policy, LatticeSchemaRule rule) =>
        policy is not null && policy.Rules.Any(candidate => candidate.Equals(rule));

    /// <summary>Whether two policies hold the same rules, in any order, with the same strictness.</summary>
    /// <param name="actual">The stored policy.</param>
    /// <param name="expected">The pinned policy.</param>
    public static bool SameAs(LatticeSchemaPolicy? actual, LatticeSchemaPolicy expected) =>
        actual is not null
        && actual.StrictIngest == expected.StrictIngest
        && actual.Rules.Count == expected.Rules.Count
        && expected.Rules.All(rule => Holds(actual, rule));

    private static LatticePredicateNode Null => LatticePredicateNode.Const(LatticeConstant.Null());

    private static LatticePredicateNode Integer(long value) => LatticePredicateNode.Const(LatticeConstant.Integer(value));

    private static LatticePredicateNode IsMissing(string path) =>
        LatticePredicateNode.Compare(LatticeComparisonOperator.Equal, LatticePredicateNode.Member(path), Null);

    private static LatticePredicateNode OneOf(string path, IReadOnlyList<string> values)
    {
        var options = values
            .Select(value => LatticePredicateNode.Compare(LatticeComparisonOperator.Equal, LatticePredicateNode.Member(path), LatticePredicateNode.Const(LatticeConstant.Text(value))))
            .ToArray();
        return options.Length == 1 ? options[0] : LatticePredicateNode.Bool(LatticeBooleanOperator.Or, options);
    }
}
