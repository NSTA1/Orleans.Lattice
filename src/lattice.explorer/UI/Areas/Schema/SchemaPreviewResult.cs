using Orleans.Lattice.Schema;

namespace Orleans.Lattice.Explorer.UI.Areas.Schema;

/// <summary>
/// A draft rule set checked against a sample: how many sampled values pass and
/// fail, how many fail each rule, and the first few failures with their reasons.
/// Computed locally with the cluster's own validator, so it agrees with what
/// enforcement and the compliance scan would say about the same values.
/// </summary>
/// <param name="Checked">How many values were checked.</param>
/// <param name="Passed">How many satisfy every rule.</param>
/// <param name="FailuresByRule">How many values fail each rule, by rule position.</param>
/// <param name="Failures">The first failing values, in key order.</param>
/// <param name="Error">Why the draft could not be checked at all, or <see langword="null"/>.</param>
internal sealed record SchemaPreviewResult(
    int Checked,
    int Passed,
    IReadOnlyList<int> FailuresByRule,
    IReadOnlyList<SchemaPreviewFailure> Failures,
    string? Error)
{
    /// <summary>How many failing values are listed.</summary>
    public const int FailureLimit = 5;

    /// <summary>How many values fail at least one rule.</summary>
    public int Failed => Checked - Passed;

    /// <summary>Whether the draft could be checked.</summary>
    public bool IsValid => Error is null;

    /// <summary>Checks <paramref name="rules"/> against <paramref name="sample"/>.</summary>
    /// <param name="rules">The draft rules.</param>
    /// <param name="sample">The sample.</param>
    /// <returns>The result.</returns>
    public static SchemaPreviewResult Evaluate(IReadOnlyList<LatticeSchemaRule> rules, SchemaSample sample)
    {
        ArgumentNullException.ThrowIfNull(rules);
        ArgumentNullException.ThrowIfNull(sample);
        LatticeSchemaPolicyValidator validator;
        try
        {
            validator = new LatticeSchemaPolicyValidator(new LatticeSchemaPolicy(rules));
        }
        catch (ArgumentException exception)
        {
            return new SchemaPreviewResult(0, 0, new int[rules.Count], [], "The cluster would refuse this rule set: " + exception.Message);
        }

        var byRule = new int[rules.Count];
        var failures = new List<SchemaPreviewFailure>(FailureLimit);
        var passed = 0;
        foreach (var value in sample.Values)
        {
            var first = -1;
            string? reason = null;
            for (var index = 0; index < byRule.Length; index++)
            {
                if (validator.ValidateRule(index, value.Value) is { } failure)
                {
                    byRule[index]++;
                    if (first < 0)
                    {
                        first = index;
                        reason = failure;
                    }
                }
            }

            if (first < 0)
            {
                passed++;
            }
            else if (failures.Count < FailureLimit)
            {
                failures.Add(new SchemaPreviewFailure(value.Key, first, reason!, SchemaFormat.Preview(value.Value)));
            }
        }

        return new SchemaPreviewResult(sample.Values.Count, passed, byRule, failures, null);
    }
}
