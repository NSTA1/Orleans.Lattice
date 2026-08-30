using System.Text;

namespace Orleans.Lattice.Schema.Tests;

/// <summary>
/// Unit tests for <see cref="LatticeSchemaRemediation"/>: the pure remediation
/// dry-run that rewrites each existing value and validates it against a candidate
/// policy, aborting on the first offending key. This is the tested precursor to a
/// background shadow build; physical cutover is a deferred follow-up.
/// </summary>
public class LatticeSchemaRemediationTests
{
    private static byte[] Utf8(string s) => Encoding.UTF8.GetBytes(s);

    private static async IAsyncEnumerable<KeyValuePair<string, byte[]>> Entries(
        params (string Key, string Value)[] items)
    {
        foreach (var (key, value) in items)
        {
            yield return new KeyValuePair<string, byte[]>(key, Utf8(value));
        }

        await Task.CompletedTask;
    }

    private static LatticeSchemaPolicy JsonPolicy() =>
        new(new[] { LatticeSchemaRule.Json() });

    [Test]
    public async Task DryRunAsync_all_values_remediate_reports_success()
    {
        var outcome = await LatticeSchemaRemediation.DryRunAsync(
            Entries(("k1", "{\"a\":1}"), ("k2", "{\"b\":2}")),
            LatticeValueTransform.Passthrough(),
            JsonPolicy());

        Assert.That(outcome.Succeeded, Is.True);
        Assert.That(outcome.ScannedCount, Is.EqualTo(2));
        Assert.That(outcome.OffendingKey, Is.Null);
    }

    [Test]
    public async Task DryRunAsync_empty_source_succeeds_with_zero_scanned()
    {
        var outcome = await LatticeSchemaRemediation.DryRunAsync(
            Entries(), LatticeValueTransform.Passthrough(), JsonPolicy());

        Assert.That(outcome.Succeeded, Is.True);
        Assert.That(outcome.ScannedCount, Is.Zero);
    }

    [Test]
    public async Task DryRunAsync_policy_violation_aborts_on_first_offending_key()
    {
        var policy = new LatticeSchemaPolicy(new[] { LatticeSchemaRule.MaxLength(3) });

        var outcome = await LatticeSchemaRemediation.DryRunAsync(
            Entries(("k1", "{}"), ("k2", "{\"too\":\"big\"}")),
            LatticeValueTransform.Passthrough(),
            policy);

        Assert.That(outcome.Succeeded, Is.False);
        Assert.That(outcome.OffendingKey, Is.EqualTo("k2"));
        Assert.That(outcome.ScannedCount, Is.EqualTo(2));
        Assert.That(outcome.Reason, Is.Not.Null.And.Not.Empty);
        Assert.That(outcome.OffendingValuePreview, Is.Not.Null);
    }

    [Test]
    public async Task DryRunAsync_transform_failure_aborts_with_original_preview()
    {
        // A non-JSON value makes the transform throw; the abort carries the
        // original value's preview.
        var outcome = await LatticeSchemaRemediation.DryRunAsync(
            Entries(("k1", "not json")),
            LatticeValueTransform.Passthrough(),
            JsonPolicy());

        Assert.That(outcome.Succeeded, Is.False);
        Assert.That(outcome.OffendingKey, Is.EqualTo("k1"));
        Assert.That(Encoding.UTF8.GetString(outcome.OffendingValuePreview!), Does.StartWith("not"));
    }

    [Test]
    public async Task DryRunAsync_bounds_offending_preview_to_max()
    {
        var policy = new LatticeSchemaPolicy(new[] { LatticeSchemaRule.MaxLength(1) });

        var outcome = await LatticeSchemaRemediation.DryRunAsync(
            Entries(("k1", "{\"a\":123456789}")),
            LatticeValueTransform.Passthrough(),
            policy,
            previewMaxBytes: 4);

        Assert.That(outcome.OffendingValuePreview!.Length, Is.EqualTo(4));
    }

    [Test]
    public void DryRunAsync_null_arguments_throw()
    {
        Assert.ThrowsAsync<ArgumentNullException>(
            async () => await LatticeSchemaRemediation.DryRunAsync(null!, LatticeValueTransform.Passthrough(), JsonPolicy()));
        Assert.ThrowsAsync<ArgumentNullException>(
            async () => await LatticeSchemaRemediation.DryRunAsync(Entries(), LatticeValueTransform.Passthrough(), null!));
    }

    [Test]
    public void DryRunAsync_uncompilable_candidate_policy_throws()
    {
        var policy = new LatticeSchemaPolicy(new[] { LatticeSchemaRule.Regex("(unclosed") });
        Assert.ThrowsAsync<ArgumentException>(
            async () => await LatticeSchemaRemediation.DryRunAsync(Entries(), LatticeValueTransform.Passthrough(), policy));
    }

    [Test]
    public async Task DryRunCoreAsync_transform_failure_with_empty_value_reports_empty_preview()
    {
        async IAsyncEnumerable<KeyValuePair<string, byte[]>> EmptyValue()
        {
            yield return new KeyValuePair<string, byte[]>("k1", Array.Empty<byte>());
            await Task.CompletedTask;
        }

        var outcome = await LatticeSchemaRemediation.DryRunCoreAsync(
            EmptyValue(),
            _ => throw new InvalidOperationException("cannot rewrite"),
            policyView: null,
            policy: null,
            previewMaxBytes: 4,
            CancellationToken.None);

        Assert.That(outcome.Succeeded, Is.False);
        Assert.That(outcome.OffendingValuePreview, Is.Empty);
    }
}
