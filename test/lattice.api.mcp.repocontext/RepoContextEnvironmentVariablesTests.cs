namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests;

/// <summary>
/// The published registry exists so a host does not have to restate this package's
/// environment surface. These tests hold it to that: they check it is derived from the
/// option classes rather than copied from them, because a copy that happens to be correct
/// today is exactly what issue #2460 was.
/// </summary>
[TestFixture]
public sealed class RepoContextEnvironmentVariablesTests
{
    [Test]
    public void Every_published_key_is_the_option_classes_own_constant()
    {
        Assert.Multiple(() =>
        {
            Assert.That(
                RepoContextEnvironmentVariables.IndexingKeys,
                Does.Contain(RepoContextIndexingOptions.ReconcileIntervalSecondsKey));
            Assert.That(
                RepoContextEnvironmentVariables.IndexingKeys,
                Does.Contain(RepoContextIndexingOptions.AnnIndexReclamationKey));
            Assert.That(
                RepoContextEnvironmentVariables.GitSourceKeys,
                Is.EqualTo(new[]
                {
                    RepoContextGitSourceRegistry.ReposVariable,
                    RepoContextGitSourceRegistry.StagingRootVariable,
                }));
            Assert.That(
                RepoContextEnvironmentVariables.GitSourceSettingPrefix,
                Is.EqualTo(RepoContextGitSourceRegistry.SettingPrefix),
                "the prefix is the registry's own constant, so a rename cannot leave the "
                + "published family pointing at a name nothing uses");
        });
    }

    /// <summary>
    /// The registry's whole purpose is that it cannot fall behind the option classes, so
    /// the count is checked against a reflection scan rather than against a number.
    /// </summary>
    [Test]
    public void Every_LATTICE_constant_this_package_declares_is_published()
    {
        var declared = new[] { typeof(RepoContextIndexingOptions), typeof(RepoContextGitSourceRegistry) }
            .SelectMany(t => t.GetFields(
                System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Static))
            .Where(f => f.IsLiteral && f.FieldType == typeof(string))
            .Select(f => (string?)f.GetRawConstantValue())
            .Where(v => v is not null && v.StartsWith("LATTICE_", StringComparison.Ordinal))
            .Select(v => v!)
            .Distinct(StringComparer.Ordinal)
            .ToList();

        Assert.Multiple(() =>
        {
            Assert.That(
                declared,
                Is.Not.Empty,
                "a scan that matched nothing would pass vacuously; if this floor trips, fix "
                + "the scan rather than deleting it");

            Assert.That(
                declared.Where(v =>
                    !RepoContextEnvironmentVariables.All.Contains(v, StringComparer.Ordinal)
                    && !RepoContextEnvironmentVariables.Prefixes.Contains(v, StringComparer.Ordinal)),
                Is.Empty,
                "an unpublished constant is a variable the host will report as read by "
                + "nothing while this package reads it, which is issue #2460 recurring. It "
                + "belongs in All if it is a whole key, or in Prefixes if it names a family");
        });
    }

    [Test]
    public void All_is_the_union_of_the_published_groups()
        => Assert.That(
            RepoContextEnvironmentVariables.All,
            Is.EqualTo(RepoContextEnvironmentVariables.IndexingKeys
                .Concat(RepoContextEnvironmentVariables.GitSourceKeys)),
            "All is what a host consumes, so it must not be a third hand-maintained list "
            + "that can disagree with the two it is built from");

    [Test]
    public void No_published_key_is_duplicated()
        => Assert.That(
            RepoContextEnvironmentVariables.All.Distinct(StringComparer.OrdinalIgnoreCase).Count(),
            Is.EqualTo(RepoContextEnvironmentVariables.All.Count),
            "a duplicate would double a host's reported setting count and make the header's "
            + "coverage figure quietly wrong");

    [Test]
    public void A_member_of_a_published_family_is_prefix_matched()
        => Assert.That(
            RepoContextEnvironmentVariables.IsPrefixMatched(
                RepoContextEnvironmentVariables.GitSourceSettingPrefix + "MY_REPO_URL"),
            Is.True);

    [Test]
    public void A_bare_prefix_is_not_prefix_matched()
        => Assert.That(
            RepoContextEnvironmentVariables.IsPrefixMatched(
                RepoContextEnvironmentVariables.GitSourceSettingPrefix),
            Is.False,
            "a variable equal to the prefix names no setting; admitting it would let a bare "
            + "prefix pass as a recognised one");

    [Test]
    public void An_unrelated_variable_is_not_prefix_matched()
        => Assert.That(
            RepoContextEnvironmentVariables.IsPrefixMatched("LATTICE_DATA_ROOT"),
            Is.False);

    [Test]
    public void A_null_name_is_rejected_rather_than_silently_unmatched()
        => Assert.That(
            () => RepoContextEnvironmentVariables.IsPrefixMatched(null!),
            Throws.ArgumentNullException,
            "returning false for null would report an absent name as 'not in a family', "
            + "which is the caller's bug rendered as a legitimate answer");

    [Test]
    public void Every_published_key_gets_a_resolved_snapshot()
    {
        var snapshots = RepoContextEnvironmentVariables.DescribeResolvedSettings();

        Assert.Multiple(() =>
        {
            Assert.That(
                snapshots.Select(s => s.Name),
                Is.EqualTo(RepoContextEnvironmentVariables.All),
                "a published key with no snapshot would be recognised as read and then "
                + "vanish from the host's report, which is worse for an operator than the "
                + "wrong label it replaced - a wrong label is at least visible");

            Assert.That(
                snapshots.Where(s => string.IsNullOrWhiteSpace(s.Resolved)),
                Is.Empty,
                "an empty resolved value renders as '<unset>' beside a setting that always "
                + "has a value, which reads as a fault that is not there");

            Assert.That(
                snapshots.Where(s => string.IsNullOrWhiteSpace(s.Default)),
                Is.Empty,
                "the default is half the pair: without it 'is this overridden?' is not "
                + "answerable from the line, which is the question the report exists for");
        });
    }

    [Test]
    public void A_snapshot_states_the_parsed_value_not_the_raw_string()
    {
        Environment.SetEnvironmentVariable(
            RepoContextIndexingOptions.ReconcileIntervalSecondsKey, "not-a-number");
        try
        {
            var snapshot = RepoContextEnvironmentVariables.DescribeResolvedSettings()
                .Single(s => s.Name == RepoContextIndexingOptions.ReconcileIntervalSecondsKey);

            Assert.That(
                snapshot.Resolved,
                Is.EqualTo(snapshot.Default),
                "this package falls back to the default on a malformed value, so a report "
                + "echoing the raw string would state a value nothing applied - and would do "
                + "it in the one case where an operator most needs to be told otherwise");
        }
        finally
        {
            Environment.SetEnvironmentVariable(
                RepoContextIndexingOptions.ReconcileIntervalSecondsKey, null);
        }
    }

    [Test]
    public void An_override_is_visible_as_a_difference_from_the_default()
    {
        Environment.SetEnvironmentVariable(
            RepoContextIndexingOptions.ReconcileIntervalSecondsKey, "60");
        try
        {
            var snapshot = RepoContextEnvironmentVariables.DescribeResolvedSettings()
                .Single(s => s.Name == RepoContextIndexingOptions.ReconcileIntervalSecondsKey);

            Assert.Multiple(() =>
            {
                Assert.That(snapshot.Resolved, Is.EqualTo("60s"));
                Assert.That(
                    snapshot.Default, Is.EqualTo("900s"),
                    "issue #2294's own evidence was a resolved value read against an expected "
                    + "one; the pair is what makes the divergence recoverable from the log");
            });
        }
        finally
        {
            Environment.SetEnvironmentVariable(
                RepoContextIndexingOptions.ReconcileIntervalSecondsKey, null);
        }
    }
}
