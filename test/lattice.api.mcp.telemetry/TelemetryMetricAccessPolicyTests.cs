namespace Orleans.Lattice.Api.Mcp.Telemetry.Tests;

/// <summary>
/// Tests for <see cref="TelemetryMetricAccessPolicy"/>: the read-all posture
/// admits every metric, and the deny-all posture admits only exact allow-list
/// names and <c>*</c>-wildcard pattern matches while rejecting everything else.
/// </summary>
[TestFixture]
public sealed class TelemetryMetricAccessPolicyTests
{
    private static TelemetryMetricAccessPolicy DenyAll(params string[] allowed)
    {
        var options = new LatticeApiMcpTelemetryOptions
        {
            MetricAccess = LatticeTelemetryMetricAccessMode.DenyAllExceptAllowed,
        };
        foreach (var entry in allowed)
        {
            options.AllowedMetrics.Add(entry);
        }

        return new TelemetryMetricAccessPolicy(options);
    }

    [Test]
    public void ReadAll_admits_every_metric()
    {
        var policy = new TelemetryMetricAccessPolicy(new LatticeApiMcpTelemetryOptions());

        Assert.Multiple(() =>
        {
            Assert.That(policy.IsReadAll, Is.True);
            Assert.That(policy.IsAdmitted("up"), Is.True);
            Assert.That(policy.IsAdmitted("anything_at_all"), Is.True);
        });
    }

    [Test]
    public void DenyAll_admits_an_exact_name()
    {
        var policy = DenyAll("lattice_wal_append_total");

        Assert.Multiple(() =>
        {
            Assert.That(policy.IsReadAll, Is.False);
            Assert.That(policy.IsAdmitted("lattice_wal_append_total"), Is.True);
        });
    }

    [Test]
    public void DenyAll_rejects_a_name_not_in_the_allow_list()
    {
        var policy = DenyAll("lattice_wal_append_total");
        Assert.That(policy.IsAdmitted("up"), Is.False);
    }

    [Test]
    public void DenyAll_admits_a_wildcard_prefix_match()
    {
        var policy = DenyAll("lattice_wal_*");

        Assert.Multiple(() =>
        {
            Assert.That(policy.IsAdmitted("lattice_wal_append_total"), Is.True);
            Assert.That(policy.IsAdmitted("lattice_wal_flush_seconds"), Is.True);
        });
    }

    [Test]
    public void DenyAll_rejects_a_name_outside_the_wildcard_pattern()
    {
        var policy = DenyAll("lattice_wal_*");
        Assert.That(policy.IsAdmitted("lattice_shard_count"), Is.False);
    }

    [Test]
    public void A_wildcard_is_anchored_to_the_whole_name()
    {
        var policy = DenyAll("lattice_*_total");

        Assert.Multiple(() =>
        {
            // Whole-name match required: a prefix or suffix beyond the pattern is rejected.
            Assert.That(policy.IsAdmitted("lattice_wal_append_total"), Is.True);
            Assert.That(policy.IsAdmitted("x_lattice_wal_append_total"), Is.False);
            Assert.That(policy.IsAdmitted("lattice_wal_append_total_extra"), Is.False);
        });
    }

    [Test]
    public void The_wildcard_is_the_only_special_character()
    {
        // A '.' in a pattern is a literal dot, not a regex any-char.
        var policy = DenyAll("lattice.wal");

        Assert.Multiple(() =>
        {
            Assert.That(policy.IsAdmitted("lattice.wal"), Is.True);
            Assert.That(policy.IsAdmitted("latticeXwal"), Is.False);
        });
    }

    [Test]
    public void A_single_star_admits_everything_in_deny_all()
    {
        var policy = DenyAll("*");

        Assert.Multiple(() =>
        {
            Assert.That(policy.IsReadAll, Is.False);
            Assert.That(policy.IsAdmitted("up"), Is.True);
            Assert.That(policy.IsAdmitted("lattice_wal_append_total"), Is.True);
        });
    }

    [Test]
    public void An_empty_deny_all_allow_list_admits_nothing()
    {
        var policy = DenyAll();
        Assert.That(policy.IsAdmitted("up"), Is.False);
    }

    [Test]
    public void Null_or_empty_allow_list_entries_are_ignored()
    {
        var options = new LatticeApiMcpTelemetryOptions
        {
            MetricAccess = LatticeTelemetryMetricAccessMode.DenyAllExceptAllowed,
        };
        options.AllowedMetrics.Add(string.Empty);
        options.AllowedMetrics.Add("up");
        var policy = new TelemetryMetricAccessPolicy(options);

        Assert.Multiple(() =>
        {
            Assert.That(policy.IsAdmitted("up"), Is.True);
            Assert.That(policy.IsAdmitted(string.Empty), Is.False);
        });
    }

    [Test]
    public void A_null_options_is_rejected()
        => Assert.Throws<ArgumentNullException>(() => new TelemetryMetricAccessPolicy(options: null!));

    [Test]
    public void A_null_metric_is_rejected()
    {
        var policy = new TelemetryMetricAccessPolicy(new LatticeApiMcpTelemetryOptions());
        Assert.Throws<ArgumentNullException>(() => policy.IsAdmitted(metric: null!));
    }
}
