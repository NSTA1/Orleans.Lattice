namespace Orleans.Lattice.Api.Telemetry.Tests;

/// <summary>
/// Tests for <see cref="TelemetryQueryAuthorizer"/>: the read-all posture admits
/// without scanning, and the deny-all posture admits only an expression whose
/// every extractable metric name is allow-listed - failing closed on a
/// <c>__name__</c> regex or negative matcher, an unconstrained label-only
/// selector, an unterminated string, and an expression that names no metric at
/// all. These are the allow-list bypasses the gate exists to close, so each is
/// asserted against its exact caller-facing message.
/// </summary>
[TestFixture]
public sealed class TelemetryQueryAuthorizerTests
{
    private const string RegexOrNegativeDenial =
        "The query references a metric by a '__name__' pattern or negative matcher, "
        + "which the telemetry metric-access allow-list cannot admit.";

    private const string UnconstrainedDenial =
        "The query selects series by label without constraining the metric name, "
        + "which the telemetry metric-access allow-list cannot admit.";

    private const string NoNameDenial =
        "The query does not name a metric the telemetry metric-access allow-list can admit.";

    private static TelemetryMetricAccessPolicy ReadAll() => new(new LatticeTelemetryOptions());

    private static TelemetryMetricAccessPolicy DenyAll(params string[] allowed)
    {
        var options = new LatticeTelemetryOptions
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
    public void Read_all_admits_any_expression_without_scanning_it()
    {
        var admitted = TelemetryQueryAuthorizer.TryAuthorizeQuery(
            ReadAll(), "{__name__=~\".+\"} or {job=\"api\"}", out var denial);

        Assert.Multiple(() =>
        {
            Assert.That(admitted, Is.True);
            Assert.That(denial, Is.Null);
        });
    }

    [Test]
    public void Deny_all_admits_an_allow_listed_metric()
    {
        var admitted = TelemetryQueryAuthorizer.TryAuthorizeQuery(
            DenyAll("lattice_wal_append_total"),
            "rate(lattice_wal_append_total[5m])",
            out var denial);

        Assert.Multiple(() =>
        {
            Assert.That(admitted, Is.True);
            Assert.That(denial, Is.Null);
        });
    }

    [Test]
    public void Deny_all_admits_a_wildcard_matched_metric()
    {
        var admitted = TelemetryQueryAuthorizer.TryAuthorizeQuery(
            DenyAll("lattice_wal_*"), "lattice_wal_append_total", out var denial);

        Assert.Multiple(() =>
        {
            Assert.That(admitted, Is.True);
            Assert.That(denial, Is.Null);
        });
    }

    [Test]
    public void Deny_all_rejects_a_metric_outside_the_allow_list_by_name()
    {
        var admitted = TelemetryQueryAuthorizer.TryAuthorizeQuery(
            DenyAll("lattice_wal_*"), "up", out var denial);

        Assert.Multiple(() =>
        {
            Assert.That(admitted, Is.False);
            Assert.That(denial, Is.EqualTo(TelemetryQueryAuthorizer.DeniedMessage("up")));
        });
    }

    [Test]
    public void Deny_all_rejects_the_first_non_admitted_metric_in_a_mixed_expression()
    {
        var admitted = TelemetryQueryAuthorizer.TryAuthorizeQuery(
            DenyAll("lattice_wal_append_total"),
            "lattice_wal_append_total + secret_metric",
            out var denial);

        Assert.Multiple(() =>
        {
            Assert.That(admitted, Is.False);
            Assert.That(denial, Is.EqualTo(TelemetryQueryAuthorizer.DeniedMessage("secret_metric")));
        });
    }

    [Test]
    public void Deny_all_fails_closed_on_a_name_regex_matcher()
    {
        var admitted = TelemetryQueryAuthorizer.TryAuthorizeQuery(
            DenyAll("lattice_wal_*"), "{__name__=~\"lattice_wal_.*\"}", out var denial);

        Assert.Multiple(() =>
        {
            Assert.That(admitted, Is.False);
            Assert.That(denial, Is.EqualTo(RegexOrNegativeDenial));
        });
    }

    [Test]
    public void Deny_all_fails_closed_on_a_negative_name_matcher()
    {
        var admitted = TelemetryQueryAuthorizer.TryAuthorizeQuery(
            DenyAll("lattice_wal_*"), "{__name__!=\"up\"}", out var denial);

        Assert.Multiple(() =>
        {
            Assert.That(admitted, Is.False);
            Assert.That(denial, Is.EqualTo(RegexOrNegativeDenial));
        });
    }

    [Test]
    public void Deny_all_fails_closed_on_an_unconstrained_label_only_selector()
    {
        var admitted = TelemetryQueryAuthorizer.TryAuthorizeQuery(
            DenyAll("lattice_wal_append_total"),
            "lattice_wal_append_total or {job=\"api\"}",
            out var denial);

        Assert.Multiple(() =>
        {
            Assert.That(
                admitted,
                Is.False,
                "An unanchored selector matches every metric name, so naming an admitted metric "
                + "elsewhere in the expression must not admit it.");
            Assert.That(denial, Is.EqualTo(UnconstrainedDenial));
        });
    }

    [Test]
    public void Deny_all_fails_closed_on_an_unterminated_selector()
    {
        var admitted = TelemetryQueryAuthorizer.TryAuthorizeQuery(
            DenyAll("lattice_wal_*"), "{job=\"api\"", out var denial);

        Assert.Multiple(() =>
        {
            Assert.That(admitted, Is.False);
            Assert.That(denial, Is.EqualTo(UnconstrainedDenial));
        });
    }

    [Test]
    public void Deny_all_fails_closed_on_an_unterminated_name_matcher_string()
    {
        var admitted = TelemetryQueryAuthorizer.TryAuthorizeQuery(
            DenyAll("lattice_wal_*"), "{__name__=\"lattice_wal_append_total", out var denial);

        Assert.Multiple(() =>
        {
            Assert.That(admitted, Is.False);
            Assert.That(denial, Is.EqualTo(RegexOrNegativeDenial));
        });
    }

    [Test]
    public void Deny_all_fails_closed_on_an_expression_naming_no_metric()
    {
        var admitted = TelemetryQueryAuthorizer.TryAuthorizeQuery(
            DenyAll("lattice_wal_*"), "sum(1)", out var denial);

        Assert.Multiple(() =>
        {
            Assert.That(admitted, Is.False);
            Assert.That(denial, Is.EqualTo(NoNameDenial));
        });
    }

    [Test]
    public void Deny_all_admits_an_exact_name_matcher_for_an_allow_listed_metric()
    {
        var admitted = TelemetryQueryAuthorizer.TryAuthorizeQuery(
            DenyAll("lattice_wal_append_total"),
            "{__name__=\"lattice_wal_append_total\"}",
            out var denial);

        Assert.Multiple(() =>
        {
            Assert.That(admitted, Is.True);
            Assert.That(denial, Is.Null);
        });
    }

    [Test]
    public void DeniedMessage_names_the_metric_it_refused()
        => Assert.That(
            TelemetryQueryAuthorizer.DeniedMessage("secret_metric"),
            Is.EqualTo("Metric 'secret_metric' is not permitted by the telemetry metric-access allow-list."));

    [Test]
    public void A_null_policy_is_rejected()
        => Assert.Throws<ArgumentNullException>(
            () => TelemetryQueryAuthorizer.TryAuthorizeQuery(null!, "up", out _));

    [Test]
    public void A_null_query_is_rejected()
        => Assert.Throws<ArgumentNullException>(
            () => TelemetryQueryAuthorizer.TryAuthorizeQuery(ReadAll(), null!, out _));
}
