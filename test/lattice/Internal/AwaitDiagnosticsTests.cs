namespace Orleans.Lattice.Tests.Internal;

/// <summary>
/// Covers <see cref="AwaitDiagnostics"/>, the benchmark-only slow-await diagnostic.
/// The test host never sets <c>LATTICE_BENCH_AWAIT_DIAG_MS</c>, so these tests also
/// pin the disabled path that production pays for.
/// </summary>
[TestFixture]
[Category("Unit")]
public sealed class AwaitDiagnosticsTests
{
    [TestCase(null)]
    [TestCase("")]
    [TestCase("0")]
    [TestCase("-5")]
    [TestCase("abc")]
    [TestCase("1.5")]
    public void ParseThresholdMs_disables_on_anything_but_a_positive_integer(string? value)
    {
        Assert.That(AwaitDiagnostics.ParseThresholdMs(value), Is.EqualTo(0));
    }

    [Test]
    public void ParseThresholdMs_accepts_a_positive_integer()
    {
        Assert.That(AwaitDiagnostics.ParseThresholdMs("5000"), Is.EqualTo(5000));
    }

    [Test]
    public void FormatLine_renders_state_region_duration_grain_and_detail()
    {
        var line = AwaitDiagnostics.FormatLine("done", "leaf.replay", 6123.4, "leaf/abc", "keys=12");

        Assert.That(line, Is.EqualTo("[await-diag] state=done region=leaf.replay ms=6123 grain=leaf/abc keys=12"));
    }

    [Test]
    public void FormatLine_uses_a_dash_for_a_missing_grain_and_omits_an_empty_detail()
    {
        var line = AwaitDiagnostics.FormatLine("suppressed", "shard.serial-turn", 0, null, null);

        Assert.That(line, Is.EqualTo("[await-diag] state=suppressed region=shard.serial-turn ms=0 grain=-"));
    }

    [Test]
    public void Diagnostic_is_disabled_in_the_test_host()
    {
        Assert.That(AwaitDiagnostics.Enabled, Is.False);
    }

    [Test]
    public void Begin_returns_a_no_op_scope_when_disabled()
    {
        var scope = AwaitDiagnostics.Begin("leaf.split", default, "keys=1");

        Assert.That(scope, Is.EqualTo(default(AwaitDiagnostics.Scope)));
        Assert.DoesNotThrow(() =>
        {
            scope.Dispose();
            scope.Dispose();
        });
    }

    [Test]
    public void Time_returns_the_same_task_instance_when_disabled()
    {
        var source = new TaskCompletionSource<int>();

        var timed = AwaitDiagnostics.Time(source.Task, "tx.terminal-hlc-leaf", default);

        Assert.That(timed, Is.SameAs(source.Task));
    }
}
