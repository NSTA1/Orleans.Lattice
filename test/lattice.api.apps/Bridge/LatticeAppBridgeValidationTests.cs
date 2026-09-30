namespace Orleans.Lattice.Api.Apps.Tests.Bridge;

/// <summary>
/// The app bridge's syntactic validation and size bounds, which run before any authorization step, so a
/// malformed request learns nothing about the install and never reaches the data path.
/// </summary>
[TestFixture]
public sealed class LatticeAppBridgeValidationTests
{
    private static BridgeHarness Editor() => new BridgeHarness().Installed("alice", BridgeHarness.Editors);

    private static void AssertRefusedUntouched(BridgeHarness harness, AppBridgeFailure expected, Func<Task> call)
    {
        BridgeAssert.Fails(expected, call);
        harness.AssertDataPathUntouched();
    }

    [Test]
    public void A_null_target_is_invalid()
    {
        var harness = Editor();
        BridgeAssert.EveryVerbFails(harness.Bridge, null!, AppBridgeFailure.Invalid);
        harness.AssertDataPathUntouched();
    }

    [TestCase("")]
    [TestCase("CRM")]
    [TestCase("a/crm")]
    [TestCase("x")]
    public void A_malformed_app_slug_is_invalid(string slug)
    {
        var harness = Editor();
        BridgeAssert.EveryVerbFails(harness.Bridge, BridgeHarness.Target(slug: slug), AppBridgeFailure.Invalid);
        harness.AssertDataPathUntouched();
    }

    [Test]
    public void A_null_or_empty_key_is_invalid()
    {
        var harness = Editor();
        var bridge = harness.Bridge;
        foreach (var key in new[] { null!, string.Empty })
        {
            AssertRefusedUntouched(harness, AppBridgeFailure.Invalid, () => bridge.GetAsync(BridgeHarness.Target(), key));
            AssertRefusedUntouched(harness, AppBridgeFailure.Invalid, () => bridge.SetAsync(BridgeHarness.Target(), key, new byte[] { 1 }));
            AssertRefusedUntouched(harness, AppBridgeFailure.Invalid, () => bridge.DeleteAsync(BridgeHarness.Target(), key));
        }
    }

    [Test]
    public void A_key_longer_than_the_bound_is_too_large()
    {
        var harness = Editor();
        var bridge = harness.Bridge;
        var key = new string('k', AppBridgeLimits.MaxKeyLength + 1);

        AssertRefusedUntouched(harness, AppBridgeFailure.TooLarge, () => bridge.GetAsync(BridgeHarness.Target(), key));
        AssertRefusedUntouched(harness, AppBridgeFailure.TooLarge, () => bridge.SetAsync(BridgeHarness.Target(), key, new byte[] { 1 }));
        AssertRefusedUntouched(harness, AppBridgeFailure.TooLarge, () => bridge.DeleteAsync(BridgeHarness.Target(), key));
    }

    [Test]
    public async Task A_key_exactly_at_the_bound_is_accepted()
    {
        var harness = Editor();
        var key = new string('k', AppBridgeLimits.MaxKeyLength);

        await harness.Bridge.SetAsync(BridgeHarness.Target(), key, new byte[] { 1 });

        Assert.That(harness.Store(BridgeHarness.NotesTree).ContainsKey(key), Is.True);
    }

    [Test]
    public void A_value_larger_than_the_bound_is_too_large()
    {
        var harness = Editor();

        AssertRefusedUntouched(harness, AppBridgeFailure.TooLarge,
            () => harness.Bridge.SetAsync(BridgeHarness.Target(), "k", new byte[AppBridgeLimits.MaxValueBytes + 1]));
    }

    [Test]
    public void A_null_scan_prefix_is_invalid()
    {
        var harness = Editor();
        AssertRefusedUntouched(harness, AppBridgeFailure.Invalid, () => harness.Bridge.ScanAsync(BridgeHarness.Target(), null!, 10));
    }

    [Test]
    public void A_scan_prefix_longer_than_the_bound_is_too_large()
    {
        var harness = Editor();
        AssertRefusedUntouched(harness, AppBridgeFailure.TooLarge,
            () => harness.Bridge.ScanAsync(BridgeHarness.Target(), new string('p', AppBridgeLimits.MaxKeyLength + 1), 10));
    }

    [TestCase(0)]
    [TestCase(-1)]
    [TestCase(int.MinValue)]
    public void A_non_positive_page_size_is_invalid(int pageSize)
    {
        var harness = Editor();
        AssertRefusedUntouched(harness, AppBridgeFailure.Invalid, () => harness.Bridge.ScanAsync(BridgeHarness.Target(), string.Empty, pageSize));
    }

    [TestCase("")]
    [TestCase("k1:")]
    [TestCase("cursor-17")]
    [TestCase("k2:n/1")]
    public void A_malformed_continuation_is_invalid(string continuation)
    {
        var harness = Editor();
        AssertRefusedUntouched(harness, AppBridgeFailure.Invalid,
            () => harness.Bridge.ScanAsync(BridgeHarness.Target(), "n/", 10, continuation));
    }

    [Test]
    public void A_continuation_outside_the_prefix_cannot_widen_the_scan()
    {
        var harness = Editor();
        AssertRefusedUntouched(harness, AppBridgeFailure.Invalid,
            () => harness.Bridge.ScanAsync(BridgeHarness.Target(), "n/", 10, AppBridgeContinuation.Encode("a")));
    }

    [Test]
    public void A_continuation_longer_than_the_bound_is_too_large()
    {
        var harness = Editor();
        AssertRefusedUntouched(harness, AppBridgeFailure.TooLarge,
            () => harness.Bridge.ScanAsync(BridgeHarness.Target(), string.Empty, 10, new string('c', AppBridgeLimits.MaxContinuationLength + 1)));
    }

    [Test]
    public void Validation_runs_before_the_install_is_resolved()
    {
        // No install exists, so an authorization step would deny; the malformed request is refused as invalid.
        var harness = new BridgeHarness().As("alice", BridgeHarness.Editors);

        BridgeAssert.Fails(AppBridgeFailure.Invalid, () => harness.Bridge.GetAsync(BridgeHarness.Target(), string.Empty));
        BridgeAssert.Fails(AppBridgeFailure.TooLarge, () => harness.Bridge.SetAsync(BridgeHarness.Target(), "k", new byte[AppBridgeLimits.MaxValueBytes + 1]));
    }
}
