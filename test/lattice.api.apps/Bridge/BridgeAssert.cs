namespace Orleans.Lattice.Api.Apps.Tests.Bridge;

/// <summary>Assertion helpers shared by the app bridge tests.</summary>
internal static class BridgeAssert
{
    /// <summary>
    /// Asserts <paramref name="call"/> fails with <paramref name="expected"/> and exactly its fixed, sanitised
    /// message.
    /// </summary>
    public static void Fails(AppBridgeFailure expected, Func<Task> call)
    {
        var ex = Assert.ThrowsAsync<AppBridgeException>(call);
        Assert.That(ex!.Failure, Is.EqualTo(expected));
        Assert.That(ex.Message, Is.EqualTo(AppBridgeException.DefaultMessage(expected)));
        Assert.That(ex.InnerException, Is.Null, "no inner exception escapes the bridge");
    }

    /// <summary>Asserts every data verb fails with <paramref name="expected"/> for <paramref name="target"/>.</summary>
    public static void EveryVerbFails(ILatticeAppBridge bridge, AppBridgeTarget target, AppBridgeFailure expected)
    {
        Fails(expected, () => bridge.GetAsync(target, "k"));
        Fails(expected, () => bridge.ScanAsync(target, string.Empty, 10));
        Fails(expected, () => bridge.SetAsync(target, "k", new byte[] { 1 }));
        Fails(expected, () => bridge.DeleteAsync(target, "k"));
    }
}
