using System.Reflection;
using NUnit.Framework;

namespace Orleans.Lattice.Testing;

/// <summary>
/// A silo's runtime grain-call response timeout, read and changed while the silo runs. The
/// configured <c>SiloMessagingOptions.ResponseTimeout</c> is read once, when the runtime client
/// is built, so a fixture that wants a short timeout for one section only (and the default for
/// its setup) has to go through the runtime client, which Orleans keeps internal. Grain calls
/// sent from the silo take both their deadline and their message expiry from this value, unless
/// the called method declares its own <c>[ResponseTimeout]</c>.
/// </summary>
/// <remarks>
/// Resolved by reflection so this library keeps no Orleans reference. Every lookup asserts, so an
/// Orleans upgrade that renames the seam fails loudly at the fixture instead of silently leaving
/// the timeout unchanged.
/// </remarks>
public sealed class SiloResponseTimeout
{
    private const string RuntimeClientType = "Orleans.Runtime.IRuntimeClient, Orleans.Core";

    private readonly object _runtimeClient;
    private readonly MethodInfo _get;
    private readonly MethodInfo _set;

    private SiloResponseTimeout(object runtimeClient, MethodInfo get, MethodInfo set)
    {
        _runtimeClient = runtimeClient;
        _get = get;
        _set = set;
    }

    /// <summary>The silo's current runtime response timeout.</summary>
    public TimeSpan Current => (TimeSpan)_get.Invoke(_runtimeClient, null)!;

    /// <summary>Resolves the runtime response timeout of the silo whose services are given.</summary>
    /// <param name="siloServices">The silo host's service provider.</param>
    /// <returns>The silo's runtime response timeout.</returns>
    public static SiloResponseTimeout Resolve(IServiceProvider siloServices)
    {
        ArgumentNullException.ThrowIfNull(siloServices);
        var type = Type.GetType(RuntimeClientType, throwOnError: false);
        Assert.That(type, Is.Not.Null,
            "Orleans.Runtime.IRuntimeClient was renamed or moved; update SiloResponseTimeout.");
        var get = type!.GetMethod("GetResponseTimeout", Type.EmptyTypes);
        var set = type.GetMethod("SetResponseTimeout", [typeof(TimeSpan)]);
        var client = siloServices.GetService(type);
        Assert.Multiple(() =>
        {
            Assert.That(get, Is.Not.Null, "IRuntimeClient.GetResponseTimeout was renamed; update SiloResponseTimeout.");
            Assert.That(set, Is.Not.Null, "IRuntimeClient.SetResponseTimeout was renamed; update SiloResponseTimeout.");
            Assert.That(client, Is.Not.Null, "the silo no longer registers IRuntimeClient; update SiloResponseTimeout.");
        });
        return new SiloResponseTimeout(client!, get!, set!);
    }

    /// <summary>Sets the silo's runtime response timeout.</summary>
    /// <param name="timeout">The new timeout.</param>
    public void Set(TimeSpan timeout) => _set.Invoke(_runtimeClient, [timeout]);
}
