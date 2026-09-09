using System.Reflection;
using NUnit.Framework;

namespace Orleans.Lattice.Testing;

/// <summary>
/// Reusable guard proving every <b>server-side</b> gRPC interceptor in a package
/// reaches an explicit allow/deny decision on <em>every</em> call shape, not only
/// the shapes the package happens to expose today.
/// <para>
/// <c>Grpc.Core.Interceptors.Interceptor</c> is a pass-through base: each handler
/// it declares simply invokes the continuation. An interceptor that overrides
/// <c>UnaryServerHandler</c> but leaves <c>ClientStreamingServerHandler</c> or
/// <c>DuplexStreamingServerHandler</c> inherited therefore authorizes unary calls
/// and silently admits streaming ones - the gate is absent rather than closed.
/// Nothing fails at the time the gap is introduced; it becomes live the moment a
/// streaming RPC is added to an already-gated service, which is exactly the
/// change least likely to prompt a review of the interceptor. That violates the
/// repository invariant that a security gate must have an explicit deny/allow
/// decision on every branch with deny as the default arm.
/// </para>
/// <para>
/// The audit is self-scoping: only types that already override at least one
/// server handler are held to the contract, so a purely client-side interceptor
/// (one that forwards credentials on outbound calls) is correctly ignored rather
/// than forced to implement server handlers it has no business implementing.
/// Because the type list is rebuilt from reflection on every run, an interceptor
/// added later is audited automatically - a future gap fails
/// <c>build-and-test</c> instead of shipping as a latent authorization hole.
/// </para>
/// <para>
/// This library stays product-agnostic: it references no gRPC type at compile
/// time and identifies the base class and the handler methods purely through
/// <see cref="System.Reflection"/>. The base is <see langword="abstract"/> so it
/// is never discovered on its own; the inherited <c>[Test]</c> runs through the
/// concrete subclass in the consuming assembly.
/// </para>
/// </summary>
public abstract class GrpcServerInterceptorCallShapeContractTestsBase
{
    /// <summary>
    /// The full name of the gRPC interceptor base class, matched by name so this
    /// library needs no compile-time gRPC reference.
    /// </summary>
    private const string InterceptorBaseTypeFullName = "Grpc.Core.Interceptors.Interceptor";

    /// <summary>
    /// The four server-side handler shapes. An interceptor that gates any one of
    /// them must gate all four, because the ungated remainder is an open door.
    /// </summary>
    private static readonly string[] ServerHandlerNames =
    [
        "UnaryServerHandler",
        "ServerStreamingServerHandler",
        "ClientStreamingServerHandler",
        "DuplexStreamingServerHandler",
    ];

    /// <summary>
    /// The package assembly whose interceptors are audited. Only types
    /// <em>declared</em> in this assembly are considered, so each package audits
    /// exactly its own interceptors.
    /// </summary>
    protected abstract Assembly PackageAssembly { get; }

    /// <summary>
    /// Every server-side interceptor in the package overrides all four handler
    /// shapes, so no call shape bypasses the authorization decision.
    /// </summary>
    [Test]
    public void Every_server_side_interceptor_decides_on_every_call_shape()
    {
        var interceptors = PackageAssembly
            .GetTypes()
            .Where(type => type is { IsClass: true, IsAbstract: false } && DerivesFromInterceptor(type))
            .Where(type => ServerHandlerNames.Any(handler => OverridesHandler(type, handler)))
            .OrderBy(type => type.FullName, StringComparer.Ordinal)
            .ToList();

        Assert.That(
            interceptors,
            Is.Not.Empty,
            $"No server-side gRPC interceptor was discovered in '{PackageAssembly.GetName().Name}'. "
            + "The audit would pass vacuously, so the fixture is either in the wrong assembly or the "
            + "interceptor it was written to guard has been removed.");

        var gaps = interceptors
            .SelectMany(type => ServerHandlerNames
                .Where(handler => !OverridesHandler(type, handler))
                .Select(handler => $"{type.FullName}.{handler}"))
            .ToList();

        Assert.That(
            gaps,
            Is.Empty,
            "A server-side gRPC interceptor leaves a call shape on the pass-through base implementation, "
            + "so calls of that shape reach the service with no authorization decision at all: "
            + string.Join(", ", gaps));
    }

    /// <summary>
    /// Whether <paramref name="type"/> derives from the gRPC interceptor base.
    /// </summary>
    private static bool DerivesFromInterceptor(Type type)
    {
        for (var current = type.BaseType; current is not null; current = current.BaseType)
        {
            if (string.Equals(current.FullName, InterceptorBaseTypeFullName, StringComparison.Ordinal))
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>
    /// Whether <paramref name="type"/> - or any of its own base classes below the
    /// gRPC interceptor base - declares <paramref name="handlerName"/>. Walking the
    /// chain means an intermediate base that implements a handler for its
    /// subclasses satisfies the contract, rather than being reported as a gap.
    /// </summary>
    private static bool OverridesHandler(Type type, string handlerName)
    {
        for (var current = type; current is not null; current = current.BaseType)
        {
            if (string.Equals(current.FullName, InterceptorBaseTypeFullName, StringComparison.Ordinal))
            {
                return false;
            }

            var declared = current.GetMethods(
                BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance | BindingFlags.DeclaredOnly);
            if (declared.Any(method => string.Equals(method.Name, handlerName, StringComparison.Ordinal)))
            {
                return true;
            }
        }

        return false;
    }
}
