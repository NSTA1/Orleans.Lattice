using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Net;
using System.Net.Http.Headers;
using System.Reflection;
using System.Runtime.CompilerServices;
using Grpc.Core;

namespace Orleans.Lattice.Explorer.Tests.UI.Transport;

/// <summary>
/// An in-memory gRPC peer for the Shell transport tests. It is the
/// <see cref="HttpMessageHandler"/> under a real <c>GrpcChannel</c>, so every call
/// goes through the real gRPC client stack - the Core call invoker, its
/// credential interceptors and the typed client's marshallers - and only the
/// network hop is replaced. No host, socket or timer is involved, so every
/// answer is deterministic.
/// </summary>
/// <remarks>
/// <para>
/// It records each request's RPC path and headers, then answers by the current
/// script: a trailers-only status (for fault mapping), or a success frame built
/// from the RPC's own response marshaller (for success).
/// </para>
/// </remarks>
internal sealed class ShellTransportPeer : HttpMessageHandler
{
    private readonly ConcurrentQueue<ShellTransportRequest> _requests = new();
    private readonly Dictionary<string, IMethod> _methods = new(StringComparer.Ordinal);
    private readonly Dictionary<string, object> _responses = new(StringComparer.Ordinal);

    private StatusCode _status = StatusCode.OK;
    private string _detail = string.Empty;

    /// <summary>The Orleans serializer provider the success frames are encoded with.</summary>
    public IServiceProvider? Serializers { get; set; }

    /// <summary>The requests seen so far, in arrival order.</summary>
    public IReadOnlyList<ShellTransportRequest> Requests => [.. _requests];

    /// <summary>Answers every later call with a trailers-only <paramref name="status"/>.</summary>
    /// <param name="status">The status to answer with.</param>
    /// <param name="detail">The status detail.</param>
    public void AnswerWith(StatusCode status, string detail = "")
    {
        _status = status;
        _detail = detail;
    }

    /// <summary>Answers every later call with success, using the registered responses or a default one.</summary>
    public void AnswerWithSuccess() => AnswerWith(StatusCode.OK);

    /// <summary>
    /// Learns the RPCs a typed gRPC client can make, so a success answer can be
    /// encoded with each RPC's own response marshaller.
    /// </summary>
    /// <param name="adapterOrClient">A Shell transport adapter, or a typed gRPC client, whose RPCs to learn.</param>
    public void Learn(object adapterOrClient)
    {
        const BindingFlags Instance = BindingFlags.Instance | BindingFlags.NonPublic;
        var client = adapterOrClient.GetType().GetField("_methods", Instance) is null
            ? adapterOrClient.GetType().GetProperty("Client", Instance)!.GetValue(adapterOrClient)!
            : adapterOrClient;
        var holder = client.GetType().GetField("_methods", Instance)!.GetValue(client)!;

        foreach (var property in holder.GetType().GetProperties(BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic))
        {
            if (property.GetValue(holder) is IMethod method)
            {
                _methods[method.FullName] = method;
            }
        }
    }

    /// <summary>Answers the RPC <paramref name="fullName"/> with <paramref name="response"/> on success.</summary>
    /// <param name="fullName">The RPC's full name, <c>/service/Method</c>.</param>
    /// <param name="response">The response message.</param>
    public void Respond(string fullName, object response) => _responses[fullName] = response;

    /// <inheritdoc />
    protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
    {
        var path = request.RequestUri!.AbsolutePath;
        var headers = request.Headers.ToDictionary(
            header => header.Key.ToLowerInvariant(),
            header => string.Join(",", header.Value),
            StringComparer.Ordinal);
        if (request.Content is not null)
        {
            await request.Content.ReadAsByteArrayAsync(cancellationToken).ConfigureAwait(false);
        }

        _requests.Enqueue(new ShellTransportRequest(path, headers));

        var response = new HttpResponseMessage(HttpStatusCode.OK)
        {
            Version = HttpVersion.Version20,
            RequestMessage = request,
        };

        if (_status != StatusCode.OK)
        {
            response.Content = new ByteArrayContent([]);
            response.Content.Headers.ContentType = new MediaTypeHeaderValue("application/grpc");
            response.Headers.Add("grpc-status", ((int)_status).ToString(System.Globalization.CultureInfo.InvariantCulture));
            if (_detail.Length > 0)
            {
                response.Headers.Add("grpc-message", _detail);
            }

            return response;
        }

        var frames = Encode(path);
        response.Content = new ByteArrayContent(frames);
        response.Content.Headers.ContentType = new MediaTypeHeaderValue("application/grpc");
        response.TrailingHeaders.Add("grpc-status", "0");
        return response;
    }

    private byte[] Encode(string path)
    {
        if (!_methods.TryGetValue(path, out var method))
        {
            throw new InvalidOperationException($"The peer has not learned the RPC '{path}'.");
        }

        if (method.Type == MethodType.ServerStreaming && !_responses.ContainsKey(path))
        {
            return [];
        }

        var responseType = method.GetType().GetGenericArguments()[1];
        var serializerType = typeof(Orleans.Serialization.Serializer<>).MakeGenericType(responseType);
        var serializer = Serializers!.GetService(serializerType)
            ?? throw new InvalidOperationException($"No serializer for '{responseType}'.");
        var message = _responses.TryGetValue(path, out var scripted) ? scripted : CreateDefault(responseType);
        var serialize = serializerType.GetMethods()
            .Single(candidate => candidate.Name == "SerializeToArray" && candidate.GetParameters().Length == 1);
        var payload = (byte[])serialize.Invoke(serializer, [message])!;

        var frame = new byte[5 + payload.Length];
        BinaryPrimitives.WriteUInt32BigEndian(frame.AsSpan(1, 4), (uint)payload.Length);
        payload.CopyTo(frame, 5);
        return frame;
    }

    private static object CreateDefault(Type type)
    {
        var parameterless = type.GetConstructor(BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic, Type.EmptyTypes);
        var message = parameterless is not null ? parameterless.Invoke(null) : RuntimeHelpers.GetUninitializedObject(type);

        // A wire message with a required string left null would be rejected by the
        // typed client's own mapping, so every null string member gets a value.
        foreach (var property in type.GetProperties(BindingFlags.Instance | BindingFlags.Public))
        {
            if (property.PropertyType == typeof(string) && property.CanWrite && property.GetValue(message) is null)
            {
                property.SetValue(message, "x");
            }
        }

        return message;
    }
}
