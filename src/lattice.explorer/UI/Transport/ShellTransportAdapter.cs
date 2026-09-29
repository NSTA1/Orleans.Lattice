using System.Runtime.CompilerServices;
using Grpc.Core;

namespace Orleans.Lattice.Explorer.UI.Transport;

/// <summary>
/// The shared shape of every Shell transport adapter: a typed gRPC client built
/// once over the circuit's <see cref="ShellTransportChannel.Invoker"/>, and call
/// helpers that map every transport fault through <see cref="ShellTransportFaults"/>.
/// </summary>
/// <typeparam name="TClient">The typed gRPC client the adapter wraps.</typeparam>
/// <remarks>
/// <para>
/// The call helpers take the call's arguments as a <c>TState</c> value and a
/// <see langword="static"/> lambda, so a call allocates no closure: only the
/// async state machine the fault mapping needs, plus whatever the gRPC client
/// itself allocates for the request.
/// </para>
/// <para>
/// An adapter overrides <see cref="MapFault"/> only to name a facade-specific
/// exception type for a status the shared table maps generically.
/// </para>
/// </remarks>
internal abstract class ShellTransportAdapter<TClient>
    where TClient : class
{
    /// <summary>Builds the typed client over the circuit's channel.</summary>
    /// <param name="channel">The circuit's transport channel.</param>
    /// <param name="create">The typed client's factory, for example <c>LatticeAuthApiGrpcClient.Create</c>.</param>
    /// <exception cref="ArgumentNullException">Either argument is <see langword="null"/>.</exception>
    protected ShellTransportAdapter(
        ShellTransportChannel channel,
        Func<CallInvoker, IServiceProvider, TClient> create)
    {
        ArgumentNullException.ThrowIfNull(channel);
        ArgumentNullException.ThrowIfNull(create);
        Client = create(channel.Invoker, channel.SerializerServices);
    }

    /// <summary>The typed gRPC client, bound to the circuit's current channel on every call.</summary>
    protected TClient Client { get; }

    /// <summary>
    /// Maps a transport fault to the exception the facade documents. The default
    /// is the shared table; an adapter overrides it for its facade-specific types.
    /// </summary>
    /// <param name="exception">The transport fault.</param>
    /// <param name="subject">The id the call was about (a tenant or query id), when the facade's exception carries one.</param>
    /// <param name="cancellationToken">The caller's token.</param>
    /// <returns>The exception to throw.</returns>
    protected virtual Exception MapFault(RpcException exception, string? subject, CancellationToken cancellationToken) =>
        ShellTransportFaults.Map(exception, cancellationToken);

    /// <summary>Runs a call that returns a value, mapping any transport fault.</summary>
    /// <typeparam name="TState">The call's arguments.</typeparam>
    /// <typeparam name="TResult">The call's result.</typeparam>
    /// <param name="state">The call's arguments.</param>
    /// <param name="call">The call; pass a <see langword="static"/> lambda.</param>
    /// <param name="subject">The id the call was about, for <see cref="MapFault"/>.</param>
    /// <param name="cancellationToken">The caller's token.</param>
    /// <returns>The call's result.</returns>
    protected async Task<TResult> CallAsync<TState, TResult>(
        TState state,
        Func<TClient, TState, CancellationToken, Task<TResult>> call,
        string? subject,
        CancellationToken cancellationToken)
    {
        try
        {
            return await call(Client, state, cancellationToken).ConfigureAwait(false);
        }
        catch (RpcException exception)
        {
            throw MapFault(exception, subject, cancellationToken);
        }
    }

    /// <summary>Runs a call that returns no value, mapping any transport fault.</summary>
    /// <typeparam name="TState">The call's arguments.</typeparam>
    /// <param name="state">The call's arguments.</param>
    /// <param name="call">The call; pass a <see langword="static"/> lambda.</param>
    /// <param name="subject">The id the call was about, for <see cref="MapFault"/>.</param>
    /// <param name="cancellationToken">The caller's token.</param>
    /// <returns>A task that completes with the call.</returns>
    protected async Task CallAsync<TState>(
        TState state,
        Func<TClient, TState, CancellationToken, Task> call,
        string? subject,
        CancellationToken cancellationToken)
    {
        try
        {
            await call(Client, state, cancellationToken).ConfigureAwait(false);
        }
        catch (RpcException exception)
        {
            throw MapFault(exception, subject, cancellationToken);
        }
    }

    /// <summary>
    /// Enumerates a server stream, mapping any transport fault raised while reading
    /// the next item (a server stream opens lazily, on the first read).
    /// </summary>
    /// <typeparam name="TState">The call's arguments.</typeparam>
    /// <typeparam name="TItem">The streamed item.</typeparam>
    /// <param name="state">The call's arguments.</param>
    /// <param name="open">Opens the stream; pass a <see langword="static"/> lambda.</param>
    /// <param name="subject">The id the call was about, for <see cref="MapFault"/>.</param>
    /// <param name="cancellationToken">The caller's token.</param>
    /// <returns>The streamed items.</returns>
    protected async IAsyncEnumerable<TItem> StreamAsync<TState, TItem>(
        TState state,
        Func<TClient, TState, CancellationToken, IAsyncEnumerable<TItem>> open,
        string? subject,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var enumerator = open(Client, state, cancellationToken).GetAsyncEnumerator(cancellationToken);
        await using (enumerator.ConfigureAwait(false))
        {
            while (true)
            {
                bool moved;
                try
                {
                    moved = await enumerator.MoveNextAsync().ConfigureAwait(false);
                }
                catch (RpcException exception)
                {
                    throw MapFault(exception, subject, cancellationToken);
                }

                if (!moved)
                {
                    yield break;
                }

                yield return enumerator.Current;
            }
        }
    }
}
