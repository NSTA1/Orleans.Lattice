namespace Orleans.Lattice.Explorer.Tests.UI.Transport;

/// <summary>One facade member, the RPC it must reach, and how to invoke it.</summary>
/// <typeparam name="TFacade">The facade interface.</typeparam>
/// <param name="Member">The facade member's name, for failure messages.</param>
/// <param name="Rpc">The RPC's full name, <c>/service/Method</c>.</param>
/// <param name="Invoke">Invokes the member, draining it when it streams.</param>
internal sealed record ShellTransportCall<TFacade>(string Member, string Rpc, Func<TFacade, CancellationToken, Task> Invoke);
