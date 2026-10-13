using Grpc.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Replication.Grpc.Tests;

public partial class GrpcRemoteSnapshotTransportClientIntegrationTests
{
    [TestCase(true, true)]
    [TestCase(false, true)]
    [TestCase(true, false)]
    public async Task Tenant_export_only_uses_credential_bound_requester(
        bool binding, bool authentication)
    {
        const string tenantTree = "t/acme/orders";
        _sender.Stage(tenantTree, new SnapshotStream(tenantTree, HybridLogicalClock.Zero,
            new VersionVector(), AsAsync([new SnapshotEntry { Key = "pending", Value = [42], IsPrepared = true }])));
        var options = _host.Services.GetRequiredService<IOptionsMonitor<LatticeReplicationSecurityOptions>>().CurrentValue;
        var oldAuthentication = options.RequireAuthentication;
        var oldBinding = options.BindCredentialToOriginCluster;
        options.RequireAuthentication = authentication;
        options.BindCredentialToOriginCluster = binding;
        _exportGate.ClearReceivedCalls();
        using var transport = CreateTransport("peer-token");
        try
        {
            if (authentication && binding)
            {
                var metadata = await transport.GetMetadataAsync(tenantTree, Source, HybridLogicalClock.Zero);
                Assert.That(metadata.TreeName, Is.EqualTo(tenantTree));
                var entries = new List<SnapshotEntry>();
                await foreach (var entry in transport.RequestSnapshotAsync(tenantTree, Source, HybridLogicalClock.Zero))
                    entries.Add(entry);
                Assert.That(entries.Single().Value, Is.EqualTo(new byte[] { 42 }));
                Assert.That(entries.Single().IsPrepared, Is.True);
                await _exportGate.Received(2).AuthorizeExportAsync(tenantTree, "local", Arg.Any<CancellationToken>());
            }
            else
            {
                var metadataError = Assert.ThrowsAsync<RpcException>(async () =>
                    await transport.GetMetadataAsync(tenantTree, Source, HybridLogicalClock.Zero));
                var streamError = Assert.ThrowsAsync<RpcException>(async () =>
                {
                    await foreach (var _ in transport.RequestSnapshotAsync(tenantTree, Source, HybridLogicalClock.Zero)) { }
                });
                Assert.That(metadataError!.StatusCode, Is.EqualTo(StatusCode.PermissionDenied));
                Assert.That(streamError!.StatusCode, Is.EqualTo(StatusCode.PermissionDenied));
                await _exportGate.DidNotReceiveWithAnyArgs().AuthorizeExportAsync(default!, default!, default);
            }
        }
        finally
        {
            options.RequireAuthentication = oldAuthentication;
            options.BindCredentialToOriginCluster = oldBinding;
        }
    }

    [Test]
    public async Task Tenant_export_credential_cannot_claim_another_requester()
    {
        var options = _host.Services.GetRequiredService<IOptionsMonitor<LatticeReplicationSecurityOptions>>().CurrentValue;
        var oldAuthentication = options.RequireAuthentication;
        options.RequireAuthentication = true;
        using var transport = CreateTransport("peer-token", "other-peer");
        try
        {
            var ex = Assert.ThrowsAsync<RpcException>(async () =>
                await transport.GetMetadataAsync("t/acme/orders", Source, HybridLogicalClock.Zero));
            Assert.That(ex!.StatusCode, Is.EqualTo(StatusCode.PermissionDenied));
        }
        finally
        {
            options.RequireAuthentication = oldAuthentication;
        }
    }
}
