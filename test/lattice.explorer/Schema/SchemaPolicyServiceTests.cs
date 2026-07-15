using Grpc.Core;
using Orleans.Lattice.Explorer.Schema;
using Orleans.Lattice.Schema;

namespace Orleans.Lattice.Explorer.Tests.Schema;

[TestFixture]
public class SchemaPolicyServiceTests
{
    private static SchemaPolicyService Create(FakeSchemaAdminClient client) => new(client);

    private static LatticeSchemaPolicy Policy() =>
        new(new[] { LatticeSchemaRule.Utf8() });

    [Test]
    public void Constructor_null_client_throws()
    {
        Assert.That(() => new SchemaPolicyService(null!), Throws.ArgumentNullException);
    }

    [Test]
    public async Task GetPolicyAsync_success_returns_policy()
    {
        var policy = Policy();
        var client = new FakeSchemaAdminClient { PolicyResult = policy };
        var service = Create(client);

        var view = await service.GetPolicyAsync("t");

        Assert.Multiple(() =>
        {
            Assert.That(view.IsSuccess, Is.True);
            Assert.That(view.Value, Is.SameAs(policy));
            Assert.That(client.LastTreeId, Is.EqualTo("t"));
        });
    }

    [Test]
    public async Task GetPolicyAsync_absent_policy_is_success_with_null_value()
    {
        var client = new FakeSchemaAdminClient { PolicyResult = null };
        var service = Create(client);

        var view = await service.GetPolicyAsync("t");

        Assert.Multiple(() =>
        {
            Assert.That(view.IsSuccess, Is.True);
            Assert.That(view.Value, Is.Null);
        });
    }

    [Test]
    public async Task GetPolicyAsync_denied_returns_denied_view()
    {
        var client = new FakeSchemaAdminClient { ReadThrows = new LatticeAuthorizationDeniedException("denied") };
        var service = Create(client);

        var view = await service.GetPolicyAsync("t");

        Assert.That(view.Status, Is.EqualTo(SchemaOperationStatus.Denied));
    }

    [Test]
    public async Task GetPolicyAsync_transport_failure_returns_failed_view()
    {
        var client = new FakeSchemaAdminClient { ReadThrows = new RpcException(new Status(StatusCode.Unavailable, "gone")) };
        var service = Create(client);

        var view = await service.GetPolicyAsync("t");

        Assert.That(view.Status, Is.EqualTo(SchemaOperationStatus.Failed));
    }

    [Test]
    public void GetPolicyAsync_empty_tree_throws()
    {
        var service = Create(new FakeSchemaAdminClient());

        Assert.That(() => service.GetPolicyAsync(string.Empty), Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public async Task SetPolicyAsync_forwards_and_succeeds()
    {
        var client = new FakeSchemaAdminClient();
        var service = Create(client);
        var policy = Policy();

        var result = await service.SetPolicyAsync("t", policy);

        Assert.Multiple(() =>
        {
            Assert.That(result.IsSuccess, Is.True);
            Assert.That(client.LastSetPolicy, Is.SameAs(policy));
            Assert.That(client.LastTreeId, Is.EqualTo("t"));
        });
    }

    [Test]
    public void SetPolicyAsync_null_policy_throws()
    {
        var service = Create(new FakeSchemaAdminClient());

        Assert.That(() => service.SetPolicyAsync("t", null!), Throws.ArgumentNullException);
    }

    [Test]
    public void SetPolicyAsync_empty_tree_throws()
    {
        var service = Create(new FakeSchemaAdminClient());

        Assert.That(() => service.SetPolicyAsync(string.Empty, Policy()), Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public async Task SetPolicyAsync_denied_folds_into_denied_result()
    {
        var client = new FakeSchemaAdminClient { MutationThrows = new LatticeAuthorizationDeniedException("denied") };
        var service = Create(client);

        var result = await service.SetPolicyAsync("t", Policy());

        Assert.That(result.Status, Is.EqualTo(SchemaOperationStatus.Denied));
    }

    [Test]
    public async Task SetPolicyAsync_transport_failure_folds_into_failed_result()
    {
        var client = new FakeSchemaAdminClient { MutationThrows = new RpcException(new Status(StatusCode.Unavailable, "gone")) };
        var service = Create(client);

        var result = await service.SetPolicyAsync("t", Policy());

        Assert.That(result.Status, Is.EqualTo(SchemaOperationStatus.Failed));
    }

    [Test]
    public async Task ClearPolicyAsync_removed_reports_success()
    {
        var client = new FakeSchemaAdminClient { ClearPolicyResult = true };
        var service = Create(client);

        var result = await service.ClearPolicyAsync("t");

        Assert.That(result.IsSuccess, Is.True);
    }

    [Test]
    public async Task ClearPolicyAsync_absent_still_reports_success()
    {
        var client = new FakeSchemaAdminClient { ClearPolicyResult = false };
        var service = Create(client);

        var result = await service.ClearPolicyAsync("t");

        Assert.That(result.IsSuccess, Is.True);
    }

    [Test]
    public void ClearPolicyAsync_empty_tree_throws()
    {
        var service = Create(new FakeSchemaAdminClient());

        Assert.That(() => service.ClearPolicyAsync(string.Empty), Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public async Task ClearPolicyAsync_denied_folds_into_denied_result()
    {
        var client = new FakeSchemaAdminClient { MutationThrows = new LatticeAuthorizationDeniedException("denied") };
        var service = Create(client);

        var result = await service.ClearPolicyAsync("t");

        Assert.That(result.Status, Is.EqualTo(SchemaOperationStatus.Denied));
    }
}
