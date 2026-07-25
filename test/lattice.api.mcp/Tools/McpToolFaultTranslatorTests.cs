using System.IO;
using Grpc.Core;
using ModelContextProtocol;

namespace Orleans.Lattice.Api.Mcp.Tests;

/// <summary>
/// Unit tests for <see cref="McpToolFaultTranslator"/>, the shared seam that
/// converts any fault escaping a facade-backed MCP tool into an actionable
/// <see cref="McpException"/> so nothing reaches the ModelContextProtocol SDK's
/// generic mask (issue #1352). Proves each gRPC <see cref="StatusCode"/> is
/// surfaced with its code and sanitised detail, a local MCP-host fault is surfaced
/// with its type and message, an <see cref="McpException"/> passes through
/// unchanged, a fail-closed denial stays a denial, and the seam never names a
/// satellite-assembly type.
/// </summary>
[TestFixture]
public sealed class McpToolFaultTranslatorTests
{
    [Test]
    public void Translate_null_fault_is_rejected()
        => Assert.That(() => McpToolFaultTranslator.Translate(null!), Throws.ArgumentNullException);

    [Test]
    public void Translate_returns_an_mcp_exception_unchanged()
    {
        var original = new McpException("already actionable");

        var translated = McpToolFaultTranslator.Translate(original);

        Assert.That(translated, Is.SameAs(original),
            "an McpException already carries an actionable message and must not be re-wrapped");
    }

    [Test]
    public void Translate_failed_precondition_surfaces_the_detail_verbatim()
    {
        const string detail =
            "Replication for tree 'orders' is already enabled; disable then re-enable it under OrSet.";

        var translated = McpToolFaultTranslator.Translate(
            new RpcException(new Status(StatusCode.FailedPrecondition, detail)));

        Assert.That(translated.Message, Is.EqualTo(detail),
            "the precondition detail is the operator-facing guidance and is surfaced directly");
    }

    [Test]
    public void Translate_internal_names_a_server_side_fault_and_points_at_cluster_logs()
    {
        // The Internal wire message is deliberately generic (no server internals);
        // the operator must at least learn it is a server-side fault and where to
        // look. The generic detail is surfaced, never a raw exception or stack.
        var translated = McpToolFaultTranslator.Translate(
            new RpcException(new Status(StatusCode.Internal, "The replication control-API request failed")));

        Assert.Multiple(() =>
        {
            Assert.That(translated.Message, Does.Contain("server-side fault"));
            Assert.That(translated.Message, Does.Contain(nameof(StatusCode.Internal)));
            Assert.That(translated.Message, Does.Contain("cluster logs"));
            Assert.That(translated.Message, Does.Contain("The replication control-API request failed"));
        });
    }

    [Test]
    public void Translate_permission_denied_stays_a_fail_closed_denial()
    {
        // Security: a fail-closed denial keeps propagating as a denial with its
        // safe message; it is surfaced, never downgraded or swallowed.
        var translated = McpToolFaultTranslator.Translate(
            new RpcException(new Status(StatusCode.PermissionDenied, "caller lacks the Replication grant")));

        Assert.Multiple(() =>
        {
            Assert.That(translated.Message, Does.Contain(nameof(StatusCode.PermissionDenied)));
            Assert.That(translated.Message, Does.Contain("denied"));
            Assert.That(translated.Message, Does.Contain("caller lacks the Replication grant"));
        });
    }

    [Test]
    public void Translate_unauthenticated_is_surfaced_as_a_denial()
    {
        var translated = McpToolFaultTranslator.Translate(
            new RpcException(new Status(StatusCode.Unauthenticated, "no credential presented")));

        Assert.Multiple(() =>
        {
            Assert.That(translated.Message, Does.Contain(nameof(StatusCode.Unauthenticated)));
            Assert.That(translated.Message, Does.Contain("no credential presented"));
        });
    }

    [TestCase(StatusCode.Unavailable)]
    [TestCase(StatusCode.DeadlineExceeded)]
    [TestCase(StatusCode.Cancelled)]
    [TestCase(StatusCode.Aborted)]
    [TestCase(StatusCode.ResourceExhausted)]
    public void Translate_transport_and_timing_faults_report_an_incomplete_request(StatusCode code)
    {
        var translated = McpToolFaultTranslator.Translate(
            new RpcException(new Status(code, "peer unreachable")));

        Assert.Multiple(() =>
        {
            Assert.That(translated.Message, Does.Contain(code.ToString()));
            Assert.That(translated.Message, Does.Contain("could not be completed"));
            Assert.That(translated.Message, Does.Contain("peer unreachable"));
        });
    }

    [TestCase(StatusCode.InvalidArgument)]
    [TestCase(StatusCode.NotFound)]
    [TestCase(StatusCode.Unimplemented)]
    public void Translate_other_statuses_label_the_code_and_detail(StatusCode code)
    {
        var translated = McpToolFaultTranslator.Translate(
            new RpcException(new Status(code, "bad tree id")));

        Assert.Multiple(() =>
        {
            Assert.That(translated.Message, Does.Contain(code.ToString()));
            Assert.That(translated.Message, Does.Contain("bad tree id"));
        });
    }

    [Test]
    public void Translate_an_rpc_fault_with_no_detail_still_names_the_status()
    {
        var translated = McpToolFaultTranslator.Translate(
            new RpcException(new Status(StatusCode.Internal, string.Empty)));

        Assert.That(translated.Message, Does.Contain(nameof(StatusCode.Internal)));
    }

    [Test]
    public void Translate_a_missing_assembly_load_failure_is_surfaced_with_type_and_message()
    {
        // Reproduces the estate scenario: the MCP host is missing a satellite
        // assembly, so a FileNotFoundException is raised locally. It never crossed
        // the trust boundary, so its type and message are safe - and the most
        // actionable - to show.
        const string message =
            "Could not load file or assembly 'Orleans.Lattice.Replication, Version=8.0.6.0'.";
        var translated = McpToolFaultTranslator.Translate(new FileNotFoundException(message));

        Assert.Multiple(() =>
        {
            Assert.That(translated.Message, Does.Contain(nameof(FileNotFoundException)));
            Assert.That(translated.Message, Does.Contain(message));
            Assert.That(translated.Message, Does.Contain("locally"));
        });
    }

    [Test]
    public void Translate_a_type_load_failure_is_surfaced_with_type_and_message()
    {
        var translated = McpToolFaultTranslator.Translate(
            new TypeLoadException("Could not load type 'Orleans.Lattice.Replication.SomeType'."));

        Assert.Multiple(() =>
        {
            Assert.That(translated.Message, Does.Contain(nameof(TypeLoadException)));
            Assert.That(translated.Message, Does.Contain("Orleans.Lattice.Replication.SomeType"));
        });
    }

    [Test]
    public void Translate_an_argument_error_is_surfaced_as_a_local_fault()
    {
        var translated = McpToolFaultTranslator.Translate(
            new ArgumentException("Unrecognised merge mode 'Nonsense'."));

        Assert.Multiple(() =>
        {
            Assert.That(translated.Message, Does.Contain(nameof(ArgumentException)));
            Assert.That(translated.Message, Does.Contain("Unrecognised merge mode 'Nonsense'."));
        });
    }

    [Test]
    public void Translate_an_in_silo_authorization_denial_stays_a_surfaced_denial()
    {
        // The in-silo fail-closed denial is a local (non-RpcException) fault. It is
        // surfaced with its safe message, keeping it a denial - never swallowed.
        var denial = new LatticeAuthorizationDeniedException(
            "orders", LatticeOperation.Replication, "subject-1", "not granted");

        var translated = McpToolFaultTranslator.Translate(denial);

        Assert.Multiple(() =>
        {
            Assert.That(translated.Message, Does.Contain(nameof(LatticeAuthorizationDeniedException)));
            Assert.That(translated.Message, Does.Contain("Access denied"));
        });
    }

    [Test]
    public void Translate_an_unknown_domain_exception_is_classified_purely_by_reflection()
    {
        // The seam must reference only always-loaded types so it can surface a
        // missing satellite assembly rather than fault on it. This exception type
        // stands in for a satellite domain exception the translator has never heard
        // of: it is surfaced with its runtime type name and message via
        // reflection, proving the translator needs no static reference to it.
        var fault = new SatelliteLikeException("the satellite facade rejected the request");

        var translated = McpToolFaultTranslator.Translate(fault);

        Assert.Multiple(() =>
        {
            Assert.That(translated.Message, Does.Contain(nameof(SatelliteLikeException)));
            Assert.That(translated.Message, Does.Contain("the satellite facade rejected the request"));
        });
    }

    private sealed class SatelliteLikeException(string message) : Exception(message);
}

