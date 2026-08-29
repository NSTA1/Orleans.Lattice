using Orleans.Lattice.Explorer.Tenancy;
using Orleans.Lattice.Explorer.Tenants;

namespace Orleans.Lattice.Explorer.Tests.Tenants;

/// <summary>
/// The cross-tenant grant row: the fixture that proves a grant's lifecycle state
/// is unmistakable.
/// <para>
/// Only an active grant authorizes anything. A pending offer, a rejected one,
/// and a revoked one all authorize nothing, and the row must say so in words as
/// well as in a label - because the two-step agreement means a grant exists from
/// the moment it is offered, and an operator must never read a pending offer as
/// live access.
/// </para>
/// </summary>
[TestFixture]
public sealed class TenantGrantRowTests
{
    private static TenantGrantRow Row(
        ExplorerTenantGrantState state,
        ExplorerTenantGrantAccess operations = ExplorerTenantGrantAccess.Read,
        TenantGrantDirection direction = TenantGrantDirection.Issued) =>
        TenantGrantRow.From(SampleTenants.Grant(state, operations), direction);

    [Test]
    public void Only_an_active_grant_authorizes_anything()
    {
        Assert.Multiple(() =>
        {
            Assert.That(Row(ExplorerTenantGrantState.Active).Authorizes, Is.True);
            Assert.That(Row(ExplorerTenantGrantState.Pending).Authorizes, Is.False);
            Assert.That(Row(ExplorerTenantGrantState.Rejected).Authorizes, Is.False);
            Assert.That(Row(ExplorerTenantGrantState.Revoked).Authorizes, Is.False);
        });
    }

    [Test]
    public void Every_state_carries_a_distinct_label()
    {
        var labels = new[]
        {
            Row(ExplorerTenantGrantState.Active).StateLabel,
            Row(ExplorerTenantGrantState.Pending).StateLabel,
            Row(ExplorerTenantGrantState.Rejected).StateLabel,
            Row(ExplorerTenantGrantState.Revoked).StateLabel,
        };

        Assert.Multiple(() =>
        {
            Assert.That(labels, Is.Unique);
            Assert.That(labels[0], Is.EqualTo("Active"));

            // "Pending" alone could be read as "in progress, nearly live". The
            // label says what it is waiting for.
            Assert.That(labels[1], Is.EqualTo("Pending approval"));
            Assert.That(labels[2], Is.EqualTo("Rejected"));
            Assert.That(labels[3], Is.EqualTo("Revoked"));
        });
    }

    [Test]
    public void A_pending_grant_says_in_words_that_it_authorizes_nothing_yet()
    {
        var row = Row(ExplorerTenantGrantState.Pending);

        Assert.Multiple(() =>
        {
            Assert.That(row.AuthorityText, Does.StartWith("Authorizes nothing yet"));
            Assert.That(row.AuthorityText, Does.Contain("approval"));
        });
    }

    [Test]
    public void A_rejected_grant_says_in_words_that_it_authorizes_nothing()
    {
        Assert.That(
            Row(ExplorerTenantGrantState.Rejected).AuthorityText,
            Does.StartWith("Authorizes nothing.").And.Contain("declined"));
    }

    [Test]
    public void A_revoked_grant_says_in_words_that_it_authorizes_nothing()
    {
        Assert.That(
            Row(ExplorerTenantGrantState.Revoked).AuthorityText,
            Does.StartWith("Authorizes nothing.").And.Contain("withdrawn"));
    }

    [Test]
    public void An_active_grant_says_what_it_authorizes_now()
    {
        var row = Row(ExplorerTenantGrantState.Active, ExplorerTenantGrantAccess.ReadWrite);

        Assert.That(row.AuthorityText, Is.EqualTo("Authorizes read and write now."));
    }

    [Test]
    public void Every_active_authority_line_is_an_interned_literal_rather_than_a_concatenation()
    {
        // Read once per row per render, so a concatenation here would allocate a
        // string per grant on every render of the list.
        foreach (var operations in new[]
                 {
                     ExplorerTenantGrantAccess.Read,
                     ExplorerTenantGrantAccess.Write,
                     ExplorerTenantGrantAccess.ReadWrite,
                     ExplorerTenantGrantAccess.None,
                 })
        {
            var row = Row(ExplorerTenantGrantState.Active, operations);

            Assert.That(
                row.AuthorityText,
                Is.SameAs(row.AuthorityText),
                $"{operations} must return the same instance on each read");
        }
    }

    [Test]
    public void An_active_grant_that_names_no_operations_still_says_so()
    {
        var row = Row(ExplorerTenantGrantState.Active, ExplorerTenantGrantAccess.None);

        Assert.Multiple(() =>
        {
            Assert.That(row.Authorizes, Is.True, "the grant is active");
            Assert.That(row.AuthorityText, Does.Contain("no operations at all"));
        });
    }

    [Test]
    public void Only_an_active_grant_reads_as_authorizing_in_words()
    {
        // A surface could omit the state badge and still not mislead, because
        // the authority line never says "now" for a grant that authorizes
        // nothing.
        Assert.Multiple(() =>
        {
            foreach (var state in new[]
                     {
                         ExplorerTenantGrantState.Pending,
                         ExplorerTenantGrantState.Rejected,
                         ExplorerTenantGrantState.Revoked,
                     })
            {
                Assert.That(
                    Row(state).AuthorityText,
                    Does.StartWith("Authorizes nothing"),
                    $"{state} must not read as live access");
            }
        });
    }

    [Test]
    public void A_pending_grant_is_styled_apart_from_an_active_one()
    {
        Assert.Multiple(() =>
        {
            Assert.That(Row(ExplorerTenantGrantState.Active).StateClass, Is.EqualTo("is-active"));
            Assert.That(Row(ExplorerTenantGrantState.Pending).StateClass, Is.EqualTo("is-pending"));
            Assert.That(Row(ExplorerTenantGrantState.Rejected).StateClass, Is.EqualTo("is-closed"));
            Assert.That(Row(ExplorerTenantGrantState.Revoked).StateClass, Is.EqualTo("is-closed"));
        });
    }

    [Test]
    public void Only_a_pending_grant_can_be_answered()
    {
        Assert.Multiple(() =>
        {
            Assert.That(Row(ExplorerTenantGrantState.Pending).CanAnswer, Is.True);
            Assert.That(Row(ExplorerTenantGrantState.Active).CanAnswer, Is.False);
            Assert.That(Row(ExplorerTenantGrantState.Rejected).CanAnswer, Is.False);
            Assert.That(Row(ExplorerTenantGrantState.Revoked).CanAnswer, Is.False);
        });
    }

    [Test]
    public void Only_an_active_grant_can_be_revoked()
    {
        Assert.Multiple(() =>
        {
            Assert.That(Row(ExplorerTenantGrantState.Active).CanRevoke, Is.True);
            Assert.That(Row(ExplorerTenantGrantState.Pending).CanRevoke, Is.False);
            Assert.That(Row(ExplorerTenantGrantState.Rejected).CanRevoke, Is.False);
            Assert.That(Row(ExplorerTenantGrantState.Revoked).CanRevoke, Is.False);
        });
    }

    [Test]
    public void A_terminal_grant_offers_no_transition_at_all()
    {
        foreach (var state in new[] { ExplorerTenantGrantState.Rejected, ExplorerTenantGrantState.Revoked })
        {
            var row = Row(state);
            Assert.Multiple(() =>
            {
                Assert.That(row.IsClosed, Is.True, $"{state} is terminal");
                Assert.That(row.CanAnswer, Is.False);
                Assert.That(row.CanRevoke, Is.False);
            });
        }
    }

    [TestCase(ExplorerTenantGrantAccess.Read, "read")]
    [TestCase(ExplorerTenantGrantAccess.Write, "write")]
    [TestCase(ExplorerTenantGrantAccess.ReadWrite, "read and write")]
    [TestCase(ExplorerTenantGrantAccess.None, "no operations")]
    public void The_operations_a_grant_names_are_spelled_out(
        ExplorerTenantGrantAccess operations,
        string expected)
    {
        Assert.That(Row(ExplorerTenantGrantState.Active, operations).OperationsText, Is.EqualTo(expected));
    }

    [Test]
    public void A_row_carries_the_parties_and_scope_it_was_built_from()
    {
        var row = Row(ExplorerTenantGrantState.Pending);

        Assert.Multiple(() =>
        {
            Assert.That(row.GrantId, Is.EqualTo("grant-1"));
            Assert.That(row.GranterTenantId, Is.EqualTo(SampleTenants.Acme));
            Assert.That(row.GranteeTenantId, Is.EqualTo(SampleTenants.Globex));
            Assert.That(row.Scope, Is.EqualTo(SampleTenants.Scope));
            Assert.That(row.State, Is.EqualTo(ExplorerTenantGrantState.Pending));
        });
    }

    [Test]
    public void A_row_states_which_side_of_the_agreement_it_is_on()
    {
        Assert.Multiple(() =>
        {
            Assert.That(
                Row(ExplorerTenantGrantState.Pending, direction: TenantGrantDirection.Issued).Direction,
                Is.EqualTo(TenantGrantDirection.Issued));
            Assert.That(
                Row(ExplorerTenantGrantState.Pending, direction: TenantGrantDirection.Received).Direction,
                Is.EqualTo(TenantGrantDirection.Received));
        });
    }

    [Test]
    public void An_unrecognised_wire_state_never_renders_as_live_access()
    {
        // The seam fails an unknown wire state closed to Revoked, but the row is
        // defensive in the same direction: any state it does not know is treated
        // as authorizing nothing.
        var row = TenantGrantRow.From(
            new ExplorerTenantGrant(
                "grant-x",
                SampleTenants.Acme,
                SampleTenants.Globex,
                SampleTenants.Scope,
                ExplorerTenantGrantAccess.ReadWrite,
                (ExplorerTenantGrantState)99),
            TenantGrantDirection.Issued);

        Assert.Multiple(() =>
        {
            Assert.That(row.Authorizes, Is.False);
            Assert.That(row.CanAnswer, Is.False);
            Assert.That(row.CanRevoke, Is.False);
            Assert.That(row.AuthorityText, Does.StartWith("Authorizes nothing"));
        });
    }
}
