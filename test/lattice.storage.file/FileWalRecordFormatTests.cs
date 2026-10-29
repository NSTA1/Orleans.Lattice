namespace Orleans.Lattice.Storage.File.Tests;

[TestFixture]
public sealed class FileWalRecordFormatTests
{
    // Fixed vectors were computed independently with Python struct and zlib.crc32.
    [TestCase(0L, "", "01080000000000000000000000F9E9EED4")]
    [TestCase(72623859790382856L, "00117FFF", "010C000000080706050403020100117FFF6C609EDC")]
    [TestCase(-1L, "FF", "0109000000FFFFFFFFFFFFFFFFFF679E08C6")]
    public void WriteDataRecord_preserves_the_persisted_bytes(long offset, string payloadHex, string expectedHex)
    {
        var payload = Convert.FromHexString(payloadHex);
        var expected = Convert.FromHexString(expectedHex);
        var destination = CreateDestination(expected.Length);

        var written = FileWalRecordFormat.WriteDataRecord(destination, offset, payload);

        AssertRecord(destination, written, expected, FileWalRecordFormat.DataRecordLength(payload.Length));
    }

    [TestCase(0, "020400000000000000D232B54C")]
    [TestCase(16909060, "020400000004030201C849A084")]
    [TestCase(-1, "0204000000FFFFFFFF31120E92")]
    public void WriteCommitRecord_preserves_the_persisted_bytes(int count, string expectedHex)
    {
        var expected = Convert.FromHexString(expectedHex);
        var destination = CreateDestination(expected.Length);

        var written = FileWalRecordFormat.WriteCommitRecord(destination, count);

        AssertRecord(destination, written, expected, FileWalRecordFormat.CommitRecordLength);
    }

    [TestCase(0L, "03080000000000000000000000B25CB2B4")]
    [TestCase(72623859790382856L, "03080000000807060504030201FE6E5C74")]
    [TestCase(-1L, "0308000000FFFFFFFFFFFFFFFFC75CD4F0")]
    public void WriteTrimRecord_preserves_the_persisted_bytes(long throughOffset, string expectedHex)
    {
        var expected = Convert.FromHexString(expectedHex);
        var destination = CreateDestination(expected.Length);

        var written = FileWalRecordFormat.WriteTrimRecord(destination, throughOffset);

        AssertRecord(destination, written, expected, FileWalRecordFormat.TrimRecordLength);
    }

    [Test]
    public void Writing_records_does_not_allocate()
    {
        var destination = new byte[128];
        var payload = new byte[32];
        for (var i = 0; i < 1_000; i++)
        {
            WriteAllRecords(destination, payload);
        }

        var before = GC.GetAllocatedBytesForCurrentThread();
        for (var i = 0; i < 1_000; i++)
        {
            WriteAllRecords(destination, payload);
        }
        var allocated = GC.GetAllocatedBytesForCurrentThread() - before;

        Assert.That(allocated, Is.Zero);
    }

    private static void WriteAllRecords(byte[] destination, byte[] payload)
    {
        FileWalRecordFormat.WriteDataRecord(destination, 42, payload);
        FileWalRecordFormat.WriteCommitRecord(destination, 1);
        FileWalRecordFormat.WriteTrimRecord(destination, 42);
    }

    private static byte[] CreateDestination(int recordLength)
    {
        var destination = new byte[recordLength + 8];
        Array.Fill(destination, (byte)0xA5);
        return destination;
    }

    private static void AssertRecord(byte[] destination, int written, byte[] expected, int recordLength)
    {
        Assert.Multiple(() =>
        {
            Assert.That(written, Is.EqualTo(expected.Length));
            Assert.That(recordLength, Is.EqualTo(expected.Length));
            Assert.That(destination[..written], Is.EqualTo(expected));
            Assert.That(destination[written..], Is.All.EqualTo(0xA5));
        });
    }
}
