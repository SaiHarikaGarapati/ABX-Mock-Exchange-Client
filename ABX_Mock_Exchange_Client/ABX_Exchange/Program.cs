using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Sockets;
using System.Text.Json;

class ABXPacket
{
    public string Symbol { get; set; }
    public string BuySell { get; set; }
    public int Quantity { get; set; }
    public int Price { get; set; }
    public int PacketSequence { get; set; }
}

class Program
{
    const string Host = "127.0.0.1";
    const int Port = 3000;
    const int PacketSize = 17;

    static void Main()
    {
        var packets = new Dictionary<int, ABXPacket>();
        var receivedData = new List<byte>();

        Console.WriteLine("Connecting to ABX server for full packet stream...");

        // Step 1: Stream All Packets
        using (var client = new TcpClient(Host, Port))
        using (var stream = client.GetStream())
        {
            stream.Write(new byte[] { 1, 0 }); // CallType 1: Stream All Packets

            byte[] buffer = new byte[4096];
            int bytesRead;
            while ((bytesRead = stream.Read(buffer, 0, buffer.Length)) > 0)
            {
                receivedData.AddRange(buffer.Take(bytesRead));
            }
        }

        var allBytes = receivedData.ToArray();

        // Step 2: Parse received packets
        for (int i = 0; i + PacketSize <= allBytes.Length; i += PacketSize)
        {
            var packet = ParsePacket(allBytes.Skip(i).Take(PacketSize).ToArray());
            packets[packet.PacketSequence] = packet;
        }

        // Step 3: Detect missing sequences
        var maxSequence = packets.Keys.Max();
        var missingSequences = Enumerable.Range(1, maxSequence).Where(i => !packets.ContainsKey(i)).ToList();

        Console.WriteLine($"Received {packets.Count} packets. Missing: {missingSequences.Count}");

        // Step 4: Re-request missing packets
        foreach (var seq in missingSequences)
        {
            using (var client = new TcpClient(Host, Port))
            using (var stream = client.GetStream())
            {
                stream.Write(new byte[] { 2, (byte)seq }); // CallType 2: Resend Packet

                byte[] buffer = new byte[PacketSize];
                int bytesRead = stream.Read(buffer, 0, PacketSize);
                if (bytesRead == PacketSize)
                {
                    var packet = ParsePacket(buffer);
                    packets[packet.PacketSequence] = packet;
                    Console.WriteLine($"Re-requested packet {seq}");
                }
                else
                {
                    Console.WriteLine($"Failed to receive packet {seq}");
                }
            }
        }

        // Step 5: Save to JSON
        var orderedPackets = packets.Values.OrderBy(p => p.PacketSequence).ToList();
        var json = JsonSerializer.Serialize(orderedPackets, new JsonSerializerOptions { WriteIndented = true });
        File.WriteAllText("ticker_output.json", json);

        Console.WriteLine("Complete ticker data saved to ticker_output.json");
    }

    static ABXPacket ParsePacket(byte[] data)
    {
        if (data.Length != PacketSize)
            throw new ArgumentException("Invalid packet size.");

        string symbol = System.Text.Encoding.ASCII.GetString(data, 0, 4);
        char buySell = (char)data[4];
        int quantity = BitConverter.ToInt32(data.Skip(5).Take(4).Reverse().ToArray(), 0);
        int price = BitConverter.ToInt32(data.Skip(9).Take(4).Reverse().ToArray(), 0);
        int sequence = BitConverter.ToInt32(data.Skip(13).Take(4).Reverse().ToArray(), 0);

        return new ABXPacket
        {
            Symbol = symbol,
            BuySell = buySell.ToString(),
            Quantity = quantity,
            Price = price,
            PacketSequence = sequence
        };
    }
}
