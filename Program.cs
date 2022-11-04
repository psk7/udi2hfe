using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

// ReSharper disable InconsistentNaming

namespace udi2hfe
{
    partial class Program
    {
        private class TrackInfo
        {
            public record ByteFlux(byte Data, bool IsSyncMark = false);

            public ByteFlux[] Data { get; }

            public TrackInfo(IEnumerable<ByteFlux> Data)
            {
                this.Data = Data.ToArray();
            }

            public TrackInfo(byte[] Data, byte[] Flags)
            {
                var pos = 0;
                var l = new List<ByteFlux>();

                while (pos < Data.Length)
                {
                    var isSyncMark = (Flags[pos / 8] & (1 << pos % 8)) != 0;

                    l.Add(new ByteFlux(Data[pos++], isSyncMark));
                }

                this.Data = l.ToArray();
            }

            public void Write(Action<int> MfmWriter)
            {
                var prevDataBit = 0;

                foreach (var (data, isSyncMark) in Data)
                {
                    for (var i = 7; i >= 0; i--)
                    {
                        var dataBit = (data & (1 << i)) != 0 ? 1 : 0;

                        var syncBit = (prevDataBit | dataBit) ^ 1;

                        if (isSyncMark)
                            syncBit = (data, i) switch
                            {
                                (_, 2) => 0,
                                (0xc2, 3) => 0,
                                _ => syncBit
                            };

                        /*if (isSyncMark && data != 0xa1)
                            Console.WriteLine($"SyncMark {data:X2}");*/

                        //if (i == 2 && isSyncMark)
                        //    syncBit = 0;

                        MfmWriter(syncBit);
                        MfmWriter(dataBit);
                        prevDataBit = dataBit;
                    }
                }
            }
        }

        class TrackImage
        {
            public byte[] Track0 { get; }
            public byte[] Track1 { get; }

            public TrackImage(byte[] Track0, byte[] Track1)
            {
                this.Track0 = Track0;
                this.Track1 = Track1;
            }

            public byte[] MixHFE()
            {
                var ms = new MemoryStream();
                var rd0 = new BinaryReader(new MemoryStream(Track0));
                var rd1 = new BinaryReader(new MemoryStream(Track1));

                while (true)
                {
                    var p0 = rd0.ReadBytes(256);
                    var p1 = rd1.ReadBytes(256);

                    ms.Write(p0.AsSpan());
                    if (p0.Length != 256)
                        Pad(ms, 256);

                    ms.Write(p1.AsSpan());
                    if (p1.Length != 256)
                        Pad(ms, 256);

                    if (p0.Length != 256 && p1.Length != 256)
                        return ms.ToArray();
                }
            }
        }

        static byte[] GenTrack(TrackInfo Track)
        {
            var bitcnt = 0;
            byte bitacc = 0;
            var tms = new MemoryStream();

            Track.Write(i =>
            {
                bitacc |= (byte)(i << bitcnt++);
                if (bitcnt != 8)
                    return;

                bitcnt = 0;
                tms.WriteByte(bitacc);
                bitacc = 0;
            });

            return tms.ToArray();
        }

        static IEnumerable<TrackImage> JoinSides(IEnumerator<TrackInfo> Tracks)
        {
            var isEnd = false;

            while (!isEnd)
            {
                isEnd = !Tracks.MoveNext();
                if (isEnd)
                    yield break;

                var side0 = GenTrack(Tracks.Current);
                isEnd = !Tracks.MoveNext();
                var side1 = GenTrack(Tracks.Current);

                if (side0.Length == 0 || side1.Length == 0)
                    continue;

                yield return new TrackImage(side0, side1);
            }
        }

        static void Pad(Stream Writer, int Page)
        {
            var pos = Writer.Position;
            var tail = Page - pos % Page;

            for (var i = 0; i < tail; i++)
                Writer.WriteByte(0xff);
        }

        static void Pad(BinaryWriter Writer, int Page)
        {
            Pad(Writer.BaseStream, Page);
        }

        static List<TrackInfo> ReadUDI(string Path)
        {
            var rd = new BinaryReader(File.OpenRead(Path));

            var signature = rd.ReadBytes(4);
            rd.ReadUInt32(); // File size w/o checksum
            var version = rd.ReadByte();
            var cylinders = rd.ReadByte();
            var sides = rd.ReadByte();
            rd.ReadByte(); // unused
            var ext_hdl_size = rd.ReadInt32();

            rd.BaseStream.Seek(0x10 + ext_hdl_size, SeekOrigin.Begin);

            var images = new List<TrackInfo>();

            for (var i = 0; i < cylinders; i++)
            {
                for (var j = 0; j < sides + 1; j++)
                {
                    var trackType = rd.ReadByte();
                    var tlen = rd.ReadUInt16();
                    var img = rd.ReadBytes(tlen);
                    var clen = tlen / 8 + (tlen % 8 + 7) / 8;
                    var flags = rd.ReadBytes(clen);
                    images.Add(new TrackInfo(img, flags));

                    //File.WriteAllBytes("kokoko.bin", img);
                    //File.WriteAllBytes("kokokof.bin", flags);
                }
            }

            return images;
        }

        private record FDITrackDesctiptor(int Offset, FDISectorDesctiptor[] Sectors);

        private record FDISectorDesctiptor(int Offset, byte Cylinder, byte Head, byte Sector, byte SectorSize,
            byte Flags);

        static List<TrackInfo> ReadFDI(string Path)
        {
            var rd = new BinaryReader(File.OpenRead(Path));

            var signature = Encoding.ASCII.GetString(rd.ReadBytes(3));
            var writeProtect = rd.ReadBoolean();
            var cylinders = rd.ReadUInt16();
            var heads = rd.ReadUInt16();
            var descriptionOffset = rd.ReadUInt16();
            var dataOffset = rd.ReadUInt16();
            var additionalDataOffset = rd.ReadUInt16();

            rd.BaseStream.Seek(0xe + additionalDataOffset, SeekOrigin.Begin);

            var descriptors = new List<FDITrackDesctiptor>();

            for (var i = 0; i < cylinders; i++)
            {
                for (var j = 0; j < heads; j++)
                {
                    var offset = rd.ReadInt32();
                    rd.ReadInt16(); // Reserved
                    var sectors = rd.ReadByte();

                    var ls = new List<FDISectorDesctiptor>();

                    for (var k = 0; k < sectors; k++)
                    {
                        var cylinder = rd.ReadByte();
                        var head = rd.ReadByte();
                        var sector = rd.ReadByte();
                        var size = rd.ReadByte();
                        var flags = rd.ReadByte();
                        var sect_offset = rd.ReadUInt16();

                        ls.Add(new FDISectorDesctiptor(sect_offset, cylinder, head, sector, size, flags));
                    }

                    descriptors.Add(new FDITrackDesctiptor(offset, ls.ToArray()));
                }
            }

            var images = descriptors.Select(x => ReadFDITrack(dataOffset, rd, x)).ToList();

            return images;
        }

        static List<TrackInfo> ReadTRD(string Path)
        {
            var rd = new BinaryReader(File.OpenRead(Path));

            var tracks = (rd.BaseStream.Length / 256 + 15) / 16;

            var tl = new List<TrackInfo>();

            for (byte t = 0; t < tracks; t++)
            for (byte h = 0; h < 2; h++)
            {
                var flux = new List<TrackInfo.ByteFlux>();

                flux.AddRange(Enumerable.Repeat(new TrackInfo.ByteFlux(0x4e), 80)); // GAP
                flux.AddRange(Enumerable.Repeat(new TrackInfo.ByteFlux(0), 12)); // GAP
                flux.AddRange(Enumerable.Repeat(new TrackInfo.ByteFlux(0xc2, true), 3)); // Index mark
                flux.AddRange(Enumerable.Repeat(new TrackInfo.ByteFlux(0xfc), 1)); // Index mark
                flux.AddRange(Enumerable.Repeat(new TrackInfo.ByteFlux(0x4e), 50)); // GAP

                for (byte s = 1; s <= 16; s++)
                {
                    var crcms = new MemoryStream();
                    crcms.WriteByte(t);
                    crcms.WriteByte(h);
                    crcms.WriteByte(s);
                    crcms.WriteByte(1);
                    var indexCrc = CalcCRC16(new byte[] { 0xa1, 0xa1, 0xa1, 0xfe, t, h, s, 1 });

                    flux.AddRange(Enumerable.Repeat(new TrackInfo.ByteFlux(0), 12)); // GAP
                    flux.AddRange(Enumerable.Repeat(new TrackInfo.ByteFlux(0xa1, true), 3)); // Sync mark
                    flux.Add(new TrackInfo.ByteFlux(0xfe));
                    flux.Add(new TrackInfo.ByteFlux(t));
                    flux.Add(new TrackInfo.ByteFlux(h));
                    flux.Add(new TrackInfo.ByteFlux(s));
                    flux.Add(new TrackInfo.ByteFlux(1));
                    flux.Add(new TrackInfo.ByteFlux((byte)(indexCrc >> 8)));
                    flux.Add(new TrackInfo.ByteFlux((byte)(indexCrc & 0xff))); // CRC
                    flux.AddRange(Enumerable.Repeat(new TrackInfo.ByteFlux(0x4e), 22)); // GAP
                    flux.AddRange(Enumerable.Repeat(new TrackInfo.ByteFlux(0x0), 12)); // GAP
                    flux.AddRange(Enumerable.Repeat(new TrackInfo.ByteFlux(0xa1, true), 3)); // Sync mark
                    flux.Add(new TrackInfo.ByteFlux(0xfb));

                    var sectorData = rd.ReadBytes(256);

                    flux.AddRange(sectorData.Select(x => new TrackInfo.ByteFlux(x)));

                    crcms = new MemoryStream();
                    crcms.WriteByte(0xa1);
                    crcms.WriteByte(0xa1);
                    crcms.WriteByte(0xa1);
                    crcms.WriteByte(0xfb);
                    crcms.Write(sectorData.AsSpan());
                    var sectorCrc = CalcCRC16(crcms.ToArray());

                    flux.Add(new TrackInfo.ByteFlux((byte)(sectorCrc >> 8)));
                    flux.Add(new TrackInfo.ByteFlux((byte)(sectorCrc & 0xff))); // CRC
                    flux.AddRange(Enumerable.Repeat(new TrackInfo.ByteFlux(0x4e), 54)); // GAP
                }

                flux.AddRange(Enumerable.Repeat(new TrackInfo.ByteFlux(0x4e), 200)); // GAP

                tl.Add(new TrackInfo(flux));
            }

            return tl;
        }

        public static UInt16 CalcCRC16(byte[] Data)
        {
            uint crc = 0xffff;
            foreach (var b in Data)
            {
                crc ^= (uint)(b << 8);
                for (var j = 0; j < 8; j++)
                    if (((crc <<= 1) & 0x10000) != 0)
                        crc ^= 0x1021; // bit representation of x^12+x^5+1
            }

            return (UInt16)crc;
        }

        static TrackInfo ReadFDITrack(int DataOffset, BinaryReader Reader, FDITrackDesctiptor Track)
        {
            var flux = new List<TrackInfo.ByteFlux>();

            flux.AddRange(Enumerable.Repeat(new TrackInfo.ByteFlux(0x4e), 80)); // GAP
            flux.AddRange(Enumerable.Repeat(new TrackInfo.ByteFlux(0), 12)); // GAP
            flux.AddRange(Enumerable.Repeat(new TrackInfo.ByteFlux(0xc2, true), 3)); // Index mark
            flux.AddRange(Enumerable.Repeat(new TrackInfo.ByteFlux(0xfc), 1)); // Index mark
            flux.AddRange(Enumerable.Repeat(new TrackInfo.ByteFlux(0x4e), 50)); // GAP

            foreach (var sector in Track.Sectors)
            {
                var crcms = new MemoryStream();
                crcms.WriteByte(sector.Cylinder);
                crcms.WriteByte(sector.Head);
                crcms.WriteByte(sector.Sector);
                crcms.WriteByte(sector.SectorSize);
                var indexCrc = CalcCRC16(new byte[]
                    { 0xa1, 0xa1, 0xa1, 0xfe, sector.Cylinder, sector.Head, sector.Sector, sector.SectorSize });

                var realSectorSize = (sector.Flags & 0x3f) switch
                {
                    1 => 0,
                    2 => 1,
                    4 => 2,
                    8 => 3,
                    16 => 4,
                    32 => 5,
                    _ => sector.SectorSize < 5
                        ? sector.SectorSize
                        : throw new ArgumentException("Cannot determine sector size")
                };

                var crcIsValidMask = sector.Flags & 1 << realSectorSize;
                var crcIsValid = crcIsValidMask != 0;
                var crcFlags = sector.Flags & 0x3f & ~crcIsValidMask;
                var bit7 = (sector.Flags & 0x80) != 0;
                var bit6 = (sector.Flags & 0x40) != 0;

                if (crcFlags != 0)
                    throw new ArgumentException("crcFlags != 0");

                if (bit7)
                    throw new ArgumentException("bit7");

                flux.AddRange(Enumerable.Repeat(new TrackInfo.ByteFlux(0), 12)); // GAP
                flux.AddRange(Enumerable.Repeat(new TrackInfo.ByteFlux(0xa1, true), 3)); // Sync mark
                flux.Add(new TrackInfo.ByteFlux(0xfe));
                flux.Add(new TrackInfo.ByteFlux(sector.Cylinder));
                flux.Add(new TrackInfo.ByteFlux(sector.Head));
                flux.Add(new TrackInfo.ByteFlux(sector.Sector));
                flux.Add(new TrackInfo.ByteFlux(sector.SectorSize));
                flux.Add(new TrackInfo.ByteFlux((byte)(indexCrc >> 8)));
                flux.Add(new TrackInfo.ByteFlux((byte)(indexCrc & 0xff))); // CRC
                flux.AddRange(Enumerable.Repeat(new TrackInfo.ByteFlux(0x4e), 22)); // GAP
                flux.AddRange(Enumerable.Repeat(new TrackInfo.ByteFlux(0x0), 12)); // GAP
                flux.AddRange(Enumerable.Repeat(new TrackInfo.ByteFlux(0xa1, true), 3)); // Sync mark
                flux.Add(new TrackInfo.ByteFlux(0xfb));

                if (bit6)
                    continue;

                var dataSize = 128 << realSectorSize;
                Reader.BaseStream.Seek(DataOffset + Track.Offset + sector.Offset, SeekOrigin.Begin);
                var sectorData = Reader.ReadBytes(dataSize);

                flux.AddRange(sectorData.Select(x => new TrackInfo.ByteFlux(x)));

                crcms = new MemoryStream();
                crcms.WriteByte(0xa1);
                crcms.WriteByte(0xa1);
                crcms.WriteByte(0xa1);
                crcms.WriteByte(0xfb);
                crcms.Write(sectorData.AsSpan());
                var sectorCrc = CalcCRC16(crcms.ToArray());

                if (!crcIsValid)
                    sectorCrc = (ushort)~sectorCrc;

                flux.Add(new TrackInfo.ByteFlux((byte)(sectorCrc >> 8)));
                flux.Add(new TrackInfo.ByteFlux((byte)(sectorCrc & 0xff))); // CRC
                flux.AddRange(Enumerable.Repeat(new TrackInfo.ByteFlux(0x4e), 54)); // GAP
            }

            var rem = 6250 - flux.Count;

            if (rem > 0)
                flux.AddRange(Enumerable.Repeat(new TrackInfo.ByteFlux(0x4e), rem)); // GAP

            return new TrackInfo(flux);
        }

        static void Main(string[] args)
        {
            if (args.Length != 2)
            {
                Console.WriteLine("Usage: udi2hfe (<source.udi>|<source.fdi>|<source.trd>) <target.hfe>");
                return;
            }

            var images = Path.GetExtension(args[0]).ToLowerInvariant() switch
            {
                ".udi" => ReadUDI(args[0]),
                ".fdi" => ReadFDI(args[0]),
                ".trd" => ReadTRD(args[0]),
                _ => throw new ArgumentOutOfRangeException($"Bad filename {args[0]}")
            };

            var imgs = JoinSides(images.GetEnumerator()).ToArray();

            var wr = new BinaryWriter(File.Create(args[1]));
            // Header
            wr.Write(Encoding.ASCII.GetBytes("HXCPICFE"));
            wr.Write((byte)0); // FormatRevision
            wr.Write((byte)imgs.Length); // Tracks
            wr.Write((byte)2); // Sides
            wr.Write((byte)0xff); // Track encoding
            wr.Write((UInt16)250); // Bitrate
            wr.Write((UInt16)0); // RPM
            wr.Write((byte)0xff); // floppy interface mode
            wr.Write((byte)1); // dnu
            wr.Write((UInt16)1); // Track List Offset
            Pad(wr, 512);
            Pad(wr, 512);

            var tocwriter = new BinaryWriter(new MemoryStream());

            foreach (var trackImage in imgs)
            {
                var pos = wr.BaseStream.Position / 512;

                var data = trackImage.MixHFE();
                wr.Write(data);

                tocwriter.Write((UInt16)pos);
                tocwriter.Write((UInt16)data.Length);
            }

            wr.BaseStream.Seek(0x200, SeekOrigin.Begin);
            tocwriter.BaseStream.Seek(0, SeekOrigin.Begin);
            tocwriter.BaseStream.CopyTo(wr.BaseStream);

            wr.Close();
        }
    }
}