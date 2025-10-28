// generate bit sequence for schema instruction use offset padding

using System;
using System.Collections;
public static class BitGenerator
{
    public static BitArray GenerateBitPadding(int[] offset, int recordSize)
    {
        BitArray bits = new BitArray(recordSize + offset.Length - 1, true);
        // bit split by a 0 
        if (offset.Length == 0) return bits;
        int pad = 0;
        for (int i = 1; i < offset.Length; i++)
        {
            // assign bit
            bits[offset[i] + pad] = false;
            pad++;
        }
        return bits;
    }
    
    public static int[] DecodeBitPadding(BitArray bits)
    {
        var offsets = new System.Collections.Generic.List<int>();
        offsets.Add(0); // first offset is always 0
        for (int i = 0; i < bits.Count; i++)
        {
            if (!bits[i])
            {
                offsets.Add(i - offsets.Count + 1);
            }
        }
        return offsets.ToArray();
    }
    //helper

    public static string BitArrayToString(BitArray bits)
    {
        char[] chars = new char[bits.Count];
        for (int i = 0; i < bits.Count; i++)
        {
            chars[i] = bits[i] ? '1' : '0';
        }
        
        
        return new string(chars);
    }
}