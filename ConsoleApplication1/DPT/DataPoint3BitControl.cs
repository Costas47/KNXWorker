﻿using System;
using System.Globalization;
using System.Linq;

namespace KNXWorker.DPT
{
    internal sealed class DataPoint3BitControl : DataPoint
    {
        public override string[] Ids
        {
            get { return new[] { "3.008", "3.007" }; }
        }

        public override object FromDataPoint(string data)
        {
            var dataConverted = new byte[data.Length];
            for (var i = 0; i < data.Length; i++)
                dataConverted[i] = (byte)data[i];

            return FromDataPoint(dataConverted);
        }

        public override object FromDataPoint(byte[] data)
        {
            if (data == null || data.Length != 1)
                return 0;

            int input = data[0] & 0x0F;

            bool direction = (input >> 3) == 1;
            int step = input & 0x07;

            return direction ? step : (step * -1);
        }

        public override byte[] ToDataPoint(string value)
        {
            return ToDataPoint(float.Parse(value, CultureInfo.InvariantCulture));
        }

        public override byte[] ToDataPoint(object val)
        {
            var dataPoint = new byte[1];
            dataPoint[0] = 0x00;
            
            int input = 0;
            if (val is int)
                input = ((int) val);
            else if (val is float)
                input = (int) ((float) val);
            else if (val is long)
                input = (int) ((long) val);
            else if (val is double)
                input = (int) ((double) val);
            else if (val is decimal)
                input = (int) ((decimal) val);
            else
            {
                Console.WriteLine("6.xxx - input value received is not a valid type");
                return dataPoint;
            }

            if (input > 7 || input < -7)
            {
                Console.WriteLine("3.xxx - input value received is not in a valid range");
                return dataPoint;
            }

            var direction = 8; // binary 1000

            if (input <= 0)
            {
                direction = 0;
                input = input * -1;
            }

            int step = (input & 7);

            dataPoint[0] = (byte)(step | direction);

            return dataPoint;
        }
    }
}