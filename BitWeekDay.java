public static class BitWeekDay {
        private static final int WEEK_BITS = 3;
        private static final int HOUR_BITS = 4;
        private static final int Hour_PERIOD_BITS = 1;

        private static final int HOUR_LEFT_SHIFT_BITS = Hour_PERIOD_BITS;
        private static final int WEEK_LEFT_SHIFT_BITS = HOUR_LEFT_SHIFT_BITS + HOUR_BITS;

        public static int toBits(int week, int hour) {
            if (hour > 12) {
                // pm
                return toBits(week, hour - 12, 1);
            }
            // am
            return toBits(week, hour, 0);
        }

        private static int toBits(int week, int hour, int hourPeriod) {
            return week << WEEK_LEFT_SHIFT_BITS | hour << HOUR_LEFT_SHIFT_BITS | hourPeriod;
        }

        public static int getWeek(int bits) {
            return (bits >> WEEK_LEFT_SHIFT_BITS) & ((1 << WEEK_BITS) - 1);
        }

        public static int getHour(int bits) {
            if (getHourPeriod(bits) == 0) {
                // am
                return (bits >> HOUR_LEFT_SHIFT_BITS) & ((1 << HOUR_BITS) - 1);
            }
            // pm
            return ((bits >> HOUR_LEFT_SHIFT_BITS) & ((1 << HOUR_BITS) - 1)) + 12;
        }

        private static int getHourPeriod(int bits) {
            return bits & ((1 << Hour_PERIOD_BITS) - 1);
        }
    }
