import unittest
from strategy.domain.types.trend_direction_enum import TimeframeCategoryEnum, get_timeframe_category

class TimeframeCategoryEnumTest(unittest.TestCase):
    """Test cases for TimeframeCategoryEnum"""
    
    def test_get_timeframe_category(self):
        """Test that timeframe categories are correctly assigned"""
        # Test high timeframes
        self.assertEqual(get_timeframe_category('1d'), TimeframeCategoryEnum.HTF)
        self.assertEqual(get_timeframe_category('4h'), TimeframeCategoryEnum.HTF)
        self.assertEqual(get_timeframe_category('1h'), TimeframeCategoryEnum.HTF)
        
        # Test medium timeframes
        self.assertEqual(get_timeframe_category('30m'), TimeframeCategoryEnum.MTF)
        self.assertEqual(get_timeframe_category('15m'), TimeframeCategoryEnum.MTF)
        
        # Test low timeframes
        self.assertEqual(get_timeframe_category('5m'), TimeframeCategoryEnum.LTF)
        self.assertEqual(get_timeframe_category('1m'), TimeframeCategoryEnum.LTF)
        
        # Test default
        self.assertEqual(get_timeframe_category('unknown'), TimeframeCategoryEnum.MTF)
        
        # Print all timeframe categories for debugging
        print("Timeframe categories:")
        for tf, category in [
            ('1d', get_timeframe_category('1d')),
            ('4h', get_timeframe_category('4h')),
            ('1h', get_timeframe_category('1h')),
            ('30m', get_timeframe_category('30m')),
            ('15m', get_timeframe_category('15m')),
            ('5m', get_timeframe_category('5m')),
            ('1m', get_timeframe_category('1m'))
        ]:
            print(f"{tf}: {category}")

if __name__ == '__main__':
    unittest.main()
