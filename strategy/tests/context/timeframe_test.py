import unittest
from strategy.context.types import TimeframeCategory, get_timeframe_category

class TimeframeCategoryTest(unittest.TestCase):
    """Test cases for TimeframeCategory"""
    
    def test_get_timeframe_category(self):
        """Test that timeframe categories are correctly assigned"""
        # Test high timeframes
        self.assertEqual(get_timeframe_category('1d'), TimeframeCategory.HTF)
        self.assertEqual(get_timeframe_category('4h'), TimeframeCategory.HTF)
        self.assertEqual(get_timeframe_category('1h'), TimeframeCategory.HTF)
        
        # Test medium timeframes
        self.assertEqual(get_timeframe_category('30m'), TimeframeCategory.MTF)
        self.assertEqual(get_timeframe_category('15m'), TimeframeCategory.MTF)
        
        # Test low timeframes
        self.assertEqual(get_timeframe_category('5m'), TimeframeCategory.LTF)
        self.assertEqual(get_timeframe_category('1m'), TimeframeCategory.LTF)
        
        # Test default
        self.assertEqual(get_timeframe_category('unknown'), TimeframeCategory.MTF)
        
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
