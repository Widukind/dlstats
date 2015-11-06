import os.path
import unittest
import pep8
import dlstats


class TestPep8(unittest.TestCase):
    """Run PEP8 on all modules in dlstats"""
    @unittest.skipUnless('FULL_REMOTE_TEST' in os.environ,
                         "Skip - not full remote test")
    def test_pep8(self):
        style = pep8.StyleGuide(quiet=True)
        # style.options.ignore += ('',)
        dir = os.path.dirname(dlstats.__file__)
        style.input_dir(dir)
        self.assertEqual(style.options.report.total_errors, 0)

if __name__ == '__main__':
    unittest.main()
