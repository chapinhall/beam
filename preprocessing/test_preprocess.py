'''
Test preprocessing functions fix_fname, fix_lname and get_aliases.

To run:
    In command line, type python3 test_postprocessing.py
    For more verbose testing, include the -v flag
'''
import unittest
import pandas as pd
from record_linkage_shared import preprocess_functions


TEST_CASES = {
    "fname": {
        "test": preprocess_functions.fix_fname,
        "cleaning": {
                        ("Kyle", "") : ("KYLE", None, None, None),
                        ("", ""): (None, None, None, None),
                        ("Kyle Jr", ""): ("KYLE", None, None, "JR"),
                        ("Jason Lee", "John"): ("JASON", "LEE JOHN", None, None),
                        ("John Jay (Jonny)", "Kyle"): ("JOHN", "JAY KYLE", "JONNY", None),
                        ("KA'lee", "Meg"): ("KALEE", "MEG", None, None),
                        ("KA'lee Meg", ""): ("KALEE", "MEG", None, None),
                        ("Hay   LEe", "Carmen"): ("HAY", "LEE CARMEN", None, None),
                        ("TINA KATE", "KATE"): ("TINA", "KATE", None, None),
                        ("MATT", "MATT"): ("MATT", None, None, None),
                        ("TERRYII", ""): ("TERRYII", None, None, None),
                        ("Kyle", "Jo-seph"): ("KYLE", "JO SEPH", None, None),
                        ("Ja332IN", "(Jason)"): ("JAIN", "JASON", None, None)
                    },
        "aliases": (
                         ["KYLE", None, None, None],
                         [None, None, None, None],
                         ["KYLE", None, None, "JR"],
                         ["JASON", "LEE JOHN", None, None],
                         ["JOHN", "JAY KYLE", "JONNY", None],
                         ["JONNY", "JAY KYLE", None, None],
                         ["KALEE", "MEG", None, None],
                         ["KALEE", "MEG", None, None],
                         ["HAY", "LEE CARMEN", None, None],
                         ["TINA", "KATE", None, None],
                         ["MATT", None, None, None],
                         ["TERRYII", None, None, None],
                         ["KYLE", "JO SEPH", None, None],
                         ["JAIN", "JASON", None, None]
                    )
        },
    "lname": {
        "test": preprocess_functions.fix_lname,
        "cleaning": {
                        ("SMITH", "") : ("SMITH", None, None),
                        ("", ""): (None, None, None),
                        ("Smith Jr", ""): ("SMITH","JR", None),
                        ("Smith", "Johnson"): ("SMITH", None, "JOHNSON"),
                        ("Smith-James", "Johnson"): ("SMITH",  None, "JOHNSON"),
                        ("des'Cartes", "JAMESON"): ("DESCARTES", None, "JAMESON"),
                        ("dEs'CartEs Jameson", ""): ("DESCARTES", None, "JAMESON"),
                        ("BRAND     Lee", "Carson"): ("BRAND", None, "CARSON"),
                        ("Smith Lee", "SMITH"): ("SMITH", None, "LEE"),
                        ("Smith", "Smith"): ("SMITH", None, None),
                        ("SMITHII", ""): ("SMITHII", None, None),
                        ("SMITH", "JA MEson"): ("SMITH", None, "JAMESON")
                    },
        "aliases":  (
                         ["SMITH", None, None],
                         [None, None, None],
                         ["SMITH", None, "JR"],
                         ["SMITH", "JOHNSON", None],
                         ["JOHNSON", None, None],
                         ["SMITH", "JOHNSON", None],
                         ["JOHNSON", None, None],
                         ["DESCARTES", "JAMESON", None],
                         ["JAMESON", None, None],
                         ["DESCARTES", "JAMESON", None],
                         ["JAMESON", None, None],
                         ["BRAND", "CARSON", None],
                         ["CARSON", None, None],
                         ["SMITH", "LEE", None],
                         ["LEE", None, None],
                         ["SMITH", None, None],
                         ["SMITHII", None, None],
                         ["SMITH", "JAMESON", None],
                         ["JAMESON", None, None]
                 )
        }
    }


def get_fixed_names(function, names):
    '''
    This function calculates the fixed name for a dictionary of
    names. Returns two lists, one of fixed names and one of correct
    versions
    '''
    fixed_names = []
    correct_names = []
    for name_input, correct in names.items():
        results = tuple(function(name_input[0], name_input[1], []))
        fixed_names.append(results)
        correct_names.append(correct)
    return fixed_names, correct_names


class TestPreprocessFunctions(unittest.TestCase):
    '''
    Tests the preprocessing functions
    '''
    def setUp(self):
        '''
        Set up variables to compare.
        '''
        self.fname_test, self.fname_expected = \
            get_fixed_names(TEST_CASES["fname"]["test"],
                            TEST_CASES["fname"]["cleaning"])
        self.lname_test, self.lname_expected = \
            get_fixed_names(TEST_CASES["lname"]["test"],
                            TEST_CASES["lname"]["cleaning"])
        self.fname_aliases_test = pd.DataFrame(self.fname_test,
                                               columns=["fname", "mname",
                                                        "altfname", "suffix"])
        self.fname_aliases_corr = pd.DataFrame(TEST_CASES["fname"]["aliases"],
                                               columns=["fname", "mname",
                                                        "altfname", "suffix"]
                                                ).sort_values(["fname",
                                                               "mname",
                                                               "altfname",
                                                               "suffix"]
                                                ).reset_index(drop=True)
        self.lname_aliases_test = pd.DataFrame(self.lname_test,
                                               columns=["lname",
                                                        "suffix",
                                                        "altlname"])
        self.lname_aliases_corr = pd.DataFrame(TEST_CASES["lname"]["aliases"],
                                               columns=["lname",
                                                        "altlname",
                                                        "suffix"]
                                                ).sort_values(["lname",
                                                               "altlname",
                                                               "suffix"]
                                                ).reset_index(drop=True)

    def assertDataframeEqual(self, a, b, msg):
        '''
        Set up function to assert that two dataframes are equal.
        '''
        try:
            pd.testing.assert_frame_equal(a, b, check_like=True)
        except AssertionError as e:
            raise self.failureException(msg) from e

    def test_fixfname(self):
        '''
        Check if fixed first names match expected.
        '''
        for i, name in enumerate(self.fname_test):
            self.assertEqual(name, self.fname_expected[i],
                msg=f"expected: {self.fname_expected[i]}, result: {name}")

    def test_fixlname(self):
        '''
        Check if fixed last names match expected.
        '''
        for i, name in enumerate(self.lname_test):
            self.assertEqual(name, self.lname_expected[i],
                msg=f"expected: {self.lname_expected[i]}, result: {name}")

    def test_fname_aliases(self):
        '''
        Check if aliases calculates the correct first name aliases.
        '''
        fname_aliases = preprocess_functions.get_aliases(self.fname_aliases_test,
                                                         lname=False
                                                         ).sort_values(["fname",
                                                                        "mname",
                                                                        "altfname",
                                                                        "suffix"]
                                                         ).reset_index(drop=True)
        self.assertDataframeEqual(fname_aliases, self.fname_aliases_corr,
            msg=f"expected: {self.fname_aliases_corr}, results: {fname_aliases}")

    def test_lname_aliases(self):
        '''
        Check if aliases calculates the correct last name aliases.
        '''
        lname_aliases = preprocess_functions.get_aliases(self.lname_aliases_test,
                                                         lname=True
                                                         ).sort_values(["lname",
                                                                        "altlname",
                                                                        "suffix"]
                                                         ).reset_index(drop=True)
        self.assertDataframeEqual(lname_aliases, self.lname_aliases_corr,
            msg=f"expected: {self.lname_aliases_corr}, results: {lname_aliases}")

if __name__ == '__main__':
    unittest.main()
