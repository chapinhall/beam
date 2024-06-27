'''
Test code to ensure post-processing is creating expected groups

To run:
    In command line, type python3 test_postprocessing.py
    For more verbose testing, include the -v flag
'''
import unittest
import json
import os
import subprocess
import postprocess as pp


test_directory = "/wd2/match/record-linkage-v0/output_data/test/"
# load in the data
with open(os.path.join(test_directory, "expected.json"), 'r') as f:
    data = f.read()
    TEST_RESULTS = json.loads(data)


def get_expected_results(expected_test, testtype='121'):
    '''
    Converts expected results from json file to proper format. For dedup/M2M,
    also combines grouping and converts to sets for easier comparision in
    case new ids don't match
    '''
    expected =  {}

    for k, v in expected_test.items():
        if '1' in testtype:
            v_strings = v.split(',')
            expected[k] = [v_strings[0], v_strings[1],
                           v_strings[2] == "True", v_strings[3]]
        elif testtype == "dedup":
            if type(v) == set:
                expected[int(k)] = frozenset(v)
            elif v not in expected:
                expected[v] = set(k)
            else:
                expected[v].add(k)
        elif testtype == "M2M":
            expected[int(k)] = set()
            for match in v:
                if type(match) == str and "," in match:
                    match = tuple(match.split(','))
                if type(match) == tuple:
                    expected[int(k)].add(match)
            expected[int(k)] = frozenset(expected[int(k)])

    # for dedup, ensure that each value is a frozen set for comparison
    if testtype == "dedup":
        for k, v in expected.items():
            expected[k] = frozenset(v)
    return expected


def reverse_file(old_file, testtype, testnum):
    '''
    Reverses the order of columns for a 3-columned csv file, inputting values
    into a new file
    '''
    new_file = f"{test_directory}reverse_{testtype}_{testnum}.csv"
    command = ('''awk 'BEGIN{FS=OFS=","}{sub(/\r$/,"");print $4,$3,$2,$1}' ''' +
               old_file +" > " + new_file)
    os.system(command)
    return new_file


class TestEdgeCases(unittest.TestCase):
    '''
    Test cases to ensure that each edge case anticipated is handled correctly
    '''
    def test_121(self):
        '''
        Tests all the cases for 121 matching:
            1. Highest match for an id is already taken
            2. Weight is equal to another match for another id
            3. Weight is equal to more than 2 other matches for an id
            4. Weight is equal to another match for another id, only 2 matches
            5. A pair of ids is seen twice with same weight
        '''
        for i, test in enumerate(TEST_RESULTS['121']["expected"]):
            with self.subTest(case=i):
                file = f"{test_directory}121_{i}.csv"
                expected = get_expected_results(test, '121')
                result = pp.one_to_one_matching(file)
                self.assertEqual(result, expected, msg=TEST_RESULTS['121']['msgs'][i])
            with self.subTest(case=str(i) + " reverse"):
                new_file = reverse_file(file, "121", i)
                result = pp.one_to_one_matching(new_file)
                self.assertEqual(result, expected, msg=f"Misread column for test {i}")
                os.remove(new_file)

    def test_12M(self):
        '''
        Tests all the cases for 12M matching:
            1. Many id matches two 1-ids with same weight
            2. Many id matches two 1 ids, one with higher weights
            3. Many id matches more than two 1 ids with the same weight
            4. Weight is equal to another match for another id, only 2 matches
            5. One id matches more than 1 many ids with different weights
        '''
        for i, test in enumerate(TEST_RESULTS['12M']["expected"]):
            with self.subTest(case=i):
                file = f"{test_directory}12M_{i}.csv"
                expected = get_expected_results(test, '12M')
                result = pp.mone_or_onem_matching(file)
                self.assertEqual(result, expected, msg=TEST_RESULTS['12M']['msgs'][i])
            with self.subTest(case=str(i) + " reverse"):
                new_file = reverse_file(file, "12M", i)
                result = pp.mone_or_onem_matching(new_file)
                self.assertEqual(result, expected, msg=f"Misread column for test {i}")
                os.remove(new_file)

    def test_M21(self):
        '''
        Tests all the cases for M21 matching:
            1. Many id matches two 1-ids with same weight
            2. Many id matches two 1 ids, one with higher weights
            3. Many id matches more than two 1 ids with the same weight
            4. Weight is equal to another match for another id, only 2 matches
            5. One id matches more than 1 many ids with different weights
        '''
        for i, test in enumerate(TEST_RESULTS['M21']["expected"]):
            with self.subTest(case=i):
                file = f"{test_directory}M21_{i}.csv"
                expected = get_expected_results(test, 'M21')
                result = pp.mone_or_onem_matching(file, onecol="indv_id_b")
                self.assertEqual(result, expected, msg=TEST_RESULTS['M21']['msgs'][i])
            with self.subTest(case=str(i) + " reverse"):
                new_file = reverse_file(file, "M21", i)
                result = pp.mone_or_onem_matching(new_file, onecol="indv_id_b")
                self.assertEqual(result, expected, msg=f"Misread column for test {i}")
                os.remove(new_file)

    def test_M2M(self):
        '''
        Tests all the cases for M2M matching:
            1. Both indices have been seen before
            2. 1 index has been seen before
            3. Multiple groups are formed
        '''
        for i, test in enumerate(TEST_RESULTS['M2M']["expected"]):
            with self.subTest(case=i):
                file = f"{test_directory}M2M_{i}.csv"
                result = pp.mtom_or_dedup_matching(file, "M2M")
                expected = get_expected_results(test, 'M2M')
                results = get_expected_results(result, 'M2M')
                self.assertSetEqual(set(results.values()), set(expected.values()),
                                msg=TEST_RESULTS['M2M']['msgs'][i])
            with self.subTest(case=str(i) + " reverse"):
                new_file = reverse_file(file, "M2M", i)
                result = pp.mtom_or_dedup_matching(new_file, "M2M")
                results = get_expected_results(result, 'M2M')
                self.assertSetEqual(set(results.values()), set(expected.values()),
                                msg=TEST_RESULTS['M2M']['msgs'][i])
                os.remove(new_file)

    def test_dedup(self):
        '''
        Tests all the cases for dedup matching:
            1. Both indices have been seen before
            2. 1 index has been seen before
            3. Multiple groups are formed
        '''
        for i, test in enumerate(TEST_RESULTS['dedup']["expected"]):
            with self.subTest(case=i):
                file = f"{test_directory}dedup_{i}.csv"
                result = pp.mtom_or_dedup_matching(file, 'dedup')
                expected = get_expected_results(test, 'dedup')
                results = get_expected_results(result, 'dedup')
                self.assertSetEqual(set(results.values()), set(expected.values()),
                                msg=TEST_RESULTS['dedup']['msgs'][i])
            with self.subTest(case=str(i) + " reverse"):
                new_file = reverse_file(file, "dedup", i)
                result = pp.mtom_or_dedup_matching(new_file, 'dedup')
                results = get_expected_results(result, 'dedup')
                self.assertSetEqual(set(results.values()), set(expected.values()),
                                msg=TEST_RESULTS['dedup']['msgs'][i])
                os.remove(new_file)

    def tests_simple(self):
        '''
        Tests all match types for simple cases:
            1. Only one row included in matches
            2. No rows included in matches
            3. All matches found are included in results
        '''
        curtest = 0
        for i in range(3):
            file = f"{test_directory}simple_test_{i}.csv"
            for case in ("121", "12M", "M21", "M2M", "dedup"):
                with self.subTest(case=case + " " + str(i)):
                    expected = get_expected_results(TEST_RESULTS["simple_test"]["expected"][curtest], case)
                    if case in ("121", "12M", "M21"):
                        if case == "121":
                            results = pp.one_to_one_matching(file)
                        elif case  == "12M":
                            results = pp.mone_or_onem_matching(file, onecol="indv_id_a")
                        else:
                            results = pp.mone_or_onem_matching(file,onecol="indv_id_b")
                        self.assertEqual(results, expected,
                                         msg=TEST_RESULTS['simple_test']['msgs'][curtest])
                    elif case in ("M2M", "dedup"):
                        result = pp.mtom_or_dedup_matching(file, case)
                        results = get_expected_results(result, case)
                        self.assertSetEqual(set(results.values()), set(expected.values()),
                                        msg=TEST_RESULTS['simple_test']['msgs'][curtest])
                if i != 1 or (i == 1 and case == "dedup"):
                    curtest += 1

if __name__ == '__main__':
    unittest.main()
