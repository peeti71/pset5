"""Module for testing Luigi/ Dask tasks"""

# Python Builtin's need for tests
from unittest import TestCase, main
from unittest.mock import MagicMock
from tempfile import TemporaryDirectory, NamedTemporaryFile
import os
import boto3
from moto import mock_s3

# Import luigi for Testing Tasks
from luigi import Task, build, BoolParameter, LuigiStatusCode
from luigi.contrib.s3 import S3Client



# Import the created modules for testing.
from csci_utils.luigi.dask.target import CSVTarget, ParquetTarget
from csci_utils.luigi.task import TargetOutput, Requires, Requirement


from pset_5.tasks.yelp import YelpReviews, ByDecade, ByStars, CleanedReviews
from csci_utils.hashing.hash_str import hash_str


class HashTests(TestCase):
    """
    test hash utils

    """
    def test_basic(self):
        self.assertEqual(hash_str("world!", salt="hello, ").hex()[:6], "68e656")

AWS_ACCESS_KEY = "xxxxxxxxxxxxxxxxxxxx"
AWS_SECRET_KEY = "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"


def create_bucket(bucket, dir, temp_file_path, download_path):
    """
    Create mock buckets for testing and put a test file in bucket
    at directory location if provided
    """
    conn = boto3.resource("s3", region_name="us-east-1")
    conn.create_bucket(Bucket=bucket)
    client = S3Client(AWS_ACCESS_KEY, AWS_SECRET_KEY)

    # if a directory was provided create corresponding path in the bucket
    if dir:
        client.mkdir("s3://{}/{}".format(bucket, dir))
        bucket_path = os.path.join(bucket, dir)
    else:
        bucket_path = bucket

    client.put(temp_file_path, "s3://{}/{}".format(bucket_path, download_path))

def create_temp_files():
    f = NamedTemporaryFile(mode='wb', delete=False, suffix='.csv')
    tempFileContents = (
            b"review_id,user_id,business_id,stars,date,text,useful,funny,cool\n"
            + b"llkkoooooiuu78kLKOOOOO,1,test-id,5,2017-10-14,It was excellent.,0,0,0\n"
            + b"kltn99hUUENW45mmJIltew,2,test-id,5,2020-07-18,Great service!,0,0,0\n"
            + b"BXpiLmvMRSQetE8LW4OI9A,20,test-id,2,2030-10-11,Food was subpar but that was pretty much expec,0,0,0\n"
    )

def setupTest(self):
    self.tempFilePath, self.tempFileContents = create_temp_files()
    #remove once created
    self.addCleanup(os.remove, self.tempFilePath)

def get_mock_clean_reviews(tmp):
    class MockCleanedReviews(CleanedReviews):
        subset = BoolParameter(default=True)
        requires = Requires()
        reviews = Requirement(YelpReviews)
        output = TargetOutput(
            target_class=ParquetTarget,
            file_pattern=tmp+ "/data/{task.__class__.__name__}-{task.subset}",
            ext="",
        )



def get_dataframe(target):
    """
    Return a computed dataframe with it's rows and cols
    from the provided target
    :param target:
    :return:
    """
    target.run()
    target = target.output()
    df = target.read_dask()
    pdf = df.compute()
    rows, cols, = pdf.shape
    return rows, cols, pdf


def verify_dataframe_dimensions(self, target, rows, cols):
    """
    Confirm a given target has expected rows and cols and return
    resulting dataframe from target for further testing
    :param self:
    :param target:
    :param rows:
    :param cols:
    :return:
    """
    test_rows, test_cols, pdf = get_dataframe(target)
    self.assertEqual(test_rows, rows)
    self.assertEqual(test_cols, cols)
    return pdf


def verify_dataframe(self, tmp, rows, cols):
    """
    validate files in tmp location to ensure they
    contain the given number of rows and cols in total
    :param self:
    :param tmp:
    :param rows:
    :param cols:
    :return:
    """
    target = get_mock_clean_reviews(tmp)
    verify_dataframe_dimensions(self, target, rows, cols)

# @mock_s3
# class TestYelpReviews(TestCase):
#
#     def test_yelp_reviews(self):
#
#         t1 = YelpReviews()
#         target = t1.output()
#         self.assertTrue(isinstance(target, CSVTarget))

@mock_s3
class TestCleanedReviews(TestCase):
    """
    Test Cleaned Reviews
    """


    def create_error_files(self):
        """
        Create a temporary file with bad content in data
        :return: (file path, file contents)
        """
        f = NamedTemporaryFile(mode="wb", delete=False, suffix=".csv")
        tempFileContents = (
            b"review_id,user_id,business_id,stars,date,text,useful,funny,cool\n"
            + b"llkkoooooiuu78kLKOOOOO,1,test-id,5,2017-10-14,It was excellent.,0,0,0\n"
            + b"kltn99hUUENW45mmJIltew,2,test-id,5,2030-07-18,Great service!,0,0,0\n"
        )
        tempFilePath = f.name
        f.write(tempFileContents)
        f.close()
        return tempFilePath, tempFileContents



class TestLuigiDaskYelpReviewsTask(TestCase):

    def test_class_vars(self):

        t = YelpReviews()
        target = t.output()
        self.assertTrue(isinstance(target, CSVTarget))

    def test_output_CSVTarget(self):

        t = YelpReviews()

        result = t.output()
        self.assertEqual(result.__class__, CSVTarget)



class TestCleanedReview(TestCase):
    def test_requires(self):
        """

        :return: The requirement task of CleanedReview
        """
        t = CleanedReviews()
        result = t.output()
        self.assertEqual(result.__class__, ParquetTarget)

class TestYelpCleanedReview(TestCase):
    def test_run(self):
        t = CleanedReviews()
        t.run = MagicMock(name='run')
        t.run()
        t.run.assert_called()


class TestByDecade(TestCase):
    def test_requires(self):
        t = ByDecade()
        result = t.requires()
        self.assertEqual(result['cleaned_reviews'].__class__, CleanedReviews)

    def test_run(self):
        t = ByDecade()
        r = t.output()
        self.assertEqual(r.__class__, ParquetTarget)

class MockByStars(ByStars):
    subset = BoolParameter(default=True)
    requires = Requires()
    output = TargetOutput(
            target_class=ParquetTarget,
            file_pattern=os.path.abspath("data/ByStars"),
            ext="",
        )
# class TestByStars(TestCase):
#
#     tempFilePath = create_temp_files()
#     create_bucket("peeti-cscie-29", "pset_5/yelp_data", tempFilePath, "test_file.csv")
#     def test_run(self):
#         with TemporaryDirectory() as tmp:
#             target = MockByStars(tmp)
#             df = verify_dataframe_dimensions(self, target, 2, 1)
#             self.assertEqual(df['length_reviews'].values[0], 11)
#             self.assertEqual(df['length_reviews'].values[1], 26)

class TestMain(TestCase):
    def Test_main(self):
        t = main.__class__.print_results()
        c = t.index
        self.assertEqual(c, 'stars')
#
#     def Test_main_Decade(self):
#         t = ByDecade().print_results()
#         c = t.index()
#         self.assertEqual(c, 'date')




if __name__ == "__main__":
    main()