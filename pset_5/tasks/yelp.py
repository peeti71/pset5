from csci_utils.luigi.dask.target import CSVTarget, ParquetTarget
from csci_utils.luigi.task import TargetOutput, Requires, Requirement
from luigi import ExternalTask, Task, BoolParameter, LocalTarget
import os



class YelpReviews(ExternalTask):
    """
    Running CSVTarget for dask.target
    to retrieve CSV yelp file from s3
    """
    output = TargetOutput(
        target_class=CSVTarget,
        file_pattern="s3://peeti-csci-e-29/pset_5/yelp_data/",
        ext="",
        glob="*.csv",
        flag=None,
        storage_options=dict(requester_pays=True),
    )


class CleanedReviews(Task):
    subset = BoolParameter(default=True)
    requires = Requires()
    yelp_reviews = Requirement(YelpReviews)


    output = TargetOutput(
        target_class=ParquetTarget,
        file_pattern=os.path.abspath("data/CleanedReviews"),
        ext="",
        glob="*.parquet"
    )

    def run(self):
        ddf = self.input()["yelp_reviews"].read_dask(
            parse_dates=["date"],
            dtype={"cool": "float64", "funny": "float64", "useful": "float64", "stars": "float64"},
            storage_options=dict(requester_pays=True),
        )

    # Output should be a local ParquetTarget in ./data, ideally a salted output,
    # and with the subset parameter either reflected via salted output or
    # as part of the directory structure
        if self.subset:
            ddf = ddf.get_partition(0)
        #
        # # create condition logic to exclude reviews that are less than 22 characters
        # #user_id is not null
        out = (
            ddf.dropna(subset=["user_id", "date"])[ddf["review_id"].str.len() == 22]
            .set_index("review_id")
            .fillna(value={"cool": float(0), "funny": float(0), "useful": float(0), "stars": float(0)})
            .astype({"cool": "int32", "funny": "int32", "useful": "int32", "stars": "int32"})

        )
        # #create the length_review column to dataframe
        out["length_reviews"] = out["text"].str.len()
        self.output().write_dask(out, compression='gzip')



# class ByAggregate(Task):
#     """
#     Aggregate tasks
#     """
#     subset = BoolParameter(default=True)
#     requires = Requires()
#     output = TargetOutput(
#         target_class=ParquetTarget,
#         file_pattern=os.path.abspath("data/{task.__class__.__name__}-{task.subset}"),
#         ext="",
#     )
#
#     def run(self):
#         raise NotImplementedError()
#
#     def print_results(self):
#         print(self.output().read_dask().compute())

class ByDecade(Task):
    """
    What is the average length of a review by the decade (eg 2000 <= year < 2010)?
    """
    subset = BoolParameter(default=True)
    requires = Requires()
    output = TargetOutput(
            target_class=ParquetTarget,
            file_pattern=os.path.abspath("data/ByDecade"),
            ext="",
        )
    cleaned_reviews = Requirement(CleanedReviews)

    def run(self):
        #create a dataframe for date and lenght_review from cleaned reviews
        ddf = self.input()["cleaned_reviews"].read_dask(
            columns=['date', 'length_reviews'], parse_dates=['date']).set_index('date')

        if self.subset:
            ddf = ddf.get_partition(0)

        # take an average on date and convert length_review series to dataframe
        # convert length reviews to integer

        out = (
            ddf.groupby((ddf.index.year // 10) * 10)['length_reviews']
            .mean()
            .round()
            .to_frame()
            .astype({'length_reviews': 'int32'})
        )
        self.output().write_dask(out, compression="gzip")
        self.print_results()

    def print_results(self):
        print(self.output().read_dask().compute())

class ByStars(Task):
    """
    What is the average length of a review by the number of stars?
    """
    subset = BoolParameter(default=True)
    requires = Requires()
    output = TargetOutput(
            target_class=ParquetTarget,
            file_pattern=os.path.abspath("data/ByStars"),
            ext="",
        )
    cleaned_reviews = Requirement(CleanedReviews)

    def run(self):

        # repeat the class above by creating a dataframe for stars and lenght_review
        dsk = self.input()["cleaned_reviews"].read_dask(columns=['length_reviews', 'stars'])

        if self.subset:
            dsk = dsk.get_partition(0)

        # take an average on stars and convert lenght_review series to dataframe
        # convert lenght_reviews to integer

        out = (
            dsk.groupby('stars')['length_reviews']
            .mean()
            .round()
            .to_frame()
            .astype({'length_reviews': 'int32'})
        )
        self.output().write_dask(out, compression='gzip')
        self.print_results()

    def print_results(self):
        print(self.output().read_dask().compute())


