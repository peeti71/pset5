from csci_utils.luigi.dask.target import CSVTarget, ParquetTarget
from csci_utils.luigi.task import TargetOutput, Requires, Requirement
from luigi import ExternalTask, Task, BoolParameter
import os


class YelpReviews(ExternalTask):
    """
    Running CSVTarget for dask.target
    to retrieve CSV yelp file from s3
    """
    output = TargetOutput(
        target_class=CSVTarget,
        file_pattern='s3://peeti-csci-e-29/pset_5/yelp_data/',
        ext="",
        glob="*.csv",
        storage_options=dict(requester_pays=True),
        flag=None,
    )

class CleanedReviews(Task):
    subset = BoolParameter(default=True)
    requires = Requires()
    reviews = Requirement(YelpReviews)
    output = TargetOutput(
        target_class=ParquetTarget,
        file_pattern=os.path.abspath("data/{task.__class__.__name__}-{task.subset}"),
        ext=""
    )


    # Output should be a local ParquetTarget in ./data, ideally a salted output,
    # and with the subset parameter either reflected via salted output or
    # as part of the directory structure

    def run(self):

        numcols = ["funny", "cool", "useful", "stars"]
        ddf = self.input()["reviews"].read_dask(
            storage_options=dict(request_pays=True),
            dtype={col: "float64" for col in numcols},
            parse_dates=["date"],
        )

        if self.subset:
            ddf = ddf.get_partition(0)

        # create condition logic to exclude reviews that are less than 22 characters
        exclude = ddf["review_id"].str.len() != 22

        #user_id is not null
        out = (
            ddf.mask(exclude)
            .dropna(subset=["user_id", "date"])
            .fillna(value={col: 0.0 for col in numcols})
            .astype({col: "int32" for col in numcols})
            .set_index("review_id")
        )
        #create the length_review column to dataframe
        out["length_reviews"] = out["text"].str.len()
        self.output().write_dask(out, compression='gzip')

class ByAggregate(Task):
    """
    Aggregate tasks
    """
    subset = BoolParameter(default=True)
    requires = Requires()
    output = TargetOutput(
        target_class=ParquetTarget,
        file_pattern=os.path.abspath("data/{task.__class__.__name__}-{task.subset}"),
        ext="",
    )

    def run(self):
        raise NotImplementedError()

    def print_results(self):
        print(self.output().read_dask().compute())

class ByDecade(ByAggregate):
    """
    What is the average length of a review by the decade (eg 2000 <= year < 2010)?
    """
    cleaned_reviews = Requirement(CleanedReviews)

    def run(self):

        #create a dataframe for date and lenght_review from cleaned reviews
        dsk = self.input()["cleaned_reviews"].read_dask(
            columns=['date', 'length_reviews'], parse_dates=['date']
        ).set_index('date')

        if self.subset:
            dsk = dsk.get_partition(0)

        # take an average on date and convert length_review series to dataframe
        # convert length reviews to integer

        out = (
            dsk.groupby((dsk.index.year // 10) * 10)['length_reviews']
            .mean()
            .round()
            .to_frame()
            .astype({'lenght_reviews': 'int32'})
        )
        self.output().write_dask(out, compression="gzip")
        self.print_results()

class ByStars(ByAggregate):
    """
    What is the average length of a review by the number of stars?
    """
    cleaned_reviews = Requirement(CleanedReviews)

    def run(self):

        # repeat the class above by creating a dataframe for stars and lenght_review
        dsk = self.input()["cleaned_reviews"].read_dask(columns=['lenght_reviews', 'stars'])

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






