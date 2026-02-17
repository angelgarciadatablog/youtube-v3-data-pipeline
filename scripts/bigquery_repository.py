import os
from google.cloud import bigquery


class BigQueryRepository:

    def __init__(self):
        self.project_id = os.getenv("GCP_PROJECT")
        self.dataset_id = "angelgarciadatablog"

        if not self.project_id:
            raise ValueError("GCP_PROJECT not configured")

        self.client = bigquery.Client(project=self.project_id)

    def load_dataframe(self, table_name, dataframe, write_disposition="WRITE_TRUNCATE"):
        full_table_id = f"{self.project_id}.{self.dataset_id}.{table_name}"

        job_config = bigquery.LoadJobConfig(
            write_disposition=write_disposition
        )

        job = self.client.load_table_from_dataframe(
            dataframe,
            full_table_id,
            job_config=job_config
        )

        job.result()


    def delete_snapshot_by_date(self, table_name, snapshot_date):
        full_table_id = f"{self.project_id}.{self.dataset_id}.{table_name}"

        query = f"""
        DELETE FROM `{full_table_id}`
        WHERE snapshot_date = @snapshot_date
        """

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter(
                    "snapshot_date",
                    "DATE",
                    snapshot_date
                )
            ]
        )

        job = self.client.query(query, job_config=job_config)
        job.result()
        
