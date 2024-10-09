import csv

from apache_beam import DoFn


class ParseCSV(DoFn):
    def process(self, element, *args, **kwargs):
        for row in csv.DictReader([element]):
            yield {"ID": int(row["ID"]), "Name": row["Name"], "Age": int(row["Age"])}
