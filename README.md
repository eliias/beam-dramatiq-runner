# beam-dramatiq-runner

> A simple and incomplete implementation of the Apache Beam Runner API for Python and Dramatiq.

## Getting started

```bash
mise trust # optional
pip install poetry # Python 3.12 must be ready to use
up # spin up backing services
dev # run dev environment
```

## First pipeline

```python
import csv

from apache_beam import Pipeline, DoFn, ParDo, Map, CombineGlobally
from apache_beam.io import ReadFromText
from apache_beam.options.pipeline_options import PipelineOptions
from beam_dramatiq_runner import DramatiqPipelineRunner

class ParseCSV(DoFn):
    def process(self, element):
        for row in csv.DictReader([element]):
            yield {
                "ID": int(row["ID"]),
                "Name": row["Name"],
                "Age": int(row["Age"])
            }

options = PipelineOptions()
with Pipeline(options=options, runner=DramatiqPipelineRunner()) as p:
    age_sum = (
        p
        | 'Read CSV' >> ReadFromText('data/sample_humans.csv', skip_header_lines=1)
        | 'Parse CSV' >> ParDo(ParseCSV())
        | 'Extract Age' >> Map(lambda row: row['Age'])
        | 'Sum Ages' >> CombineGlobally(sum)
    )

    age_sum | 'Print Result' >> Map(print)
```
