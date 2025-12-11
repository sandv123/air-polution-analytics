# Analytics of the air polution situation in the Belgrade, RS vicinity

## Goal of the project

This is a pet project. Main goal is for me to get my hands as dirty as I can with the insides of Databricks, Spark, Python, and everything else associated with the profession of Data Engineer.

I will have to see as I go whether this projects steers into something that can be useful to others.

## Data used in analysis

I will use the publicly available data. For now I plan to use these data sources:

1.  Air quality data from [OpenAQ](https://openaq.org)

> OpenAQ API provides a **measurement** method, that returns all measurement for a given sensor for the specified time period. I decided to retrieve data for all sensors within am 8 km radius of the Belgrade center for the past 5 years. To avoid getting timed out by the OpenAQ service due to very long responses, I request the data year-by-year, and store the data for each sensor for each year in a separate file.
> 
> The data is retrieved in JSON format, and to significantly save on storage space (I use Databricks Free Edition, so I suppose there is a limit to what I can store for free), I zip every file before I store it on disk. This reduces the size of the raw data aproximately 50-fold.

## Data architecture

I will use the Medalion Architecture to model the data. The following picture illustrates the data flow:

```
Insert picture here
```

1.  A job in Databricks runs a Python script to call the OpenAQ API to extract the air quality data.
2.  Air quality data is stored in DBFS volume.

## CI/CD