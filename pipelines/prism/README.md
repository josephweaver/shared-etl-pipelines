
## PRISM Shared ETL Pipelines



## PRISM Download 

Download PRISM time series precipatation data
ftp://prism.oregonstate.edu/time_series/us/an/800m/ppt/monthly


```{commandline}
python cli.py run prism/download.yml `
  --env hpcc_msu
```



> NOTE: downloads 


